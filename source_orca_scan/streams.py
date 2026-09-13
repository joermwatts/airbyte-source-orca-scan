"""Base HTTP stream for the Orca Scan REST API, plus the shared auth, rate-limit
and error-handling policy every stream inherits.

API reference: https://orcascan.com/guides/barcode-scanning-rest-api-f09a21c3
OpenAPI:       https://api.orcascan.com/v1/openapi.json
"""

import logging
from abc import ABC
from datetime import timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import requests
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.call_rate import (
    APIBudget,
    HttpAPIBudget,
    MovingWindowCallRatePolicy,
    Rate,
)
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.error_handlers import (
    BackoffStrategy,
    ErrorHandler,
    ErrorResolution,
    HttpStatusErrorHandler,
    ResponseAction,
)
from airbyte_cdk.sources.streams.http.error_handlers.default_error_mapping import (
    DEFAULT_ERROR_MAPPING,
)
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

API_URL = "https://api.orcascan.com/v1/"

# Orca Scan allows 15 requests/second per key and answers 429 + Retry-After above that.
# We self-throttle a little under the limit so a normal sync never trips it.
REQUESTS_PER_SECOND = 12

# Every list endpoint on this API returns the whole result set in one response; there
# is no pagination. Large sheets (thousands of rows, hundreds of thousands of history
# entries) mean the read can take a while, so the timeout is generous.
REQUEST_TIMEOUT_SECONDS = 300

ORCA_ERROR_MAPPING: Mapping[Union[int, str, type[Exception]], ErrorResolution] = {
    **DEFAULT_ERROR_MAPPING,
    401: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message=(
            "Orca Scan rejected the API key (401 Invalid API key). Check the API Key in the "
            "connector configuration: it should start with `orca_` and is created under Account "
            "Settings in the Orca Scan web app."
        ),
    ),
    403: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message=(
            "Orca Scan returned 403 Forbidden. API access requires an Orca Scan Business "
            "subscription, and the key's user must have access to the requested sheet."
        ),
    ),
    404: ErrorResolution(
        response_action=ResponseAction.FAIL,
        failure_type=FailureType.config_error,
        error_message=(
            "Orca Scan returned 404 Not found. The sheet may have been deleted, or the key's user "
            "no longer has access to it."
        ),
    ),
    429: ErrorResolution(
        response_action=ResponseAction.RATE_LIMITED,
        failure_type=FailureType.transient_error,
        error_message="Orca Scan rate limit reached (15 requests/second). Waiting before retrying.",
    ),
}


class RetryAfterBackoffStrategy(BackoffStrategy):
    """Wait exactly as long as Orca Scan asks in `Retry-After`; otherwise let the CDK's
    default exponential backoff decide."""

    def backoff_time(
        self,
        response_or_exception: Optional[Union[requests.Response, requests.RequestException]],
        attempt_count: int,
    ) -> Optional[float]:
        if isinstance(response_or_exception, requests.Response):
            retry_after = response_or_exception.headers.get("Retry-After")
            if retry_after:
                try:
                    return max(float(retry_after), 0.0)
                except ValueError:
                    return None
        return None


def make_authenticator(config: Mapping[str, Any]) -> TokenAuthenticator:
    return TokenAuthenticator(token=config["api_key"], auth_method="Bearer")


def make_api_budget() -> APIBudget:
    """One shared budget for all streams so the per-key limit is respected across them."""
    policy = MovingWindowCallRatePolicy(
        rates=[Rate(limit=REQUESTS_PER_SECOND, interval=timedelta(seconds=1))],
        matchers=[],  # no matchers = applies to every request
    )
    return HttpAPIBudget(policies=[policy])


class OrcaScanStream(HttpStream, ABC):
    """Shared behaviour for every Orca Scan stream.

    - GET only, JSON, Bearer auth.
    - Responses are a `{"data": ...}` envelope; `data` is a list for collections.
    - No pagination anywhere in the API.
    """

    url_base = API_URL
    primary_key = "_id"

    def __init__(self, authenticator: TokenAuthenticator, api_budget: Optional[APIBudget] = None):
        super().__init__(authenticator=authenticator, api_budget=api_budget)

    @property
    def http_method(self) -> str:
        return "GET"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {"Accept": "application/json"}

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {"timeout": REQUEST_TIMEOUT_SECONDS}

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        yield from unwrap_envelope(response.json())

    def get_error_handler(self) -> Optional[ErrorHandler]:
        return HttpStatusErrorHandler(
            logger=logging.getLogger("airbyte"),
            error_mapping=ORCA_ERROR_MAPPING,
            max_retries=5,
            max_time=timedelta(minutes=10),
        )

    def get_backoff_strategy(self) -> Optional[Union[BackoffStrategy, List[BackoffStrategy]]]:
        return RetryAfterBackoffStrategy()


def unwrap_envelope(body: Any) -> Iterable[Mapping[str, Any]]:
    """Yield the records inside an Orca Scan `{"data": ...}` envelope.

    `data` is a list for collection endpoints and an object for single-item endpoints
    (for example `/rows/count`), so both are handled.
    """
    if not isinstance(body, dict):
        return
    data = body.get("data")
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
    elif isinstance(data, dict):
        yield data


class Sheets(OrcaScanStream):
    """`GET /sheets` - every sheet the API key's user can access. Parent of all other streams."""

    name = "sheets"

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "sheets"


class SheetSubStream(HttpSubStream, OrcaScanStream, ABC):
    """`GET /sheets/{sheetId}/<endpoint>` for every sheet the parent stream yields.

    Each record gets a `sheet_id` property so it can be joined back to `sheets`
    (and so the primary key is unique across sheets).
    """

    endpoint: str  # e.g. "fields", "users", "hooks", "triggers"

    def __init__(self, parent: Sheets, authenticator: TokenAuthenticator, api_budget: Optional[APIBudget] = None):
        super().__init__(parent=parent, authenticator=authenticator, api_budget=api_budget)

    @staticmethod
    def sheet_id(stream_slice: Optional[Mapping[str, Any]]) -> str:
        return (stream_slice or {})["parent"]["_id"]

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return f"sheets/{self.sheet_id(stream_slice)}/{self.endpoint}"

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        sheet_id = self.sheet_id(stream_slice)
        for record in unwrap_envelope(response.json()):
            yield {"sheet_id": sheet_id, **record}


class SheetFields(SheetSubStream):
    """`GET /sheets/{sheetId}/fields` - the column definitions of each sheet."""

    name = "sheet_fields"
    endpoint = "fields"
    primary_key = ["sheet_id", "key"]


class SheetUsers(SheetSubStream):
    """`GET /sheets/{sheetId}/users` - users with access to each sheet and their permissions."""

    name = "sheet_users"
    endpoint = "users"
    primary_key = ["sheet_id", "_id"]


class SheetHooks(SheetSubStream):
    """`GET /sheets/{sheetId}/hooks` - webhooks registered on each sheet."""

    name = "sheet_hooks"
    endpoint = "hooks"
    primary_key = ["sheet_id", "_id"]


class SheetTriggers(SheetSubStream):
    """`GET /sheets/{sheetId}/triggers` - automation rules on each sheet."""

    name = "sheet_triggers"
    endpoint = "triggers"
    primary_key = ["sheet_id", "_id"]
