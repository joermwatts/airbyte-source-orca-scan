"""S2: auth header, envelope extraction, 429/Retry-After backoff and error mapping."""

import logging

import pytest
import requests
from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from source_orca_scan.source import SourceOrcaScan
from source_orca_scan.streams import (
    API_URL,
    RetryAfterBackoffStrategy,
    Sheets,
    make_api_budget,
    make_authenticator,
    unwrap_envelope,
)

SHEETS_URL = API_URL + "sheets"


def _sheets(config) -> Sheets:
    return Sheets(authenticator=make_authenticator(config), api_budget=make_api_budget())


def _read_all(stream):
    return list(stream.read_records(sync_mode=SyncMode.full_refresh))


@pytest.fixture(autouse=True)
def no_sleep(mocker):
    """Backoff sleeps for real otherwise; the tests only care that a retry happened."""
    return mocker.patch("time.sleep")


def test_requests_carry_bearer_header_and_accept_json(config, requests_mock):
    requests_mock.get(SHEETS_URL, json={"data": []})

    _read_all(_sheets(config))

    sent = requests_mock.request_history[0]
    assert sent.headers["Authorization"] == f"Bearer {config['api_key']}"
    assert sent.headers["Accept"] == "application/json"
    assert sent.method == "GET"


def test_envelope_yields_inner_records(config, requests_mock):
    requests_mock.get(SHEETS_URL, json={"data": [{"_id": "a", "name": "One"}, {"_id": "b", "name": "Two"}]})

    records = _read_all(_sheets(config))

    assert [dict(r) for r in records] == [{"_id": "a", "name": "One"}, {"_id": "b", "name": "Two"}]


def test_unwrap_envelope_handles_list_object_and_junk():
    assert list(unwrap_envelope({"data": [{"x": 1}, "not a record", {"y": 2}]})) == [{"x": 1}, {"y": 2}]
    assert list(unwrap_envelope({"data": {"count": 5}})) == [{"count": 5}]
    assert list(unwrap_envelope({"data": None})) == []
    assert list(unwrap_envelope([1, 2, 3])) == []
    assert list(unwrap_envelope({})) == []


def test_429_with_retry_after_is_retried_then_succeeds(config, requests_mock, no_sleep):
    requests_mock.get(
        SHEETS_URL,
        [
            {"status_code": 429, "headers": {"Retry-After": "2"}, "json": {"error": "Too many requests", "status": 429}},
            {"status_code": 200, "json": {"data": [{"_id": "a"}]}},
        ],
    )

    records = _read_all(_sheets(config))

    assert [dict(r) for r in records] == [{"_id": "a"}]
    assert requests_mock.call_count == 2
    # The CDK sleeps Retry-After (+1s safety) before the retry.
    slept = [call.args[0] for call in no_sleep.call_args_list if call.args]
    assert any(s >= 2 for s in slept), f"expected a sleep of at least 2s, got {slept}"


def test_backoff_strategy_reads_retry_after_header():
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "7"
    assert RetryAfterBackoffStrategy().backoff_time(response, attempt_count=1) == 7.0

    response.headers["Retry-After"] = "not-a-number"
    assert RetryAfterBackoffStrategy().backoff_time(response, attempt_count=1) is None

    plain = requests.Response()
    plain.status_code = 500
    assert RetryAfterBackoffStrategy().backoff_time(plain, attempt_count=1) is None


def test_401_is_a_config_error_naming_the_api_key(config, requests_mock):
    requests_mock.get(SHEETS_URL, status_code=401, json={"error": "Unauthorized", "message": "Invalid API key", "status": 401})

    with pytest.raises(AirbyteTracedException) as raised:
        _read_all(_sheets(config))

    assert raised.value.failure_type == FailureType.config_error
    assert "API key" in raised.value.message
    assert "401" in raised.value.message
    assert requests_mock.call_count == 1  # no retry on a bad key


def test_403_message_mentions_business_subscription(config, requests_mock):
    requests_mock.get(SHEETS_URL, status_code=403, json={"error": "Forbidden", "status": 403})

    with pytest.raises(AirbyteTracedException) as raised:
        _read_all(_sheets(config))

    assert raised.value.failure_type == FailureType.config_error
    assert "Business subscription" in raised.value.message


def test_5xx_is_retried(config, requests_mock):
    requests_mock.get(
        SHEETS_URL,
        [
            {"status_code": 503, "json": {"error": "Service unavailable", "status": 503}},
            {"status_code": 200, "json": {"data": [{"_id": "a"}]}},
        ],
    )

    records = _read_all(_sheets(config))

    assert len(records) == 1
    assert requests_mock.call_count == 2


def test_check_connection_succeeds(config, requests_mock):
    requests_mock.get(SHEETS_URL, json={"data": [{"_id": "a", "name": "One"}]})

    ok, error = SourceOrcaScan().check_connection(logging.getLogger("airbyte"), config)

    assert ok is True
    assert error is None


def test_check_connection_reports_bad_key_without_stack_trace(config, requests_mock):
    requests_mock.get(SHEETS_URL, status_code=401, json={"error": "Unauthorized", "message": "Invalid API key", "status": 401})

    ok, error = SourceOrcaScan().check_connection(logging.getLogger("airbyte"), config)

    assert ok is False
    assert "API key" in str(error)
    assert "Traceback" not in str(error)


def test_check_connection_reports_network_failure(config, requests_mock):
    requests_mock.get(SHEETS_URL, exc=requests.exceptions.ConnectionError("boom"))

    ok, error = SourceOrcaScan().check_connection(logging.getLogger("airbyte"), config)

    assert ok is False
    assert error  # some human-readable explanation
