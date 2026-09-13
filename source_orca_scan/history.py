"""`sheet_history` - the change log of every sheet, synced incrementally on `_changedOn`.

`GET /sheets/{sheetId}/history` returns every add/update/delete ever recorded for a sheet in
one unpaginated response (a busy sheet: ~175k entries, ~120 MB). The API has no date filter,
so incremental sync downloads the full history each run, streams it with ijson and emits only
the entries at or after the per-sheet cursor. Equal timestamps are re-emitted on purpose
(at-least-once); destinations dedupe on the primary key.
"""

from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import CheckpointMixin
from airbyte_cdk.sources.streams.call_rate import APIBudget
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .rows import iter_data_items
from .streams import REQUEST_TIMEOUT_SECONDS, SheetSubStream, Sheets

CURSOR_FIELD = "_changedOn"


def parse_timestamp(value: Any) -> Optional[datetime]:
    """ISO 8601 (`2026-09-13T10:11:12.123Z` or `...Z` without millis, or with an offset) to an
    aware datetime; None if it is not a timestamp. Comparing datetimes rather than strings keeps
    `10:11:12Z` and `10:11:12.123Z` in the right order."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class SheetHistory(SheetSubStream, CheckpointMixin):
    """`GET /sheets/{sheetId}/history`, one partition per sheet, incremental on `_changedOn`.

    State shape: `{"<sheet_id>": {"_changedOn": "<latest seen>"}, ...}`.
    """

    name = "sheet_history"
    endpoint = "history"
    primary_key = ["sheet_id", "_id"]
    cursor_field = CURSOR_FIELD

    def __init__(
        self,
        parent: Sheets,
        authenticator: TokenAuthenticator,
        api_budget: Optional[APIBudget] = None,
        start_date: Optional[str] = None,
    ):
        self._state: MutableMapping[str, Any] = {}
        self._start_date = start_date
        super().__init__(parent=parent, authenticator=authenticator, api_budget=api_budget)

    # -- state -------------------------------------------------------------------------------

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = dict(value or {})

    def cursor_for(self, sheet_id: str) -> Optional[str]:
        """The per-sheet cursor, falling back to the configured start_date."""
        sheet_state = self._state.get(sheet_id) or {}
        return sheet_state.get(CURSOR_FIELD) or self._start_date

    # -- reading -----------------------------------------------------------------------------

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {"timeout": REQUEST_TIMEOUT_SECONDS, "stream": True}

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        sheet_id = self.sheet_id(stream_slice)
        for entry in iter_data_items(response):
            yield {"sheet_id": sheet_id, **entry}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        sheet_id = self.sheet_id(stream_slice)
        incremental = sync_mode == SyncMode.incremental
        floor_text = self.cursor_for(sheet_id) if incremental else None
        floor = parse_timestamp(floor_text)
        latest_text, latest = floor_text, floor

        for record in super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        ):
            changed_on = parse_timestamp(record.get(CURSOR_FIELD))
            if floor is not None and changed_on is not None and changed_on < floor:
                continue
            if changed_on is not None and (latest is None or changed_on > latest):
                latest, latest_text = changed_on, record[CURSOR_FIELD]
            yield record

        # Checkpoint only once the whole sheet has been read: the history is not sorted, so a
        # partial read cannot safely advance the cursor.
        if incremental and latest_text:
            self._state = {**self._state, sheet_id: {CURSOR_FIELD: latest_text}}
