"""S5: sheet_history incremental on _changedOn - filtering, per-sheet state, checkpoints."""

import logging
from collections import Counter

import pytest
from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    StreamDescriptor,
    SyncMode,
    Type,
)

from source_orca_scan.history import SheetHistory, parse_timestamp
from source_orca_scan.rows import SheetRows
from source_orca_scan.source import SourceOrcaScan
from source_orca_scan.streams import API_URL

SHEET_A = {"_id": "aaaaaaaaaaaaaaaaaaaaaaaa", "name": "Stock"}
SHEET_B = {"_id": "bbbbbbbbbbbbbbbbbbbbbbbb", "name": "Returns"}

# Deliberately NOT in chronological order, like the real API.
HISTORY_A = [
    {"_id": "h3", "_change": "update", "_changedOn": "2026-09-03T00:00:00.000Z", "barcode": "1"},
    {"_id": "h1", "_change": "add", "_changedOn": "2026-09-01T00:00:00.000Z", "barcode": "1"},
    {"_id": "h2", "_change": "update", "_changedOn": "2026-09-02T00:00:00.000Z", "barcode": "1"},
    {"_id": "h4", "_change": "update", "_changedOn": "2026-09-02T00:00:00.000Z", "barcode": "2"},
]
HISTORY_B = [
    {"_id": "k1", "_change": "add", "_changedOn": "2026-08-01T00:00:00.000Z", "barcode": "9"},
]


def _mock_api(requests_mock, history_a=HISTORY_A, history_b=HISTORY_B):
    requests_mock.get(API_URL + "sheets", json={"data": [SHEET_A, SHEET_B]})
    for sheet in (SHEET_A, SHEET_B):
        requests_mock.get(f"{API_URL}sheets/{sheet['_id']}/fields", json={"data": [{"key": "barcode", "type": "string"}]})
        requests_mock.get(f"{API_URL}sheets/{sheet['_id']}/rows", json={"data": []})
        for endpoint in ("users", "hooks", "triggers"):
            requests_mock.get(f"{API_URL}sheets/{sheet['_id']}/{endpoint}", json={"data": []})
    requests_mock.get(f"{API_URL}sheets/{SHEET_A['_id']}/history", json={"data": history_a})
    requests_mock.get(f"{API_URL}sheets/{SHEET_B['_id']}/history", json={"data": history_b})


def _history(config) -> SheetHistory:
    return next(s for s in SourceOrcaScan().streams(config) if isinstance(s, SheetHistory))


def _read(stream, sync_mode=SyncMode.incremental):
    out = []
    for stream_slice in stream.stream_slices(sync_mode=sync_mode):
        out.extend(dict(r) for r in stream.read_records(sync_mode=sync_mode, stream_slice=stream_slice))
    return out


def _catalog(source, config, only=("sheet_history",), sync_mode=SyncMode.incremental):
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=s.as_airbyte_stream(),
                sync_mode=sync_mode,
                cursor_field=["_changedOn"] if sync_mode == SyncMode.incremental else None,
                destination_sync_mode=DestinationSyncMode.append,
            )
            for s in source.streams(config)
            if s.name in only
        ]
    )


def _state_message(stream_state: dict) -> AirbyteStateMessage:
    return AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(stream_descriptor=StreamDescriptor(name="sheet_history"), stream_state=AirbyteStateBlob(stream_state)),
    )


def test_parse_timestamp():
    assert parse_timestamp("2026-09-01T10:11:12Z") < parse_timestamp("2026-09-01T10:11:12.123Z")
    assert parse_timestamp("2026-09-01T10:11:12+01:00") < parse_timestamp("2026-09-01T10:11:12Z")
    # The spec's start_date has no zone and may omit the time; both are read as UTC.
    assert parse_timestamp("2026-09-01T10:11:12") == parse_timestamp("2026-09-01T10:11:12Z")
    assert parse_timestamp("2026-09-01") == parse_timestamp("2026-09-01T00:00:00Z")
    assert parse_timestamp("") is None
    assert parse_timestamp(None) is None
    assert parse_timestamp("yesterday") is None
    assert parse_timestamp(12345) is None


def test_discover_marks_history_incremental_and_rows_full_refresh_only(config, requests_mock):
    _mock_api(requests_mock)
    streams = SourceOrcaScan().streams(config)

    history = next(s for s in streams if s.name == "sheet_history").as_airbyte_stream()
    assert SyncMode.incremental in history.supported_sync_modes
    assert history.default_cursor_field == ["_changedOn"]
    assert history.source_defined_cursor is True
    assert history.source_defined_primary_key == [["sheet_id"], ["_id"]]

    for s in streams:
        if isinstance(s, SheetRows):
            assert s.as_airbyte_stream().supported_sync_modes == [SyncMode.full_refresh], s.name


def test_full_refresh_emits_everything_and_keeps_no_state(config, requests_mock):
    _mock_api(requests_mock)
    stream = _history(config)

    records = _read(stream, sync_mode=SyncMode.full_refresh)

    assert [r["_id"] for r in records] == ["h3", "h1", "h2", "h4", "k1"]
    assert all(r["sheet_id"] in (SHEET_A["_id"], SHEET_B["_id"]) for r in records)
    assert stream.state == {}


def test_incremental_filters_out_entries_older_than_the_cursor(config, requests_mock):
    _mock_api(requests_mock)
    stream = _history(config)
    stream.state = {SHEET_A["_id"]: {"_changedOn": "2026-09-02T00:00:00.000Z"}}

    records = _read(stream)

    # h1 (Sept 1) is older than the cursor; h2 and h4 equal it and are re-emitted; h3 is newer.
    assert sorted(r["_id"] for r in records if r["sheet_id"] == SHEET_A["_id"]) == ["h2", "h3", "h4"]
    # Sheet B had no cursor, so everything comes through.
    assert [r["_id"] for r in records if r["sheet_id"] == SHEET_B["_id"]] == ["k1"]


def test_start_date_is_the_floor_when_there_is_no_state(requests_mock):
    config = {"api_key": "orca_test_key_not_real", "start_date": "2026-09-02T00:00:00Z"}
    _mock_api(requests_mock)
    stream = _history(config)

    records = _read(stream)

    assert sorted(r["_id"] for r in records) == ["h2", "h3", "h4"]  # k1 (Aug) and h1 (Sept 1) dropped
    assert stream.state == {
        SHEET_A["_id"]: {"_changedOn": "2026-09-03T00:00:00.000Z"},
        # Sheet B had nothing at or after the floor, so its cursor is the floor itself.
        SHEET_B["_id"]: {"_changedOn": "2026-09-02T00:00:00Z"},
    }


def test_state_is_kept_per_sheet_with_the_latest_seen_timestamp(config, requests_mock):
    _mock_api(requests_mock)
    stream = _history(config)

    _read(stream)

    assert stream.state == {
        SHEET_A["_id"]: {"_changedOn": "2026-09-03T00:00:00.000Z"},
        SHEET_B["_id"]: {"_changedOn": "2026-08-01T00:00:00.000Z"},
    }


def test_state_setter_replaces_and_getter_survives_round_trip(config, requests_mock):
    _mock_api(requests_mock)
    stream = _history(config)
    stream.state = {"x": {"_changedOn": "2026-01-01T00:00:00.000Z"}}
    assert stream.state == {"x": {"_changedOn": "2026-01-01T00:00:00.000Z"}}
    stream.state = {}
    assert stream.state == {}


def test_source_read_emits_a_state_message_after_each_sheet(config, requests_mock):
    _mock_api(requests_mock)
    source = SourceOrcaScan()

    messages = list(source.read(logging.getLogger("airbyte"), config, _catalog(source, config)))

    records = [m for m in messages if m.type == Type.RECORD]
    states = [m for m in messages if m.type == Type.STATE]
    assert len(records) == 5
    assert len(states) >= 2, "expected a checkpoint after each sheet"
    final = states[-1].state.stream.stream_state.__dict__
    assert final[SHEET_A["_id"]] == {"_changedOn": "2026-09-03T00:00:00.000Z"}
    assert final[SHEET_B["_id"]] == {"_changedOn": "2026-08-01T00:00:00.000Z"}


def test_second_run_with_first_runs_state_emits_only_new_or_equal_entries(config, requests_mock):
    _mock_api(requests_mock)
    source = SourceOrcaScan()
    first = list(source.read(logging.getLogger("airbyte"), config, _catalog(source, config)))
    first_state = [m for m in first if m.type == Type.STATE][-1].state.stream.stream_state.__dict__

    # A new change lands on sheet A between syncs.
    new_entry = {"_id": "h5", "_change": "update", "_changedOn": "2026-09-04T00:00:00.000Z", "barcode": "3"}
    _mock_api(requests_mock, history_a=HISTORY_A + [new_entry])
    source = SourceOrcaScan()
    second = list(source.read(logging.getLogger("airbyte"), config, _catalog(source, config), state=[_state_message(first_state)]))

    ids = sorted(m.record.data["_id"] for m in second if m.type == Type.RECORD)
    assert ids == ["h3", "h5", "k1"]  # h3 and k1 equal their sheet's cursor (re-emitted); h5 is new
    assert len([m for m in first if m.type == Type.RECORD]) > len(ids)
    final = [m for m in second if m.type == Type.STATE][-1].state.stream.stream_state.__dict__
    assert final[SHEET_A["_id"]] == {"_changedOn": "2026-09-04T00:00:00.000Z"}


def test_history_is_streamed(config, requests_mock):
    _mock_api(requests_mock)
    _read(_history(config))
    history_calls = [r for r in requests_mock.request_history if r.url.endswith("/history")]
    assert history_calls and all(r.stream is True for r in history_calls)
