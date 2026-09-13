"""S3: sheets + the four per-sheet configuration substreams, validated against their schemas."""

import logging
from collections import Counter

import jsonschema
import pytest
from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)

from source_orca_scan.source import SourceOrcaScan
from source_orca_scan.streams import API_URL

SHEET_A = {"_id": "aaaaaaaaaaaaaaaaaaaaaaaa", "name": "Stock", "isOwner": True, "canAdmin": True, "canUpdate": True, "canDelete": True, "canExport": True}
SHEET_B = {"_id": "bbbbbbbbbbbbbbbbbbbbbbbb", "name": "Returns", "isOwner": False, "canAdmin": False, "canUpdate": True, "canDelete": False, "canExport": True}

FIELDS_A = [
    {"key": "barcode", "label": "Barcode", "type": "string", "format": "barcode", "required": True, "index": 0, "autofocus": True, "locked": False},
    {"key": "qty", "label": "Quantity", "type": "integer", "format": "number", "required": False, "index": 1, "prefix": "", "suffix": "pcs", "length": 6},
    {"key": "photo1", "label": "Photo", "type": "image", "format": "photo", "index": 2},
]
FIELDS_B = [{"key": "barcode", "label": "Barcode", "type": "string", "format": "barcode", "index": 0}]
USERS_A = [{"_id": "u1", "email": "someone@example.com", "canAdmin": True, "canUpdate": True, "canDelete": False, "canExport": True, "inviteId": "inv1"}]
HOOKS_A = [{"_id": "h1", "targetUrl": "https://example.com/hook", "eventName": "rows:add"}]
TRIGGERS_A = [
    {"_id": "t1", "sheetId": SHEET_A["_id"], "name": "Low stock", "enabled": True, "conditionField": "qty", "conditionType": "is less than",
     "conditionValue": "5", "actionType": "notify me", "notifyMethod": "email", "notifyType": "warning", "actionValue": ""}
]


def _mock_api(requests_mock, sheets=(SHEET_A, SHEET_B)):
    requests_mock.get(API_URL + "sheets", json={"data": list(sheets)})
    a, b = SHEET_A["_id"], SHEET_B["_id"]
    requests_mock.get(f"{API_URL}sheets/{a}/fields", json={"data": FIELDS_A})
    requests_mock.get(f"{API_URL}sheets/{a}/users", json={"data": USERS_A})
    requests_mock.get(f"{API_URL}sheets/{a}/hooks", json={"data": HOOKS_A})
    requests_mock.get(f"{API_URL}sheets/{a}/triggers", json={"data": TRIGGERS_A})
    requests_mock.get(f"{API_URL}sheets/{b}/fields", json={"data": FIELDS_B})
    # Sheet B has nothing configured - every list is empty.
    for endpoint in ("users", "hooks", "triggers", "rows", "history"):
        requests_mock.get(f"{API_URL}sheets/{b}/{endpoint}", json={"data": []})
    requests_mock.get(f"{API_URL}sheets/{a}/rows", json={"data": [{"_id": "r1", "barcode": "1", "qty": 2}, {"_id": "r2", "barcode": "3"}]})
    requests_mock.get(f"{API_URL}sheets/{a}/history", json={"data": [{"_id": "h1", "_change": "add", "_changedOn": "2026-09-01T00:00:00.000Z", "barcode": "1"}]})


def _streams(config):
    return {s.name: s for s in SourceOrcaScan().streams(config)}


def _read(stream):
    records = []
    for stream_slice in stream.stream_slices(sync_mode=SyncMode.full_refresh):
        records.extend(dict(r) for r in stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice))
    return records


def test_source_exposes_the_five_static_streams(config, requests_mock):
    _mock_api(requests_mock)
    assert set(_streams(config)) >= {"sheets", "sheet_fields", "sheet_users", "sheet_hooks", "sheet_triggers"}


@pytest.mark.parametrize("name", ["sheets", "sheet_fields", "sheet_users", "sheet_hooks", "sheet_triggers"])
def test_every_record_validates_against_its_schema(config, requests_mock, name):
    _mock_api(requests_mock)
    stream = _streams(config)[name]
    schema = stream.get_json_schema()
    validator = jsonschema.Draft7Validator(schema)

    records = _read(stream)

    assert records, f"{name} produced no records from the fixture"
    for record in records:
        errors = sorted(validator.iter_errors(record), key=str)
        assert not errors, f"{name} record {record} failed: {[e.message for e in errors]}"


def test_sheets_records_are_the_raw_sheet_objects(config, requests_mock):
    _mock_api(requests_mock)

    assert _read(_streams(config)["sheets"]) == [SHEET_A, SHEET_B]


def test_substreams_hit_every_sheet_and_tag_records_with_sheet_id(config, requests_mock):
    _mock_api(requests_mock)
    streams = _streams(config)

    fields = _read(streams["sheet_fields"])

    assert [f["sheet_id"] for f in fields] == [SHEET_A["_id"]] * 3 + [SHEET_B["_id"]]
    assert fields[0] == {"sheet_id": SHEET_A["_id"], **FIELDS_A[0]}
    called = {r.url for r in requests_mock.request_history}
    assert f"{API_URL}sheets/{SHEET_A['_id']}/fields" in called
    assert f"{API_URL}sheets/{SHEET_B['_id']}/fields" in called


def test_empty_substreams_yield_nothing_and_do_not_error(config, requests_mock):
    _mock_api(requests_mock, sheets=(SHEET_B,))
    streams = _streams(config)

    assert _read(streams["sheet_users"]) == []
    assert _read(streams["sheet_hooks"]) == []
    assert _read(streams["sheet_triggers"]) == []


def test_full_catalog_read_emits_records_for_every_static_stream(config, requests_mock):
    """Regression: substreams must still get slices after `sheets` itself has been synced in
    the same process (a shared, already-synced parent yields nothing)."""
    _mock_api(requests_mock)
    source = SourceOrcaScan()
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=s.as_airbyte_stream(),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
            for s in source.streams(config)
        ]
    )

    counts = Counter(
        m.record.stream for m in source.read(logging.getLogger("airbyte"), config, catalog) if m.type == Type.RECORD
    )

    assert counts == {"sheets": 2, "sheet_fields": 4, "sheet_users": 1, "sheet_hooks": 1, "sheet_triggers": 1, "sheet_history": 1, "rows_stock": 2}


def test_primary_keys(config, requests_mock):
    _mock_api(requests_mock)
    streams = _streams(config)
    assert streams["sheets"].primary_key == "_id"
    assert streams["sheet_fields"].primary_key == ["sheet_id", "key"]
    assert streams["sheet_users"].primary_key == ["sheet_id", "_id"]
    assert streams["sheet_hooks"].primary_key == ["sheet_id", "_id"]
    assert streams["sheet_triggers"].primary_key == ["sheet_id", "_id"]
