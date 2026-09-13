"""S4: one typed stream per sheet - schema generation, naming, streaming reads, empty sheets."""

import json

import jsonschema
import pytest
from airbyte_cdk.models import SyncMode

from source_orca_scan.rows import (
    SYSTEM_PROPERTIES,
    SheetRows,
    build_rows_schema,
    field_to_json_schema,
    rows_stream_names,
    slugify,
)
from source_orca_scan.source import SourceOrcaScan
from source_orca_scan.streams import API_URL

SHEET_A = {"_id": "aaaaaaaaaaaaaaaaaaaaaaaa", "name": "Stock Count", "isOwner": True}
SHEET_B = {"_id": "bbbbbbbbbbbbbbbbbbbbbbbb", "name": "Returns", "isOwner": True}

FIELDS_A = [
    {"key": "barcode", "label": "Barcode", "type": "string", "format": "barcode", "index": 0},
    {"key": "qty", "label": "Quantity", "type": "integer", "format": "number", "index": 1},
    {"key": "instock", "label": "In stock", "type": "boolean", "format": "true/false", "index": 2},
    {"key": "photo1", "label": "Photo", "type": "image", "format": "photo", "index": 3},
    {"key": "counted", "label": "Counted on", "type": "datetime", "format": "date", "index": 4},
    {"key": "scanned", "label": "Scanned", "type": "datetime", "format": "date time (automatic)", "index": 5},
    {"key": "where", "label": "Where", "type": "gps", "format": "gps location (automatic)", "index": 6},
    {"key": "weird", "label": "Weird", "type": "hologram", "format": "??", "index": 7},
]
ROWS_A = [
    {"_id": "r1", "barcode": "5012345678900", "qty": 3, "instock": True, "photo1": [], "counted": "2026-09-01", "scanned": "2026-09-01T10:11:12Z"},
    {"_id": "r2", "barcode": "5012345678917", "qty": 0, "instock": False, "photo1": "https://cdn.example.com/p.jpg", "legacy_col": "x"},
    {"_id": "r3", "barcode": "5012345678924"},
]


def _mock_api(requests_mock, sheets=(SHEET_A, SHEET_B), rows_a=ROWS_A):
    requests_mock.get(API_URL + "sheets", json={"data": list(sheets)})
    requests_mock.get(f"{API_URL}sheets/{SHEET_A['_id']}/fields", json={"data": FIELDS_A})
    requests_mock.get(f"{API_URL}sheets/{SHEET_B['_id']}/fields", json={"data": FIELDS_A[:1]})
    requests_mock.get(f"{API_URL}sheets/{SHEET_A['_id']}/rows", json={"data": rows_a})
    requests_mock.get(f"{API_URL}sheets/{SHEET_B['_id']}/rows", json={"data": []})


def _row_streams(config):
    return {s.name: s for s in SourceOrcaScan().streams(config) if isinstance(s, SheetRows)}


def _read(stream):
    return [dict(r) for r in stream.read_records(sync_mode=SyncMode.full_refresh)]


@pytest.mark.parametrize(
    "field, expected_type, expected_format",
    [
        ({"type": "string", "format": "text"}, ["string", "null"], None),
        ({"type": "integer", "format": "number"}, ["integer", "null"], None),
        ({"type": "number", "format": "currency"}, ["number", "null"], None),
        ({"type": "boolean", "format": "true/false"}, ["boolean", "null"], None),
        ({"type": "datetime", "format": "date"}, ["string", "null"], "date"),
        ({"type": "datetime", "format": "date (automatic)"}, ["string", "null"], "date"),
        ({"type": "datetime", "format": "date time"}, ["string", "null"], "date-time"),
        ({"type": "datetime", "format": "date time (automatic)"}, ["string", "null"], "date-time"),
        ({"type": "datetime", "format": "time"}, ["string", "null"], None),
        ({"type": "image", "format": "photo"}, ["array", "string", "null"], None),
        ({"type": "gps", "format": "gps location"}, ["string", "object", "null"], None),
        ({"type": "hologram"}, ["string", "number", "boolean", "object", "array", "null"], None),
        ({}, ["string", "number", "boolean", "object", "array", "null"], None),
    ],
)
def test_field_type_mapping(field, expected_type, expected_format):
    schema = field_to_json_schema({"key": "k", "label": "K", **field})
    assert schema["type"] == expected_type
    assert schema.get("format") == expected_format


def test_schema_properties_match_the_sheets_fields():
    schema = build_rows_schema(FIELDS_A)

    assert set(schema["properties"]) == set(SYSTEM_PROPERTIES) | {f["key"] for f in FIELDS_A}
    assert schema["additionalProperties"] is True
    assert schema["properties"]["_id"]["type"] == "string"


def test_schema_skips_fields_without_a_key_and_never_clobbers_system_properties():
    schema = build_rows_schema([{"label": "no key"}, {"key": "_id", "type": "integer"}, {"key": "sheet_id", "type": "boolean"}])
    assert set(schema["properties"]) == set(SYSTEM_PROPERTIES)
    assert schema["properties"]["_id"]["type"] == "string"


def test_slugify():
    assert slugify("Stock Count") == "stock_count"
    assert slugify("  Returns / Q3 - 2026 (UK) ") == "returns_q3_2026_uk"
    assert slugify("") == ""
    assert slugify(None) == ""


def test_duplicate_sheet_names_get_distinct_stable_stream_names():
    sheets = [
        {"_id": "111111111111111111aaaaaa", "name": "Stock"},
        {"_id": "222222222222222222bbbbbb", "name": "stock!"},
        {"_id": "333333333333333333cccccc", "name": "Returns"},
        {"_id": "444444444444444444dddddd", "name": ""},
    ]

    names = rows_stream_names(sheets)

    assert names == ["rows_stock_aaaaaa", "rows_stock_bbbbbb", "rows_returns", "rows_sheet_444444444444444444dddddd"]
    assert len(set(names)) == len(names)
    # Order-independent: reversing the input gives the same name per sheet.
    assert dict(zip([s["_id"] for s in sheets], names)) == dict(zip([s["_id"] for s in reversed(sheets)], rows_stream_names(list(reversed(sheets)))))


def test_discover_emits_one_typed_stream_per_sheet(config, requests_mock):
    _mock_api(requests_mock)

    streams = _row_streams(config)

    assert set(streams) == {"rows_stock_count", "rows_returns"}
    assert set(streams["rows_stock_count"].get_json_schema()["properties"]) == set(SYSTEM_PROPERTIES) | {f["key"] for f in FIELDS_A}
    assert set(streams["rows_returns"].get_json_schema()["properties"]) == set(SYSTEM_PROPERTIES) | {"barcode"}
    assert streams["rows_stock_count"].primary_key == "_id"


def test_rows_are_tagged_with_sheet_id_and_validate_against_the_schema(config, requests_mock):
    _mock_api(requests_mock)
    stream = _row_streams(config)["rows_stock_count"]
    validator = jsonschema.Draft7Validator(stream.get_json_schema())

    records = _read(stream)

    assert [r["_id"] for r in records] == ["r1", "r2", "r3"]
    assert all(r["sheet_id"] == SHEET_A["_id"] for r in records)
    for record in records:
        assert not list(validator.iter_errors(record)), record
    # Rows are streamed, not buffered.
    assert requests_mock.request_history[-1].stream is True


def test_a_sheet_with_zero_rows_completes_with_no_records(config, requests_mock):
    _mock_api(requests_mock)

    assert _read(_row_streams(config)["rows_returns"]) == []


def test_large_response_is_parsed_incrementally(config, requests_mock):
    """5,000 rows in one body come through intact via ijson (the same path a 100 MB sheet takes)."""
    rows = [{"_id": f"{i:024d}", "barcode": str(i), "qty": i} for i in range(5000)]
    _mock_api(requests_mock, rows_a=rows)

    records = _read(_row_streams(config)["rows_stock_count"])

    assert len(records) == 5000
    assert records[-1]["qty"] == 4999
