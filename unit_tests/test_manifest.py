"""S9: the Connector Builder manifest validates against the CDK's declarative schema and resolves
the same streams as the Python connector, using the scrubbed fixture recordings."""

import json
import logging
from collections import Counter
from pathlib import Path

import pytest
import yaml
from airbyte_cdk.models import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.declarative.concurrent_declarative_source import ConcurrentDeclarativeSource

from source_orca_scan.streams import API_URL as _API_URL

API_URL = _API_URL.rstrip("/")

MANIFEST = Path(__file__).parent.parent / "connector-builder" / "manifest.yaml"
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def request_cache_dir(tmp_path, monkeypatch):
    """The declarative runtime caches parent-stream responses in a sqlite file under
    REQUEST_CACHE_PATH; the platform entrypoint sets it, tests must too."""
    monkeypatch.setenv("REQUEST_CACHE_PATH", str(tmp_path))


def load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _mock_api(requests_mock):
    sheets = load("sheets")
    requests_mock.get(API_URL + "/sheets", json=sheets)
    for sheet in sheets["data"]:
        sid = sheet["_id"]
        for endpoint in ("fields", "users", "hooks", "triggers", "rows", "history"):
            requests_mock.get(f"{API_URL}/sheets/{sid}/{endpoint}", json=load(f"{endpoint}_{sid}"))
    return sheets["data"]


def _source(config) -> ConcurrentDeclarativeSource:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    return ConcurrentDeclarativeSource(source_config=manifest, config=config)


def _discover(source, config):
    return {s.name: s for s in source.discover(logging.getLogger("airbyte"), config).streams}


def test_manifest_validates_and_exposes_the_spec(config, requests_mock):
    _mock_api(requests_mock)
    spec = _source(config).spec(logging.getLogger("airbyte"))
    schema = spec.connectionSpecification
    assert schema["required"] == ["api_key"]
    assert schema["properties"]["api_key"]["airbyte_secret"] is True


def test_manifest_resolves_fixed_and_per_sheet_streams(config, requests_mock):
    _mock_api(requests_mock)
    streams = _discover(_source(config), config)

    assert {"sheets", "sheet_fields", "sheet_users", "sheet_hooks", "sheet_triggers", "sheet_history"} <= set(streams)
    assert {"rows_sample_sheet_a", "rows_sample_sheet_b"} <= set(streams)
    rows_schema = streams["rows_sample_sheet_a"].json_schema["properties"]
    assert {"_id", "sheet_id"} <= set(rows_schema)
    assert set(f["key"] for f in load("fields_" + load("sheets")["data"][0]["_id"])["data"]) <= set(rows_schema)
    assert SyncMode.incremental in streams["sheet_history"].supported_sync_modes
    assert streams["sheet_history"].default_cursor_field == ["_changedOn"]
    for name, stream in streams.items():
        if name.startswith("rows_"):
            assert stream.supported_sync_modes == [SyncMode.full_refresh]


def test_manifest_reads_every_stream_from_fixtures(config, requests_mock):
    _mock_api(requests_mock)
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=s,
                sync_mode=SyncMode.incremental if s.name == "sheet_history" else SyncMode.full_refresh,
                cursor_field=["_changedOn"] if s.name == "sheet_history" else None,
                destination_sync_mode=DestinationSyncMode.append,
            )
            for s in _discover(_source(config), config).values()
        ]
    )
    # A fresh source with the catalog, the way the platform constructs it for a read.
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    source = ConcurrentDeclarativeSource(source_config=manifest, config=config, catalog=catalog)

    messages = list(source.read(logging.getLogger("airbyte"), config, catalog))

    counts = Counter(m.record.stream for m in messages if m.type == Type.RECORD)
    assert counts["sheets"] == 2
    assert counts["sheet_fields"] == 33
    assert counts["sheet_hooks"] == 1
    assert counts["sheet_history"] == 5
    assert counts["rows_sample_sheet_a"] == 5
    assert not [m for m in messages if m.type == Type.TRACE and m.trace.type.value == "ERROR"]
    # Every per-sheet record is tagged with its sheet.
    for m in messages:
        if m.type == Type.RECORD and m.record.stream != "sheets":
            assert m.record.data.get("sheet_id")
