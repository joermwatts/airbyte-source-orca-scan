"""S6: every stream read end to end from recorded (scrubbed) API responses, validated against
its own schema. The fixtures in unit_tests/fixtures/ were recorded from the live API on
13/09/2026 and scrubbed - ids re-hashed, names/emails/URLs/text replaced - so they keep the
real key sets and value types without any real data."""

import json
import logging
from collections import Counter
from pathlib import Path

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

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def recorded_api(requests_mock):
    sheets = load("sheets")
    requests_mock.get(API_URL + "sheets", json=sheets)
    for sheet in sheets["data"]:
        sid = sheet["_id"]
        for endpoint in ("fields", "users", "hooks", "triggers", "rows", "history"):
            requests_mock.get(f"{API_URL}sheets/{sid}/{endpoint}", json=load(f"{endpoint}_{sid}"))
    return sheets["data"]


def _catalog(source, config, sync_mode=SyncMode.full_refresh):
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=s.as_airbyte_stream(),
                sync_mode=sync_mode if sync_mode in s.as_airbyte_stream().supported_sync_modes else SyncMode.full_refresh,
                cursor_field=["_changedOn"] if s.name == "sheet_history" and sync_mode == SyncMode.incremental else None,
                destination_sync_mode=DestinationSyncMode.append,
            )
            for s in source.streams(config)
        ]
    )


def test_fixtures_contain_no_secrets():
    """Recorded fixtures must stay scrubbed: no API key, and no email or URL outside example.com."""
    import re

    blob = "".join(p.read_text(encoding="utf-8") for p in FIXTURES.glob("*.json"))
    assert "orca_" not in blob.replace("orca_test_key_not_real", "")
    assert not [e for e in re.findall(r"[\w.+-]+@[\w.-]+", blob) if not e.endswith("@example.com")]
    assert not [u for u in re.findall(r"https?://[^\s\"']+", blob) if not u.startswith("https://example.com")]


def test_every_stream_reads_from_recorded_responses_and_validates(config, recorded_api):
    source = SourceOrcaScan()
    streams = {s.name: s for s in source.streams(config)}
    validators = {name: jsonschema.Draft7Validator(s.get_json_schema()) for name, s in streams.items()}

    messages = list(source.read(logging.getLogger("airbyte"), config, _catalog(source, config)))
    records = [m.record for m in messages if m.type == Type.RECORD]
    counts = Counter(r.stream for r in records)

    # Everything the fixtures hold comes through: 2 sheets, their fields, users, triggers,
    # the synthetic hook, 5 rows and 5 history entries for the sheet that has them.
    assert counts["sheets"] == 2
    assert counts["sheet_fields"] == sum(len(load(f"fields_{s['_id']}")["data"]) for s in recorded_api)
    assert counts["sheet_hooks"] == 1
    assert counts["sheet_history"] == sum(len(load(f"history_{s['_id']}")["data"]) for s in recorded_api)
    assert sum(v for k, v in counts.items() if k.startswith("rows_")) == sum(len(load(f"rows_{s['_id']}")["data"]) for s in recorded_api)
    assert set(counts) <= set(streams)

    for record in records:
        errors = [e.message for e in validators[record.stream].iter_errors(record.data)]
        assert not errors, f"{record.stream}: {errors} in {record.data}"

    assert not [m for m in messages if m.type == Type.TRACE and m.trace.type.value == "ERROR"]


def test_incremental_read_from_recorded_responses_checkpoints_each_sheet(config, recorded_api):
    source = SourceOrcaScan()
    catalog = _catalog(source, config, sync_mode=SyncMode.incremental)
    catalog.streams = [s for s in catalog.streams if s.stream.name == "sheet_history"]

    messages = list(source.read(logging.getLogger("airbyte"), config, catalog))

    states = [m.state.stream.stream_state.__dict__ for m in messages if m.type == Type.STATE]
    assert states, "no STATE emitted"
    final = states[-1]
    with_history = [s["_id"] for s in recorded_api if load(f"history_{s['_id']}")["data"]]
    assert set(final) == set(with_history)
    for sid in with_history:
        assert final[sid]["_changedOn"] == max(e["_changedOn"] for e in load(f"history_{sid}")["data"])
