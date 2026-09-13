"""Live smoke test against the real Orca Scan API.

Runs only when ORCA_SCAN_API_KEY is set (a Business-subscription key); otherwise every test
here is skipped so `pytest` stays green for contributors without an account.

    ORCA_SCAN_API_KEY=orca_... poetry run pytest integration_tests
"""

import logging
import os
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

API_KEY = os.environ.get("ORCA_SCAN_API_KEY")

pytestmark = pytest.mark.skipif(not API_KEY, reason="ORCA_SCAN_API_KEY is not set; skipping live API tests")


@pytest.fixture(scope="module")
def config() -> dict:
    return {"api_key": API_KEY}


@pytest.fixture(scope="module")
def source() -> SourceOrcaScan:
    return SourceOrcaScan()


def test_check_succeeds(source, config):
    ok, error = source.check_connection(logging.getLogger("airbyte"), config)
    assert ok, error


def test_check_fails_cleanly_with_a_bad_key(source):
    ok, error = source.check_connection(logging.getLogger("airbyte"), {"api_key": "orca_not_a_real_key_000000000"})
    assert ok is False
    assert "API key" in str(error)


def test_discover_lists_fixed_streams_and_one_rows_stream_per_sheet(source, config):
    streams = source.streams(config)
    names = {s.name for s in streams}
    assert {"sheets", "sheet_fields", "sheet_users", "sheet_hooks", "sheet_triggers", "sheet_history"} <= names
    sheet_count = len(list(next(s for s in streams if s.name == "sheets").read_records(sync_mode=SyncMode.full_refresh)))
    assert len([n for n in names if n.startswith("rows_")]) == sheet_count


def test_full_refresh_read_of_the_small_streams_validates(source, config):
    """Reads everything except sheet_history (which can be >100 MB) and validates each record."""
    streams = [s for s in source.streams(config) if s.name != "sheet_history"]
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=s.as_airbyte_stream(), sync_mode=SyncMode.full_refresh, destination_sync_mode=DestinationSyncMode.overwrite
            )
            for s in streams
        ]
    )
    validators = {s.name: jsonschema.Draft7Validator(s.get_json_schema()) for s in streams}

    messages = list(source.read(logging.getLogger("airbyte"), config, catalog))

    records = [m.record for m in messages if m.type == Type.RECORD]
    counts = Counter(r.stream for r in records)
    assert counts["sheets"] >= 1
    assert counts["sheet_fields"] >= 1
    for record in records:
        errors = [e.message for e in validators[record.stream].iter_errors(record.data)]
        assert not errors, f"{record.stream}: {errors}"
    assert not [m for m in messages if m.type == Type.TRACE and m.trace.type.value == "ERROR"]
