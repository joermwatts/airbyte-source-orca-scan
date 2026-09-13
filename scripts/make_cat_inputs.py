"""Generate the account-specific inputs for the Connector Acceptance Tests into secrets/.

    poetry run python scripts/make_cat_inputs.py [secrets/config.json]

Writes (all gitignored, because they name the account's sheets):
  secrets/configured_catalog_cat.json              full-refresh catalog of every stream that has data
                                                   (sheet_history excluded: it is huge and drifts as
                                                   Orca prunes old entries, so two reads never match)
  secrets/configured_catalog_cat_incremental.json  sheet_history, incremental on _changedOn
  secrets/abnormal_state.json                      a far-future cursor per sheet, for the
                                                   incremental test's "no new records" check
"""

import json
import logging
import sys
from pathlib import Path

from airbyte_cdk.models import AirbyteMessage, AirbyteMessageSerializer, SyncMode, Type

from source_orca_scan.source import SourceOrcaScan
from source_orca_scan.streams import Sheets, make_api_budget, make_authenticator

ROOT = Path(__file__).resolve().parent.parent
FUTURE = "2999-01-01T00:00:00.000Z"


def main(config_path: str) -> None:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    logging.getLogger("airbyte").setLevel(logging.WARNING)
    source = SourceOrcaScan()
    streams = {s.name: s for s in source.streams(config)}
    catalog = source.discover(logging.getLogger("airbyte"), config)
    as_json = AirbyteMessageSerializer.dump(AirbyteMessage(type=Type.CATALOG, catalog=catalog))
    by_name = {s["name"]: s for s in as_json["catalog"]["streams"]}

    # Which streams actually have records right now? Empty streams need a bypass in CAT.
    sheets = list(Sheets(authenticator=make_authenticator(config), api_budget=make_api_budget()).read_records(sync_mode=SyncMode.full_refresh))
    non_empty = []
    for name, stream in streams.items():
        if name == "sheet_history":
            continue
        count = sum(1 for _ in stream.read_only_records()) if not name.startswith("rows_") else _row_count(stream)
        if count or name == "sheet_hooks":  # sheet_hooks is declared empty in acceptance-test-config.yml
            non_empty.append(name)
        else:
            print(f"skipping {name}: no records")

    full = {
        "streams": [
            {"stream": by_name[n], "sync_mode": "full_refresh", "destination_sync_mode": "overwrite"} for n in non_empty
        ]
    }
    incremental = {
        "streams": [
            {
                "stream": by_name["sheet_history"],
                "sync_mode": "incremental",
                "cursor_field": ["_changedOn"],
                "destination_sync_mode": "append",
            }
        ]
    }
    abnormal = [
        {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "sheet_history"},
                "stream_state": {str(s["_id"]): {"_changedOn": FUTURE} for s in sheets},
            },
        }
    ]
    out = ROOT / "secrets"
    out.mkdir(exist_ok=True)
    (out / "configured_catalog_cat.json").write_text(json.dumps(full, indent=2), encoding="utf-8")
    (out / "configured_catalog_cat_incremental.json").write_text(json.dumps(incremental, indent=2), encoding="utf-8")
    (out / "abnormal_state.json").write_text(json.dumps(abnormal, indent=2), encoding="utf-8")
    print(f"full-refresh catalog: {non_empty}")
    print(f"incremental catalog: ['sheet_history']; abnormal state for {len(sheets)} sheet(s)")


def _row_count(stream) -> int:
    """Cheap emptiness check for a rows stream via /rows/count."""
    from source_orca_scan.streams import unwrap_envelope

    request, response = stream._http_client.send_request(
        http_method="GET",
        url=f"{stream.url_base}sheets/{stream.sheet_id}/rows/count",
        request_kwargs={"timeout": 60},
        headers={"Accept": "application/json"},
    )
    return int(next(unwrap_envelope(response.json()), {}).get("count", 0))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "secrets" / "config.json"))
