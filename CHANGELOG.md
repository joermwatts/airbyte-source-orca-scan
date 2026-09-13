# Changelog

All notable changes to the Orca Scan source connector. Dates are ISO (YYYY-MM-DD).

## Unreleased

- Added `connector-builder/manifest.yaml`: a declarative (low-code) version of the connector for Airbyte's Connector Builder, with the same streams, auth, rate limiting, per-sheet typed row streams and incremental `sheet_history`. Runnable locally via `scripts/manifest_config.py` + `source-declarative-manifest`.

## 0.1.0 - 2026-09-13

Initial release.

- Streams: `sheets`, `sheet_fields`, `sheet_users`, `sheet_hooks`, `sheet_triggers`, `sheet_history`, and one typed `rows_<sheet>` stream per sheet built from the sheet's column definitions.
- `sheet_history` syncs incrementally on `_changedOn` with per-sheet state and an optional `start_date` floor; everything else is full refresh (rows carry no timestamp).
- Bearer API-key auth, self-throttling to 12 requests/second, `Retry-After` backoff on 429, actionable messages for 401/403/404.
- Responses are streamed and parsed incrementally (the API has no pagination).
- Unit tests with scrubbed recordings of the real API, live integration tests (skipped without a key), Airbyte Connector Acceptance Test configuration, Dockerfile.
