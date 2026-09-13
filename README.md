# Airbyte source connector for Orca Scan

A read-only [Airbyte](https://airbyte.com) source for [Orca Scan](https://orcascan.com), the
barcode scanning and inventory app. It pulls every sheet the API key can see - the rows, the
column definitions, the change history, the users, webhooks and automation rules - into any
Airbyte destination (BigQuery, Snowflake, Postgres, S3, ...).

Built on the [Airbyte Python CDK](https://docs.airbyte.com/platform/connector-development/cdk-python)
against the [Orca Scan REST API](https://orcascan.com/guides/barcode-scanning-rest-api-f09a21c3).
MIT licensed. Not affiliated with Orca Scan or Airbyte.

## Prerequisites

- An Orca Scan account on a **Business** subscription - the REST API is not available on lower plans.
- An Orca Scan **API key**. In the Orca Scan web app go to *Account Settings* and create one; it starts with `orca_`.
- Airbyte OSS / Self-Managed (to add a custom connector image) or a local Python 3.10-3.13 environment.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `api_key` | yes | Your Orca Scan API key. Sent as `Authorization: Bearer <key>`. |
| `start_date` | no | `YYYY-MM-DDTHH:MM:SS` (UTC). On the first incremental sync of `sheet_history`, only changes at or after this instant are emitted. Leave blank for everything. |

`secrets/config.json`:

```json
{ "api_key": "orca_...", "start_date": "2025-01-01T00:00:00" }
```

## Streams

| Stream | Endpoint | Sync modes | Primary key | Notes |
|--------|----------|------------|-------------|-------|
| `sheets` | `GET /sheets` | full refresh | `_id` | Every sheet the key's user can access, with their permissions on it. |
| `sheet_fields` | `GET /sheets/{id}/fields` | full refresh | `sheet_id`, `key` | Column definitions: key, label, type, format, validation and UI flags. |
| `sheet_users` | `GET /sheets/{id}/users` | full refresh | `sheet_id`, `_id` | Users with access to each sheet and their permissions. |
| `sheet_hooks` | `GET /sheets/{id}/hooks` | full refresh | `sheet_id`, `_id` | Webhooks registered on each sheet. |
| `sheet_triggers` | `GET /sheets/{id}/triggers` | full refresh | `sheet_id`, `_id` | Automation rules on each sheet. |
| `sheet_history` | `GET /sheets/{id}/history` | full refresh, **incremental** | `sheet_id`, `_id` | Every add/update/delete ever recorded, with the row values at that moment. Cursor: `_changedOn`. |
| `rows_<sheet>` | `GET /sheets/{id}/rows` | full refresh | `_id` | **One stream per sheet**, typed from that sheet's columns. |

Every per-sheet record carries a `sheet_id` so it can be joined back to `sheets`.

### One stream per sheet

Orca Scan sheets are spreadsheets: each has its own columns. `discover` reads the column
definitions of every sheet and publishes one stream per sheet, named `rows_<sheet name>`
(lower-cased, non-alphanumerics collapsed to `_`; sheets whose names collide get a `_<id suffix>`;
a nameless sheet is `rows_sheet_<id>`), with a JSON schema built from the columns:

| Orca field type | Format examples | JSON schema |
|-----------------|-----------------|-------------|
| `string` | text, barcode, email, url, drop-down list | `string` |
| `integer` | number | `integer` |
| `number` | currency | `number` |
| `boolean` | true/false | `boolean` |
| `datetime` | date, date (automatic) | `string`, `format: date` |
| `datetime` | date time, date time (automatic), created date, last modified date | `string`, `format: date-time` |
| `image` | photo, attachment, signature | `array` (empty) or `string` (a URL) |
| `gps` | gps location | `string` or `object` |
| anything else | | untyped |

Every property is nullable, and rows may carry columns that were deleted after the row was
written, so `additionalProperties` is left open. Values are passed through exactly as the API
returns them.

## Sync behaviour and limitations

Please read these before relying on the connector - they follow from how the Orca Scan API works.

- **No pagination.** Every endpoint returns the whole result set in one response. The connector
  streams and parses responses incrementally, so memory stays flat, but a very large sheet still
  means one very large HTTP response (a 5,500-row sheet is ~3 MB; its history, ~175,000 entries,
  is ~120 MB and takes about a minute).
- **Rows are full refresh only.** Rows have no created/updated timestamp, so there is nothing to
  filter on. Use *Full refresh | Overwrite* (or *Append + Deduped* on `_id`).
- **`sheet_history` incremental is client-side.** The API has no date filter, so every sync still
  downloads each sheet's full history; the connector then emits only entries whose `_changedOn`
  is at or after the per-sheet cursor (or `start_date` on the first run). Entries exactly equal
  to the cursor are re-emitted (at-least-once) - use *Append + Deduped*.
- **History is pruned by Orca Scan.** Old entries drop off the API over time, so a full-refresh
  read of `sheet_history` may return fewer entries than the previous one.
- **Rate limit.** Orca Scan allows 15 requests/second per key. The connector throttles itself to
  12/s across all streams and honours `Retry-After` on any 429.
- **Discover calls the API.** Building the per-sheet schemas costs 1 + *number of sheets* requests.
- **Read only.** No endpoint that creates, updates or deletes anything is implemented.

## Running it

### Locally

```bash
poetry install
poetry run source-orca-scan spec
poetry run source-orca-scan check --config secrets/config.json
poetry run source-orca-scan discover --config secrets/config.json
poetry run source-orca-scan read --config secrets/config.json --catalog integration_tests/configured_catalog_static.json
```

On Windows, the connector forces UTF-8 output, so no extra environment variables are needed.

### Docker

```bash
docker build -t airbyte/source-orca-scan:dev .
docker run --rm airbyte/source-orca-scan:dev spec
docker run --rm -v "$PWD/secrets:/secrets" airbyte/source-orca-scan:dev check --config /secrets/config.json
```

### In Airbyte

In Airbyte OSS / Self-Managed: *Settings -> Sources -> + New connector*, set the image to
`airbyte/source-orca-scan` and the tag to the version you built (e.g. `dev` or `0.1.0`), then
add an *Orca Scan* source and paste your API key. Airbyte Cloud only runs connectors from the
Airbyte registry; this connector is not (yet) listed there.

## Connector Builder (low-code) version

[`connector-builder/manifest.yaml`](connector-builder/manifest.yaml) is a declarative version of
the same connector for Airbyte's **Connector Builder**: in Airbyte go to *Builder -> New custom
connector -> Import a YAML manifest*, paste the file, enter your API key in *Testing values* and
publish. It gives you the same streams, auth, rate limiting, per-sheet typed row streams (via
dynamic streams and a dynamic schema loader) and client-side incremental `sheet_history`,
without building a Docker image.

Differences from the Python connector, all inherent to the low-code runtime:

- responses are parsed in one go rather than streamed, so a very large history is held in
  memory during the sync;
- per-sheet streams are named `rows_<slug>` without the collision suffix, so two sheets whose
  names differ only in punctuation would clash - rename one in Orca Scan;
- `start_date` must be the full `YYYY-MM-DDTHH:MM:SS` form;
- dynamic streams and API budgets need a recent Airbyte platform (the manifest targets CDK 7.x).

To run it locally with the CDK's generic runner:

```bash
poetry run python scripts/manifest_config.py      # merges secrets/config.json with the manifest
poetry run source-declarative-manifest check --config secrets/config_manifest.json
poetry run source-declarative-manifest discover --config secrets/config_manifest.json
```

## Development

```bash
poetry install
poetry run pytest                                   # unit tests; live tests skip without a key
ORCA_SCAN_API_KEY=orca_... poetry run pytest integration_tests   # live smoke tests
poetry run pytest --cov=source_orca_scan            # coverage
```

Airbyte's [Connector Acceptance Tests](https://docs.airbyte.com/platform/connector-development/testing-connectors/connector-acceptance-tests-reference):

```bash
docker build -t airbyte/source-orca-scan:dev .
poetry run python scripts/make_cat_inputs.py        # writes the account-specific catalogs to secrets/
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:/test_input" \
  airbyte/connector-acceptance-test:latest --acceptance-test-config /test_input \
  -k "not backward_compatibility"   # those need a previously published image to compare against
```

Expect the full-refresh and incremental suites to take several minutes: they read every stream
at least twice, including the full history.

Layout:

```
source_orca_scan/
  spec.yaml        connector specification (config form)
  source.py        SourceOrcaScan: check_connection, streams()
  streams.py       base HTTP stream: auth, rate limit, retries, error mapping; sheets + substreams
  rows.py          one typed stream per sheet, schema built from /fields
  history.py       sheet_history, incremental on _changedOn with per-sheet state
  schemas/         JSON schemas of the fixed streams
unit_tests/        pytest, mocked HTTP; fixtures/ are scrubbed recordings of the real API
integration_tests/ live smoke tests (need ORCA_SCAN_API_KEY) and a sample configured catalog
docs/              the Airbyte-format documentation page
```

Never commit `secrets/` or `.env` - both are gitignored.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Licence

[MIT](LICENSE) - Joe R M Watts Limited (WattsWeb).
