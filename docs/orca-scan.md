# Orca Scan

This page contains the setup guide and reference information for the Orca Scan source connector.

[Orca Scan](https://orcascan.com) is a barcode scanning and inventory app. Data lives in *sheets*
(spreadsheets with user-defined columns); this connector replicates every sheet the API key can
see - rows, column definitions, change history, users, webhooks and automation rules - into your
destination.

## Prerequisites

- An Orca Scan account on a **Business** subscription (the REST API requires it).
- An Orca Scan API key: in the Orca Scan web app open **Account Settings** and create one. It starts with `orca_`.

## Setup guide

1. In Airbyte, go to **Sources** and add a new **Orca Scan** source.
2. Enter a name for the source.
3. Paste your **API Key**.
4. Optionally set a **Start Date** (`YYYY-MM-DDTHH:MM:SSZ`, UTC). On the first incremental sync of `sheet_history` only changes at or after this instant are replicated.
5. Click **Set up source**. Airbyte checks the key by listing your sheets.

## Supported sync modes

| Feature | Supported? |
|---------|------------|
| Full Refresh Sync | Yes |
| Incremental Sync | Yes - `sheet_history` only |
| Replicate Incremental Deletes | No |
| SSL connection | Yes |
| Namespaces | No |

## Supported streams

| Stream | Sync modes | Primary key | Description |
|--------|------------|-------------|-------------|
| `sheets` | Full refresh | `_id` | The sheets the key's user can access, and their permissions. |
| `sheet_fields` | Full refresh | `sheet_id`, `key` | Column definitions of each sheet. |
| `sheet_users` | Full refresh | `sheet_id`, `_id` | Users with access to each sheet. |
| `sheet_hooks` | Full refresh | `sheet_id`, `_id` | Webhooks registered on each sheet. |
| `sheet_triggers` | Full refresh | `sheet_id`, `_id` | Automation rules on each sheet. |
| `sheet_history` | Full refresh, Incremental | `sheet_id`, `_id` | Every change recorded on each sheet, with the row's values at that moment. Cursor `_changedOn`. |
| `rows_<sheet name>` | Full refresh | `_id` | One stream per sheet: its rows, typed from its columns. |

Every per-sheet record carries a `sheet_id`.

### Per-sheet row streams

Each sheet becomes its own stream named `rows_<sheet name>` (lower-cased, non-alphanumerics
collapsed to `_`; sheets with clashing names get a short id suffix). Its schema is built from the
sheet's columns: `string`, `integer`, `number` and `boolean` fields map directly; `date` formats
become `string` with `format: date`; other date/time formats become `format: date-time`; image and
attachment fields are an empty array or a URL string; unknown Orca types are left untyped. All
properties are nullable and additional properties are allowed, because rows can still hold
columns that were later deleted.

## Limitations and performance considerations

- The Orca Scan API has **no pagination and no date filters**: every endpoint returns the whole
  result set in one response. The connector streams responses so memory stays flat, but a very
  large sheet or history still means a very large download (a ~5,500-row sheet is about 3 MB;
  a history of ~175,000 entries is about 120 MB and takes around a minute).
- **Rows are full refresh only** - they have no created/updated timestamp. Use *Full refresh |
  Overwrite* or *Append + Deduped* on `_id`.
- **`sheet_history` incremental syncs filter client-side.** Every sync downloads each sheet's
  full history and emits only entries at or after the per-sheet cursor. Entries whose
  `_changedOn` equals the cursor are re-emitted, so use *Incremental | Append + Deduped*.
- Orca Scan **prunes old history**, so a full-refresh read of `sheet_history` may return fewer
  entries than an earlier one.
- The API allows **15 requests/second** per key. The connector throttles itself to 12/s across all
  streams and respects `Retry-After` on 429 responses.
- Discovering the per-sheet schemas costs one request per sheet.
- The connector is read-only.

## Troubleshooting

| Message | Cause | Fix |
|---------|-------|-----|
| `401 Invalid API key` | The key is wrong, revoked, or pasted with whitespace. | Create a new key under Account Settings and paste it exactly. |
| `403 Forbidden ... Business subscription` | The account is not on a Business plan, or the key's user cannot access the sheet. | Upgrade the plan or share the sheet with the key's user. |
| `404 Not found` | A sheet was deleted mid-sync or access was removed. | Re-run **discover** to refresh the stream list. |
| A `rows_*` stream disappeared | The sheet was renamed or deleted. | Re-run **discover**; the stream is named after the sheet. |

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date | Pull Request | Subject |
|:--------|:-----|:-------------|:--------|
| 0.1.0 | 2026-09-13 | - | Initial release: sheets, sheet_fields, sheet_users, sheet_hooks, sheet_triggers, sheet_history (incremental) and one typed rows stream per sheet. |

</details>
