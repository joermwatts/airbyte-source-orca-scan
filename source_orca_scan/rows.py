"""One typed Airbyte stream per Orca Scan sheet.

`discover` reads `/sheets` and then `/sheets/{id}/fields` for each sheet, and turns every
sheet into its own stream whose JSON schema is built from that sheet's column definitions.
Rows are read from `/sheets/{id}/rows`, which returns the whole sheet in one unpaginated
response, so the body is streamed and parsed incrementally with ijson.
"""

import re
from collections import Counter
from typing import Any, Iterable, List, Mapping, Optional, Sequence

import ijson
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.call_rate import APIBudget
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import REQUEST_TIMEOUT_SECONDS, OrcaScanStream, SheetFields, Sheets

ROWS_STREAM_PREFIX = "rows_"

# Every row has these regardless of the sheet's columns.
SYSTEM_PROPERTIES: Mapping[str, Mapping[str, Any]] = {
    "_id": {"type": "string", "description": "Row id (24-character hex), unique within the sheet."},
    "sheet_id": {"type": "string", "description": "Id of the sheet this row belongs to (added by the connector)."},
}

ANY_JSON = ["string", "number", "boolean", "object", "array", "null"]


def field_to_json_schema(field: Mapping[str, Any]) -> Mapping[str, Any]:
    """Map an Orca Scan column definition to a JSON Schema property.

    Orca types seen in the wild: string, integer, boolean, datetime, image, gps. Formats refine
    them (e.g. `date` vs `date time (automatic)`, `photo`). Anything unknown is left open so a
    new Orca field type never breaks a sync.
    """
    ftype = str(field.get("type") or "").strip().lower()
    fmt = str(field.get("format") or "").strip().lower()
    label = field.get("label") or field.get("key") or ""
    description = f"{label} ({fmt or ftype})" if label else (fmt or ftype)

    if ftype == "integer":
        schema: dict = {"type": ["integer", "null"]}
    elif ftype in ("number", "float", "decimal", "double", "currency"):
        schema = {"type": ["number", "null"]}
    elif ftype == "boolean":
        schema = {"type": ["boolean", "null"]}
    elif ftype in ("datetime", "date", "timestamp"):
        if fmt == "time":
            schema = {"type": ["string", "null"]}
        elif fmt.startswith("date time") or fmt in ("created date", "last modified date"):
            schema = {"type": ["string", "null"], "format": "date-time"}
        elif fmt.startswith("date"):
            schema = {"type": ["string", "null"], "format": "date"}
        else:
            schema = {"type": ["string", "null"], "format": "date-time"}
    elif ftype in ("image", "photo", "attachment", "file", "signature"):
        # The API returns an empty list when there is no photo, and a URL string when there is.
        schema = {"type": ["array", "string", "null"], "items": {"type": ["string", "null"]}}
    elif ftype in ("gps", "location"):
        schema = {"type": ["string", "object", "null"], "additionalProperties": True}
    elif ftype == "string":
        schema = {"type": ["string", "null"]}
    else:
        schema = {"type": ANY_JSON}

    schema["description"] = description
    return schema


def build_rows_schema(fields: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    properties: dict = dict(SYSTEM_PROPERTIES)
    for field in fields:
        key = field.get("key")
        if not key or key in properties:
            continue
        properties[key] = field_to_json_schema(field)
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        # Rows can carry columns that were deleted after the row was written.
        "additionalProperties": True,
        "properties": properties,
    }


def slugify(title: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (title or "").lower()).strip("_")


def rows_stream_names(sheets: Sequence[Mapping[str, Any]]) -> List[str]:
    """`rows_<slug of sheet name>`, one per sheet, in the order given.

    Sheets whose names slugify to the same thing all get a `_<last 6 of sheet id>` suffix, so
    the names are unique and do not depend on the order the API lists sheets in. A sheet with
    no usable name is named after its id.
    """
    slugs = [slugify(sheet.get("name")) or f"sheet_{sheet['_id']}" for sheet in sheets]
    repeats = Counter(slugs)
    names = []
    for sheet, slug in zip(sheets, slugs):
        if repeats[slug] > 1:
            slug = f"{slug}_{str(sheet['_id'])[-6:]}"
        names.append(f"{ROWS_STREAM_PREFIX}{slug}")
    return names


def iter_data_items(response: requests.Response) -> Iterable[Mapping[str, Any]]:
    """Yield the objects inside `{"data": [...]}` without loading the whole body into memory."""
    raw = response.raw
    try:
        raw.decode_content = True  # let urllib3 gunzip on the fly
    except AttributeError:
        pass
    for item in ijson.items(raw, "data.item"):
        if isinstance(item, dict):
            yield item


class SheetRows(OrcaScanStream):
    """`GET /sheets/{sheetId}/rows` for one sheet, with a schema built from that sheet's fields."""

    primary_key = "_id"

    def __init__(
        self,
        sheet: Mapping[str, Any],
        fields: Sequence[Mapping[str, Any]],
        stream_name: str,
        authenticator: TokenAuthenticator,
        api_budget: Optional[APIBudget] = None,
    ):
        self.sheet = sheet
        self.sheet_id = str(sheet["_id"])
        self.fields = list(fields)
        self._stream_name = stream_name
        self._schema = build_rows_schema(self.fields)
        super().__init__(authenticator=authenticator, api_budget=api_budget)

    @property
    def name(self) -> str:
        return self._stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._schema

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return f"sheets/{self.sheet_id}/rows"

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # The whole sheet comes back in one response; stream it rather than buffer it.
        return {"timeout": REQUEST_TIMEOUT_SECONDS, "stream": True}

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        for row in iter_data_items(response):
            yield {"sheet_id": self.sheet_id, **row}


def fetch_sheets_with_fields(
    authenticator: TokenAuthenticator, api_budget: Optional[APIBudget] = None
) -> List[tuple]:
    """[(sheet, fields), ...] for every sheet the key can see - the input to `build_row_streams`."""
    sheets = [dict(s) for s in Sheets(authenticator=authenticator, api_budget=api_budget).read_records(sync_mode=SyncMode.full_refresh)]
    fields_stream = SheetFields(parent=Sheets(authenticator=authenticator, api_budget=api_budget), authenticator=authenticator, api_budget=api_budget)
    result = []
    for sheet in sheets:
        fields = [
            dict(f)
            for f in fields_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice={"parent": sheet})
        ]
        result.append((sheet, fields))
    return result


def build_row_streams(
    authenticator: TokenAuthenticator, api_budget: Optional[APIBudget] = None
) -> List[SheetRows]:
    sheets_with_fields = fetch_sheets_with_fields(authenticator, api_budget)
    names = rows_stream_names([sheet for sheet, _ in sheets_with_fields])
    return [
        SheetRows(sheet=sheet, fields=fields, stream_name=name, authenticator=authenticator, api_budget=api_budget)
        for (sheet, fields), name in zip(sheets_with_fields, names)
    ]
