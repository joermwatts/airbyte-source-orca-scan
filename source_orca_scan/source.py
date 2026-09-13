import logging
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .history import SheetHistory
from .rows import build_row_streams
from .streams import (
    SheetFields,
    SheetHooks,
    Sheets,
    SheetTriggers,
    SheetUsers,
    make_api_budget,
    make_authenticator,
)


class SourceOrcaScan(AbstractSource):
    """Airbyte source for the Orca Scan REST API (https://api.orcascan.com/v1).

    Read-only. Authenticates with an Orca Scan API key, which requires a
    Business subscription.
    """

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """Prove the key works by listing sheets - the cheapest authenticated call."""
        try:
            sheets = Sheets(authenticator=make_authenticator(config), api_budget=make_api_budget())
            count = sum(1 for _ in sheets.read_records(sync_mode=SyncMode.full_refresh))
        except AirbyteTracedException as exc:
            return False, exc.message or exc.internal_message
        except Exception as exc:  # network failures, malformed responses
            return False, f"Could not reach the Orca Scan API: {exc}"
        logger.info(f"Orca Scan connection check succeeded: the API key can see {count} sheet(s).")
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = make_authenticator(config)
        api_budget = make_api_budget()
        # Each substream gets its own parent instance. A `Sheets` stream that has already been
        # synced in this process carries a resumable-full-refresh cursor marked complete, and
        # would hand its substreams zero slices if shared.
        def sheets() -> Sheets:
            return Sheets(authenticator=authenticator, api_budget=api_budget)

        static_streams = [
            sheets(),
            SheetFields(parent=sheets(), authenticator=authenticator, api_budget=api_budget),
            SheetUsers(parent=sheets(), authenticator=authenticator, api_budget=api_budget),
            SheetHooks(parent=sheets(), authenticator=authenticator, api_budget=api_budget),
            SheetTriggers(parent=sheets(), authenticator=authenticator, api_budget=api_budget),
            SheetHistory(
                parent=sheets(), authenticator=authenticator, api_budget=api_budget, start_date=config.get("start_date")
            ),
        ]
        # One typed `rows_<sheet>` stream per sheet, schema built from the sheet's fields.
        # This calls the API (1 + number-of-sheets requests) at discover and read time.
        row_streams = build_row_streams(authenticator, api_budget)
        return [*static_streams, *row_streams]
