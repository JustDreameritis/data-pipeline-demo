"""
Google Sheets exporter (stub — requires google-auth setup).

This module is documented and ready to use, but it will gracefully skip
export if Google credentials are not configured.

## How to enable Google Sheets export

1. Go to https://console.cloud.google.com/
2. Create a project (or select an existing one)
3. Enable the "Google Sheets API" and "Google Drive API"
4. Create a Service Account:
   - IAM & Admin → Service Accounts → Create
   - Download the JSON key file
5. Share your target Google Sheet with the service account email
   (the email looks like: name@project.iam.gserviceaccount.com)
6. Set environment variables in your .env file:
   GOOGLE_CREDENTIALS_PATH=/path/to/service-account-key.json
   GOOGLE_SPREADSHEET_ID=your-spreadsheet-id-from-the-url

7. Install additional dependencies:
   pip install google-auth google-auth-httplib2 google-api-python-client

Note: These are NOT in requirements.txt because Sheets is an optional feature.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import config as cfg
from models import BaseRecord

log = logging.getLogger(__name__)

_SHEETS_AVAILABLE = False
_MISSING_DEPS: list[str] = []

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    _SHEETS_AVAILABLE = True
except ImportError as _e:
    _MISSING_DEPS.append(str(_e))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def _build_service(credentials_path: str) -> Any:
    """Build an authenticated Google Sheets API service object."""
    creds = service_account.Credentials.from_service_account_file(  # type: ignore[attr-defined]
        credentials_path, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)  # type: ignore[call-arg]


def _records_to_values(records: list[BaseRecord]) -> list[list[Any]]:
    """Convert records to a 2-D list (rows × columns) for the Sheets API."""
    if not records:
        return []
    rows = [r.to_flat_dict() for r in records]
    headers = list(rows[0].keys())
    data: list[list[Any]] = [headers]
    for row in rows:
        data.append([str(v) if v is not None else "" for v in row.values()])
    return data


# ---------------------------------------------------------------------------
# Exporter class
# ---------------------------------------------------------------------------

class SheetsExporter:
    """
    Exports records to a Google Sheets spreadsheet.

    Each call to export() writes to a named worksheet (tab).
    If the sheet does not exist, it is created automatically.
    Existing data in the sheet is overwritten.
    """

    def __init__(
        self,
        spreadsheet_id: str | None = None,
        credentials_path: str | None = None,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id or cfg.export.sheets_spreadsheet_id
        self.credentials_path = credentials_path or cfg.export.sheets_credentials_path

    def _is_configured(self) -> bool:
        return bool(self.spreadsheet_id and self.credentials_path)

    def export(self, records: list[BaseRecord], sheet_name: str | None = None) -> bool:
        """
        Write records to a Google Sheet.

        Args:
            records: Records to export.
            sheet_name: Name of the worksheet tab. Defaults to the source name.

        Returns:
            True if export succeeded, False if skipped/failed.
        """
        if not _SHEETS_AVAILABLE:
            log.warning(
                "Google Sheets export skipped — missing dependencies: %s. "
                "Run: pip install google-auth google-auth-httplib2 google-api-python-client",
                ", ".join(_MISSING_DEPS),
            )
            return False

        if not self._is_configured():
            log.warning(
                "Google Sheets export skipped — credentials not configured. "
                "Set GOOGLE_CREDENTIALS_PATH and GOOGLE_SPREADSHEET_ID in .env"
            )
            return False

        if not records:
            log.warning("Sheets exporter: no records to write")
            return False

        tab = sheet_name or records[0].source
        values = _records_to_values(records)

        try:
            service = _build_service(self.credentials_path)  # type: ignore[arg-type]
            sheets = service.spreadsheets()

            # Ensure the worksheet exists
            self._ensure_sheet(sheets, tab)

            # Clear and write
            range_name = f"'{tab}'!A1"
            sheets.values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
            ).execute()
            sheets.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": values},
            ).execute()

            log.info(
                "Sheets: wrote %d rows to '%s' in spreadsheet %s",
                len(values) - 1, tab, self.spreadsheet_id,
            )
            return True

        except Exception as exc:
            log.error("Sheets export failed: %s", exc)
            return False

    def _ensure_sheet(self, sheets: Any, sheet_name: str) -> None:
        """Create a worksheet tab if it doesn't already exist."""
        meta = sheets.get(spreadsheetId=self.spreadsheet_id).execute()
        existing = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if sheet_name not in existing:
            body = {
                "requests": [{
                    "addSheet": {
                        "properties": {"title": sheet_name}
                    }
                }]
            }
            sheets.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body,
            ).execute()
            log.debug("Created worksheet '%s'", sheet_name)


def export_sheets(
    records: list[BaseRecord],
    sheet_name: str | None = None,
    spreadsheet_id: str | None = None,
    credentials_path: str | None = None,
) -> bool:
    """Convenience function for one-shot Google Sheets export."""
    exporter = SheetsExporter(
        spreadsheet_id=spreadsheet_id,
        credentials_path=credentials_path,
    )
    return exporter.export(records, sheet_name=sheet_name)
