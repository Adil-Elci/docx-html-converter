#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SENSITIVE_NAME_MARKERS = ("password", "secret", "token", "key", "credential", "auth")

# One-way export allowlist. Credentials/secrets are intentionally excluded.
SAFE_EXPORT_COLUMNS: dict[str, list[str]] = {
    "clients": [
        "id",
        "name",
        "primary_domain",
        "backlink_url",
        "email",
        "phone_number",
        "status",
        "created_at",
        "updated_at",
    ],
    "publishing_sites": [
        "id",
        "publishing_site_name",
        "publishing_site_url",
        "wp_rest_base",
        "hosted_by",
        "host_panel",
        "status",
        "created_at",
        "updated_at",
    ],
    "master_site_info": [
        "id",
        "publishing_site_url",
        "name",
        "wp_rest_base",
        "hosted_by",
        "host_panel",
        "status",
        "auth_type",
        "wp_username",
        "enabled",
        "created_at",
        "updated_at",
    ],
    "publishing_site_categories": [
        "id",
        "publishing_site_id",
        "wp_category_id",
        "name",
        "slug",
        "parent_wp_category_id",
        "post_count",
        "enabled",
        "created_at",
        "updated_at",
    ],
    "publishing_site_default_categories": [
        "id",
        "publishing_site_id",
        "wp_category_id",
        "category_name",
        "position",
        "enabled",
        "created_at",
        "updated_at",
    ],
    "client_publishing_site_access": ["id", "client_id", "publishing_site_id", "enabled", "created_at", "updated_at"],
    "client_target_sites": [
        "id",
        "client_id",
        "target_site_domain",
        "target_site_url",
        "is_primary",
        "created_at",
        "updated_at",
    ],
    "submissions": [
        "id",
        "client_id",
        "publishing_site_id",
        "source_type",
        "backlink_placement",
        "post_status",
        "title",
        "status",
        "created_at",
        "updated_at",
    ],
    "jobs": [
        "id",
        "submission_id",
        "client_id",
        "publishing_site_id",
        "job_status",
        "attempt_count",
        "wp_post_id",
        "wp_post_url",
        "created_at",
        "updated_at",
    ],
    "job_events": ["id", "job_id", "event_type", "created_at"],
    "assets": ["id", "job_id", "asset_type", "provider", "created_at"],
}


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required.")
    return value


def _assert_non_local_database_url(database_url: str) -> None:
    parsed_url = make_url(database_url)
    db_host = (parsed_url.host or "").strip().lower()
    if db_host in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        raise RuntimeError(
            "DATABASE_URL must point to the production database; localhost/loopback hosts are not allowed."
        )


def _assert_safe_column_map() -> None:
    for table_name, columns in SAFE_EXPORT_COLUMNS.items():
        for column_name in columns:
            lowered = column_name.lower()
            if any(marker in lowered for marker in SENSITIVE_NAME_MARKERS):
                raise RuntimeError(f"Unsafe column configured for export: {table_name}.{column_name}")


def _load_google_credentials():
    inline_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()

    if inline_json:
        info = json.loads(inline_json)
        return service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    if file_path:
        return service_account.Credentials.from_service_account_file(file_path, scopes=GOOGLE_SCOPES)
    raise RuntimeError("Either GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE must be set.")


def _export_tables() -> list[str]:
    configured = os.getenv("GOOGLE_SHEETS_EXPORT_TABLES", "").strip()
    if not configured:
        return list(SAFE_EXPORT_COLUMNS.keys())

    tables = [value.strip() for value in configured.split(",") if value.strip()]
    invalid = [table_name for table_name in tables if table_name not in SAFE_EXPORT_COLUMNS]
    if invalid:
        raise RuntimeError(f"Unsupported table(s) in GOOGLE_SHEETS_EXPORT_TABLES: {', '.join(invalid)}")
    return tables


def _serialize_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        if value.startswith(("=", "+", "-", "@")):
            return f"'{value}"
        return value
    return value


def _table_rows(engine: Engine, table_name: str, configured_columns: list[str]) -> tuple[list[str], list[list[Any]]]:
    inspector = inspect(engine)
    columns_in_db = {column["name"] for column in inspector.get_columns(table_name, schema="public")}
    selected_columns = [column for column in configured_columns if column in columns_in_db]
    if not selected_columns:
        return [], []

    select_columns_sql = ", ".join(f'"{column}"' for column in selected_columns)
    order_by_parts = []
    if "created_at" in selected_columns:
        order_by_parts.append('"created_at" NULLS LAST')
    if "id" in selected_columns:
        order_by_parts.append('"id"')
    if not order_by_parts:
        order_by_parts.append(f'"{selected_columns[0]}"')
    order_by_sql = ", ".join(order_by_parts)

    query = text(f'SELECT {select_columns_sql} FROM public."{table_name}" ORDER BY {order_by_sql}')

    with engine.connect() as connection:
        result = connection.execute(query)
        rows = [[_serialize_cell(value) for value in row] for row in result.fetchall()]
    return selected_columns, rows


def _ensure_sheet(spreadsheets_api, spreadsheet_id: str, known_titles: set[str], title: str) -> None:
    if title in known_titles:
        return
    spreadsheets_api.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
    ).execute()
    known_titles.add(title)


def _write_tab(values_api, spreadsheet_id: str, title: str, values: list[list[Any]]) -> None:
    tab_range = f"'{title}'!A1:ZZ"
    values_api.clear(spreadsheetId=spreadsheet_id, range=tab_range).execute()
    values_api.update(
        spreadsheetId=spreadsheet_id,
        range=f"'{title}'!A1",
        valueInputOption="RAW",
        body={"majorDimension": "ROWS", "values": values},
    ).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="One-way sync from Postgres to Google Sheets.")
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip(),
        help="Target Google spreadsheet ID (or set GOOGLE_SHEETS_SPREADSHEET_ID).",
    )
    args = parser.parse_args()

    if not args.spreadsheet_id:
        raise RuntimeError("Spreadsheet ID is required via --spreadsheet-id or GOOGLE_SHEETS_SPREADSHEET_ID.")

    _assert_safe_column_map()

    database_url = _required_env("DATABASE_URL")
    _assert_non_local_database_url(database_url)
    engine = create_engine(database_url, pool_pre_ping=True)

    credentials = _load_google_credentials()
    sheets_service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    spreadsheets_api = sheets_service.spreadsheets()
    values_api = spreadsheets_api.values()

    spreadsheet = spreadsheets_api.get(spreadsheetId=args.spreadsheet_id).execute()
    known_titles = {sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])}

    synced_at = datetime.now(timezone.utc).isoformat()
    export_counts: list[tuple[str, int]] = []

    for table_name in _export_tables():
        _ensure_sheet(spreadsheets_api, args.spreadsheet_id, known_titles, table_name)
        headers, rows = _table_rows(engine, table_name, SAFE_EXPORT_COLUMNS[table_name])
        values = [headers] + rows if headers else [["no columns selected"]]
        _write_tab(values_api, args.spreadsheet_id, table_name, values)
        export_counts.append((table_name, len(rows)))
        print(f"synced {table_name}: {len(rows)} rows")

    status_tab = "sync_status"
    _ensure_sheet(spreadsheets_api, args.spreadsheet_id, known_titles, status_tab)
    status_values = [["table_name", "rows_exported", "synced_at_utc"]]
    status_values.extend([[table_name, row_count, synced_at] for table_name, row_count in export_counts])
    _write_tab(values_api, args.spreadsheet_id, status_tab, status_values)

    print(f"sync complete at {synced_at}")


if __name__ == "__main__":
    main()
