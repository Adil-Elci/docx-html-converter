#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Engine

import import_tabular_to_db as updater


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
DEFAULT_WP_REST_BASE = "/wp-json/wp/v2"


def _script_dir() -> Path:
    return Path(__file__).resolve().parent


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _clean_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _normalize_site_url(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = (parsed.hostname or "").strip().lower()
        if not host:
            return ""
        return f"https://{host}"
    except Exception:
        host = raw.lower()
        if "://" in host:
            host = host.split("://", 1)[1]
        host = host.split("/", 1)[0].strip().lower()
        return f"https://{host}" if host else ""


def _site_name_from_url(value: Any) -> str:
    normalized = _normalize_site_url(value)
    return normalized.replace("https://", "", 1) if normalized else ""


def _reflect_table(engine: Engine, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def _build_engine_from_env() -> Engine:
    return updater._build_engine({"database_url_env": "DATABASE_URL"})


def _list_master_files(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    return sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS])


def _read_rows(path: Path) -> List[Dict[str, Any]]:
    return updater.read_rows_from_input(
        {
            "type": "xlsx" if path.suffix.lower() == ".xlsx" else "csv",
            "path": str(path),
            "encoding": "utf-8",
        }
    )


def _bool_or_default(value: Any, default: bool) -> bool:
    text = _clean_text(value)
    if not text:
        return default
    return bool(updater._apply_transform(text, "bool"))  # type: ignore[attr-defined]


def _prepare_master_rows(raw_rows: List[Dict[str, Any]]) -> tuple[list[dict[str, Any]], list[updater.RowIssue]]:
    prepared: list[dict[str, Any]] = []
    issues: list[updater.RowIssue] = []
    for idx, row in enumerate(raw_rows, start=2):
        if updater._row_blank(row):  # type: ignore[attr-defined]
            continue
        try:
            site_url = _normalize_site_url(row.get("publishing_site_url"))
            if not site_url:
                raise ValueError("publishing_site_url is required.")
            wp_username = _clean_text(row.get("wp_username"))
            wp_app_password = _clean_text(row.get("wp_app_password"))
            auth_type = _clean_text(row.get("auth_type")) or "application_password"
            if auth_type != "application_password":
                raise ValueError("auth_type must be application_password.")
            status = (_clean_text(row.get("status")) or "active").lower()
            if status not in {"active", "inactive"}:
                raise ValueError("status must be active or inactive.")
            prepared.append(
                {
                    "publishing_site_url": site_url,
                    "name": _site_name_from_url(site_url),
                    "wp_rest_base": DEFAULT_WP_REST_BASE,
                    "hosted_by": (_clean_text(row.get("hosted_by")) or None),
                    "host_panel": (_clean_text(row.get("host_panel")) or None),
                    "status": status,
                    "auth_type": auth_type,
                    "wp_username": (wp_username or None),
                    "wp_app_password": (wp_app_password or None),
                    "enabled": _bool_or_default(row.get("enabled"), True),
                }
            )
        except Exception as exc:
            issues.append(updater.RowIssue(row_number=idx, reason=str(exc), row=dict(row)))
    return prepared, issues


def _upsert_table(
    engine: Engine,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    match_columns: list[str],
    update_columns: list[str] | None = None,
    dry_run: bool,
) -> None:
    if not rows:
        return
    table = _reflect_table(engine, table_name)
    updater.apply_upsert(
        engine,
        table,
        rows,
        match_columns=match_columns,
        update_columns=update_columns,
        dry_run=dry_run,
    )


def _filter_new_or_changed_rows(
    engine: Engine,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    match_columns: list[str],
) -> list[dict[str, Any]]:
    if not rows:
        return []
    table = _reflect_table(engine, table_name)
    # Build in-memory index of existing rows by match key. Tables are small in this workflow.
    existing_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    with engine.connect() as conn:
        for rec in conn.execute(select(table)).mappings():
            key = tuple(rec.get(col) for col in match_columns)
            existing_map[key] = dict(rec)

    changed: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row.get(col) for col in match_columns)
        existing = existing_map.get(key)
        if existing is None:
            changed.append(row)
            continue
        row_changed = False
        for col, value in row.items():
            if col in {"id", "created_at", "updated_at"}:
                continue
            if existing.get(col) != value:
                row_changed = True
                break
        if row_changed:
            changed.append(row)
    return changed


def _load_site_ids_by_url(engine: Engine) -> dict[str, Any]:
    table = _reflect_table(engine, "publishing_sites")
    out: dict[str, Any] = {}
    with engine.connect() as conn:
        for row in conn.execute(select(table.c.id, table.c.publishing_site_url)):
            out[_normalize_site_url(row.publishing_site_url)] = row.id
    return out


def _prepare_publishing_sites_rows(master_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "publishing_site_url": row["publishing_site_url"],
            "name": row["name"],
            "wp_rest_base": DEFAULT_WP_REST_BASE,
            "hosted_by": row.get("hosted_by"),
            "host_panel": row.get("host_panel"),
            "status": row.get("status") or "active",
        }
        for row in master_rows
    ]


def _prepare_credentials_rows(master_rows: list[dict[str, Any]], site_ids_by_url: dict[str, Any]) -> tuple[list[dict[str, Any]], list[updater.RowIssue]]:
    rows: list[dict[str, Any]] = []
    issues: list[updater.RowIssue] = []
    for idx, row in enumerate(master_rows, start=2):
        site_url = row["publishing_site_url"]
        site_id = site_ids_by_url.get(site_url)
        if site_id is None:
            issues.append(updater.RowIssue(row_number=idx, reason="publishing_site_id lookup failed after site upsert", row=row))
            continue
        # Credentials are optional in master file, but if one of user/password exists, require both.
        wp_username = _clean_text(row.get("wp_username"))
        wp_app_password = _clean_text(row.get("wp_app_password"))
        if not wp_username and not wp_app_password:
            continue
        if not wp_username or not wp_app_password:
            issues.append(updater.RowIssue(row_number=idx, reason="Both wp_username and wp_app_password are required when credentials are present", row=row))
            continue
        rows.append(
            {
                "publishing_site_id": site_id,
                "auth_type": "application_password",
                "wp_username": wp_username,
                "wp_app_password": wp_app_password,
                "enabled": bool(row.get("enabled", True)),
            }
        )
    return rows, issues


def _write_report(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def run_master_sync(*, dry_run: bool = False) -> int:
    base = _script_dir()
    inbox_dir = base / "master_site_info"
    reports_dir = base / "reports"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    files = _list_master_files(inbox_dir)
    if not files:
        print(f"No files found in {inbox_dir}")
        return 0
    if len(files) > 1:
        print("Error: keep only one file at a time in master_site_info folder.", file=sys.stderr)
        for f in files:
            print(f"  - {f.name}", file=sys.stderr)
        return 1

    file_path = files[0]
    stamp = _utc_stamp()
    try:
        engine = _build_engine_from_env()
        raw_rows = _read_rows(file_path)
        master_rows, issues = _prepare_master_rows(raw_rows)

        # 1) master snapshot table
        master_rows_to_write = _filter_new_or_changed_rows(engine, "master_site_info", master_rows, match_columns=["publishing_site_url"])
        _upsert_table(
            engine,
            "master_site_info",
            master_rows_to_write,
            match_columns=["publishing_site_url"],
            dry_run=dry_run,
        )

        # 2) publishing_sites (site metadata)
        site_rows = _prepare_publishing_sites_rows(master_rows)
        site_rows_to_write = _filter_new_or_changed_rows(engine, "publishing_sites", site_rows, match_columns=["publishing_site_url"])
        _upsert_table(
            engine,
            "publishing_sites",
            site_rows_to_write,
            match_columns=["publishing_site_url"],
            dry_run=dry_run,
        )

        # 3) publishing_site_credentials (one row per site)
        site_ids_by_url = _load_site_ids_by_url(engine) if not dry_run else {row["publishing_site_url"]: None for row in master_rows}
        if dry_run:
            # For dry-run, still validate presence by using DB lookup if possible
            site_ids_by_url = _load_site_ids_by_url(engine)
        credential_rows, cred_issues = _prepare_credentials_rows(master_rows, site_ids_by_url)
        issues.extend(cred_issues)
        credential_rows_to_write = _filter_new_or_changed_rows(
            engine,
            "publishing_site_credentials",
            credential_rows,
            match_columns=["publishing_site_id"],
        )
        _upsert_table(
            engine,
            "publishing_site_credentials",
            credential_rows_to_write,
            match_columns=["publishing_site_id"],
            dry_run=dry_run,
        )

        report = {
            "timestamp_utc": stamp,
            "file_name": file_path.name,
            "dry_run": dry_run,
            "master_rows_input": len(raw_rows),
            "master_rows_prepared": len(master_rows),
            "master_rows_to_write": len(master_rows_to_write),
            "publishing_sites_rows": len(site_rows),
            "publishing_sites_rows_to_write": len(site_rows_to_write),
            "credentials_rows": len(credential_rows),
            "credentials_rows_to_write": len(credential_rows_to_write),
            "issues_count": len(issues),
            "issues_preview": [{"row_number": i.row_number, "reason": i.reason} for i in issues[:20]],
        }
        report_path = reports_dir / f"{file_path.stem}__{stamp}.report.json"
        _write_report(report_path, report)
        print(f"Processed file: {file_path.name}")
        print(f"Prepared rows: {len(master_rows)}")
        print(f"Rows to write -> master:{len(master_rows_to_write)} sites:{len(site_rows_to_write)} credentials:{len(credential_rows_to_write)}")
        print(f"Issues: {len(issues)}")
        print(f"Report: {report_path}")

        if issues:
            updater._print_issue_summary(issues)  # type: ignore[attr-defined]

        if dry_run:
            print("Dry run complete. File left in place.")
            return 0

        print("Sync complete. Master file left in place.")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync one master site file from db_updater/master_site_info into master_site_info, publishing_sites, and publishing_site_credentials."
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview without writing DB changes.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    return run_master_sync(dry_run=bool(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
