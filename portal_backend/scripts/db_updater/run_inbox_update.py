#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.engine import Engine

import import_tabular_to_db as updater


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}


def _script_dir() -> Path:
    return Path(__file__).resolve().parent


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _clean_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _build_engine_from_env() -> Engine:
    return updater._build_engine({"database_url_env": "DATABASE_URL"})  # re-use existing logic


def _list_inbox_files(inbox_dir: Path) -> List[Path]:
    if not inbox_dir.exists():
        return []
    return sorted([p for p in inbox_dir.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS])


def _read_headers(file_path: Path) -> List[str]:
    input_type = "xlsx" if file_path.suffix.lower() == ".xlsx" else "csv"
    rows = updater.read_rows_from_input({"type": input_type, "path": str(file_path)})
    if not rows:
        # still need header; read direct
        if input_type == "csv":
            import csv

            with file_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                return [h.strip() for h in (next(reader, []) or []) if str(h).strip()]
        return []
    headers: List[str] = []
    for key in rows[0].keys():
        header = _clean_text(key)
        if header:
            headers.append(header)
    return headers


def _reflect_tables(engine: Engine) -> Dict[str, Table]:
    metadata = MetaData()
    table_names = inspect(engine).get_table_names()
    return {name: Table(name, metadata, autoload_with=engine) for name in table_names}


def _unique_sets(engine: Engine, table_name: str) -> List[List[str]]:
    insp = inspect(engine)
    out: List[List[str]] = []
    pk = insp.get_pk_constraint(table_name) or {}
    pk_cols = [c for c in (pk.get("constrained_columns") or []) if c]
    if pk_cols:
        out.append(pk_cols)
    for uq in insp.get_unique_constraints(table_name) or []:
        cols = [c for c in (uq.get("column_names") or []) if c]
        if cols and cols not in out:
            out.append(cols)
    return out


def _direct_config_for_table(
    *,
    file_path: Path,
    table_name: str,
    headers: Sequence[str],
    engine: Engine,
) -> Optional[Dict[str, Any]]:
    tables = _reflect_tables(engine)
    table = tables.get(table_name)
    if table is None:
        return None
    header_set = set(headers)
    table_cols = set(table.c.keys())
    if not header_set:
        raise updater.ImportConfigError("Input file has no headers.")
    if not header_set.issubset(table_cols):
        return None

    match_columns: Optional[List[str]] = None
    for cols in _unique_sets(engine, table_name):
        if set(cols).issubset(header_set):
            match_columns = cols
            break
    if not match_columns:
        return None

    return {
        "database_url_env": "DATABASE_URL",
        "input": {
            "type": "xlsx" if file_path.suffix.lower() == ".xlsx" else "csv",
            "path": str(file_path),
            "encoding": "utf-8",
        },
        "table": table_name,
        "match_columns": match_columns,
        "allow_issues": True,
        "column_map": {header: {"source": header} for header in headers},
    }


def _publishing_site_credentials_adapter(file_path: Path, headers: Sequence[str]) -> Optional[Dict[str, Any]]:
    header_set = set(headers)
    required = {"publishing_site_url", "wp_username", "wp_app_password"}
    allowed = {
        "publishing_site_url",
        "auth_type",
        "wp_username",
        "wp_app_password",
        "enabled",
        "author_name",
        "author_id",
    }
    if not required.issubset(header_set):
        return None
    if not header_set.issubset(allowed):
        return None

    column_map: Dict[str, Any] = {
        "publishing_site_id": {
            "lookup": {
                "table": "publishing_sites",
                "match_column": "publishing_site_url",
                "return_column": "id",
                "source_column": "publishing_site_url",
                "lookup_normalize": "url_host",
            },
            "required": True,
        },
        "wp_username": {"source": "wp_username", "transform": "trim", "required": True},
        "wp_app_password": {"source": "wp_app_password", "transform": "trim", "required": True},
    }
    if "auth_type" in header_set:
        column_map["auth_type"] = {"source": "auth_type", "transform": "trim", "default": "application_password"}
    else:
        column_map["auth_type"] = {"value": "application_password"}
    if "enabled" in header_set:
        column_map["enabled"] = {"source": "enabled", "transform": "bool", "default": True}
    if "author_name" in header_set:
        column_map["author_name"] = {"source": "author_name", "transform": "trim", "default": None}
    if "author_id" in header_set:
        column_map["author_id"] = {"source": "author_id", "transform": "int", "default": None}

    return {
        "database_url_env": "DATABASE_URL",
        "input": {
            "type": "xlsx" if file_path.suffix.lower() == ".xlsx" else "csv",
            "path": str(file_path),
            "encoding": "utf-8",
        },
        "table": "publishing_site_credentials",
        "match_columns": ["publishing_site_id", "wp_username"],
        "allow_issues": True,
        "column_map": column_map,
    }


def _infer_target_from_filename(file_path: Path) -> Optional[str]:
    stem = file_path.stem.strip().lower()
    candidates = [stem]
    for suffix in ("_patch", "-patch", "_update", "-update", "_updates", "-updates", "_batch", "-batch"):
        if stem.endswith(suffix):
            candidates.append(stem[: -len(suffix)])
    for cand in candidates:
        if cand:
            return cand
    return None


def _auto_config_for_file(file_path: Path, *, engine: Engine, headers: Sequence[str]) -> Tuple[Dict[str, Any], str]:
    # Special adapter first (header helper columns not present in target table).
    creds_cfg = _publishing_site_credentials_adapter(file_path, headers)
    if creds_cfg is not None:
        return creds_cfg, "adapter:publishing_site_credentials"

    # Filename-based direct match (least ambiguous).
    hinted_table = _infer_target_from_filename(file_path)
    if hinted_table:
        cfg = _direct_config_for_table(file_path=file_path, table_name=hinted_table, headers=headers, engine=engine)
        if cfg is not None:
            return cfg, f"direct:{hinted_table}"

    # Header-based direct match scan.
    tables = _reflect_tables(engine)
    matching_tables: List[str] = []
    header_set = set(headers)
    for table_name, table in tables.items():
        table_cols = set(table.c.keys())
        if header_set and header_set.issubset(table_cols):
            if any(set(cols).issubset(header_set) for cols in _unique_sets(engine, table_name)):
                matching_tables.append(table_name)

    if not matching_tables:
        raise updater.ImportConfigError(
            "Could not auto-detect target table. Name the file after the table (e.g. publishing_sites.csv) "
            "or use a config-driven import."
        )
    if len(matching_tables) > 1:
        raise updater.ImportConfigError(
            "Ambiguous target table match from headers: " + ", ".join(sorted(matching_tables)) + ". "
            "Rename the file to the table name or use a config."
        )
    table_name = matching_tables[0]
    cfg = _direct_config_for_table(file_path=file_path, table_name=table_name, headers=headers, engine=engine)
    if cfg is None:
        raise updater.ImportConfigError(f"Failed to build direct config for table '{table_name}'.")
    return cfg, f"direct:{table_name}"


def _write_run_report(report_path: Path, payload: Dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _move_file(src: Path, dest_dir: Path, *, suffix: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"{src.stem}__{suffix}{src.suffix.lower()}"
    if target.exists():
        target = dest_dir / f"{src.stem}__{suffix}__{_utc_stamp()}{src.suffix.lower()}"
    shutil.move(str(src), str(target))
    return target


def run_auto_inbox(*, dry_run: bool = False) -> int:
    base_dir = _script_dir()
    inbox_dir = base_dir / "inbox"
    processed_dir = base_dir / "processed"
    failed_dir = base_dir / "failed"
    reports_dir = base_dir / "reports"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    files = _list_inbox_files(inbox_dir)
    if not files:
        print(f"No input files found in: {inbox_dir}")
        return 0
    if len(files) > 1:
        print("Error: more than one file found in inbox. Keep only one file at a time.", file=sys.stderr)
        for path in files:
            print(f"  - {path.name}", file=sys.stderr)
        return 1

    file_path = files[0]
    stamp = _utc_stamp()
    print(f"Inbox file: {file_path.name}")
    try:
        engine = _build_engine_from_env()
        headers = _read_headers(file_path)
        if not headers:
            raise updater.ImportConfigError("Input file has no usable headers.")
        config, mode = _auto_config_for_file(file_path, engine=engine, headers=headers)
        issues_jsonl = reports_dir / f"{file_path.stem}__{stamp}.issues.jsonl"
        config["issues_output_jsonl"] = str(issues_jsonl)

        print(f"Detected mode: {mode}")
        print(f"Target table: {config.get('table')}")
        print(f"Match columns: {config.get('match_columns')}")
        print(f"Headers: {', '.join(headers)}")

        exit_code = updater.run_import(config, dry_run=dry_run)
        report_payload = {
            "timestamp_utc": stamp,
            "file_name": file_path.name,
            "file_path": str(file_path),
            "dry_run": dry_run,
            "mode": mode,
            "table": config.get("table"),
            "match_columns": config.get("match_columns"),
            "headers": headers,
            "exit_code": exit_code,
            "issues_output_jsonl": str(issues_jsonl),
        }
        report_path = reports_dir / f"{file_path.stem}__{stamp}.report.json"
        _write_run_report(report_path, report_payload)
        print(f"Report written to: {report_path}")

        if dry_run:
            print("Dry run complete. File left in inbox.")
            return exit_code

        if exit_code == 0:
            moved_to = _move_file(file_path, processed_dir, suffix=stamp)
            print(f"Moved to processed: {moved_to}")
            return 0

        moved_to = _move_file(file_path, failed_dir, suffix=stamp)
        print(f"Moved to failed: {moved_to}")
        return exit_code
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if not dry_run and file_path.exists():
            moved_to = _move_file(file_path, failed_dir, suffix=f"failed__{stamp}")
            print(f"Moved to failed: {moved_to}", file=sys.stderr)
        return 1


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-run DB updater against one file in db_updater/inbox using DB metadata + built-in adapters."
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview without writing to DB or moving the file.")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    return run_auto_inbox(dry_run=bool(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
