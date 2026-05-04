#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List
from urllib.parse import urlparse

import requests
from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Engine

try:
    from . import import_tabular_to_db as updater
except ImportError:  # pragma: no cover
    import import_tabular_to_db as updater

# Add parent directories to path for helper imports
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent))
if str(_SCRIPT_DIR.parent.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent.parent))

try:
    from ssh_tunnel_helper import setup_ssh_tunnel
except ImportError:
    def setup_ssh_tunnel():  # noqa: F811
        pass

try:
    from api.credential_crypto import decrypt_credential, encrypt_credential
    _CREDENTIAL_ENCRYPTION_AVAILABLE = True
except ImportError:
    _CREDENTIAL_ENCRYPTION_AVAILABLE = False


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
DEFAULT_WP_REST_BASE = "/wp-json/wp/v2"
ProgressCallback = Callable[[int, str, str | None], None]


def _script_dir() -> Path:
    return Path(__file__).resolve().parent


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _clean_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _first_present_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


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


def _default_wp_admin_login_url(site_url: Any) -> str | None:
    normalized = _normalize_site_url(site_url)
    if not normalized:
        return None
    return f"{normalized}/wp-admin"


def _row_has_meaningful_site_data(row: dict[str, Any]) -> bool:
    meaningful_keys = (
        "publishing_site_url",
        "site_url",
        "name",
        "hosted_by",
        "host_panel",
        "status",
        "auth_type",
        "wp_username",
        "wp_app_password",
        "wp_admin_login_url",
        "admin_login_url",
        "wp_admin_username",
        "admin_username",
        "wp_admin_password",
        "admin_password",
        "notes",
    )
    return any(_clean_text(_first_present_value(row, key)) for key in meaningful_keys)


def _reflect_table(engine: Engine, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def _build_engine_from_env() -> Engine:
    return updater._build_engine({"database_url_env": "DATABASE_URL"})


def _list_master_files(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    return sorted(
        [
            p
            for p in folder.iterdir()
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS and not p.name.startswith("~$")
        ]
    )


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
            raw_site_url = _first_present_value(row, "publishing_site_url", "site_url")
            site_url = _normalize_site_url(raw_site_url)
            if not site_url and not _row_has_meaningful_site_data(row):
                continue
            if not site_url:
                raise ValueError("publishing_site_url or site_url is required.")
            wp_username = _clean_text(row.get("wp_username"))
            wp_app_password = _clean_text(row.get("wp_app_password"))
            wp_admin_login_url = _clean_text(_first_present_value(row, "wp_admin_login_url", "admin_login_url")) or (
                _default_wp_admin_login_url(site_url) or ""
            )
            wp_admin_username = _clean_text(_first_present_value(row, "wp_admin_username", "admin_username"))
            wp_admin_password = _clean_text(_first_present_value(row, "wp_admin_password", "admin_password"))
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
                    "wp_admin_login_url": (_clean_text(wp_admin_login_url) or None),
                    "wp_admin_username": (wp_admin_username or None),
                    "wp_admin_password": (wp_admin_password or None),
                    "wp_admin_enabled": _bool_or_default(row.get("wp_admin_enabled"), True),
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
    decrypt_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    table = _reflect_table(engine, table_name)
    # Build in-memory index of existing rows by match key. Tables are small in this workflow.
    existing_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    with engine.connect() as conn:
        for rec in conn.execute(select(table)).mappings():
            key = tuple(rec.get(col) for col in match_columns)
            existing = dict(rec)
            if decrypt_columns and _CREDENTIAL_ENCRYPTION_AVAILABLE:
                for col in decrypt_columns:
                    if existing.get(col):
                        existing[col] = decrypt_credential(existing[col])
            existing_map[key] = existing

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


def _encrypt_credential_fields(rows: list[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    """Return copies of rows with specified fields encrypted. No-ops if encryption is unavailable."""
    if not _CREDENTIAL_ENCRYPTION_AVAILABLE:
        return rows
    result = []
    for row in rows:
        encrypted_row = dict(row)
        for field in fields:
            if encrypted_row.get(field):
                encrypted_row[field] = encrypt_credential(encrypted_row[field])
        result.append(encrypted_row)
    return result


def _load_site_ids_by_url(engine: Engine) -> dict[str, Any]:
    table = _reflect_table(engine, "publishing_sites")
    out: dict[str, Any] = {}
    with engine.connect() as conn:
        for row in conn.execute(select(table.c.id, table.c.publishing_site_url)):
            out[_normalize_site_url(row.publishing_site_url)] = row.id
    return out


def _publishing_sites_name_column(table: Table) -> str:
    if "publishing_site_name" in table.c:
        return "publishing_site_name"
    return "name"


def _collect_missing_publishing_sites(engine: Engine, master_site_urls: set[str]) -> list[dict[str, Any]]:
    table = _reflect_table(engine, "publishing_sites")
    name_col = _publishing_sites_name_column(table)
    rows: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for rec in conn.execute(select(table.c.id, table.c.publishing_site_url, table.c[name_col])).mappings():
            normalized = _normalize_site_url(rec.get("publishing_site_url"))
            if not normalized or normalized in master_site_urls:
                continue
            rows.append(
                {
                    "publishing_site_id": rec.get("id"),
                    "publishing_site_url": rec.get("publishing_site_url"),
                    "publishing_site_name": rec.get(name_col),
                }
            )
    return rows


def _find_site_delete_blockers(engine: Engine, site_ids: list[Any]) -> dict[Any, list[str]]:
    if not site_ids:
        return {}
    blockers: dict[Any, list[str]] = {}
    checks = [
        ("submissions", "publishing_site_id"),
        ("jobs", "publishing_site_id"),
    ]
    with engine.connect() as conn:
        for table_name, col_name in checks:
            table = _reflect_table(engine, table_name)
            if col_name not in table.c:
                continue
            rows = conn.execute(select(table.c[col_name]).where(table.c[col_name].in_(site_ids)).distinct()).fetchall()
            for (site_id,) in rows:
                blockers.setdefault(site_id, []).append(table_name)
    return blockers


def _delete_publishing_sites(engine: Engine, site_ids: list[Any], *, dry_run: bool) -> int:
    if not site_ids:
        return 0
    if dry_run:
        return len(site_ids)
    table = _reflect_table(engine, "publishing_sites")
    with engine.begin() as conn:
        result = conn.execute(table.delete().where(table.c.id.in_(site_ids)))
        return int(result.rowcount or 0)


def _delete_site_references_for_force_prune(engine: Engine, site_ids: list[Any], *, dry_run: bool) -> dict[str, int]:
    counts = {"submissions_deleted": 0}
    if not site_ids:
        return counts
    submissions = _reflect_table(engine, "submissions")
    if dry_run:
        with engine.connect() as conn:
            rows = conn.execute(select(submissions.c.id).where(submissions.c.publishing_site_id.in_(site_ids))).fetchall()
            counts["submissions_deleted"] = len(rows)
        return counts
    with engine.begin() as conn:
        result = conn.execute(submissions.delete().where(submissions.c.publishing_site_id.in_(site_ids)))
        counts["submissions_deleted"] = int(result.rowcount or 0)
    return counts


def _prepare_publishing_sites_rows(master_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "publishing_site_url": row["publishing_site_url"],
            "publishing_site_name": row["name"],
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


def _prepare_admin_credentials_rows(master_rows: list[dict[str, Any]], site_ids_by_url: dict[str, Any]) -> tuple[list[dict[str, Any]], list[updater.RowIssue]]:
    rows: list[dict[str, Any]] = []
    issues: list[updater.RowIssue] = []
    for idx, row in enumerate(master_rows, start=2):
        site_url = row["publishing_site_url"]
        site_id = site_ids_by_url.get(site_url)
        if site_id is None:
            issues.append(updater.RowIssue(row_number=idx, reason="publishing_site_id lookup failed for wp admin credentials", row=row))
            continue
        wp_admin_username = _clean_text(row.get("wp_admin_username"))
        wp_admin_password = _clean_text(row.get("wp_admin_password"))
        if not wp_admin_username and not wp_admin_password:
            continue
        if not wp_admin_username or not wp_admin_password:
            issues.append(
                updater.RowIssue(
                    row_number=idx,
                    reason="Both wp_admin_username and wp_admin_password are required when wp admin credentials are present",
                    row=row,
                )
            )
            continue
        rows.append(
            {
                "publishing_site_id": site_id,
                "wp_admin_login_url": (_clean_text(row.get("wp_admin_login_url")) or _default_wp_admin_login_url(site_url)),
                "wp_admin_username": wp_admin_username,
                "wp_admin_password": wp_admin_password,
                "enabled": bool(row.get("wp_admin_enabled", True)),
            }
        )
    return rows, issues


def _fetch_wp_author(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    timeout_seconds: int,
) -> tuple[int, str]:
    base = (site_url or "").strip().rstrip("/")
    rest = (wp_rest_base or "/wp-json/wp/v2").strip()
    if not rest.startswith("/"):
        rest = f"/{rest}"
    url = f"{base}{rest}/users/me?_fields=id,name"
    try:
        response = requests.get(
            url,
            auth=(wp_username, wp_app_password),
            headers={"Accept": "application/json"},
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"request failed for {url}: {exc}") from exc
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} from {url}: {response.text[:350].replace(chr(10), ' ')}")
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError(f"non-JSON response from {url}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected payload type from {url}: {type(payload).__name__}")
    author_id = payload.get("id")
    author_name = payload.get("name")
    if not isinstance(author_id, int) or author_id <= 0:
        raise RuntimeError(f"invalid author id in users/me response: {author_id}")
    if not isinstance(author_name, str) or not author_name.strip():
        raise RuntimeError(f"invalid author name in users/me response: {author_name}")
    return author_id, author_name.strip()


def _sync_author_info_for_credentials(
    engine: Engine,
    credential_rows: list[dict[str, Any]],
    master_rows: list[dict[str, Any]],
    site_ids_by_url: dict[str, Any],
    *,
    timeout_seconds: int = 20,
) -> dict[str, int]:
    if not credential_rows:
        return {"attempted": 0, "updated": 0, "failed": 0}

    table = _reflect_table(engine, "publishing_site_credentials")
    if "author_id" not in table.c or "author_name" not in table.c:
        return {"attempted": 0, "updated": 0, "failed": 0}

    id_to_url = {v: k for k, v in site_ids_by_url.items()}
    master_by_url = {row["publishing_site_url"]: row for row in master_rows}

    attempted = updated = failed = 0
    for cred in credential_rows:
        site_id = cred.get("publishing_site_id")
        wp_username = (cred.get("wp_username") or "").strip()
        wp_app_password = (cred.get("wp_app_password") or "").strip()
        if not wp_username or not wp_app_password:
            continue

        site_url = id_to_url.get(site_id, "")
        master_row = master_by_url.get(site_url, {})
        wp_rest_base = master_row.get("wp_rest_base") or DEFAULT_WP_REST_BASE

        attempted += 1
        try:
            author_id, author_name = _fetch_wp_author(
                site_url=site_url,
                wp_rest_base=wp_rest_base,
                wp_username=wp_username,
                wp_app_password=wp_app_password,
                timeout_seconds=timeout_seconds,
            )
        except RuntimeError as exc:
            failed += 1
            print(f"  author sync failed site={site_url}: {exc}", file=sys.stderr)
            continue

        with engine.begin() as conn:
            conn.execute(
                table.update()
                .where(table.c.publishing_site_id == site_id)
                .values(author_id=author_id, author_name=author_name)
            )
        updated += 1
        print(f"  author synced site={site_url} author_id={author_id} author_name={author_name}")

    return {"attempted": attempted, "updated": updated, "failed": failed}


def _write_report(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _emit_progress(callback: ProgressCallback | None, percent: int, stage: str, message: str | None = None) -> None:
    if callback is None:
        return
    callback(max(0, min(100, int(percent))), stage, message)


def run_master_sync_for_file(
    file_path: str | Path,
    *,
    dry_run: bool = False,
    delete_missing_sites: bool = False,
    force_delete_missing_sites: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    base = _script_dir()
    reports_dir = base / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    file_path = Path(file_path).resolve()
    stamp = _utc_stamp()

    _emit_progress(progress_callback, 5, "starting", "Initializing sync.")
    engine = _build_engine_from_env()

    _emit_progress(progress_callback, 15, "reading", f"Reading {file_path.name}.")
    raw_rows = _read_rows(file_path)

    _emit_progress(progress_callback, 28, "preparing_master", "Preparing master rows.")
    master_rows, issues = _prepare_master_rows(raw_rows)

    _emit_progress(progress_callback, 40, "sync_master_site_info", "Syncing master_site_info.")
    master_rows_to_write = _filter_new_or_changed_rows(engine, "master_site_info", master_rows, match_columns=["publishing_site_url"])
    _upsert_table(
        engine,
        "master_site_info",
        master_rows_to_write,
        match_columns=["publishing_site_url"],
        dry_run=dry_run,
    )

    _emit_progress(progress_callback, 58, "sync_publishing_sites", "Syncing publishing_sites.")
    site_rows = _prepare_publishing_sites_rows(master_rows)
    site_rows_to_write = _filter_new_or_changed_rows(engine, "publishing_sites", site_rows, match_columns=["publishing_site_url"])
    _upsert_table(
        engine,
        "publishing_sites",
        site_rows_to_write,
        match_columns=["publishing_site_url"],
        dry_run=dry_run,
    )

    missing_sites_total = 0
    missing_sites_delete_candidates = 0
    missing_sites_deleted = 0
    missing_sites_blocked = 0
    missing_sites_force_deleted = 0
    force_prune_reference_counts: dict[str, int] = {"submissions_deleted": 0}
    missing_sites_preview: list[str] = []
    if delete_missing_sites:
        _emit_progress(progress_callback, 68, "prune_missing_sites", "Checking sites missing from master file.")
        master_site_urls = {row["publishing_site_url"] for row in master_rows if row.get("publishing_site_url")}
        missing_sites = _collect_missing_publishing_sites(engine, master_site_urls)
        missing_sites_total = len(missing_sites)
        missing_sites_preview = [str(item.get("publishing_site_url") or "") for item in missing_sites[:10]]
        blockers = _find_site_delete_blockers(engine, [item["publishing_site_id"] for item in missing_sites if item.get("publishing_site_id")])
        deletable_ids: list[Any] = []
        force_deletable_ids: list[Any] = []
        for item in missing_sites:
            site_id = item.get("publishing_site_id")
            if site_id in blockers:
                if force_delete_missing_sites:
                    force_deletable_ids.append(site_id)
                else:
                    missing_sites_blocked += 1
                    issues.append(
                        updater.RowIssue(
                            row_number=0,
                            reason=f"Cannot delete missing publishing site; referenced by {', '.join(sorted(set(blockers[site_id])))}",
                            row=item,
                        )
                    )
                continue
            if site_id is not None:
                deletable_ids.append(site_id)
        missing_sites_delete_candidates = len(deletable_ids) + len(force_deletable_ids)
        if force_deletable_ids:
            _emit_progress(progress_callback, 71, "force_prune_missing_sites", "Force-deleting references for missing sites.")
            force_prune_reference_counts = _delete_site_references_for_force_prune(engine, force_deletable_ids, dry_run=dry_run)
        _emit_progress(progress_callback, 72, "prune_missing_sites", "Deleting sites missing from master file.")
        missing_sites_deleted = _delete_publishing_sites(engine, deletable_ids + force_deletable_ids, dry_run=dry_run)
        missing_sites_force_deleted = len(force_deletable_ids) if dry_run else max(0, missing_sites_deleted - len(deletable_ids))

    _emit_progress(progress_callback, 76, "loading_site_ids", "Loading publishing site IDs.")
    site_ids_by_url = _load_site_ids_by_url(engine)

    _emit_progress(progress_callback, 84, "preparing_credentials", "Preparing credentials rows.")
    credential_rows, cred_issues = _prepare_credentials_rows(master_rows, site_ids_by_url)
    issues.extend(cred_issues)

    _emit_progress(progress_callback, 90, "sync_credentials", "Syncing publishing_site_credentials.")
    credential_rows_to_write = _filter_new_or_changed_rows(
        engine,
        "publishing_site_credentials",
        credential_rows,
        match_columns=["publishing_site_id"],
        decrypt_columns=["wp_app_password"],
    )
    # Encrypt passwords for storage; keep plaintext rows for author sync below.
    credential_rows_to_store = _encrypt_credential_fields(credential_rows_to_write, ["wp_app_password"])
    _upsert_table(
        engine,
        "publishing_site_credentials",
        credential_rows_to_store,
        match_columns=["publishing_site_id"],
        dry_run=dry_run,
    )

    _emit_progress(progress_callback, 92, "sync_author_info", "Fetching WP author info for new credentials.")
    author_sync_counts: dict[str, int] = {"attempted": 0, "updated": 0, "failed": 0}
    if not dry_run and credential_rows_to_write:
        author_sync_counts = _sync_author_info_for_credentials(
            engine,
            credential_rows_to_write,  # plaintext — used to call WP API
            master_rows,
            site_ids_by_url,
        )

    _emit_progress(progress_callback, 95, "sync_admin_credentials", "Syncing publishing_site_admin_credentials.")
    admin_credential_rows, admin_cred_issues = _prepare_admin_credentials_rows(master_rows, site_ids_by_url)
    issues.extend(admin_cred_issues)
    admin_credential_rows_to_write = _filter_new_or_changed_rows(
        engine,
        "publishing_site_admin_credentials",
        admin_credential_rows,
        match_columns=["publishing_site_id"],
        decrypt_columns=["wp_admin_username", "wp_admin_password"],
    )
    _upsert_table(
        engine,
        "publishing_site_admin_credentials",
        _encrypt_credential_fields(admin_credential_rows_to_write, ["wp_admin_username", "wp_admin_password"]),
        match_columns=["publishing_site_id"],
        dry_run=dry_run,
    )

    report = {
        "timestamp_utc": stamp,
        "file_name": file_path.name,
        "dry_run": dry_run,
        "delete_missing_sites": delete_missing_sites,
        "force_delete_missing_sites": force_delete_missing_sites,
        "master_rows_input": len(raw_rows),
        "master_rows_prepared": len(master_rows),
        "master_rows_to_write": len(master_rows_to_write),
        "publishing_sites_rows": len(site_rows),
        "publishing_sites_rows_to_write": len(site_rows_to_write),
        "credentials_rows": len(credential_rows),
        "credentials_rows_to_write": len(credential_rows_to_write),
        "admin_credentials_rows": len(admin_credential_rows),
        "admin_credentials_rows_to_write": len(admin_credential_rows_to_write),
        "missing_sites_in_db_not_in_master": missing_sites_total,
        "missing_sites_delete_candidates": missing_sites_delete_candidates,
        "missing_sites_deleted": missing_sites_deleted,
        "missing_sites_blocked": missing_sites_blocked,
        "missing_sites_force_deleted": missing_sites_force_deleted,
        "force_prune_reference_counts": force_prune_reference_counts,
        "missing_sites_preview": missing_sites_preview,
        "author_sync_attempted": author_sync_counts["attempted"],
        "author_sync_updated": author_sync_counts["updated"],
        "author_sync_failed": author_sync_counts["failed"],
        "issues_count": len(issues),
        "issues_preview": [{"row_number": i.row_number, "reason": i.reason} for i in issues[:20]],
    }
    report_path = reports_dir / f"{file_path.stem}__{stamp}.report.json"
    _emit_progress(progress_callback, 98, "writing_report", "Writing sync report.")
    _write_report(report_path, report)
    report["report_path"] = str(report_path)

    _emit_progress(progress_callback, 100, "completed", "Sync complete.")
    return report


def run_master_sync(*, dry_run: bool = False, delete_missing_sites: bool = False, force_delete_missing_sites: bool = False) -> int:
    base = _script_dir()
    inbox_dir = base / "master_site_info"
    inbox_dir.mkdir(parents=True, exist_ok=True)

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
    try:
        report = run_master_sync_for_file(
            file_path,
            dry_run=dry_run,
            delete_missing_sites=delete_missing_sites,
            force_delete_missing_sites=force_delete_missing_sites,
        )
        print(f"Processed file: {file_path.name}")
        print(f"Prepared rows: {report['master_rows_prepared']}")
        print(
            "Rows to write -> "
            f"master:{report['master_rows_to_write']} "
            f"sites:{report['publishing_sites_rows_to_write']} "
            f"credentials:{report['credentials_rows_to_write']} "
            f"admin_credentials:{report.get('admin_credentials_rows_to_write', 0)}"
        )
        if report.get("author_sync_attempted", 0) > 0:
            print(
                f"Author sync: attempted={report['author_sync_attempted']} "
                f"updated={report['author_sync_updated']} "
                f"failed={report['author_sync_failed']}"
            )
        print(f"Issues: {report['issues_count']}")
        print(f"Report: {report['report_path']}")

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
    parser.add_argument(
        "--delete-missing-sites",
        action="store_true",
        help="Delete publishing_sites rows not present in the master file (skips rows still referenced by submissions/jobs).",
    )
    parser.add_argument(
        "--force-delete-missing-sites",
        action="store_true",
        help="Force delete missing sites by deleting related submissions/jobs history first (testing use only).",
    )
    return parser


def main() -> int:
    setup_ssh_tunnel()
    args = build_arg_parser().parse_args()
    return run_master_sync(
        dry_run=bool(args.dry_run),
        delete_missing_sites=bool(args.delete_missing_sites),
        force_delete_missing_sites=bool(args.force_delete_missing_sites),
    )


if __name__ == "__main__":
    raise SystemExit(main())
