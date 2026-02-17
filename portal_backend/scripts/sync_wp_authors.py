#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from uuid import UUID

import requests
from sqlalchemy.orm import Session

SCRIPT_DIR = Path(__file__).resolve().parent
PORTAL_BACKEND_DIR = SCRIPT_DIR.parent
if str(PORTAL_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(PORTAL_BACKEND_DIR))

from api.db import get_sessionmaker  # noqa: E402
from api.portal_models import Site, SiteCredential  # noqa: E402


def _normalized_host(value: str) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    with_scheme = raw if "://" in raw else f"https://{raw}"
    host = (urlparse(with_scheme).hostname or "").strip().lower().rstrip(".")
    return host or None


def _host_variants(value: str) -> set[str]:
    host = _normalized_host(value)
    if not host:
        return set()
    variants = {host}
    if host.startswith("www."):
        variants.add(host[4:])
    else:
        variants.add(f"www.{host}")
    return variants


def _wp_api_base(site_url: str, wp_rest_base: str) -> str:
    clean_site_url = (site_url or "").strip().rstrip("/")
    clean_rest_base = (wp_rest_base or "/wp-json/wp/v2").strip()
    if not clean_rest_base.startswith("/"):
        clean_rest_base = f"/{clean_rest_base}"
    return f"{clean_site_url}{clean_rest_base}"


def _users_me_url(site_url: str, wp_rest_base: str) -> str:
    return f"{_wp_api_base(site_url, wp_rest_base)}/users/me?_fields=id,name"


def _fetch_wp_author(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    timeout_seconds: int,
) -> Tuple[int, str]:
    url = _users_me_url(site_url, wp_rest_base)
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
        body = response.text[:350].replace("\n", " ")
        raise RuntimeError(f"HTTP {response.status_code} from {url}: {body}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"non-JSON response from {url}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected payload type from {url}: {type(payload).__name__}")

    author_id = payload.get("id")
    author_name = payload.get("name")
    if not isinstance(author_id, int) or author_id <= 0:
        raise RuntimeError(f"invalid author id in users/me response for {url}: {author_id}")
    if not isinstance(author_name, str) or not author_name.strip():
        raise RuntimeError(f"invalid author name in users/me response for {url}: {author_name}")
    return author_id, author_name.strip()


def _select_target_credentials(
    session: Session,
    *,
    site_url_filter: Optional[str],
    credential_id_filter: Optional[UUID],
    include_inactive_sites: bool,
) -> List[Tuple[SiteCredential, Site]]:
    query = (
        session.query(SiteCredential, Site)
        .join(Site, Site.id == SiteCredential.site_id)
        .filter(SiteCredential.enabled.is_(True))
    )
    if credential_id_filter is not None:
        query = query.filter(SiteCredential.id == credential_id_filter)
    if not include_inactive_sites:
        query = query.filter(Site.status == "active")

    rows: List[Tuple[SiteCredential, Site]] = (
        query.order_by(Site.site_url.asc(), SiteCredential.created_at.desc()).all()
    )
    if not site_url_filter:
        return rows

    target_variants = _host_variants(site_url_filter)
    if not target_variants:
        return []
    return [row for row in rows if _host_variants(row[1].site_url) & target_variants]


def _iter_rows(rows: Iterable[Tuple[SiteCredential, Site]]) -> Iterable[Tuple[SiteCredential, Site]]:
    for credential, site in rows:
        yield credential, site


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync WordPress users/me author_id and author_name into site_credentials."
    )
    parser.add_argument("--site-url", default="", help="Optional site filter by URL/host (matches with/without www).")
    parser.add_argument("--credential-id", default="", help="Optional single site_credentials.id filter.")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only fetch/update rows where author_id or author_name is currently missing.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout per site request.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print changes without writing to DB.")
    parser.add_argument(
        "--include-inactive-sites",
        action="store_true",
        help="Include credentials for inactive sites (default: active sites only).",
    )
    args = parser.parse_args()

    if args.timeout_seconds <= 0:
        raise RuntimeError("--timeout-seconds must be a positive integer.")

    credential_id_filter: Optional[UUID] = None
    if args.credential_id.strip():
        try:
            credential_id_filter = UUID(args.credential_id.strip())
        except ValueError as exc:
            raise RuntimeError("--credential-id must be a valid UUID.") from exc

    session = get_sessionmaker()()
    try:
        rows = _select_target_credentials(
            session,
            site_url_filter=args.site_url.strip() or None,
            credential_id_filter=credential_id_filter,
            include_inactive_sites=args.include_inactive_sites,
        )
        if not rows:
            print("no matching enabled site credentials found")
            return 0

        print(f"found {len(rows)} enabled site credential(s)")

        attempted = 0
        updated = 0
        unchanged = 0
        skipped_missing = 0
        failures = 0

        for credential, site in _iter_rows(rows):
            current_author_name = (credential.author_name or "").strip()
            current_author_id = int(credential.author_id) if credential.author_id is not None else None
            if args.only_missing and current_author_id and current_author_name:
                skipped_missing += 1
                continue

            attempted += 1
            try:
                author_id, author_name = _fetch_wp_author(
                    site_url=site.site_url,
                    wp_rest_base=site.wp_rest_base,
                    wp_username=credential.wp_username,
                    wp_app_password=credential.wp_app_password,
                    timeout_seconds=args.timeout_seconds,
                )
            except RuntimeError as exc:
                failures += 1
                print(
                    "failed "
                    f"credential_id={credential.id} site={site.site_url} user={credential.wp_username} error={exc}"
                )
                continue

            if current_author_id == author_id and current_author_name == author_name:
                unchanged += 1
                print(
                    "unchanged "
                    f"credential_id={credential.id} site={site.site_url} "
                    f"author_id={author_id} author_name={author_name}"
                )
                continue

            updated += 1
            print(
                "update "
                f"credential_id={credential.id} site={site.site_url} "
                f"author_id={current_author_id}->{author_id} "
                f"author_name={current_author_name or '<null>'}->{author_name}"
            )
            if not args.dry_run:
                credential.author_id = author_id
                credential.author_name = author_name
                session.add(credential)

        if not args.dry_run and updated > 0:
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                print(f"commit failed: {exc}")
                return 1

        print(
            "summary "
            f"attempted={attempted} updated={updated} unchanged={unchanged} "
            f"skipped_missing={skipped_missing} failures={failures} dry_run={args.dry_run}"
        )
        return 1 if failures > 0 else 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
