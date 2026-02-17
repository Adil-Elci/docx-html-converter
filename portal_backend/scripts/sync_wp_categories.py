#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from sqlalchemy.orm import Session

SCRIPT_DIR = Path(__file__).resolve().parent
PORTAL_BACKEND_DIR = SCRIPT_DIR.parent
if str(PORTAL_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(PORTAL_BACKEND_DIR))

from api.db import get_sessionmaker  # noqa: E402
from api.portal_models import Site, SiteCategory, SiteCredential, SiteDefaultCategory  # noqa: E402


@dataclass
class FetchedCategory:
    wp_category_id: int
    name: str
    slug: str
    parent_wp_category_id: Optional[int]
    post_count: Optional[int]


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


def _fetch_wp_categories(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    timeout_seconds: int,
) -> List[FetchedCategory]:
    base_url = f"{_wp_api_base(site_url, wp_rest_base)}/categories"
    headers = {"Accept": "application/json"}
    params = {"per_page": 100, "_fields": "id,name,slug,parent,count", "page": 1}

    try:
        first_response = requests.get(
            base_url,
            params=params,
            auth=(wp_username, wp_app_password),
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"request failed for {site_url}: {exc}") from exc

    if first_response.status_code >= 400:
        body = first_response.text[:350].replace("\n", " ")
        raise RuntimeError(f"HTTP {first_response.status_code} from {site_url}: {body}")

    try:
        first_payload = first_response.json()
    except ValueError as exc:
        raise RuntimeError(f"non-JSON response from {site_url}") from exc

    if not isinstance(first_payload, list):
        raise RuntimeError(f"unexpected categories payload from {site_url}: {type(first_payload).__name__}")

    total_pages_raw = first_response.headers.get("X-WP-TotalPages", "1").strip()
    try:
        total_pages = max(1, int(total_pages_raw))
    except ValueError:
        total_pages = 1

    all_items = list(first_payload)
    for page in range(2, total_pages + 1):
        params["page"] = page
        try:
            response = requests.get(
                base_url,
                params=params,
                auth=(wp_username, wp_app_password),
                headers=headers,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"request failed for {site_url} page={page}: {exc}") from exc
        if response.status_code >= 400:
            body = response.text[:350].replace("\n", " ")
            raise RuntimeError(f"HTTP {response.status_code} from {site_url} page={page}: {body}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"non-JSON response from {site_url} page={page}") from exc
        if not isinstance(payload, list):
            raise RuntimeError(f"unexpected categories payload from {site_url} page={page}: {type(payload).__name__}")
        all_items.extend(payload)

    categories: List[FetchedCategory] = []
    for item in all_items:
        if not isinstance(item, dict):
            continue
        raw_id = item.get("id")
        raw_name = item.get("name")
        raw_slug = item.get("slug")
        if not isinstance(raw_id, int) or raw_id <= 0:
            continue
        if not isinstance(raw_name, str) or not raw_name.strip():
            continue
        slug = raw_slug.strip() if isinstance(raw_slug, str) else ""
        parent_raw = item.get("parent")
        count_raw = item.get("count")
        parent_id = parent_raw if isinstance(parent_raw, int) and parent_raw >= 0 else None
        post_count = count_raw if isinstance(count_raw, int) and count_raw >= 0 else None
        categories.append(
            FetchedCategory(
                wp_category_id=raw_id,
                name=raw_name.strip(),
                slug=slug,
                parent_wp_category_id=parent_id,
                post_count=post_count,
            )
        )
    return categories


def _select_latest_credentials_per_site(
    session: Session,
    *,
    site_url_filter: Optional[str],
    include_inactive_sites: bool,
) -> List[Tuple[Site, SiteCredential]]:
    query = (
        session.query(Site, SiteCredential)
        .join(SiteCredential, SiteCredential.site_id == Site.id)
        .filter(SiteCredential.enabled.is_(True))
    )
    if not include_inactive_sites:
        query = query.filter(Site.status == "active")

    rows = query.order_by(Site.site_url.asc(), SiteCredential.created_at.desc()).all()
    picked: Dict[str, Tuple[Site, SiteCredential]] = {}
    for site, credential in rows:
        key = str(site.id)
        if key not in picked:
            picked[key] = (site, credential)

    selected = list(picked.values())
    if not site_url_filter:
        return selected

    target_variants = _host_variants(site_url_filter)
    if not target_variants:
        return []
    return [row for row in selected if _host_variants(row[0].site_url) & target_variants]


def _sync_site_categories(
    session: Session,
    *,
    site: Site,
    categories: List[FetchedCategory],
    dry_run: bool,
) -> Tuple[int, int, int]:
    existing_rows = session.query(SiteCategory).filter(SiteCategory.site_id == site.id).all()
    existing_by_id: Dict[int, SiteCategory] = {int(row.wp_category_id): row for row in existing_rows}
    fetched_ids = {item.wp_category_id for item in categories}

    inserted = 0
    updated = 0
    disabled = 0

    for item in categories:
        existing = existing_by_id.get(item.wp_category_id)
        if existing is None:
            inserted += 1
            if dry_run:
                continue
            session.add(
                SiteCategory(
                    site_id=site.id,
                    wp_category_id=item.wp_category_id,
                    name=item.name,
                    slug=item.slug,
                    parent_wp_category_id=item.parent_wp_category_id,
                    post_count=item.post_count,
                    enabled=True,
                )
            )
            continue

        changed = (
            existing.name != item.name
            or (existing.slug or "") != item.slug
            or existing.parent_wp_category_id != item.parent_wp_category_id
            or existing.post_count != item.post_count
            or not bool(existing.enabled)
        )
        if changed:
            updated += 1
            if dry_run:
                continue
            existing.name = item.name
            existing.slug = item.slug
            existing.parent_wp_category_id = item.parent_wp_category_id
            existing.post_count = item.post_count
            existing.enabled = True
            session.add(existing)

    for existing in existing_rows:
        category_id = int(existing.wp_category_id)
        if category_id in fetched_ids:
            continue
        if not bool(existing.enabled):
            continue
        disabled += 1
        if dry_run:
            continue
        existing.enabled = False
        session.add(existing)

    return inserted, updated, disabled


def _sync_site_default_categories_from_slugs(
    session: Session,
    *,
    site: Site,
    fetched_categories: List[FetchedCategory],
    default_slugs: List[str],
    replace_existing: bool,
    dry_run: bool,
) -> Tuple[int, int, int]:
    by_slug: Dict[str, FetchedCategory] = {}
    for category in fetched_categories:
        key = category.slug.strip().lower()
        if key and key not in by_slug:
            by_slug[key] = category

    ordered_matches: List[FetchedCategory] = []
    seen_ids: set[int] = set()
    for slug in default_slugs:
        match = by_slug.get(slug)
        if not match:
            continue
        if match.wp_category_id in seen_ids:
            continue
        seen_ids.add(match.wp_category_id)
        ordered_matches.append(match)

    existing_defaults = session.query(SiteDefaultCategory).filter(SiteDefaultCategory.site_id == site.id).all()
    existing_by_id: Dict[int, SiteDefaultCategory] = {int(row.wp_category_id): row for row in existing_defaults}

    inserted = 0
    updated = 0
    disabled = 0

    for idx, category in enumerate(ordered_matches):
        position = (idx + 1) * 10
        existing = existing_by_id.get(category.wp_category_id)
        if existing is None:
            inserted += 1
            if dry_run:
                continue
            session.add(
                SiteDefaultCategory(
                    site_id=site.id,
                    wp_category_id=category.wp_category_id,
                    category_name=category.name,
                    position=position,
                    enabled=True,
                )
            )
            continue

        changed = (
            (existing.category_name or "") != category.name
            or int(existing.position) != position
            or not bool(existing.enabled)
        )
        if changed:
            updated += 1
            if dry_run:
                continue
            existing.category_name = category.name
            existing.position = position
            existing.enabled = True
            session.add(existing)

    if replace_existing:
        target_ids = {item.wp_category_id for item in ordered_matches}
        for existing in existing_defaults:
            if int(existing.wp_category_id) in target_ids:
                continue
            if not bool(existing.enabled):
                continue
            disabled += 1
            if dry_run:
                continue
            existing.enabled = False
            session.add(existing)

    return inserted, updated, disabled


def _parse_default_slugs(value: str) -> List[str]:
    if not value.strip():
        return []
    seen: set[str] = set()
    slugs: List[str] = []
    for part in value.split(","):
        slug = part.strip().lower()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        slugs.append(slug)
    return slugs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync WordPress categories into site_categories, optionally seeding site_default_categories."
    )
    parser.add_argument("--site-url", default="", help="Optional site filter by URL/host (matches with/without www).")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="HTTP timeout per site request.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print changes without writing to DB.")
    parser.add_argument(
        "--include-inactive-sites",
        action="store_true",
        help="Include sites with status=inactive (default: active only).",
    )
    parser.add_argument(
        "--default-slugs",
        default="",
        help="Optional comma-separated slug list used to seed site_default_categories in this order.",
    )
    parser.add_argument(
        "--replace-defaults",
        action="store_true",
        help="When used with --default-slugs, disable existing defaults not in the matched slug list.",
    )
    args = parser.parse_args()

    if args.timeout_seconds <= 0:
        raise RuntimeError("--timeout-seconds must be a positive integer.")

    default_slugs = _parse_default_slugs(args.default_slugs)
    sync_defaults = len(default_slugs) > 0
    if args.replace_defaults and not sync_defaults:
        raise RuntimeError("--replace-defaults requires --default-slugs.")

    session = get_sessionmaker()()
    try:
        targets = _select_latest_credentials_per_site(
            session,
            site_url_filter=args.site_url.strip() or None,
            include_inactive_sites=args.include_inactive_sites,
        )
        if not targets:
            print("no matching enabled site credentials found")
            return 0

        print(f"found {len(targets)} site(s) with enabled credentials")

        attempted = 0
        failures = 0
        sites_changed = 0
        total_inserted = 0
        total_updated = 0
        total_disabled = 0
        total_default_inserted = 0
        total_default_updated = 0
        total_default_disabled = 0

        for site, credential in targets:
            attempted += 1
            try:
                fetched_categories = _fetch_wp_categories(
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
                    f"site={site.site_url} credential_id={credential.id} user={credential.wp_username} error={exc}"
                )
                continue

            inserted, updated, disabled = _sync_site_categories(
                session,
                site=site,
                categories=fetched_categories,
                dry_run=args.dry_run,
            )
            default_inserted = default_updated = default_disabled = 0
            if sync_defaults:
                default_inserted, default_updated, default_disabled = _sync_site_default_categories_from_slugs(
                    session,
                    site=site,
                    fetched_categories=fetched_categories,
                    default_slugs=default_slugs,
                    replace_existing=args.replace_defaults,
                    dry_run=args.dry_run,
                )

            changed = (inserted + updated + disabled + default_inserted + default_updated + default_disabled) > 0
            if changed:
                sites_changed += 1
            total_inserted += inserted
            total_updated += updated
            total_disabled += disabled
            total_default_inserted += default_inserted
            total_default_updated += default_updated
            total_default_disabled += default_disabled

            print(
                "site "
                f"{site.site_url} categories_fetched={len(fetched_categories)} "
                f"cat(inserted={inserted},updated={updated},disabled={disabled}) "
                f"default(inserted={default_inserted},updated={default_updated},disabled={default_disabled})"
            )

        if not args.dry_run:
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                print(f"commit failed: {exc}")
                return 1

        print(
            "summary "
            f"attempted={attempted} failures={failures} sites_changed={sites_changed} "
            f"cat_inserted={total_inserted} cat_updated={total_updated} cat_disabled={total_disabled} "
            f"default_inserted={total_default_inserted} default_updated={total_default_updated} "
            f"default_disabled={total_default_disabled} dry_run={args.dry_run}"
        )
        return 1 if failures > 0 else 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
