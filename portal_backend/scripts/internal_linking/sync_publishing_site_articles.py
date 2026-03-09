from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional

import requests

try:
    from portal_backend.api.db import get_sessionmaker
    from portal_backend.api.internal_linking import mark_missing_publishing_site_articles, upsert_publishing_site_article
    from portal_backend.api.portal_models import Site, SiteCredential
except ModuleNotFoundError:
    from api.db import get_sessionmaker
    from api.internal_linking import mark_missing_publishing_site_articles, upsert_publishing_site_article
    from api.portal_models import Site, SiteCredential

logger = logging.getLogger("portal_backend.scripts.internal_linking")


def _wp_api_base(site_url: str, wp_rest_base: str) -> str:
    clean_site_url = (site_url or "").strip().rstrip("/")
    clean_rest_base = (wp_rest_base or "").strip()
    if not clean_rest_base.startswith("/"):
        clean_rest_base = "/" + clean_rest_base
    return f"{clean_site_url}{clean_rest_base.rstrip('/')}"


def _wp_auth_header(username: str, app_password: str) -> str:
    import base64

    token = f"{username}:{app_password}".encode("utf-8")
    return "Basic " + base64.b64encode(token).decode("ascii")


def _fetch_posts_for_site(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    per_page: int,
    timeout_seconds: int,
) -> List[Dict[str, Any]]:
    url = f"{_wp_api_base(site_url, wp_rest_base)}/posts"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    page = 1
    out: List[Dict[str, Any]] = []
    while True:
        response = requests.get(
            url,
            headers=headers,
            params={
                "status": "publish",
                "per_page": max(1, min(100, per_page)),
                "page": page,
                "orderby": "date",
                "order": "desc",
                "context": "edit",
                "_fields": "id,link,slug,date,date_gmt,modified,modified_gmt,status,title,excerpt,categories",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            break
        out.extend(item for item in payload if isinstance(item, dict))
        total_pages_header = response.headers.get("X-WP-TotalPages", "1")
        try:
            total_pages = int(total_pages_header)
        except ValueError:
            total_pages = page
        if page >= total_pages:
            break
        page += 1
    return out


def _select_sites(session, site_url_filter: Optional[str]) -> List[tuple[Site, SiteCredential]]:
    query = (
        session.query(Site, SiteCredential)
        .join(SiteCredential, SiteCredential.site_id == Site.id)
        .filter(
            Site.status == "active",
            SiteCredential.enabled.is_(True),
        )
        .order_by(Site.site_url.asc(), SiteCredential.created_at.desc())
    )
    rows = query.all()
    latest_by_site = {}
    for site, credential in rows:
        if site.id in latest_by_site:
            continue
        username = (credential.wp_username or "").strip()
        app_password = (credential.wp_app_password or "").strip()
        if not username or not app_password:
            continue
        latest_by_site[site.id] = (site, credential)
    selected = list(latest_by_site.values())
    if not site_url_filter:
        return selected
    cleaned = site_url_filter.strip().lower()
    return [(site, credential) for site, credential in selected if cleaned in (site.site_url or "").strip().lower()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync published WordPress posts into the publishing-site article index.")
    parser.add_argument("--site-url", dest="site_url", default="", help="Only sync one publishing site URL.")
    parser.add_argument("--per-page", dest="per_page", type=int, default=100)
    parser.add_argument("--timeout-seconds", dest="timeout_seconds", type=int, default=20)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    SessionLocal = get_sessionmaker()
    synced_at = datetime.now(timezone.utc)

    with SessionLocal() as session:
        rows = _select_sites(session, args.site_url.strip() or None)
        if not rows:
            logger.info("No publishing sites matched the filter.")
            return 0

        for site, credential in rows:
            try:
                posts = _fetch_posts_for_site(
                    site_url=site.site_url,
                    wp_rest_base=site.wp_rest_base,
                    wp_username=credential.wp_username,
                    wp_app_password=credential.wp_app_password,
                    per_page=args.per_page,
                    timeout_seconds=args.timeout_seconds,
                )
            except Exception as exc:
                logger.error("sync_failed site=%s error=%s", site.site_url, exc)
                session.rollback()
                continue

            seen_post_ids: List[int] = []
            upserted = 0
            for payload in posts:
                article = upsert_publishing_site_article(
                    session,
                    site_id=site.id,
                    post_payload=payload,
                    source="wp_rest",
                    synced_at=synced_at,
                )
                if article is None:
                    continue
                seen_post_ids.append(int(article.wp_post_id))
                upserted += 1
            marked_unavailable = mark_missing_publishing_site_articles(
                session,
                site_id=site.id,
                seen_post_ids=seen_post_ids,
                synced_at=synced_at,
            )
            session.commit()
            logger.info(
                "sync_ok site=%s upserted=%s marked_unavailable=%s",
                site.site_url,
                upserted,
                marked_unavailable,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
