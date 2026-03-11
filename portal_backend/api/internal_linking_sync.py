from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import threading
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .internal_linking import mark_missing_publishing_site_articles, upsert_publishing_site_article
from .portal_models import Site, SiteCredential

logger = logging.getLogger("portal_backend.internal_linking_sync")


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


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


def _normalize_public_post_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(item)
    if not str(normalized.get("status") or "").strip():
        normalized["status"] = "publish"
    return normalized


def _fetch_posts_with_mode(
    *,
    site_url: str,
    wp_rest_base: str,
    per_page: int,
    timeout_seconds: int,
    mode: str,
    wp_username: str = "",
    wp_app_password: str = "",
) -> List[Dict[str, Any]]:
    if mode not in {"authenticated", "public"}:
        raise ValueError("Unsupported fetch mode.")
    url = f"{_wp_api_base(site_url, wp_rest_base)}/posts"
    headers = {"Content-Type": "application/json"}
    params = {
        "status": "publish",
        "per_page": max(1, min(100, per_page)),
        "orderby": "date",
        "order": "desc",
        "_fields": "id,link,slug,date,date_gmt,modified,modified_gmt,status,title,excerpt,content,categories",
    }
    if mode == "authenticated":
        headers["Authorization"] = _wp_auth_header(wp_username, wp_app_password)
        params["context"] = "edit"
    else:
        headers["User-Agent"] = "portal-backend/1.0"

    page = 1
    out: List[Dict[str, Any]] = []
    while True:
        response = requests.get(
            url,
            headers=headers,
            params={**params, "page": page},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            break
        if mode == "public":
            out.extend(_normalize_public_post_payload(item) for item in payload if isinstance(item, dict))
        else:
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


def _fetch_posts_for_site(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    per_page: int,
    timeout_seconds: int,
) -> List[Dict[str, Any]]:
    try:
        return _fetch_posts_with_mode(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            per_page=per_page,
            timeout_seconds=timeout_seconds,
            mode="authenticated",
            wp_username=wp_username,
            wp_app_password=wp_app_password,
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code not in {401, 403}:
            raise
        logger.warning("internal_linking.sync.auth_fallback site=%s status_code=%s", site_url, status_code)
        return _fetch_posts_with_mode(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            per_page=per_page,
            timeout_seconds=timeout_seconds,
            mode="public",
        )


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


def run_internal_link_inventory_sync(
    db_sessionmaker: sessionmaker,
    *,
    site_url_filter: Optional[str] = None,
    per_page: int = 100,
    timeout_seconds: int = 20,
) -> Dict[str, int]:
    synced_at = datetime.now(timezone.utc)
    summary = {"sites_processed": 0, "sites_failed": 0, "articles_upserted": 0, "articles_marked_unavailable": 0}
    with db_sessionmaker() as session:
        rows = _select_sites(session, site_url_filter)
        if not rows:
            logger.info("internal_linking.sync.no_sites")
            return summary

        for site, credential in rows:
            try:
                posts = _fetch_posts_for_site(
                    site_url=site.site_url,
                    wp_rest_base=site.wp_rest_base,
                    wp_username=credential.wp_username,
                    wp_app_password=credential.wp_app_password,
                    per_page=per_page,
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                logger.error("internal_linking.sync.failed site=%s error=%s", site.site_url, exc)
                session.rollback()
                summary["sites_failed"] += 1
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
                "internal_linking.sync.ok site=%s upserted=%s marked_unavailable=%s",
                site.site_url,
                upserted,
                marked_unavailable,
            )
            summary["sites_processed"] += 1
            summary["articles_upserted"] += upserted
            summary["articles_marked_unavailable"] += marked_unavailable
    return summary


class InternalLinkInventoryScheduler:
    def __init__(self, db_sessionmaker: sessionmaker):
        self._sessionmaker = db_sessionmaker
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._interval_seconds = max(3600, _read_int_env("INTERNAL_LINK_SYNC_INTERVAL_SECONDS", 24 * 60 * 60))
        self._timeout_seconds = max(5, _read_int_env("INTERNAL_LINK_SYNC_TIMEOUT_SECONDS", 20))
        self._per_page = max(1, min(100, _read_int_env("INTERNAL_LINK_SYNC_PER_PAGE", 100)))
        self._lock_key = _read_int_env("INTERNAL_LINK_SYNC_LOCK_KEY", 823746192345679123)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="internal-link-sync", daemon=True)
        self._thread.start()
        logger.info("internal_linking.scheduler.start interval_seconds=%s", self._interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("internal_linking.scheduler.stop")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._run_once_if_leader()
            if self._stop_event.wait(self._interval_seconds):
                break

    def _run_once_if_leader(self) -> None:
        engine = self._sessionmaker.kw.get("bind")
        if engine is None:
            logger.warning("internal_linking.scheduler.no_engine")
            return
        with engine.connect() as connection:
            locked = bool(connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": self._lock_key}).scalar())
            if not locked:
                logger.info("internal_linking.scheduler.skip reason=lock_not_acquired")
                return
            try:
                summary = run_internal_link_inventory_sync(
                    self._sessionmaker,
                    per_page=self._per_page,
                    timeout_seconds=self._timeout_seconds,
                )
                logger.info("internal_linking.scheduler.complete summary=%s", summary)
            except Exception:
                logger.exception("internal_linking.scheduler.failed")
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": self._lock_key})


def internal_link_scheduler_enabled() -> bool:
    return _read_bool_env("INTERNAL_LINK_SYNC_ENABLED", True)
