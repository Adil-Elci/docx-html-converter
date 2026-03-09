from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import threading
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .portal_models import ClientTargetSite, Site
from .site_profiles import ensure_publishing_site_profile, ensure_target_site_profile

logger = logging.getLogger("portal_backend.site_profile_sync")


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


def _select_publishing_sites(session) -> List[Site]:
    return (
        session.query(Site)
        .filter(Site.status == "active")
        .order_by(Site.name.asc(), Site.created_at.asc())
        .all()
    )


def _select_target_site_rows(session) -> List[Tuple[ClientTargetSite, str]]:
    rows = (
        session.query(ClientTargetSite)
        .filter(ClientTargetSite.target_site_url.isnot(None))
        .order_by(ClientTargetSite.created_at.asc())
        .all()
    )
    selected: List[Tuple[ClientTargetSite, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        url = (row.target_site_url or "").strip()
        if not url:
            continue
        key = (str(row.client_id), url)
        if key in seen:
            continue
        seen.add(key)
        selected.append((row, url))
    return selected


def run_site_profile_sync(
    db_sessionmaker: sessionmaker,
    *,
    publishing_only: bool = False,
    target_only: bool = False,
    force_refresh: bool = False,
    timeout_seconds: int = 10,
    max_pages: int = 3,
) -> Dict[str, int]:
    if publishing_only and target_only:
        raise ValueError("publishing_only and target_only cannot both be true.")

    _synced_at = datetime.now(timezone.utc)
    summary = {
        "publishing_processed": 0,
        "publishing_failed": 0,
        "target_processed": 0,
        "target_failed": 0,
    }
    with db_sessionmaker() as session:
        if not target_only:
            for site in _select_publishing_sites(session):
                try:
                    ensure_publishing_site_profile(
                        session,
                        site=site,
                        timeout_seconds=timeout_seconds,
                        max_pages=max_pages,
                        force_refresh=force_refresh,
                    )
                    session.commit()
                    summary["publishing_processed"] += 1
                    logger.info("site_profile_sync.publishing.ok site=%s", site.site_url)
                except Exception as exc:
                    session.rollback()
                    summary["publishing_failed"] += 1
                    logger.warning("site_profile_sync.publishing.failed site=%s error=%s", site.site_url, exc)

        if not publishing_only:
            for row, url in _select_target_site_rows(session):
                try:
                    ensure_target_site_profile(
                        session,
                        target_site_url=url,
                        client_target_site_id=row.id,
                        timeout_seconds=timeout_seconds,
                        max_pages=max_pages,
                        force_refresh=force_refresh,
                    )
                    session.commit()
                    summary["target_processed"] += 1
                    logger.info("site_profile_sync.target.ok url=%s", url)
                except Exception as exc:
                    session.rollback()
                    summary["target_failed"] += 1
                    logger.warning("site_profile_sync.target.failed url=%s error=%s", url, exc)
    return summary


class SiteProfileScheduler:
    def __init__(self, db_sessionmaker: sessionmaker):
        self._sessionmaker = db_sessionmaker
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._interval_seconds = max(3600, _read_int_env("SITE_PROFILE_SYNC_INTERVAL_SECONDS", 24 * 60 * 60))
        self._timeout_seconds = max(5, _read_int_env("SITE_PROFILE_SYNC_TIMEOUT_SECONDS", 10))
        self._max_pages = max(1, _read_int_env("SITE_PROFILE_SYNC_MAX_PAGES", 3))
        self._force_refresh = _read_bool_env("SITE_PROFILE_SYNC_FORCE_REFRESH", False)
        self._lock_key = _read_int_env("SITE_PROFILE_SYNC_LOCK_KEY", 671928345019283746)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="site-profile-sync", daemon=True)
        self._thread.start()
        logger.info("site_profile_sync.scheduler.start interval_seconds=%s", self._interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("site_profile_sync.scheduler.stop")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._run_once_if_leader()
            if self._stop_event.wait(self._interval_seconds):
                break

    def _run_once_if_leader(self) -> None:
        engine = self._sessionmaker.kw.get("bind")
        if engine is None:
            logger.warning("site_profile_sync.scheduler.no_engine")
            return
        with engine.connect() as connection:
            locked = bool(connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": self._lock_key}).scalar())
            if not locked:
                logger.info("site_profile_sync.scheduler.skip reason=lock_not_acquired")
                return
            try:
                summary = run_site_profile_sync(
                    self._sessionmaker,
                    force_refresh=self._force_refresh,
                    timeout_seconds=self._timeout_seconds,
                    max_pages=self._max_pages,
                )
                logger.info("site_profile_sync.scheduler.complete summary=%s", summary)
            except Exception:
                logger.exception("site_profile_sync.scheduler.failed")
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": self._lock_key})


def site_profile_scheduler_enabled() -> bool:
    return _read_bool_env("SITE_PROFILE_SYNC_ENABLED", True)
