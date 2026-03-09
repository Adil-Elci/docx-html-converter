from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import desc, func, text
from sqlalchemy.orm import sessionmaker

from .internal_linking import build_creator_internal_link_inventory
from .portal_models import KeywordTrendCache, PublishingSiteArticle, Site
from .site_analysis_cache import (
    DEFAULT_CACHE_PROMPT_VERSION,
    PHASE2_SITE_ANALYSIS_CACHE_KIND,
    build_site_analysis_content_hash,
    normalize_site_analysis_url,
    upsert_site_analysis_cache,
)

logger = logging.getLogger("portal_backend.seo_cache_refresh")


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


def _normalize_phrase(value: str) -> str:
    cleaned = re.sub(r"[^\wäöüÄÖÜß\s-]", " ", (value or "").strip().lower())
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _extract_site_topic_clusters(items: List[Dict[str, Any]], *, max_items: int = 8) -> List[str]:
    scores: Dict[str, int] = {}
    for item in items:
        title = _normalize_phrase(str(item.get("title") or ""))
        categories = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        for category in categories:
            normalized_category = _normalize_phrase(category)
            if normalized_category:
                scores[normalized_category] = scores.get(normalized_category, 0) + 4
        words = title.split()
        for size in (2, 3):
            for index in range(0, max(0, len(words) - size + 1)):
                phrase = " ".join(words[index : index + size]).strip()
                if len(phrase.split()) < 2:
                    continue
                scores[phrase] = scores.get(phrase, 0) + (3 if size == 2 else 2)
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [phrase for phrase, _score in ranked[:max_items]]


def _build_publishing_site_cache_payload(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    categories: List[str] = []
    titles: List[str] = []
    for item in items:
        titles.append(str(item.get("title") or "").strip())
        categories.extend(str(value).strip() for value in (item.get("categories") or []) if str(value).strip())
    deduped_categories = list(dict.fromkeys([value for value in categories if value]))
    deduped_titles = list(dict.fromkeys([value for value in titles if value]))
    topic_clusters = _extract_site_topic_clusters(items)
    allowed_topics = list(dict.fromkeys(topic_clusters + deduped_categories))[:10]
    internal_linking_opportunities = []
    for item in items[:8]:
        title = str(item.get("title") or "").strip()
        cats = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        if title and cats:
            internal_linking_opportunities.append(f"{cats[0]} -> {title}")
        elif title:
            internal_linking_opportunities.append(title)
    site_summary = ", ".join((topic_clusters[:3] or deduped_categories[:3] or deduped_titles[:2]))[:200]
    return {
        "allowed_topics": allowed_topics,
        "content_style_constraints": [
            "Deutschsprachig, sachlich und nutzerorientiert",
            "Klare Zwischenueberschriften mit Suchintention",
            "Praktische Beispiele und konkrete Hinweise statt Floskeln",
        ],
        "internal_linking_opportunities": list(dict.fromkeys(internal_linking_opportunities))[:10],
        "site_summary": site_summary,
        "site_categories": deduped_categories[:10],
        "topic_clusters": topic_clusters[:8],
        "prominent_titles": deduped_titles[:8],
        "sample_page_titles": deduped_titles[:8],
        "sample_urls": [str(item.get("url") or "").strip() for item in items[:8] if str(item.get("url") or "").strip()],
    }


def _refresh_high_frequency_keyword_trends(db_sessionmaker: sessionmaker) -> int:
    refreshed = 0
    ttl_seconds = max(3600, _read_int_env("SEO_KEYWORD_TREND_TTL_SECONDS", 7 * 24 * 60 * 60))
    refresh_window_seconds = max(3600, _read_int_env("SEO_KEYWORD_TREND_REFRESH_WINDOW_SECONDS", 12 * 60 * 60))
    lookahead = datetime.now(timezone.utc) + timedelta(seconds=refresh_window_seconds)
    limit = max(1, _read_int_env("SEO_KEYWORD_TREND_REFRESH_LIMIT", 25))
    with db_sessionmaker() as session:
        rows = (
            session.query(KeywordTrendCache)
            .filter(
                KeywordTrendCache.last_used_at.isnot(None),
                KeywordTrendCache.expires_at <= lookahead,
            )
            .order_by(desc(KeywordTrendCache.hit_count), KeywordTrendCache.last_used_at.desc())
            .limit(limit)
            .all()
        )
        for row in rows:
            query = (row.seed_query or row.normalized_seed_query or "").strip()
            if not query:
                continue
            try:
                response = requests.get(
                    "https://suggestqueries.google.com/complete/search",
                    params={"client": "firefox", "hl": "de", "gl": "de", "q": query},
                    headers={"User-Agent": "portal-backend/1.0"},
                    timeout=5,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                logger.warning("seo_cache_refresh.keyword_query_failed query=%s", query, exc_info=True)
                continue
            suggestions = payload[1] if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list) else []
            normalized_suggestions = list(
                dict.fromkeys(_normalize_phrase(str(item)) for item in suggestions if str(item).strip())
            )
            row.payload = {"suggestions": normalized_suggestions}
            row.content_hash = build_site_analysis_content_hash(row.payload)
            row.fetched_at = datetime.now(timezone.utc)
            row.expires_at = row.fetched_at + timedelta(seconds=ttl_seconds)
            row.updated_at = row.fetched_at
            session.add(row)
            refreshed += 1
        session.commit()
    return refreshed


def _refresh_top_publishing_site_caches(db_sessionmaker: sessionmaker) -> int:
    refreshed = 0
    limit = max(1, _read_int_env("SEO_PUBLISHING_SITE_CACHE_REFRESH_LIMIT", 12))
    with db_sessionmaker() as session:
        top_sites = (
            session.query(Site, func.count(PublishingSiteArticle.id).label("article_count"))
            .outerjoin(PublishingSiteArticle, PublishingSiteArticle.site_id == Site.id)
            .filter(Site.status == "active")
            .group_by(Site.id)
            .order_by(desc("article_count"), Site.updated_at.desc())
            .limit(limit)
            .all()
        )
        for site, _article_count in top_sites:
            inventory = build_creator_internal_link_inventory(session, site_id=site.id, limit=120)
            if not inventory:
                continue
            payload = _build_publishing_site_cache_payload(inventory)
            upsert_site_analysis_cache(
                session,
                site_role="host",
                site_type="publishing_site",
                normalized_url=normalize_site_analysis_url(site.site_url),
                content_hash=build_site_analysis_content_hash(payload),
                generator_mode="deterministic",
                payload=payload,
                prompt_version=DEFAULT_CACHE_PROMPT_VERSION,
                model_name="",
                cache_kind=PHASE2_SITE_ANALYSIS_CACHE_KIND,
                publishing_site_id=site.id,
            )
            refreshed += 1
        session.commit()
    return refreshed


def run_seo_cache_refresh(db_sessionmaker: sessionmaker) -> Dict[str, int]:
    trend_refreshed = _refresh_high_frequency_keyword_trends(db_sessionmaker)
    site_caches_refreshed = _refresh_top_publishing_site_caches(db_sessionmaker)
    return {
        "trend_queries_refreshed": trend_refreshed,
        "publishing_site_caches_refreshed": site_caches_refreshed,
    }


class SeoCacheRefreshScheduler:
    def __init__(self, db_sessionmaker: sessionmaker):
        self._sessionmaker = db_sessionmaker
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._interval_seconds = max(3600, _read_int_env("SEO_CACHE_REFRESH_INTERVAL_SECONDS", 24 * 60 * 60))
        self._lock_key = _read_int_env("SEO_CACHE_REFRESH_LOCK_KEY", 391827465019283746)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="seo-cache-refresh", daemon=True)
        self._thread.start()
        logger.info("seo_cache_refresh.scheduler.start interval_seconds=%s", self._interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("seo_cache_refresh.scheduler.stop")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._run_once_if_leader()
            if self._stop_event.wait(self._interval_seconds):
                break

    def _run_once_if_leader(self) -> None:
        engine = self._sessionmaker.kw.get("bind")
        if engine is None:
            logger.warning("seo_cache_refresh.scheduler.no_engine")
            return
        with engine.connect() as connection:
            locked = bool(connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": self._lock_key}).scalar())
            if not locked:
                logger.info("seo_cache_refresh.scheduler.skip reason=lock_not_acquired")
                return
            try:
                summary = run_seo_cache_refresh(self._sessionmaker)
                logger.info("seo_cache_refresh.scheduler.complete summary=%s", summary)
            except Exception:
                logger.exception("seo_cache_refresh.scheduler.failed")
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": self._lock_key})


def seo_cache_refresh_enabled() -> bool:
    return _read_bool_env("SEO_CACHE_REFRESH_ENABLED", True)
