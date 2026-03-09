from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger("creator.trend_cache")

DEFAULT_TREND_SOURCE = "google_suggest"
DEFAULT_TREND_LOCALE = "de-DE"

_ENGINE: Optional[Engine] = None
_METADATA = MetaData()
KEYWORD_TREND_CACHE = Table(
    "keyword_trend_cache",
    _METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("source", Text, nullable=False),
    Column("locale", Text, nullable=False),
    Column("seed_query", Text, nullable=False),
    Column("normalized_seed_query", Text, nullable=False),
    Column("query_family", Text, nullable=False),
    Column("content_hash", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("hit_count", Integer, nullable=False),
    Column("last_used_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


def _get_database_url() -> str:
    return (os.getenv("CREATOR_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()


def _get_engine() -> Optional[Engine]:
    global _ENGINE
    database_url = _get_database_url()
    if not database_url:
        return None
    if _ENGINE is None:
        _ENGINE = create_engine(database_url, pool_pre_ping=True)
    return _ENGINE


def _hash_payload(payload: Dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def get_keyword_trend_cache_entry(
    normalized_seed_query: str,
    *,
    source: str = DEFAULT_TREND_SOURCE,
    locale: str = DEFAULT_TREND_LOCALE,
) -> Optional[Dict[str, Any]]:
    engine = _get_engine()
    if engine is None or not normalized_seed_query:
        return None
    try:
        with engine.begin() as connection:
            row = connection.execute(
                select(KEYWORD_TREND_CACHE).where(
                    KEYWORD_TREND_CACHE.c.source == source,
                    KEYWORD_TREND_CACHE.c.locale == locale,
                    KEYWORD_TREND_CACHE.c.normalized_seed_query == normalized_seed_query,
                )
            ).mappings().first()
    except Exception:
        logger.warning("creator.trend_cache.lookup_failed query=%s", normalized_seed_query, exc_info=True)
        return None
    return dict(row) if row else None


def get_keyword_trend_cache_family_entries(
    query_family: str,
    *,
    source: str = DEFAULT_TREND_SOURCE,
    locale: str = DEFAULT_TREND_LOCALE,
    limit: int = 5,
) -> list[Dict[str, Any]]:
    engine = _get_engine()
    if engine is None or not query_family:
        return []
    try:
        with engine.begin() as connection:
            rows = (
                connection.execute(
                    select(KEYWORD_TREND_CACHE)
                    .where(
                        KEYWORD_TREND_CACHE.c.source == source,
                        KEYWORD_TREND_CACHE.c.locale == locale,
                        KEYWORD_TREND_CACHE.c.query_family == query_family,
                    )
                    .order_by(KEYWORD_TREND_CACHE.c.last_used_at.desc().nullslast(), KEYWORD_TREND_CACHE.c.updated_at.desc())
                    .limit(max(1, limit))
                )
                .mappings()
                .all()
            )
    except Exception:
        logger.warning("creator.trend_cache.family_lookup_failed family=%s", query_family, exc_info=True)
        return []
    return [dict(row) for row in rows]


def record_keyword_trend_cache_hit(
    normalized_seed_query: str,
    *,
    source: str = DEFAULT_TREND_SOURCE,
    locale: str = DEFAULT_TREND_LOCALE,
) -> None:
    engine = _get_engine()
    if engine is None or not normalized_seed_query:
        return
    now = datetime.now(timezone.utc)
    try:
        with engine.begin() as connection:
            existing = connection.execute(
                select(KEYWORD_TREND_CACHE.c.id, KEYWORD_TREND_CACHE.c.hit_count).where(
                    KEYWORD_TREND_CACHE.c.source == source,
                    KEYWORD_TREND_CACHE.c.locale == locale,
                    KEYWORD_TREND_CACHE.c.normalized_seed_query == normalized_seed_query,
                )
            ).first()
            if not existing:
                return
            current_hits = int(existing.hit_count or 0)
            connection.execute(
                KEYWORD_TREND_CACHE.update()
                .where(KEYWORD_TREND_CACHE.c.id == existing.id)
                .values(hit_count=current_hits + 1, last_used_at=now, updated_at=now)
            )
    except Exception:
        logger.warning("creator.trend_cache.hit_update_failed query=%s", normalized_seed_query, exc_info=True)


def upsert_keyword_trend_cache_entry(
    *,
    seed_query: str,
    normalized_seed_query: str,
    query_family: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    source: str = DEFAULT_TREND_SOURCE,
    locale: str = DEFAULT_TREND_LOCALE,
) -> None:
    engine = _get_engine()
    if engine is None or not normalized_seed_query:
        return
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=max(1, ttl_seconds))
    content_hash = _hash_payload(payload)
    try:
        with engine.begin() as connection:
            existing = connection.execute(
                select(KEYWORD_TREND_CACHE.c.id).where(
                    KEYWORD_TREND_CACHE.c.source == source,
                    KEYWORD_TREND_CACHE.c.locale == locale,
                    KEYWORD_TREND_CACHE.c.normalized_seed_query == normalized_seed_query,
                )
            ).scalar_one_or_none()
            values = {
                "source": source,
                "locale": locale,
                "seed_query": seed_query,
                "normalized_seed_query": normalized_seed_query,
                "query_family": query_family,
                "content_hash": content_hash,
                "payload": payload,
                "fetched_at": now,
                "expires_at": expires_at,
                "updated_at": now,
            }
            if existing is None:
                values["id"] = uuid.uuid4()
                values["created_at"] = now
                values["hit_count"] = 1
                values["last_used_at"] = now
                connection.execute(KEYWORD_TREND_CACHE.insert().values(**values))
            else:
                current_hits = connection.execute(
                    select(KEYWORD_TREND_CACHE.c.hit_count).where(KEYWORD_TREND_CACHE.c.id == existing)
                ).scalar_one_or_none()
                values["hit_count"] = max(1, int(current_hits or 0) + 1)
                values["last_used_at"] = now
                connection.execute(
                    KEYWORD_TREND_CACHE.update()
                    .where(KEYWORD_TREND_CACHE.c.id == existing)
                    .values(**values)
                )
    except Exception:
        logger.warning("creator.trend_cache.upsert_failed query=%s", normalized_seed_query, exc_info=True)
