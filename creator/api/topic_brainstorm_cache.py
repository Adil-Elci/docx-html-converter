"""Cache brainstormed editorial angles per (target site, publisher, language, year).

Brainstorming is the most expensive single step in the v2 pre-pipeline (~$0.02
Sonnet 4.6 call). When a client orders multiple guest posts on the same
target_site -> publisher pair, we only need to spend that once and reuse
the angles list across orders. 90-day TTL because the relevance of an
editorial angle decays with the news cycle but rarely faster than a quarter.

The cache key intentionally does NOT include the exclude_topics list -- if
it did, every new exclusion would force a fresh LLM call. Instead, the
caller filters the cached angles against the current exclude list at read
time and falls back to a fresh brainstorm only when no usable angles
remain (see ``topic_brainstorm.py``).

Soft-fails on any DB error -- caching is a perf/cost optimisation, never
a correctness gate.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger("creator.topic_brainstorm_cache")

PROVIDER = "anthropic"
CACHE_KIND = "brainstorm"
DEFAULT_TTL_SECONDS = 90 * 24 * 60 * 60  # 90 days
BRAINSTORM_CACHE_VERSION = "v1"

_ENGINE: Optional[Engine] = None
_METADATA = MetaData()
SEO_RESEARCH_CACHE = Table(
    "seo_research_cache",
    _METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("provider", Text, nullable=False),
    Column("cache_kind", Text, nullable=False),
    Column("lookup_key", Text, nullable=False),
    Column("locale", Text, nullable=True),
    Column("content_hash", Text, nullable=True),
    Column("payload", JSONB, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=True),
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


def _normalize_url(url: str) -> str:
    return (url or "").strip().lower().rstrip("/")


def build_lookup_key(
    *,
    target_url: str,
    publisher_url: Optional[str],
    language: str,
    current_year: int,
) -> str:
    """Site-pair + locale + year. Deliberately excludes exclude_topics."""

    raw = "|".join([
        _normalize_url(target_url),
        _normalize_url(publisher_url or ""),
        (language or "").strip().lower(),
        str(int(current_year)),
        BRAINSTORM_CACHE_VERSION,
    ])
    # Hash to keep the key short + stable; preserve a readable prefix
    # so cache rows are debuggable in psql.
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"brainstorm|{digest}"


def build_locale(language: str, current_year: int) -> str:
    return f"{(language or '').strip().lower()}-{int(current_year)}"


def get_cached_brainstorm(*, lookup_key: str, locale: str) -> Optional[Dict[str, Any]]:
    """Returns the cached brainstorm payload dict if a fresh row exists; else None."""

    engine = _get_engine()
    if engine is None or not lookup_key:
        return None
    now = datetime.now(timezone.utc)
    try:
        with engine.begin() as connection:
            row = connection.execute(
                select(SEO_RESEARCH_CACHE).where(
                    SEO_RESEARCH_CACHE.c.provider == PROVIDER,
                    SEO_RESEARCH_CACHE.c.cache_kind == CACHE_KIND,
                    SEO_RESEARCH_CACHE.c.lookup_key == lookup_key,
                    SEO_RESEARCH_CACHE.c.locale == locale,
                )
            ).mappings().first()
    except Exception:
        logger.warning(
            "creator.brainstorm_cache.lookup_failed key=%s locale=%s",
            lookup_key,
            locale,
            exc_info=True,
        )
        return None
    if not row:
        return None
    expires_at = row.get("expires_at")
    if expires_at is not None and expires_at < now:
        return None
    payload = row.get("payload")
    return dict(payload) if isinstance(payload, dict) else None


def upsert_brainstorm(
    *,
    lookup_key: str,
    locale: str,
    payload: Dict[str, Any],
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> None:
    engine = _get_engine()
    if engine is None or not lookup_key:
        return
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=max(60, int(ttl_seconds)))
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    try:
        with engine.begin() as connection:
            existing = connection.execute(
                select(SEO_RESEARCH_CACHE.c.id).where(
                    SEO_RESEARCH_CACHE.c.provider == PROVIDER,
                    SEO_RESEARCH_CACHE.c.cache_kind == CACHE_KIND,
                    SEO_RESEARCH_CACHE.c.lookup_key == lookup_key,
                    SEO_RESEARCH_CACHE.c.locale == locale,
                )
            ).scalar()
            if existing:
                connection.execute(
                    SEO_RESEARCH_CACHE.update()
                    .where(SEO_RESEARCH_CACHE.c.id == existing)
                    .values(
                        payload=payload,
                        content_hash=None,
                        fetched_at=now,
                        expires_at=expires_at,
                        updated_at=now,
                    )
                )
            else:
                connection.execute(
                    SEO_RESEARCH_CACHE.insert().values(
                        id=uuid.uuid4(),
                        provider=PROVIDER,
                        cache_kind=CACHE_KIND,
                        lookup_key=lookup_key,
                        locale=locale,
                        content_hash=None,
                        payload=payload,
                        fetched_at=now,
                        expires_at=expires_at,
                        created_at=now,
                        updated_at=now,
                    )
                )
    except Exception:
        logger.warning(
            "creator.brainstorm_cache.upsert_failed key=%s payload_bytes=%d",
            lookup_key,
            len(serialized),
            exc_info=True,
        )
