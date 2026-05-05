"""Cache derived topics in ``seo_research_cache`` (cache_kind='derived_topic').

Mirrors the ``research_cache.py`` pattern: typed ``Table`` declaration so the
creator service can read/write without owning the schema (Alembic 0050 widens
the existing CHECK constraint). 30-day TTL because target pages rarely change
their main commercial topic. Soft-fails on any DB error -- caching is a perf
optimisation, never a correctness gate.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger("creator.topic_derivation_cache")

PROVIDER = "dataforseo"
CACHE_KIND = "derived_topic"
DEFAULT_TTL_SECONDS = 30 * 24 * 60 * 60

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


def get_cached_derived_topic(*, lookup_key: str) -> Optional[Dict[str, Any]]:
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
                )
            ).mappings().first()
    except Exception:
        logger.warning(
            "creator.topic_derivation_cache.lookup_failed key=%s",
            lookup_key,
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


def upsert_derived_topic(
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
                )
            ).scalar()
            if existing:
                connection.execute(
                    SEO_RESEARCH_CACHE.update()
                    .where(SEO_RESEARCH_CACHE.c.id == existing)
                    .values(
                        payload=payload,
                        locale=locale,
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
            "creator.topic_derivation_cache.upsert_failed key=%s payload_bytes=%d",
            lookup_key,
            len(serialized),
            exc_info=True,
        )
