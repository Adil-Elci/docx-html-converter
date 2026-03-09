from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..portal_models import KeywordTrendCache

router = APIRouter(
    prefix="/admin/keyword-trends",
    tags=["keyword_trends"],
    dependencies=[Depends(require_admin)],
)


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return None


@router.get("/dashboard")
def keyword_trend_dashboard(db: Session = Depends(get_db)) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    total_queries = db.query(func.count(KeywordTrendCache.id)).scalar() or 0
    fresh_queries = (
        db.query(func.count(KeywordTrendCache.id))
        .filter(KeywordTrendCache.expires_at > now)
        .scalar()
        or 0
    )
    stale_queries = max(0, int(total_queries) - int(fresh_queries))
    latest_refresh = db.query(func.max(KeywordTrendCache.fetched_at)).scalar()

    recent_rows = (
        db.query(KeywordTrendCache)
        .order_by(KeywordTrendCache.updated_at.desc(), KeywordTrendCache.fetched_at.desc())
        .limit(12)
        .all()
    )
    recent_queries: List[Dict[str, Any]] = []
    for row in recent_rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        suggestions = payload.get("suggestions") if isinstance(payload, dict) else []
        recent_queries.append(
            {
                "query": (row.seed_query or "").strip(),
                "normalized_query": (row.normalized_seed_query or "").strip(),
                "source": (row.source or "").strip(),
                "locale": (row.locale or "").strip(),
                "fetched_at": _iso_or_none(row.fetched_at),
                "expires_at": _iso_or_none(row.expires_at),
                "updated_at": _iso_or_none(row.updated_at),
                "is_fresh": bool(row.expires_at and row.expires_at > now),
                "suggestion_count": len(suggestions) if isinstance(suggestions, list) else 0,
            }
        )

    return {
        "ok": True,
        "summary": {
            "total_queries": int(total_queries),
            "fresh_queries": int(fresh_queries),
            "stale_queries": int(stale_queries),
            "latest_refresh_at": _iso_or_none(latest_refresh),
        },
        "recent_queries": recent_queries,
    }
