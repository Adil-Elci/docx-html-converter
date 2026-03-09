from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from uuid import UUID

from sqlalchemy.orm import Session

from .portal_models import PublishingSiteArticle, PublishingSiteArticleCategory, SiteCategory


def _normalize_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(value or "").strip().lower())
    cleaned = re.sub(r"[^\w\s-]", " ", cleaned)
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _tokenize(value: str) -> set[str]:
    return {token for token in re.findall(r"\b[a-zA-Z0-9äöüÄÖÜß]{3,}\b", _normalize_text(value))}


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _extract_rendered_text(value: Any) -> str:
    if isinstance(value, dict):
        rendered = value.get("rendered")
        if isinstance(rendered, str):
            return rendered.strip()
    if isinstance(value, str):
        return value.strip()
    return ""


def upsert_publishing_site_article(
    db: Session,
    *,
    site_id: UUID,
    post_payload: Dict[str, Any],
    source: str,
    synced_at: Optional[datetime] = None,
) -> Optional[PublishingSiteArticle]:
    raw_post_id = post_payload.get("id")
    raw_url = post_payload.get("link")
    try:
        wp_post_id = int(raw_post_id)
    except (TypeError, ValueError):
        return None
    url = str(raw_url or "").strip()
    if wp_post_id <= 0 or not url:
        return None

    synced_at = synced_at or datetime.now(timezone.utc)
    article = (
        db.query(PublishingSiteArticle)
        .filter(
            PublishingSiteArticle.site_id == site_id,
            PublishingSiteArticle.wp_post_id == wp_post_id,
        )
        .first()
    )
    if article is None:
        article = PublishingSiteArticle(site_id=site_id, wp_post_id=wp_post_id, url=url)
        db.add(article)
        db.flush()

    article.url = url
    article.slug = str(post_payload.get("slug") or "").strip() or None
    article.title = _extract_rendered_text(post_payload.get("title")) or None
    article.excerpt = _extract_rendered_text(post_payload.get("excerpt")) or None
    article.status = str(post_payload.get("status") or "unknown").strip().lower() or "unknown"
    article.published_at = _coerce_datetime(post_payload.get("date_gmt")) or _coerce_datetime(post_payload.get("date"))
    article.modified_at = _coerce_datetime(post_payload.get("modified_gmt")) or _coerce_datetime(post_payload.get("modified"))
    article.source = source if source in {"wp_rest", "job"} else "wp_rest"
    article.last_synced_at = synced_at
    article.updated_at = synced_at

    db.query(PublishingSiteArticleCategory).filter(PublishingSiteArticleCategory.article_id == article.id).delete()
    raw_categories = post_payload.get("categories") or []
    category_ids: List[int] = []
    for value in raw_categories:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in category_ids:
            category_ids.append(parsed)
    if category_ids:
        site_categories = (
            db.query(SiteCategory)
            .filter(
                SiteCategory.site_id == site_id,
                SiteCategory.wp_category_id.in_(category_ids),
            )
            .all()
        )
        category_name_map = {int(row.wp_category_id): (row.name or "").strip() for row in site_categories}
        for category_id in category_ids:
            db.add(
                PublishingSiteArticleCategory(
                    article_id=article.id,
                    wp_category_id=category_id,
                    category_name=category_name_map.get(category_id) or None,
                )
            )
    return article


def mark_missing_publishing_site_articles(
    db: Session,
    *,
    site_id: UUID,
    seen_post_ids: List[int],
    synced_at: Optional[datetime] = None,
) -> int:
    synced_at = synced_at or datetime.now(timezone.utc)
    query = db.query(PublishingSiteArticle).filter(PublishingSiteArticle.site_id == site_id)
    rows = query.all()
    seen = {post_id for post_id in seen_post_ids if post_id > 0}
    updated = 0
    for row in rows:
        if int(row.wp_post_id or 0) in seen:
            continue
        row.status = "unavailable"
        row.last_synced_at = synced_at
        row.updated_at = synced_at
        db.add(row)
        updated += 1
    return updated


def build_creator_internal_link_inventory(
    db: Session,
    *,
    site_id: UUID,
    limit: int = 60,
) -> List[Dict[str, Any]]:
    articles = (
        db.query(PublishingSiteArticle)
        .filter(
            PublishingSiteArticle.site_id == site_id,
            PublishingSiteArticle.status == "publish",
            PublishingSiteArticle.url.isnot(None),
            PublishingSiteArticle.url != "",
        )
        .order_by(PublishingSiteArticle.published_at.desc().nullslast(), PublishingSiteArticle.updated_at.desc())
        .limit(max(1, limit))
        .all()
    )
    if not articles:
        return []

    article_ids = [row.id for row in articles]
    category_rows = (
        db.query(PublishingSiteArticleCategory)
        .filter(PublishingSiteArticleCategory.article_id.in_(article_ids))
        .all()
    )
    categories_by_article: Dict[UUID, List[str]] = {}
    for row in category_rows:
        categories_by_article.setdefault(row.article_id, [])
        category_name = (row.category_name or "").strip()
        if category_name and category_name not in categories_by_article[row.article_id]:
            categories_by_article[row.article_id].append(category_name)

    out: List[Dict[str, Any]] = []
    for row in articles:
        out.append(
            {
                "url": (row.url or "").strip(),
                "title": (row.title or "").strip(),
                "excerpt": (row.excerpt or "").strip(),
                "slug": (row.slug or "").strip(),
                "categories": categories_by_article.get(row.id, []),
                "published_at": row.published_at.isoformat() if row.published_at else "",
            }
        )
    return out


def score_internal_link_inventory_item(
    item: Dict[str, Any],
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
) -> float:
    topic_tokens = _tokenize(topic)
    primary_tokens = _tokenize(primary_keyword)
    secondary_tokens = _tokenize(" ".join(secondary_keywords))
    title_tokens = _tokenize(str(item.get("title") or ""))
    excerpt_tokens = _tokenize(str(item.get("excerpt") or ""))
    category_tokens = _tokenize(" ".join(item.get("categories") or []))
    combined = title_tokens | excerpt_tokens | category_tokens
    if not combined:
        return 0.0
    score = 0.0
    score += 4.0 * len(combined & topic_tokens)
    score += 3.0 * len(combined & primary_tokens)
    score += 1.5 * len(combined & secondary_tokens)
    score += 0.5 * min(4, len(title_tokens))
    published_at = str(item.get("published_at") or "").strip()
    if published_at:
        score += 0.3
    return score


def host_variants(url: str) -> set[str]:
    parsed = urlparse((url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return set()
    variants = {host}
    if host.startswith("www."):
        variants.add(host[4:])
    else:
        variants.add(f"www.{host}")
    return variants
