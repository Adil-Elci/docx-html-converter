from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse
from uuid import UUID

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from .portal_models import (
    ClientTargetSite,
    PublishingSiteArticle,
    Site,
    SiteCategory,
    SiteProfileCache,
    utcnow,
)

logger = logging.getLogger("portal_backend.site_profiles")

PROFILE_KIND_PUBLISHING = "publishing_site"
PROFILE_KIND_TARGET = "target_site"
PROFILE_VERSION = "v1"

CONTEXT_KEYWORDS = {
    "health": {"gesundheit", "therapie", "arzt", "medizin", "symptome", "vorsorge", "sucht", "behandlung"},
    "family_life": {"familie", "eltern", "kinder", "baby", "schwangerschaft", "alltag", "erziehung"},
    "education": {"lernen", "schule", "bildung", "kita", "ausbildung", "studium"},
    "daily_routine": {"alltag", "routine", "organisation", "planung", "tipps", "haushalt"},
    "finance": {"kosten", "budget", "finanzierung", "sparen", "steuer", "versicherung"},
    "home": {"wohnen", "haus", "wohnung", "garten", "immobilien", "einrichten"},
    "lifestyle": {"ratgeber", "trends", "mode", "beauty", "leben", "ideen"},
    "safety": {"sicherheit", "schutz", "risiko", "warnzeichen", "prävention"},
    "productivity": {"produktiv", "effizienz", "planung", "workflow", "management"},
    "wellbeing": {"wohlbefinden", "balance", "stress", "entspannung", "mental"},
    "shopping": {"kaufen", "shop", "produkt", "preis", "vergleich", "online"},
}
GERMAN_STOPWORDS = {
    "aber", "alle", "als", "also", "am", "an", "auch", "auf", "aus", "bei", "bin", "bis", "das", "dass",
    "de", "dem", "den", "der", "des", "die", "doch", "ein", "eine", "einer", "eines", "er", "es", "für",
    "hat", "hier", "ich", "im", "in", "ist", "mit", "nach", "nicht", "nur", "oder", "sie", "sind", "so",
    "und", "uns", "von", "vor", "wie", "wir", "zu", "zum", "zur",
}
COMMERCIAL_TERMS = {
    "kaufen", "shop", "angebot", "preis", "preise", "bestellen", "produkt", "produkte", "versand", "marke",
    "sale", "rabatt", "vergleich",
}
INFORMATIONAL_TERMS = {
    "ratgeber", "tipps", "checkliste", "anleitung", "faq", "was", "wie", "warum", "wann", "hilft",
}


def normalize_site_profile_url(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    if not parsed.netloc:
        return cleaned.rstrip("/")
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").strip().lower().rstrip("/")
    path = parsed.path or ""
    return f"{scheme}://{host}{path}".rstrip("/")


def build_site_profile_content_hash(payload: Dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def get_latest_site_profile(
    db: Session,
    *,
    profile_kind: str,
    normalized_url: str,
    publishing_site_id: Optional[UUID] = None,
    client_target_site_id: Optional[UUID] = None,
) -> Optional[SiteProfileCache]:
    query = db.query(SiteProfileCache).filter(
        SiteProfileCache.profile_kind == profile_kind,
        SiteProfileCache.normalized_url == normalized_url,
    )
    if publishing_site_id is None:
        query = query.filter(SiteProfileCache.publishing_site_id.is_(None))
    else:
        query = query.filter(SiteProfileCache.publishing_site_id == publishing_site_id)
    if client_target_site_id is None:
        query = query.filter(SiteProfileCache.client_target_site_id.is_(None))
    else:
        query = query.filter(SiteProfileCache.client_target_site_id == client_target_site_id)
    return query.order_by(SiteProfileCache.updated_at.desc(), SiteProfileCache.created_at.desc()).first()


def upsert_site_profile(
    db: Session,
    *,
    profile_kind: str,
    normalized_url: str,
    payload: Dict[str, Any],
    publishing_site_id: Optional[UUID] = None,
    client_target_site_id: Optional[UUID] = None,
    generator_mode: str = "deterministic",
) -> SiteProfileCache:
    content_hash = build_site_profile_content_hash(payload)
    record = (
        db.query(SiteProfileCache)
        .filter(
            SiteProfileCache.profile_kind == profile_kind,
            SiteProfileCache.normalized_url == normalized_url,
            SiteProfileCache.content_hash == content_hash,
            SiteProfileCache.profile_version == PROFILE_VERSION,
        )
        .order_by(SiteProfileCache.updated_at.desc(), SiteProfileCache.created_at.desc())
        .first()
    )
    if record is None:
        record = SiteProfileCache(
            id=uuid.uuid4(),
            profile_kind=profile_kind,
            publishing_site_id=publishing_site_id,
            client_target_site_id=client_target_site_id,
            normalized_url=normalized_url,
            content_hash=content_hash,
            generator_mode=generator_mode,
            profile_version=PROFILE_VERSION,
            payload=payload,
        )
    else:
        record.payload = payload
        record.generator_mode = generator_mode
        record.updated_at = utcnow()
    db.add(record)
    return record


def fetch_site_profile_payload(
    *,
    site_url: str,
    profile_kind: str,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    inventory_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_url = normalize_site_profile_url(site_url)
    pages = _build_snapshot_pages(site_url, timeout_seconds=timeout_seconds, max_pages=max_pages)
    combined_text = " ".join(page.get("text", "") for page in pages).strip()
    headings = _dedupe_preserve_order([heading for page in pages for heading in page.get("headings", [])])
    page_titles = _dedupe_preserve_order([page.get("title", "") for page in pages if page.get("title")])
    meta_descriptions = _dedupe_preserve_order(
        [page.get("meta_description", "") for page in pages if page.get("meta_description")]
    )
    keywords = _extract_keywords(" ".join(page_titles + headings + [combined_text]), limit=18)
    contexts = _infer_contexts(keywords + headings + page_titles + meta_descriptions)
    primary_context = contexts[0] if contexts else ("shopping" if profile_kind == PROFILE_KIND_TARGET else "lifestyle")
    content_tone = _detect_content_tone(combined_text, headings, page_titles)
    domain_topic = _derive_domain_topic(page_titles, headings, keywords, normalized_url)
    categories = _dedupe_preserve_order(_coerce_string_list((inventory_context or {}).get("site_categories")))
    prominent_titles = _dedupe_preserve_order(_coerce_string_list((inventory_context or {}).get("prominent_titles")))
    topic_clusters = _dedupe_preserve_order(
        _coerce_string_list((inventory_context or {}).get("topic_clusters")) + keywords[:8]
    )
    business_type, business_intent = _derive_business_intent(combined_text, keywords, headings)
    services_or_products = _derive_services_or_products(headings, keywords, page_titles)
    payload: Dict[str, Any] = {
        "normalized_url": normalized_url,
        "source_url": site_url.strip(),
        "page_title": page_titles[0] if page_titles else "",
        "meta_description": meta_descriptions[0] if meta_descriptions else "",
        "visible_headings": headings[:18],
        "repeated_keywords": keywords[:18],
        "sample_page_titles": page_titles[:8],
        "sample_urls": [page.get("url", "") for page in pages if page.get("url")][:8],
        "domain_level_topic": domain_topic,
        "primary_context": primary_context,
        "topics": _derive_topics(domain_topic, headings, keywords, categories, prominent_titles),
        "contexts": contexts,
        "content_tone": content_tone,
        "content_style": _derive_content_style(content_tone, headings, page_titles),
        "site_categories": categories[:12],
        "topic_clusters": topic_clusters[:12],
        "prominent_titles": prominent_titles[:8],
        "business_type": business_type if profile_kind == PROFILE_KIND_TARGET else "",
        "services_or_products": services_or_products if profile_kind == PROFILE_KIND_TARGET else [],
        "business_intent": business_intent if profile_kind == PROFILE_KIND_TARGET else "informational",
        "commerciality": _estimate_commerciality(combined_text, headings, keywords),
        "page_count": len(pages),
        "profile_generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return payload


def build_publishing_inventory_context(db: Session, *, site_id: UUID, article_limit: int = 80) -> Dict[str, Any]:
    categories = [
        (row.name or "").strip()
        for row in (
            db.query(SiteCategory)
            .filter(SiteCategory.site_id == site_id, SiteCategory.enabled.is_(True))
            .order_by(SiteCategory.post_count.desc().nullslast(), SiteCategory.name.asc())
            .limit(20)
            .all()
        )
        if (row.name or "").strip()
    ]
    articles = (
        db.query(PublishingSiteArticle)
        .filter(
            PublishingSiteArticle.site_id == site_id,
            PublishingSiteArticle.status == "publish",
        )
        .order_by(PublishingSiteArticle.published_at.desc().nullslast(), PublishingSiteArticle.created_at.desc())
        .limit(max(1, article_limit))
        .all()
    )
    titles = [(row.title or "").strip() for row in articles if (row.title or "").strip()]
    topic_clusters = _extract_keywords(" ".join(titles + categories), limit=14)
    return {
        "site_categories": _dedupe_preserve_order(categories),
        "prominent_titles": _dedupe_preserve_order(titles)[:12],
        "topic_clusters": topic_clusters,
    }


def ensure_publishing_site_profile(
    db: Session,
    *,
    site: Site,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    force_refresh: bool = False,
) -> SiteProfileCache:
    normalized_url = normalize_site_profile_url(site.site_url)
    existing = None if force_refresh else get_latest_site_profile(
        db,
        profile_kind=PROFILE_KIND_PUBLISHING,
        normalized_url=normalized_url,
        publishing_site_id=site.id,
    )
    if existing is not None and isinstance(existing.payload, dict) and existing.payload:
        return existing
    inventory_context = build_publishing_inventory_context(db, site_id=site.id)
    payload = fetch_site_profile_payload(
        site_url=site.site_url,
        profile_kind=PROFILE_KIND_PUBLISHING,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
        inventory_context=inventory_context,
    )
    record = upsert_site_profile(
        db,
        profile_kind=PROFILE_KIND_PUBLISHING,
        normalized_url=normalized_url,
        payload=payload,
        publishing_site_id=site.id,
    )
    db.flush()
    return record


def ensure_target_site_profile(
    db: Session,
    *,
    target_site_url: str,
    client_target_site_id: Optional[UUID] = None,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    force_refresh: bool = False,
) -> SiteProfileCache:
    normalized_url = normalize_site_profile_url(target_site_url)
    existing = None if force_refresh else get_latest_site_profile(
        db,
        profile_kind=PROFILE_KIND_TARGET,
        normalized_url=normalized_url,
        client_target_site_id=client_target_site_id,
    )
    if existing is not None and isinstance(existing.payload, dict) and existing.payload:
        return existing
    payload = fetch_site_profile_payload(
        site_url=target_site_url,
        profile_kind=PROFILE_KIND_TARGET,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
    )
    record = upsert_site_profile(
        db,
        profile_kind=PROFILE_KIND_TARGET,
        normalized_url=normalized_url,
        payload=payload,
        client_target_site_id=client_target_site_id,
    )
    db.flush()
    return record


def score_publishing_site_fit(
    publishing_profile: Dict[str, Any],
    target_profile: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:
    publishing_topics = _text_set(
        publishing_profile.get("topics"),
        publishing_profile.get("site_categories"),
        publishing_profile.get("topic_clusters"),
        publishing_profile.get("repeated_keywords"),
        publishing_profile.get("visible_headings"),
    )
    target_topics = _text_set(
        target_profile.get("topics"),
        target_profile.get("contexts"),
        target_profile.get("repeated_keywords"),
        target_profile.get("services_or_products"),
        target_profile.get("visible_headings"),
    )
    publishing_contexts = set(_coerce_string_list(publishing_profile.get("contexts")))
    target_contexts = set(_coerce_string_list(target_profile.get("contexts")))
    topic_overlap = len(publishing_topics & target_topics)
    context_overlap = len(publishing_contexts & target_contexts)
    score = topic_overlap * 8 + context_overlap * 16
    if publishing_profile.get("primary_context") and publishing_profile.get("primary_context") == target_profile.get("primary_context"):
        score += 12
    if publishing_profile.get("primary_context") in target_contexts:
        score += 6
    if target_profile.get("business_intent") == "commercial":
        score -= 4
    if score < 0:
        score = 0
    score = min(100, score)
    details = {
        "topic_overlap_terms": sorted((publishing_topics & target_topics))[:12],
        "context_overlap": sorted(publishing_contexts & target_contexts),
        "publishing_primary_context": publishing_profile.get("primary_context") or "",
        "target_primary_context": target_profile.get("primary_context") or "",
        "publishing_topics_count": len(publishing_topics),
        "target_topics_count": len(target_topics),
    }
    return score, details


def select_best_publishing_site_for_target(
    db: Session,
    *,
    target_site_url: str,
    candidate_sites: Sequence[Site],
    client_target_site_id: Optional[UUID] = None,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    min_score: int = 18,
) -> Tuple[Optional[Site], Dict[str, Any], Optional[SiteProfileCache], List[Dict[str, Any]]]:
    target_profile_record = ensure_target_site_profile(
        db,
        target_site_url=target_site_url,
        client_target_site_id=client_target_site_id,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
    )
    target_profile = dict(target_profile_record.payload or {})
    ranked: List[Dict[str, Any]] = []
    for site in candidate_sites:
        try:
            publishing_profile_record = ensure_publishing_site_profile(
                db,
                site=site,
                timeout_seconds=timeout_seconds,
                max_pages=max_pages,
            )
        except Exception:
            logger.warning("site_profiles.auto_select.profile_failed site_id=%s", site.id, exc_info=True)
            continue
        publishing_profile = dict(publishing_profile_record.payload or {})
        score, details = score_publishing_site_fit(publishing_profile, target_profile)
        ranked.append(
            {
                "site_id": str(site.id),
                "site_url": site.site_url,
                "site_name": site.name,
                "score": score,
                "details": details,
                "profile": publishing_profile,
            }
        )
    ranked.sort(key=lambda item: (-int(item.get("score") or 0), str(item.get("site_name") or "")))
    best = ranked[0] if ranked else None
    if best is None or int(best.get("score") or 0) < min_score:
        return None, target_profile, target_profile_record, ranked[:8]
    selected = next((site for site in candidate_sites if str(site.id) == str(best.get("site_id"))), None)
    return selected, target_profile, target_profile_record, ranked[:8]


def _build_snapshot_pages(site_url: str, *, timeout_seconds: int, max_pages: int) -> List[Dict[str, Any]]:
    homepage_html = _fetch_html(site_url, timeout_seconds=timeout_seconds)
    pages: List[Dict[str, Any]] = []
    if homepage_html:
        pages.append(_extract_page_signals(site_url, homepage_html))
        links = _extract_internal_links(site_url, homepage_html, limit=max(0, max_pages - 1))
        for link in links:
            html = _fetch_html(link, timeout_seconds=timeout_seconds)
            if not html:
                continue
            pages.append(_extract_page_signals(link, html))
            if len(pages) >= max_pages:
                break
    return pages or [{"url": site_url, "title": "", "meta_description": "", "headings": [], "text": ""}]


def _fetch_html(url: str, *, timeout_seconds: int) -> str:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": os.getenv("SITE_PROFILE_USER_AGENT", "portal-backend/1.0")},
            timeout=timeout_seconds,
            allow_redirects=True,
        )
        response.raise_for_status()
    except Exception:
        logger.warning("site_profiles.fetch_failed url=%s", url, exc_info=True)
        return ""
    content_type = (response.headers.get("content-type") or "").lower()
    if "html" not in content_type:
        return ""
    return response.text or ""


def _extract_internal_links(base_url: str, html: str, *, limit: int) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    base_host = urlparse(base_url).netloc.lower()
    links: List[str] = []
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower() != base_host:
            continue
        normalized = normalize_site_profile_url(absolute)
        if normalized == normalize_site_profile_url(base_url):
            continue
        if normalized in links:
            continue
        links.append(normalized)
        if len(links) >= limit:
            break
    return links


def _extract_page_signals(url: str, html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "lxml")
    title = (soup.title.string or "").strip() if soup.title and soup.title.string else ""
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    meta_description = (meta.get("content") or "").strip() if meta else ""
    headings: List[str] = []
    for tag_name in ("h1", "h2", "h3"):
        for tag in soup.find_all(tag_name):
            text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True)).strip()
            if text:
                headings.append(text)
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    body = soup.body or soup
    text = re.sub(r"\s+", " ", body.get_text(" ", strip=True)).strip()
    return {
        "url": normalize_site_profile_url(url),
        "title": title,
        "meta_description": meta_description,
        "headings": headings[:18],
        "text": text[:12000],
    }


def _extract_keywords(text: str, *, limit: int) -> List[str]:
    tokens = re.findall(r"\b[a-zA-ZäöüÄÖÜß]{3,}\b", (text or "").lower())
    counter = Counter(token for token in tokens if token not in GERMAN_STOPWORDS)
    return [token for token, _ in counter.most_common(limit)]


def _infer_contexts(values: Sequence[str]) -> List[str]:
    text = " ".join(_coerce_string_list(values)).lower()
    scores: List[Tuple[str, int]] = []
    for context, keywords in CONTEXT_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in text)
        if score > 0:
            scores.append((context, score))
    scores.sort(key=lambda item: (-item[1], item[0]))
    return [context for context, _ in scores[:5]]


def _derive_domain_topic(page_titles: Sequence[str], headings: Sequence[str], keywords: Sequence[str], normalized_url: str) -> str:
    candidates = _dedupe_preserve_order(list(page_titles[:2]) + list(headings[:4]) + list(keywords[:4]))
    for item in candidates:
        cleaned = item.strip()
        if cleaned:
            return cleaned[:140]
    host = urlparse(normalized_url).netloc.replace("www.", "")
    return host


def _derive_topics(
    domain_topic: str,
    headings: Sequence[str],
    keywords: Sequence[str],
    categories: Sequence[str],
    prominent_titles: Sequence[str],
) -> List[str]:
    topics = _dedupe_preserve_order(
        [domain_topic] + list(categories[:6]) + list(headings[:6]) + list(prominent_titles[:4]) + list(keywords[:6])
    )
    return [topic for topic in topics if topic][:12]


def _derive_content_style(content_tone: str, headings: Sequence[str], page_titles: Sequence[str]) -> List[str]:
    style = [content_tone]
    joined = " ".join(list(headings) + list(page_titles)).lower()
    if any(term in joined for term in {"tipps", "ratgeber", "checkliste"}):
        style.append("praktisch")
    if any(term in joined for term in {"vergleich", "vs", "oder"}):
        style.append("vergleichend")
    if any(term in joined for term in {"faq", "fragen", "antworten"}):
        style.append("frage_antwort")
    return _dedupe_preserve_order(style)


def _detect_content_tone(text: str, headings: Sequence[str], page_titles: Sequence[str]) -> str:
    joined = " ".join([text] + list(headings) + list(page_titles)).lower()
    if any(term in joined for term in {"ratgeber", "tipps", "checkliste", "anleitung"}):
        return "practical_informational"
    if any(term in joined for term in {"news", "magazin", "aktuell", "trends"}):
        return "editorial"
    if any(term in joined for term in {"shop", "produkt", "kaufen", "vergleich"}):
        return "commercial_informational"
    return "informational"


def _derive_business_intent(text: str, keywords: Sequence[str], headings: Sequence[str]) -> Tuple[str, str]:
    joined = " ".join([text] + list(keywords) + list(headings)).lower()
    commercial_hits = sum(1 for term in COMMERCIAL_TERMS if term in joined)
    informational_hits = sum(1 for term in INFORMATIONAL_TERMS if term in joined)
    if commercial_hits >= informational_hits + 2:
        return "merchant_or_service", "commercial"
    if any(term in joined for term in {"praxis", "kanzlei", "agentur", "beratung", "klinik"}):
        return "service_business", "commercial"
    return "content_or_resource", "informational"


def _derive_services_or_products(headings: Sequence[str], keywords: Sequence[str], page_titles: Sequence[str]) -> List[str]:
    return _dedupe_preserve_order(list(headings[:6]) + list(page_titles[:4]) + list(keywords[:8]))[:12]


def _estimate_commerciality(text: str, headings: Sequence[str], keywords: Sequence[str]) -> int:
    joined = " ".join([text] + list(headings) + list(keywords)).lower()
    hits = sum(1 for term in COMMERCIAL_TERMS if term in joined)
    return min(100, hits * 12)


def _coerce_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_preserve_order(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        cleaned = str(item or "").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def _text_set(*values: Any) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        if isinstance(value, list):
            iterable: Iterable[str] = [str(item) for item in value]
        else:
            iterable = [str(value or "")]
        for item in iterable:
            for token in re.findall(r"\b[a-zA-ZäöüÄÖÜß]{3,}\b", item.lower()):
                if token in GERMAN_STOPWORDS:
                    continue
                tokens.add(token)
    return tokens
