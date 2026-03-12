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
PROFILE_VERSION = "v2"
PROFILE_MIN_PRIMARY_TEXT_CHARS = 220
PROFILE_NOISE_TAGS = (
    "script",
    "style",
    "noscript",
    "template",
    "svg",
    "header",
    "footer",
    "nav",
    "aside",
    "form",
    "dialog",
    "button",
    "input",
    "select",
    "textarea",
    "option",
    "label",
)
PROFILE_BOILERPLATE_LINK_TOKENS = {
    "account",
    "agb",
    "cart",
    "checkout",
    "contact",
    "datenschutz",
    "impressum",
    "konto",
    "kontakt",
    "kontaktformular",
    "login",
    "privacy",
    "register",
    "registrieren",
    "suche",
    "support",
    "terms",
    "warenkorb",
}

CONTEXT_KEYWORDS = {
    "health": {"augen", "behandlung", "ernaehrung", "gesundheit", "koerper", "medizin", "praevention", "schutz", "sicht", "symptome", "therapie", "vorsorge"},
    "safety": {"absicherung", "praevention", "risiko", "sicherheit", "schutz", "uv", "vorsicht", "warnzeichen"},
    "lifestyle": {"alltag", "ideen", "leben", "mode", "ratgeber", "stil", "trends"},
    "family_life": {"alltag", "baby", "eltern", "familie", "familien", "kinder", "partnerschaft", "schwangerschaft"},
    "parenting": {"baby", "eltern", "erziehung", "familie", "kinder", "kleinkind", "schule", "schwangerschaft"},
    "education": {"lernen", "schule", "bildung", "kita", "ausbildung", "studium"},
    "daily_routine": {"alltag", "routine", "organisation", "planung", "tipps", "haushalt"},
    "finance": {"kosten", "budget", "finanzierung", "sparen", "steuer", "versicherung"},
    "home": {"wohnen", "haus", "wohnung", "garten", "immobilien", "einrichten"},
    "real_estate": {"immobilie", "immobilien", "makler", "hausverkauf", "immobilienverkauf", "eigentum", "notar", "grundbuch", "wertermittlung", "expose", "miete", "kauf", "verkauf"},
    "productivity": {"produktiv", "effizienz", "planung", "workflow", "management"},
    "wellbeing": {"wohlbefinden", "balance", "stress", "entspannung", "mental"},
    "mobility": {"auto", "fahrt", "mobil", "mobilitaet", "reise", "reisen", "unterwegs", "verkehr"},
    "outdoor": {"ausflug", "draussen", "freizeit", "natur", "outdoor", "reise", "reisen", "sommer", "sonne", "urlaub"},
    "beauty": {"beauty", "haut", "kosmetik", "pflege", "stil"},
    "shopping": {"kaufen", "shop", "produkt", "preis", "vergleich", "online"},
}
SPECIALIZED_SELECTION_CONTEXTS = {"beauty", "education", "finance", "health", "mobility", "productivity", "real_estate", "shopping"}
GERMAN_STOPWORDS = {
    "aber", "alle", "als", "also", "am", "an", "auch", "auf", "aus", "bei", "bin", "bis", "das", "dass",
    "de", "dem", "den", "der", "des", "die", "doch", "ein", "eine", "einer", "eines", "er", "es", "für",
    "hat", "hier", "ich", "im", "in", "ist", "mit", "nach", "nicht", "nur", "oder", "sie", "sind", "so",
    "und", "uns", "von", "vor", "wie", "wir", "zu", "zum", "zur",
}
EXTRA_STOPWORDS = {
    "beim", "diese", "diesem", "dieser", "dieses", "durch", "einen", "einem", "einer", "erste", "erstes",
    "haben", "hilfreiche", "ihr", "ihre", "ihren", "ihres", "jede", "jeder", "jedes", "kein", "keine", "mehr",
    "muss", "mussen", "noch", "rund", "sehr", "sich", "sollte", "sollten", "thema", "themen", "unter",
    "viele", "vielen", "vom", "warum", "was", "welche", "welcher", "welches", "wenn", "weiter", "wird",
}
LOW_SIGNAL_TOKENS = {
    "allgemein", "artikel", "beitrag", "blog", "einfach", "forum", "home", "infos", "jetzt", "magazin",
    "menu", "navigation", "news", "online", "portal", "seite", "service", "start", "startseite", "suche",
    "thema", "themen", "tipps", "weiterlesen", "wissen",
}
BOILERPLATE_PHRASES = {
    "datenschutz", "impressum", "kontakt", "login", "registrieren", "warenkorb", "konto", "agb",
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


def derive_site_root_url(value: str) -> str:
    normalized = normalize_site_profile_url(value)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    host = (parsed.netloc or "").strip().lower().rstrip("/")
    if not host:
        return ""
    return f"{(parsed.scheme or 'https').lower()}://{host}"


def normalize_site_domain(value: str) -> str:
    normalized = normalize_site_profile_url(value)
    if normalized:
        parsed = urlparse(normalized)
        host = (parsed.netloc or "").strip().lower().rstrip(".")
        if host.startswith("www."):
            host = host[4:]
        return host
    raw = (value or "").strip().lower().rstrip(".")
    if raw.startswith("www."):
        raw = raw[4:]
    return raw


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
    keywords = _extract_weighted_keywords(pages, limit=18)
    contexts = _infer_contexts(
        keywords
        + headings
        + page_titles
        + meta_descriptions
        + _coerce_string_list((inventory_context or {}).get("site_categories"))
        + _coerce_string_list((inventory_context or {}).get("prominent_titles"))
    )
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
    if (
        existing is not None
        and isinstance(existing.payload, dict)
        and existing.payload
        and str(existing.profile_version or "").strip() == PROFILE_VERSION
    ):
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
    if (
        existing is not None
        and isinstance(existing.payload, dict)
        and existing.payload
        and str(existing.profile_version or "").strip() == PROFILE_VERSION
    ):
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


def build_combined_target_profile(
    *,
    target_site_url: str,
    target_site_root_url: Optional[str],
    exact_profile: Dict[str, Any],
    root_profile: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    root_payload = dict(root_profile or {})
    exact_payload = dict(exact_profile or {})
    normalized_target_url = normalize_site_profile_url(target_site_url)
    normalized_root_url = normalize_site_profile_url(target_site_root_url or "") or derive_site_root_url(target_site_url)

    def _merge_strings(*values: Any) -> str:
        for value in values:
            cleaned = str(value or "").strip()
            if cleaned:
                return cleaned
        return ""

    merged_topics = _dedupe_preserve_order(
        _coerce_string_list(root_payload.get("topics")) + _coerce_string_list(exact_payload.get("topics"))
    )
    merged_contexts = _dedupe_preserve_order(
        _coerce_string_list(root_payload.get("contexts")) + _coerce_string_list(exact_payload.get("contexts"))
    )
    merged_keywords = _dedupe_preserve_order(
        _coerce_string_list(exact_payload.get("repeated_keywords")) + _coerce_string_list(root_payload.get("repeated_keywords"))
    )
    merged_headings = _dedupe_preserve_order(
        _coerce_string_list(exact_payload.get("visible_headings")) + _coerce_string_list(root_payload.get("visible_headings"))
    )
    merged_titles = _dedupe_preserve_order(
        _coerce_string_list(exact_payload.get("sample_page_titles")) + _coerce_string_list(root_payload.get("sample_page_titles"))
    )
    merged_urls = _dedupe_preserve_order(
        [normalized_target_url]
        + _coerce_string_list(exact_payload.get("sample_urls"))
        + ([normalized_root_url] if normalized_root_url and normalized_root_url != normalized_target_url else [])
        + _coerce_string_list(root_payload.get("sample_urls"))
    )
    merged_services = _dedupe_preserve_order(
        _coerce_string_list(root_payload.get("services_or_products")) + _coerce_string_list(exact_payload.get("services_or_products"))
    )
    merged_clusters = _dedupe_preserve_order(
        _coerce_string_list(root_payload.get("topic_clusters")) + _coerce_string_list(exact_payload.get("topic_clusters"))
    )

    return {
        "normalized_url": normalized_target_url,
        "source_url": normalized_target_url,
        "site_root_url": normalized_root_url,
        "page_title": _merge_strings(exact_payload.get("page_title"), root_payload.get("page_title")),
        "meta_description": _merge_strings(exact_payload.get("meta_description"), root_payload.get("meta_description")),
        "visible_headings": merged_headings[:18],
        "repeated_keywords": merged_keywords[:18],
        "sample_page_titles": merged_titles[:8],
        "sample_urls": merged_urls[:8],
        "domain_level_topic": _merge_strings(root_payload.get("domain_level_topic"), exact_payload.get("domain_level_topic")),
        "primary_context": _merge_strings(root_payload.get("primary_context"), exact_payload.get("primary_context")),
        "topics": merged_topics[:18],
        "contexts": merged_contexts[:12],
        "content_tone": _merge_strings(exact_payload.get("content_tone"), root_payload.get("content_tone")),
        "content_style": _merge_strings(root_payload.get("content_style"), exact_payload.get("content_style")),
        "site_categories": _dedupe_preserve_order(
            _coerce_string_list(root_payload.get("site_categories")) + _coerce_string_list(exact_payload.get("site_categories"))
        )[:12],
        "topic_clusters": merged_clusters[:12],
        "prominent_titles": _dedupe_preserve_order(
            _coerce_string_list(root_payload.get("prominent_titles")) + _coerce_string_list(exact_payload.get("prominent_titles"))
        )[:8],
        "business_type": _merge_strings(root_payload.get("business_type"), exact_payload.get("business_type")),
        "services_or_products": merged_services[:18],
        "business_intent": _merge_strings(root_payload.get("business_intent"), exact_payload.get("business_intent"), "informational"),
        "commerciality": max(
            float(root_payload.get("commerciality") or 0),
            float(exact_payload.get("commerciality") or 0),
        ),
        "page_count": int(exact_payload.get("page_count") or 0) + int(root_payload.get("page_count") or 0),
        "profile_generated_at": _merge_strings(
            exact_payload.get("profile_generated_at"),
            root_payload.get("profile_generated_at"),
            datetime.now(timezone.utc).isoformat(),
        ),
        "profile_components": {
            "exact_url": normalized_target_url,
            "root_url": normalized_root_url,
            "used_root_profile": bool(root_payload),
        },
    }


def get_combined_target_profile(
    db: Session,
    *,
    target_site_url: str,
    target_site_root_url: Optional[str] = None,
    client_target_site_id: Optional[UUID] = None,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    force_refresh: bool = False,
) -> Tuple[Dict[str, Any], str, SiteProfileCache, Optional[SiteProfileCache]]:
    exact_record = ensure_target_site_profile(
        db,
        target_site_url=target_site_url,
        client_target_site_id=client_target_site_id,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
        force_refresh=force_refresh,
    )
    normalized_root_url = normalize_site_profile_url(target_site_root_url or "") or derive_site_root_url(target_site_url)
    exact_normalized_url = normalize_site_profile_url(target_site_url)
    root_record: Optional[SiteProfileCache] = None
    if normalized_root_url and normalized_root_url != exact_normalized_url:
        root_record = ensure_target_site_profile(
            db,
            target_site_url=normalized_root_url,
            client_target_site_id=client_target_site_id,
            timeout_seconds=timeout_seconds,
            max_pages=max_pages,
            force_refresh=force_refresh,
        )
    combined_payload = build_combined_target_profile(
        target_site_url=target_site_url,
        target_site_root_url=normalized_root_url,
        exact_profile=dict(exact_record.payload or {}),
        root_profile=dict(root_record.payload or {}) if root_record and isinstance(root_record.payload, dict) else None,
    )
    component_hashes = [
        str(exact_record.content_hash or "").strip(),
        str(root_record.content_hash or "").strip() if root_record is not None else "",
    ]
    combined_hash = build_site_profile_content_hash(
        {
            "exact_url": exact_normalized_url,
            "root_url": normalized_root_url,
            "component_hashes": component_hashes,
        }
    )
    return combined_payload, combined_hash, exact_record, root_record


def _expanded_profile_contexts(profile: Dict[str, Any]) -> List[str]:
    values = (
        _coerce_string_list(profile.get("topics"))
        + _coerce_string_list(profile.get("site_categories"))
        + _coerce_string_list(profile.get("topic_clusters"))
        + _coerce_string_list(profile.get("repeated_keywords"))
        + _coerce_string_list(profile.get("services_or_products"))
        + _coerce_string_list(profile.get("visible_headings"))
        + _coerce_string_list(profile.get("sample_page_titles"))
    )
    return _dedupe_preserve_order(
        _coerce_string_list(profile.get("contexts")) + _infer_contexts(values)
    )[:8]


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
    publishing_contexts = set(_expanded_profile_contexts(publishing_profile))
    target_contexts = set(_expanded_profile_contexts(target_profile))
    publishing_primary_context = str(publishing_profile.get("primary_context") or "").strip()
    target_primary_context = str(target_profile.get("primary_context") or "").strip()
    topic_overlap = len(publishing_topics & target_topics)
    context_overlap = len(publishing_contexts & target_contexts)
    score = topic_overlap * 8 + context_overlap * 16
    context_penalty = 0
    if publishing_primary_context and publishing_primary_context == target_primary_context:
        score += 12
    if publishing_primary_context in target_contexts:
        score += 6
    if (
        target_primary_context in SPECIALIZED_SELECTION_CONTEXTS
        and publishing_primary_context
        and publishing_primary_context != target_primary_context
    ):
        context_penalty += 10 if target_primary_context not in publishing_contexts else 4
    if target_profile.get("business_intent") == "commercial" and context_overlap == 0:
        score -= 4
    score -= context_penalty
    if score < 0:
        score = 0
    score = min(100, score)
    details = {
        "topic_overlap_terms": sorted((publishing_topics & target_topics))[:12],
        "context_overlap": sorted(publishing_contexts & target_contexts),
        "publishing_primary_context": publishing_primary_context,
        "target_primary_context": target_primary_context,
        "context_penalty": context_penalty,
        "publishing_topics_count": len(publishing_topics),
        "target_topics_count": len(target_topics),
    }
    return score, details


def compute_site_selection_score(
    *,
    publishing_profile: Dict[str, Any],
    target_profile: Dict[str, Any],
    inventory_context: Optional[Dict[str, Any]] = None,
    business_priority_weight: int = 0,
) -> Tuple[int, Dict[str, Any]]:
    semantic_score, semantic_details = score_publishing_site_fit(publishing_profile, target_profile)
    inventory = inventory_context or {}
    title_count = len(_coerce_string_list(inventory.get("prominent_titles")))
    category_count = len(_coerce_string_list(inventory.get("site_categories")))
    cluster_count = len(_coerce_string_list(inventory.get("topic_clusters")))
    target_primary_context = str(target_profile.get("primary_context") or "").strip()
    publishing_primary_context = str(publishing_profile.get("primary_context") or "").strip()
    publishing_contexts = set(_expanded_profile_contexts(publishing_profile))
    primary_context_mismatch = (
        target_primary_context in SPECIALIZED_SELECTION_CONTEXTS
        and publishing_primary_context
        and publishing_primary_context != target_primary_context
    )
    support_multiplier = 1.0
    if semantic_score < 24:
        support_multiplier = 0.45
    if primary_context_mismatch and target_primary_context not in publishing_contexts:
        support_multiplier = min(support_multiplier, 0.25)
    elif primary_context_mismatch:
        support_multiplier = min(support_multiplier, 0.65)
    authority_score = int(round(min(20, category_count * 2 + min(10, title_count // 3)) * max(0.5, support_multiplier)))
    target_terms = _text_set(
        target_profile.get("topics"),
        target_profile.get("services_or_products"),
        target_profile.get("repeated_keywords"),
    )
    support_terms = _text_set(
        inventory.get("prominent_titles"),
        inventory.get("site_categories"),
        inventory.get("topic_clusters"),
    )
    internal_link_support = int(round(min(15, len(target_terms & support_terms) * 3 + min(6, title_count // 5)) * support_multiplier))
    freshness_activity = min(10, min(6, title_count // 4) + min(4, cluster_count // 3))
    final_score = min(100, semantic_score + authority_score + internal_link_support + freshness_activity + max(0, business_priority_weight))
    details = {
        "semantic_score": semantic_score,
        "authority_score": authority_score,
        "internal_link_support": internal_link_support,
        "freshness_activity": freshness_activity,
        "business_priority_weight": max(0, business_priority_weight),
        "support_multiplier": support_multiplier,
        "primary_context_mismatch": primary_context_mismatch,
        "final_site_score": final_score,
        **semantic_details,
    }
    return final_score, details


def top_ranked_publishing_sites_for_target(
    db: Session,
    *,
    target_site_url: str,
    target_site_root_url: Optional[str] = None,
    candidate_sites: Sequence[Site],
    client_target_site_id: Optional[UUID] = None,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    min_score: int = 18,
    limit: int = 5,
    business_priority_weights: Optional[Dict[str, int]] = None,
) -> Tuple[Dict[str, Any], str, List[Dict[str, Any]]]:
    target_profile, target_profile_content_hash, _, _ = get_combined_target_profile(
        db,
        target_site_url=target_site_url,
        target_site_root_url=target_site_root_url,
        client_target_site_id=client_target_site_id,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
    )
    ranked: List[Dict[str, Any]] = []
    weights = business_priority_weights or {}
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
        inventory_context = build_publishing_inventory_context(db, site_id=site.id)
        publishing_profile = dict(publishing_profile_record.payload or {})
        score, details = compute_site_selection_score(
            publishing_profile=publishing_profile,
            target_profile=target_profile,
            inventory_context=inventory_context,
            business_priority_weight=int(weights.get(str(site.id), 0)),
        )
        if score < min_score:
            continue
        ranked.append(
            {
                "site_id": str(site.id),
                "site_url": site.site_url,
                "site_name": site.name,
                "score": score,
                "details": details,
                "profile": publishing_profile,
                "content_hash": str(publishing_profile_record.content_hash or "").strip(),
                "inventory_context": inventory_context,
            }
        )
    ranked.sort(key=lambda item: (-int(item.get("score") or 0), str(item.get("site_name") or "")))
    return target_profile, target_profile_content_hash, ranked[: max(1, limit)]


def select_best_publishing_site_for_target(
    db: Session,
    *,
    target_site_url: str,
    target_site_root_url: Optional[str] = None,
    candidate_sites: Sequence[Site],
    client_target_site_id: Optional[UUID] = None,
    timeout_seconds: int = 10,
    max_pages: int = 3,
    min_score: int = 18,
) -> Tuple[Optional[Site], Dict[str, Any], str, List[Dict[str, Any]]]:
    target_profile, target_profile_content_hash, _, _ = get_combined_target_profile(
        db,
        target_site_url=target_site_url,
        target_site_root_url=target_site_root_url,
        client_target_site_id=client_target_site_id,
        timeout_seconds=timeout_seconds,
        max_pages=max_pages,
    )
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
        return None, target_profile, target_profile_content_hash, ranked[:8]
    selected = next((site for site in candidate_sites if str(site.id) == str(best.get("site_id"))), None)
    return selected, target_profile, target_profile_content_hash, ranked[:8]


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
    soup = _extract_profile_content_fragment(html or "")
    base_host = urlparse(base_url).netloc.lower()
    scored_links: List[Tuple[float, int, str]] = []
    seen_links: set[str] = set()
    for index, anchor in enumerate(soup.find_all("a", href=True)):
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
        if normalized in seen_links:
            continue
        score = _score_profile_internal_link_candidate(
            anchor_text=re.sub(r"\s+", " ", anchor.get_text(" ", strip=True)).strip(),
            absolute_url=normalized,
        )
        if score <= 0:
            continue
        seen_links.add(normalized)
        scored_links.append((score, index, normalized))
    ranked = sorted(scored_links, key=lambda item: (-item[0], item[1], item[2]))
    return [url for _score, _index, url in ranked[:limit]]


def _extract_page_signals(url: str, html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "lxml")
    title = (soup.title.string or "").strip() if soup.title and soup.title.string else ""
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    meta_description = (meta.get("content") or "").strip() if meta else ""
    content_root = _extract_profile_content_fragment(html or "")
    headings: List[str] = []
    for tag_name in ("h1", "h2", "h3"):
        for tag in content_root.find_all(tag_name):
            text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True)).strip()
            if text:
                headings.append(text)
    text = re.sub(r"\s+", " ", content_root.get_text(" ", strip=True)).strip()
    return {
        "url": normalize_site_profile_url(url),
        "title": title,
        "meta_description": meta_description,
        "headings": headings[:18],
        "text": text[:12000],
    }


def _strip_profile_noise(container: BeautifulSoup) -> BeautifulSoup:
    for tag in container(PROFILE_NOISE_TAGS):
        tag.decompose()
    return container


def _extract_profile_content_fragment(html: str) -> BeautifulSoup:
    base = BeautifulSoup(html or "", "lxml")
    _strip_profile_noise(base)
    body = base.body or base
    candidates: List[Tuple[int, str]] = []
    for source in (
        body.find("main"),
        body.find("article"),
        body.find(attrs={"role": "main"}),
        body,
    ):
        if source is None:
            continue
        fragment = BeautifulSoup(str(source), "lxml")
        root = _strip_profile_noise(fragment.body or fragment)
        text = re.sub(r"\s+", " ", root.get_text(" ", strip=True)).strip()
        if text:
            candidates.append((len(text), str(root)))
            if source is not body and len(text) >= PROFILE_MIN_PRIMARY_TEXT_CHARS:
                return root
    if candidates:
        best_html = max(candidates, key=lambda item: item[0])[1]
        best = BeautifulSoup(best_html, "lxml")
        return best.body or best
    fallback = BeautifulSoup("", "lxml")
    return fallback


def _profile_link_tokens(value: str) -> List[str]:
    tokens: List[str] = []
    for token in re.findall(r"\b[a-zA-ZäöüÄÖÜß-]{3,}\b", (value or "").lower()):
        cleaned = _normalize_signal_token(token)
        if cleaned:
            tokens.append(cleaned)
    return tokens


def _score_profile_internal_link_candidate(*, anchor_text: str, absolute_url: str) -> float:
    parsed = urlparse(absolute_url)
    path_tokens = [
        token
        for token in _profile_link_tokens((parsed.path or "").replace("/", " "))
        if token
    ]
    anchor_tokens = [token for token in _profile_link_tokens(anchor_text) if token]
    combined_tokens = set(path_tokens + anchor_tokens)
    signal_tokens = {token for token in combined_tokens if _is_signal_token(token)}
    boilerplate_hits = combined_tokens & PROFILE_BOILERPLATE_LINK_TOKENS
    if boilerplate_hits and len(signal_tokens - boilerplate_hits) <= 1:
        return -1.0
    if not signal_tokens:
        return -1.0
    depth = len([segment for segment in (parsed.path or "").split("/") if segment.strip()])
    score = 3.0 * len(signal_tokens) + min(1.5, depth * 0.5)
    if anchor_text:
        score += min(1.5, len(anchor_tokens) * 0.3)
    score -= 1.6 * len(boilerplate_hits - signal_tokens)
    return score


def _normalize_signal_token(value: str) -> str:
    return re.sub(r"[^a-zA-ZäöüÄÖÜß]", "", (value or "").lower()).strip()


def _is_signal_token(token: str) -> bool:
    cleaned = _normalize_signal_token(token)
    if len(cleaned) < 4:
        return False
    if cleaned in GERMAN_STOPWORDS or cleaned in EXTRA_STOPWORDS or cleaned in LOW_SIGNAL_TOKENS:
        return False
    if cleaned in BOILERPLATE_PHRASES:
        return False
    return True


def _extract_keywords(text: str, *, limit: int) -> List[str]:
    tokens = re.findall(r"\b[a-zA-ZäöüÄÖÜß]{3,}\b", (text or "").lower())
    counter = Counter(_normalize_signal_token(token) for token in tokens if _is_signal_token(token))
    return [token for token, _ in counter.most_common(limit)]


def _extract_weighted_keywords(pages: Sequence[Dict[str, Any]], *, limit: int) -> List[str]:
    counter: Counter[str] = Counter()
    for page in pages:
        for token in _extract_keywords(str(page.get("title") or ""), limit=12):
            counter[token] += 4
        for token in _extract_keywords(str(page.get("meta_description") or ""), limit=12):
            counter[token] += 2
        seen_heading_tokens: set[str] = set()
        for heading in page.get("headings") or []:
            for token in _extract_keywords(str(heading), limit=10):
                if token in seen_heading_tokens:
                    continue
                seen_heading_tokens.add(token)
                counter[token] += 2
        for token in _extract_keywords(str(page.get("text") or ""), limit=24):
            counter[token] += 1
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
                if not _is_signal_token(token):
                    continue
                tokens.add(_normalize_signal_token(token))
    return tokens
