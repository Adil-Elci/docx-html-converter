from __future__ import annotations

import datetime
import hashlib
import json
import logging
import math
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from .llm import LLMError, call_llm_json, call_llm_text
from .trend_cache import (
    get_keyword_trend_cache_entry,
    get_keyword_trend_cache_family_entries,
    record_keyword_trend_cache_hit,
    upsert_keyword_trend_cache_entry,
)
from .validators import (
    count_h2,
    validate_backlink_placement,
    validate_word_count,
    word_count_from_html,
)
from .web import (
    extract_internal_links,
    extract_canonical_link,
    extract_meta_content,
    extract_page_title,
    fetch_url,
    sanitize_html,
)

logger = logging.getLogger("creator.pipeline")

DEFAULT_LLM_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_LLM_MODEL = "gpt-4.1-mini"
DEFAULT_ANTHROPIC_PLANNING_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_WRITING_MODEL = "claude-sonnet-4-6"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_HTTP_RETRIES = 0
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1024
DEFAULT_IMAGE_HEIGHT = 576
DEFAULT_POLL_SECONDS = 2
DEFAULT_POLL_TIMEOUT_SECONDS = 90
PHASE1_CACHE_PROMPT_VERSION = "v3"
PHASE2_CACHE_PROMPT_VERSION = "v3"
PAIR_FIT_PROMPT_VERSION = "v3"
DEFAULT_SITE_ANALYSIS_MAX_PAGES = 4
DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS = 1400
SEO_TITLE_MIN_CHARS = 45
SEO_TITLE_MAX_CHARS = 68
SEO_DESCRIPTION_MIN_CHARS = 120
SEO_DESCRIPTION_MAX_CHARS = 160
SEO_SLUG_MAX_CHARS = 80
INTERNAL_LINK_ANCHOR_MIN_UNIQUE = 2

NEGATIVE_PROMPT = "text, watermark, logo, letters, UI, low quality, blurry, deformed"

STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "your", "you", "our", "are", "was", "were",
    "what", "when", "where", "why", "how", "who", "about", "into", "out", "over", "under", "between", "while",
    "their", "they", "them", "these", "those", "also", "but", "not", "can", "will", "just", "than", "then",
    "such", "use", "using", "used", "more", "most", "some", "any", "each", "other", "its", "it's", "our", "we",
}

GERMAN_FUNCTION_WORDS = {
    "der", "die", "das", "und", "ist", "nicht", "mit", "auf", "von", "für", "im", "in", "den", "dem", "ein",
    "eine", "als", "auch", "bei", "zu", "des", "sich", "dass", "zum", "zur", "am", "an", "oder", "wie", "wird",
}

ENGLISH_FUNCTION_WORDS = {
    "the", "and", "is", "are", "with", "for", "this", "that", "to", "of", "in", "on", "as", "be", "by", "an",
    "or", "from", "it", "at", "we", "you", "your", "our", "has", "have", "was", "were", "will", "can",
}
PAIR_FIT_EXTRA_STOPWORDS = {
    "alle", "alles", "beim", "bereits", "beste", "besten", "diese", "diesem", "dieser", "dieses", "durch",
    "einfach", "einen", "einem", "einer", "eines", "erste", "erstes", "etwa", "fuer", "gegen", "genau",
    "haben", "hilfreiche", "ihr", "ihre", "ihren", "ihres", "jede", "jeder", "jedes", "jetzt", "kein", "keine",
    "mehr", "muss", "mussen", "nachdem", "naechste", "noch", "rund", "sehr", "sich", "sollte", "sollten",
    "thema", "themen", "unsere", "unter", "viele", "vielen", "vom", "warum", "was", "welche", "welcher",
    "welches", "wenn", "weiter", "weiterlesen", "wird", "wurden", "zeigt", "zeigen", "zwischen",
}
PAIR_FIT_BOILERPLATE_TOKENS = {
    "artikel", "beitrag", "beitraege", "blog", "cookie", "datenschutz", "entdecken", "forum", "hilfe", "home",
    "impressum", "jetzt", "kategorie", "kategorien", "login", "magazin", "mehr", "menu", "navigation", "news",
    "online", "portal", "registrieren", "service", "shop", "start", "startseite", "suche", "tag", "tags",
    "uebersicht", "weiterlesen",
}
PAIR_FIT_PROMO_TOKENS = {
    "angebot", "angebote", "bestellen", "guenstig", "kaufen", "marke", "marken", "preis", "preise", "rabatt",
    "sale", "shop", "sofort", "versand",
}
PAIR_FIT_INFORMATIONAL_CUES = {
    "alltag", "antworten", "anleitung", "aufpassen", "beachten", "checkliste", "einordnung", "erklaert", "hilfe",
    "hinweise", "orientierung", "praevention", "praxis", "ratgeber", "sicherheit", "schutz", "tipps", "wissen",
    "worauf",
}
PAIR_FIT_CONTEXT_KEYWORDS = {
    "health": {"arzt", "augen", "behandlung", "ernaehrung", "gesundheit", "koerper", "medizin", "praevention", "schutz", "sicht", "symptome", "therapie", "vorsorge"},
    "safety": {"absicherung", "sicherheit", "schutz", "uv", "vorsicht", "warnung", "warnzeichen", "praevention", "risiko"},
    "lifestyle": {"alltag", "ideen", "leben", "lifestyle", "mode", "ratgeber", "stil", "trends"},
    "family_life": {"alltag", "baby", "eltern", "familie", "familien", "haushalt", "kinder", "partnerschaft", "schwangerschaft"},
    "parenting": {"baby", "eltern", "erziehung", "familie", "kinder", "kleinkind", "schule", "schwangerschaft"},
    "home": {"garten", "haus", "haushalt", "wohnen", "wohnung"},
    "finance": {"budget", "finanzierung", "kosten", "preis", "preise", "sparen", "versicherung"},
    "education": {"bildung", "kita", "lernen", "schule", "wissen"},
    "wellbeing": {"balance", "entspannung", "mental", "ruhe", "stress", "wohlbefinden"},
    "mobility": {"auto", "fahrt", "mobil", "mobilitaet", "reise", "reisen", "unterwegs", "verkehr"},
    "outdoor": {"ausflug", "draussen", "freizeit", "natur", "outdoor", "reise", "reisen", "sommer", "sonne", "urlaub"},
    "productivity": {"organisation", "planung", "produktiv", "routine", "workflow"},
    "beauty": {"beauty", "haut", "kosmetik", "pflege", "stil"},
    "shopping": {"angebot", "bestellen", "kaufen", "marke", "preis", "preise", "produkt", "produkte", "shop", "vergleich"},
}
PAIR_FIT_CONTEXT_LABELS = {
    "health": "gesundheitlichen Fragen",
    "safety": "Sicherheit im Alltag",
    "lifestyle": "alltagsnahen Entscheidungen",
    "family_life": "Familienalltag",
    "parenting": "Elternalltag",
    "home": "Zuhause",
    "finance": "Kosten und Entscheidungen",
    "education": "Orientierung und Lernen",
    "wellbeing": "Wohlbefinden",
    "mobility": "unterwegs",
    "outdoor": "Aktivitaeten im Freien",
    "productivity": "Organisation im Alltag",
    "beauty": "Pflege und Stil",
    "shopping": "Auswahl und Orientierung",
}
PAIR_FIT_CONTEXT_AUDIENCES = {
    "family_life": "Eltern und Familien",
    "parenting": "Eltern und Bezugspersonen",
    "education": "Lernende und Familien",
    "health": "gesundheitsbewusste Leserinnen und Leser",
    "wellbeing": "achtsame Leserinnen und Leser",
    "home": "Haushalte",
    "finance": "preisbewusste Leserinnen und Leser",
    "mobility": "Menschen unterwegs",
    "outdoor": "aktive Leserinnen und Leser",
    "beauty": "pflegebewusste Leserinnen und Leser",
    "productivity": "organisierte Teams und Einzelpersonen",
}
PAIR_FIT_AUDIENCE_TOKENS = {
    "baby", "babys", "eltern", "familie", "familien", "kinder", "kundinnen", "kunden", "leser",
    "leserinnen", "menschen", "nutzer", "patienten", "schueler", "schwangere", "teams",
}
PAIR_FIT_CANDIDATE_COUNT = 5

GENERIC_CONCLUSION_PHRASES = (
    "this article has examined the key factors",
    "the evidence presented demonstrates",
    "further investigation and analysis remain necessary",
    "moving forward, stakeholders must prioritize",
    "ultimately, a multifaceted approach",
    "addressing the challenges and opportunities presented by this subject matter",
)

KEYWORD_MIN_SECONDARY = 4
KEYWORD_MAX_SECONDARY = 6
KEYWORD_MIN_WORDS = 2
KEYWORD_MAX_WORDS = 8
DEFAULT_INTERNAL_LINK_MIN = 2
DEFAULT_INTERNAL_LINK_MAX = 4
KEYWORD_MAX_FAQ = 5
ARTICLE_MIN_H2 = 4
ARTICLE_MAX_H2 = 6
GOOGLE_SUGGEST_CACHE_TTL_SECONDS = 6 * 60 * 60
GOOGLE_SUGGEST_CACHE_MAX_ENTRIES = 256
DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
FAQ_MIN_QUESTIONS = 3
FAQ_MIN_WORDS = 80
ARTICLE_MIN_WORDS = 500
ARTICLE_MAX_WORDS = 1200
KEYWORD_LOW_SIGNAL_TOKENS = {
    "aktuell", "aktuelle", "aktuellen", "allgemein", "beitrag", "beitraege", "beliebt", "beliebte",
    "entdecken", "hilfe", "hilfreich", "infos", "magazin", "mehr", "ratgeber", "spannend", "spannende",
    "thema", "themen", "tipps", "wissen", "wertvolle",
}

GERMAN_KEYWORD_MODIFIERS = (
    "tipps",
    "ratgeber",
    "checkliste",
    "hilfe",
    "erfahrungen",
    "ursachen",
    "auswirkungen",
)

GERMAN_QUESTION_PREFIXES = (
    "was ist",
    "wie",
    "wann",
    "warum",
    "welche",
    "welcher",
    "welches",
    "wo",
    "woran",
    "kann",
    "darf",
)

GOOGLE_SUGGEST_CACHE: Dict[str, Dict[str, Any]] = {}
STRUCTURED_LIST_HINTS = {"tipps", "checkliste", "schritte", "anleitung", "symptome", "ursachen"}
STRUCTURED_TABLE_HINTS = {"vergleich", "kosten", "unterschied", "vs", "tabelle"}


class CreatorError(RuntimeError):
    pass


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _read_non_negative_int_env(name: str, default: int) -> int:
    return max(0, _read_int_env(name, default))


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()


def _limit_text(value: str, max_chars: int) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if max_chars <= 0 or len(cleaned) <= max_chars:
        return cleaned
    clipped = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return clipped or cleaned[:max_chars].strip()


def _merge_string_lists(*value_lists: List[str], max_items: int) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for values in value_lists:
        for value in values:
            cleaned = re.sub(r"\s+", " ", str(value or "").strip())
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
            if len(out) >= max_items:
                return out
    return out


def _extract_inventory_categories(items: List[Dict[str, Any]], *, max_items: int = 10) -> List[str]:
    categories: List[str] = []
    for item in items:
        raw = item.get("categories")
        if not isinstance(raw, list):
            continue
        categories.extend(str(category).strip() for category in raw if str(category).strip())
    return _merge_string_lists(categories, max_items=max_items)


def _extract_inventory_topic_clusters(items: List[Dict[str, Any]], *, max_items: int = 8) -> List[str]:
    scores: Dict[str, int] = {}
    for item in items:
        title = _normalize_keyword_phrase(str(item.get("title") or ""))
        categories = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        for category in categories:
            normalized_category = _normalize_keyword_phrase(category)
            if normalized_category:
                scores[normalized_category] = scores.get(normalized_category, 0) + 4
        words = title.split()
        for size in (2, 3):
            for index in range(0, max(0, len(words) - size + 1)):
                phrase = " ".join(words[index : index + size]).strip()
                if not _is_valid_keyword_phrase(phrase):
                    continue
                scores[phrase] = scores.get(phrase, 0) + (3 if size == 2 else 2)
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [phrase for phrase, _score in ranked[:max_items]]


def _build_inventory_topic_insights(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    categories = _extract_inventory_categories(items)
    topic_clusters = _extract_inventory_topic_clusters(items)
    prominent_titles = _merge_string_lists(
        [str(item.get("title") or "").strip() for item in items if str(item.get("title") or "").strip()],
        max_items=8,
    )
    internal_linking_opportunities = []
    for item in items[:8]:
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        categories_for_item = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        if categories_for_item:
            internal_linking_opportunities.append(f"{categories_for_item[0]} -> {title}")
        else:
            internal_linking_opportunities.append(title)
    return {
        "site_categories": categories,
        "topic_clusters": topic_clusters,
        "prominent_titles": prominent_titles,
        "internal_linking_opportunities": _merge_string_lists(internal_linking_opportunities, max_items=10),
    }


def _structured_content_mode(topic: str, primary_keyword: str, search_intent_type: str) -> str:
    normalized = _normalize_keyword_phrase(f"{topic} {primary_keyword}")
    tokens = set(normalized.split())
    if tokens & STRUCTURED_TABLE_HINTS or (search_intent_type or "").strip().lower() == "commercial":
        return "table"
    if tokens & STRUCTURED_LIST_HINTS:
        return "list"
    return "none"


def _format_title_case(value: str) -> str:
    words = [word for word in re.split(r"\s+", (value or "").strip()) if word]
    out: List[str] = []
    for word in words:
        if word.isupper():
            out.append(word)
        else:
            out.append(word[:1].upper() + word[1:])
    return " ".join(out)


def _truncate_title(value: str, *, max_chars: int = SEO_TITLE_MAX_CHARS) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if len(cleaned) <= max_chars:
        return cleaned
    clipped = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return clipped or cleaned[:max_chars].strip()


def _build_deterministic_title_package(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    search_intent_type: str,
    structured_mode: str,
    current_year: int,
) -> Dict[str, str]:
    keyword_title = _format_title_case(primary_keyword or topic or "Ratgeber")
    secondary_hint = _format_title_case(secondary_keywords[0]) if secondary_keywords else ""
    normalized_topic = _normalize_keyword_phrase(topic)
    include_year = "checkliste" in normalized_topic or "trend" in normalized_topic
    if structured_mode == "table":
        suffix = "Vergleich und Orientierung"
    elif structured_mode == "list":
        suffix = "Checkliste und Tipps" if "checkliste" in normalized_topic else "Tipps und Orientierung"
    elif (search_intent_type or "").strip().lower() == "commercial":
        suffix = "Vergleich, Kosten und Tipps"
    else:
        suffix = "Einordnung und Hilfe"
    if secondary_hint and _keyword_similarity(secondary_hint, keyword_title) < 0.55:
        suffix = secondary_hint
    h1 = _truncate_title(f"{keyword_title}: {suffix}")
    if len(h1) < SEO_TITLE_MIN_CHARS:
        h1 = _truncate_title(f"{h1} fuer Betroffene und Familien")
    if include_year and str(current_year) not in h1:
        h1 = _truncate_title(f"{h1} {current_year}")
    meta_title = _truncate_title(h1)
    slug_seed = primary_keyword or topic
    slug = _derive_slug(slug_seed)[:SEO_SLUG_MAX_CHARS]
    return {"h1": h1, "meta_title": meta_title, "slug": slug}


def _build_deterministic_meta_description(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    structured_mode: str,
) -> str:
    opening = _format_title_case(primary_keyword or topic or "Ratgeber")
    if structured_mode == "table":
        suffix = "mit Vergleich, Einordnung und klaren Unterschieden."
    elif structured_mode == "list":
        suffix = "mit Checkliste, Tipps und klaren Schritten."
    else:
        suffix = "mit Einordnung, Tipps und konkreten Hinweisen."
    supporting = ""
    if secondary_keywords:
        supporting = f" Fokus auf {_format_title_case(secondary_keywords[0])}."
    description = _truncate_title(f"{opening} {suffix}{supporting}", max_chars=SEO_DESCRIPTION_MAX_CHARS)
    if len(description) < SEO_DESCRIPTION_MIN_CHARS and secondary_keywords[1:2]:
        description = _truncate_title(
            f"{description} Auch {_format_title_case(secondary_keywords[1])} wird kompakt erklaert.",
            max_chars=SEO_DESCRIPTION_MAX_CHARS,
        )
    return description


def _serialize_site_snapshot_pages(pages: List[Dict[str, str]]) -> str:
    lines: List[str] = []
    for page in pages:
        title = str(page.get("title") or "").strip()
        text = _limit_text(str(page.get("text") or "").strip(), DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS)
        if not text:
            continue
        lines.append(f"URL: {str(page.get('url') or '').strip()}")
        if title:
            lines.append(f"Titel: {title}")
        lines.append(f"Inhalt: {text}")
    return "\n".join(lines).strip()


def _build_site_snapshot(
    *,
    site_url: str,
    homepage_html: str,
    candidate_urls: List[str],
    purpose_prefix: str,
    warnings: List[str],
    debug: Dict[str, Any],
    timeout_seconds: int,
    retries: int,
    max_pages: int,
) -> Dict[str, Any]:
    pages: List[Dict[str, str]] = []
    normalized_site_url = _normalize_url(site_url)
    homepage_text = sanitize_html(homepage_html)
    if homepage_text:
        pages.append(
            {
                "url": normalized_site_url,
                "title": extract_page_title(homepage_html),
                "text": _limit_text(homepage_text, DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS),
            }
        )

    normalized_candidates = _normalize_internal_link_candidates(
        candidate_urls,
        publishing_site_url=site_url,
        backlink_url="",
        max_items=max(0, max_pages - len(pages)),
    )
    for index, candidate_url in enumerate(normalized_candidates, start=1):
        if len(pages) >= max_pages:
            break
        if _normalize_url(candidate_url) == normalized_site_url:
            continue
        candidate_html = fetch_url(
            candidate_url,
            purpose=f"{purpose_prefix}_{index}",
            warnings=warnings,
            debug=debug,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
        candidate_text = sanitize_html(candidate_html)
        if not candidate_text:
            continue
        pages.append(
            {
                "url": _normalize_url(candidate_url),
                "title": extract_page_title(candidate_html),
                "text": _limit_text(candidate_text, DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS),
            }
        )

    serialized = _serialize_site_snapshot_pages(pages)
    page_titles = [str(page.get("title") or "").strip() for page in pages if str(page.get("title") or "").strip()]
    sample_urls = [str(page.get("url") or "").strip() for page in pages if str(page.get("url") or "").strip()]
    summary = " | ".join(page_titles[:4])
    return {
        "pages": pages,
        "content_hash": _hash_text(serialized),
        "combined_text": serialized,
        "site_summary": summary,
        "sample_page_titles": page_titles[:8],
        "sample_urls": sample_urls[:8],
    }


def _merge_phase2_analysis(
    current: Dict[str, Any],
    cached: Optional[Dict[str, Any]],
    *,
    inventory_categories: List[str],
) -> Dict[str, Any]:
    if not cached:
        current["allowed_topics"] = _merge_string_lists(current.get("allowed_topics") or [], max_items=12)
        current["content_style_constraints"] = _merge_string_lists(
            current.get("content_style_constraints") or [],
            max_items=6,
        )
        current["internal_linking_opportunities"] = _merge_string_lists(
            current.get("internal_linking_opportunities") or [],
            max_items=10,
        )
        current["site_categories"] = _merge_string_lists(
            current.get("site_categories") or [],
            inventory_categories,
            max_items=10,
        )
        current["topic_clusters"] = _merge_string_lists(current.get("topic_clusters") or [], max_items=8)
        current["prominent_titles"] = _merge_string_lists(current.get("prominent_titles") or [], max_items=8)
        current["sample_page_titles"] = _merge_string_lists(current.get("sample_page_titles") or [], max_items=8)
        current["sample_urls"] = _merge_string_lists(current.get("sample_urls") or [], max_items=8)
        current["site_summary"] = str(current.get("site_summary") or "").strip()
        return current

    current["allowed_topics"] = _merge_string_lists(
        current.get("allowed_topics") or [],
        cached.get("allowed_topics") or [],
        max_items=12,
    )
    current["content_style_constraints"] = _merge_string_lists(
        current.get("content_style_constraints") or [],
        cached.get("content_style_constraints") or [],
        max_items=6,
    )
    current["internal_linking_opportunities"] = _merge_string_lists(
        current.get("internal_linking_opportunities") or [],
        cached.get("internal_linking_opportunities") or [],
        max_items=10,
    )
    current["site_categories"] = _merge_string_lists(
        current.get("site_categories") or [],
        cached.get("site_categories") or [],
        inventory_categories,
        max_items=10,
    )
    current["topic_clusters"] = _merge_string_lists(
        current.get("topic_clusters") or [],
        cached.get("topic_clusters") or [],
        max_items=8,
    )
    current["prominent_titles"] = _merge_string_lists(
        current.get("prominent_titles") or [],
        cached.get("prominent_titles") or [],
        max_items=8,
    )
    current["sample_page_titles"] = _merge_string_lists(
        current.get("sample_page_titles") or [],
        cached.get("sample_page_titles") or [],
        max_items=8,
    )
    current["sample_urls"] = _merge_string_lists(
        current.get("sample_urls") or [],
        cached.get("sample_urls") or [],
        max_items=8,
    )
    current["site_summary"] = str(current.get("site_summary") or "").strip() or str(cached.get("site_summary") or "").strip()
    return current


def _model_prefers_anthropic(*models: str) -> bool:
    return any((model or "").strip().lower().startswith("claude") for model in models)


def _coerce_phase2_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    allowed_topics = value.get("allowed_topics")
    content_style_constraints = value.get("content_style_constraints")
    internal_linking_opportunities = value.get("internal_linking_opportunities")
    if not isinstance(allowed_topics, list) or not isinstance(content_style_constraints, list):
        return None
    return {
        "allowed_topics": [str(item).strip() for item in allowed_topics if str(item).strip()],
        "content_style_constraints": [str(item).strip() for item in content_style_constraints if str(item).strip()],
        "internal_linking_opportunities": [
            str(item).strip() for item in (internal_linking_opportunities or []) if str(item).strip()
        ],
        "site_summary": str(value.get("site_summary") or "").strip(),
        "site_categories": [str(item).strip() for item in (value.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (value.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (value.get("prominent_titles") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
    }


def _coerce_phase1_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    brand_name = str(value.get("brand_name") or "").strip()
    backlink_url = str(value.get("backlink_url") or "").strip()
    anchor_type = str(value.get("anchor_type") or "").strip()
    keyword_cluster = value.get("keyword_cluster")
    if anchor_type not in {"brand", "contextual_generic", "partial_match"}:
        return None
    if not isinstance(keyword_cluster, list):
        return None
    return {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": [str(item).strip() for item in keyword_cluster if str(item).strip()],
        "site_summary": str(value.get("site_summary") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
    }


def _coerce_site_profile_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    normalized_url = str(value.get("normalized_url") or "").strip()
    topics = [str(item).strip() for item in (value.get("topics") or []) if str(item).strip()]
    contexts = [str(item).strip() for item in (value.get("contexts") or []) if str(item).strip()]
    if not normalized_url and not topics and not contexts:
        return None
    return {
        "normalized_url": normalized_url,
        "page_title": str(value.get("page_title") or "").strip(),
        "meta_description": str(value.get("meta_description") or "").strip(),
        "visible_headings": [str(item).strip() for item in (value.get("visible_headings") or []) if str(item).strip()],
        "repeated_keywords": [str(item).strip() for item in (value.get("repeated_keywords") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
        "domain_level_topic": str(value.get("domain_level_topic") or "").strip(),
        "primary_context": str(value.get("primary_context") or "").strip(),
        "topics": topics,
        "contexts": contexts,
        "content_tone": str(value.get("content_tone") or "").strip(),
        "content_style": [str(item).strip() for item in (value.get("content_style") or []) if str(item).strip()],
        "site_categories": [str(item).strip() for item in (value.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (value.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (value.get("prominent_titles") or []) if str(item).strip()],
        "business_type": str(value.get("business_type") or "").strip(),
        "services_or_products": [str(item).strip() for item in (value.get("services_or_products") or []) if str(item).strip()],
        "business_intent": str(value.get("business_intent") or "").strip(),
        "commerciality": int(value.get("commerciality") or 0),
    }


def _coerce_pair_fit_topic_candidates(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    candidates: List[Dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            topic = str(item.get("topic") or "").strip()
            if not topic:
                continue
            candidates.append(
                {
                    "topic": topic,
                    "publishing_site_relevance": max(0, min(10, int(item.get("publishing_site_relevance") or 0))),
                    "backlink_naturalness": max(0, min(10, int(item.get("backlink_naturalness") or 0))),
                    "informational_value": max(0, min(10, int(item.get("informational_value") or 0))),
                    "seo_plausibility": max(0, min(10, int(item.get("seo_plausibility") or 0))),
                    "non_spamminess": max(0, min(10, int(item.get("non_spamminess") or 0))),
                    "target_site_relevance": max(0, min(10, int(item.get("target_site_relevance") or 0))),
                    "spam_risk": max(0, min(10, int(item.get("spam_risk") or 0))),
                    "total_score": max(0, min(50, int(item.get("total_score") or 0))),
                    "backlink_angle": str(item.get("backlink_angle") or "").strip(),
                    "score_breakdown": item.get("score_breakdown") if isinstance(item.get("score_breakdown"), dict) else {},
                }
            )
            continue
        topic = str(item or "").strip()
        if topic:
            candidates.append(
                {
                    "topic": topic,
                    "publishing_site_relevance": 0,
                    "backlink_naturalness": 0,
                    "informational_value": 0,
                    "seo_plausibility": 0,
                    "non_spamminess": 0,
                    "target_site_relevance": 0,
                    "spam_risk": 10,
                    "total_score": 0,
                    "backlink_angle": "",
                    "score_breakdown": {},
                }
            )
    return candidates[:5]


def _pair_fit_candidate_topics(candidates: List[Dict[str, Any]]) -> List[str]:
    return [str(item.get("topic") or "").strip() for item in candidates if str(item.get("topic") or "").strip()]


def _compact_pair_fit_profile(profile: Dict[str, Any], *, site_kind: str) -> Dict[str, Any]:
    compact = {
        "normalized_url": str(profile.get("normalized_url") or "").strip(),
        "page_title": str(profile.get("page_title") or "").strip(),
        "meta_description": str(profile.get("meta_description") or "").strip(),
        "domain_level_topic": str(profile.get("domain_level_topic") or "").strip(),
        "primary_context": str(profile.get("primary_context") or "").strip(),
        "topics": [str(item).strip() for item in (profile.get("topics") or []) if str(item).strip()][:8],
        "contexts": [str(item).strip() for item in (profile.get("contexts") or []) if str(item).strip()][:6],
        "visible_headings": [str(item).strip() for item in (profile.get("visible_headings") or []) if str(item).strip()][:6],
        "repeated_keywords": [str(item).strip() for item in (profile.get("repeated_keywords") or []) if str(item).strip()][:8],
    }
    if site_kind == "publishing":
        compact["content_style"] = [str(item).strip() for item in (profile.get("content_style") or []) if str(item).strip()][:5]
        compact["site_categories"] = [str(item).strip() for item in (profile.get("site_categories") or []) if str(item).strip()][:6]
        compact["topic_clusters"] = [str(item).strip() for item in (profile.get("topic_clusters") or []) if str(item).strip()][:6]
        compact["prominent_titles"] = [str(item).strip() for item in (profile.get("prominent_titles") or []) if str(item).strip()][:5]
    else:
        compact["business_type"] = str(profile.get("business_type") or "").strip()
        compact["services_or_products"] = [
            str(item).strip() for item in (profile.get("services_or_products") or []) if str(item).strip()
        ][:8]
        compact["business_intent"] = str(profile.get("business_intent") or "").strip()
        compact["site_root_url"] = str(profile.get("site_root_url") or "").strip()
    return compact


def _phase1_from_target_profile(profile: Dict[str, Any], *, target_site_url: str) -> Dict[str, Any]:
    brand_name = str(profile.get("page_title") or "").strip()
    if not brand_name:
        brand_name = _guess_brand_name(target_site_url, "")
    keyword_cluster = _merge_string_lists(
        profile.get("repeated_keywords") or [],
        profile.get("topics") or [],
        profile.get("services_or_products") or [],
        max_items=10,
    )
    anchor_type = "brand" if brand_name else ("partial_match" if keyword_cluster else "contextual_generic")
    return {
        "brand_name": brand_name,
        "backlink_url": target_site_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
        "site_summary": str(profile.get("meta_description") or profile.get("domain_level_topic") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (profile.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (profile.get("sample_urls") or []) if str(item).strip()],
    }


def _phase2_from_publishing_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    allowed_topics = _merge_string_lists(
        profile.get("topics") or [],
        profile.get("topic_clusters") or [],
        profile.get("site_categories") or [],
        max_items=12,
    )
    return {
        "allowed_topics": allowed_topics,
        "content_style_constraints": _merge_string_lists(
            profile.get("content_style") or [],
            [str(profile.get("content_tone") or "").strip()],
            max_items=6,
        ),
        "internal_linking_opportunities": [],
        "site_summary": str(profile.get("meta_description") or profile.get("domain_level_topic") or "").strip(),
        "site_categories": [str(item).strip() for item in (profile.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (profile.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (profile.get("prominent_titles") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (profile.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (profile.get("sample_urls") or []) if str(item).strip()],
    }


def _deterministic_style_constraints_from_titles(values: List[str]) -> List[str]:
    joined = " ".join(values).lower()
    constraints = ["Sachlicher, hilfreicher Ton", "Natuerliche Sprache ohne werbliche Uebertreibung"]
    if any(term in joined for term in {"tipps", "ratgeber", "checkliste"}):
        constraints.append("Praxisnahe Tipps und klare Struktur")
    if any(term in joined for term in {"vergleich", "vs", "oder"}):
        constraints.append("Vergleichende Einordnung mit konkreten Kriterien")
    if any(term in joined for term in {"faq", "fragen", "antworten"}):
        constraints.append("Direkte Antworten auf typische Leserfragen")
    return _merge_string_lists(constraints, max_items=6)


def _derive_deterministic_phase2_analysis(
    *,
    publishing_snapshot: Dict[str, Any],
    inventory_topic_insights: Dict[str, Any],
) -> Dict[str, Any]:
    sample_titles = [str(item).strip() for item in (publishing_snapshot.get("sample_page_titles") or []) if str(item).strip()]
    sample_urls = [str(item).strip() for item in (publishing_snapshot.get("sample_urls") or []) if str(item).strip()]
    combined_text = str(publishing_snapshot.get("combined_text") or "")
    keyword_terms = _extract_keywords(combined_text, max_terms=10)
    allowed_topics = _merge_string_lists(
        sample_titles,
        inventory_topic_insights.get("topic_clusters") or [],
        inventory_topic_insights.get("site_categories") or [],
        keyword_terms,
        max_items=12,
    )
    return {
        "allowed_topics": allowed_topics,
        "content_style_constraints": _deterministic_style_constraints_from_titles(sample_titles + keyword_terms),
        "internal_linking_opportunities": inventory_topic_insights.get("internal_linking_opportunities") or [],
        "site_summary": str(publishing_snapshot.get("site_summary") or "").strip(),
        "site_categories": inventory_topic_insights.get("site_categories") or [],
        "topic_clusters": _merge_string_lists(
            inventory_topic_insights.get("topic_clusters") or [],
            keyword_terms,
            max_items=10,
        ),
        "prominent_titles": inventory_topic_insights.get("prominent_titles") or [],
        "sample_page_titles": sample_titles,
        "sample_urls": sample_urls,
    }


def _pair_fit_cache_payload_is_usable(payload: Dict[str, Any]) -> bool:
    decision = str(payload.get("decision") or "").strip().lower()
    final_match_decision = str(payload.get("final_match_decision") or "").strip().lower()
    if decision == "rejected" or final_match_decision in {"weak_fit", "hard_reject"}:
        return True
    candidates = _coerce_pair_fit_topic_candidates(payload.get("topic_candidates"))
    return bool(
        str(payload.get("final_article_topic") or "").strip()
        and len(candidates) == 5
        and isinstance(payload.get("intersection_contexts"), list)
        and bool(payload.get("why_this_topic_was_chosen"))
        and final_match_decision in {"accepted", ""}
    )


def _pair_fit_tokens_from_text(value: str) -> List[str]:
    tokens = re.findall(r"\b[a-zA-ZäöüÄÖÜß]{3,}\b", _normalize_keyword_phrase(value))
    out: List[str] = []
    for token in tokens:
        if len(token) < 4:
            continue
        if token in STOPWORDS or token in GERMAN_FUNCTION_WORDS or token in ENGLISH_FUNCTION_WORDS:
            continue
        if token in PAIR_FIT_EXTRA_STOPWORDS or token in PAIR_FIT_BOILERPLATE_TOKENS:
            continue
        out.append(token)
    return out


def _pair_fit_term_weights(profile: Dict[str, Any], *, site_kind: str) -> Dict[str, float]:
    weighted_fields: List[tuple[Any, float]] = [
        (profile.get("topics"), 4.0),
        (profile.get("site_categories"), 3.5),
        (profile.get("topic_clusters"), 3.0),
        (profile.get("services_or_products"), 4.0 if site_kind == "target" else 2.0),
        (profile.get("prominent_titles"), 2.5),
        (profile.get("sample_page_titles"), 2.0),
        (profile.get("visible_headings"), 1.8),
        (profile.get("repeated_keywords"), 1.2),
        ([profile.get("page_title")], 2.4),
        ([profile.get("domain_level_topic")], 2.0),
        ([profile.get("meta_description")], 1.0),
    ]
    scores: Dict[str, float] = {}
    for raw_values, weight in weighted_fields:
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for raw_value in values:
            cleaned = re.sub(r"\s+", " ", str(raw_value or "").strip())
            if not cleaned:
                continue
            tokens = _pair_fit_tokens_from_text(cleaned)
            if not tokens:
                continue
            phrase = " ".join(tokens[:6]).strip()
            if len(phrase) < 4:
                continue
            scores[phrase] = scores.get(phrase, 0.0) + weight
    return scores


def _pair_fit_ranked_terms(profile: Dict[str, Any], *, site_kind: str, max_items: int = 10) -> List[str]:
    weighted = _pair_fit_term_weights(profile, site_kind=site_kind)
    ranked = sorted(weighted.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _score in ranked[:max_items]]


def _pair_fit_overlap_terms(publishing_terms: List[str], target_terms: List[str], *, max_items: int = 12) -> List[str]:
    publishing_tokens = {token for term in publishing_terms for token in _pair_fit_tokens_from_text(term)}
    target_tokens = {token for term in target_terms for token in _pair_fit_tokens_from_text(term)}
    ranked = sorted(publishing_tokens & target_tokens)
    return ranked[:max_items]


def _pair_fit_expand_contexts(profile: Dict[str, Any], *, ranked_terms: List[str]) -> List[str]:
    contexts = _dedupe_preserve_order([str(item).strip() for item in (profile.get("contexts") or []) if str(item).strip()])
    text = " ".join(
        ranked_terms
        + [str(profile.get("page_title") or "").strip(), str(profile.get("domain_level_topic") or "").strip()]
    ).lower()
    for context, keywords in PAIR_FIT_CONTEXT_KEYWORDS.items():
        matches = sum(1 for keyword in keywords if keyword in text)
        if matches > 0:
            contexts.append(context)
    return _dedupe_preserve_order(contexts)[:8]


def _pair_fit_context_label(contexts: List[str]) -> str:
    for context in contexts:
        label = PAIR_FIT_CONTEXT_LABELS.get(context)
        if label:
            return label
    return "den Alltag"


def _pair_fit_focus_term(terms: List[str], *, fallback: str) -> str:
    for term in terms:
        tokens = _pair_fit_tokens_from_text(term)
        if 1 <= len(tokens) <= 5:
            return _format_title_case(" ".join(tokens))
    return _format_title_case(" ".join(_pair_fit_tokens_from_text(fallback)[:5]) or fallback or "Hilfreiche Orientierung")


def _pair_fit_audience_term(terms: List[str], contexts: List[str]) -> str:
    for context in contexts:
        audience = PAIR_FIT_CONTEXT_AUDIENCES.get(context)
        if audience:
            return audience
    for term in terms:
        tokens = _pair_fit_tokens_from_text(term)
        if 1 <= len(tokens) <= 4 and any(token in PAIR_FIT_AUDIENCE_TOKENS for token in tokens):
            return _format_title_case(" ".join(tokens))
    return "Leserinnen und Leser"


def _pair_fit_generate_bridge_topics(
    *,
    requested_topic: str,
    exclude_topics: List[str],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
) -> List[str]:
    target_focus = _pair_fit_focus_term(target_terms, fallback=requested_topic or (target_terms[0] if target_terms else "Thema"))
    publishing_audience = _pair_fit_audience_term(publishing_terms, publishing_contexts)
    shared_contexts = _dedupe_preserve_order(publishing_contexts + target_contexts)
    context_label = _pair_fit_context_label(shared_contexts)
    templates = [
        requested_topic.strip(),
        f"{target_focus}: worauf {publishing_audience} achten sollten",
        f"{target_focus} im {context_label}: praktische Orientierung",
        f"Alltag und {target_focus}: hilfreiche Hinweise fuer {publishing_audience}",
        f"{target_focus}: sinnvolle Kriterien fuer {publishing_audience}",
        f"{target_focus} ohne Werbedruck: was im {context_label} wirklich wichtig ist",
        f"{target_focus} im Alltag: typische Fehler und bessere Entscheidungen",
    ]
    candidates: List[str] = []
    normalized_excluded = [_normalize_keyword_phrase(item) for item in exclude_topics if _normalize_keyword_phrase(item)]
    for template in templates:
        cleaned = re.sub(r"\s+", " ", str(template or "").strip())
        if not cleaned:
            continue
        normalized = _normalize_keyword_phrase(cleaned)
        if not normalized or any(_keyword_similarity(normalized, excluded) >= 0.8 for excluded in normalized_excluded):
            continue
        if any(_keyword_similarity(normalized, _normalize_keyword_phrase(existing)) >= 0.82 for existing in candidates):
            continue
        candidates.append(cleaned)
        if len(candidates) >= PAIR_FIT_CANDIDATE_COUNT:
            break
    while len(candidates) < PAIR_FIT_CANDIDATE_COUNT:
        fallback = f"{target_focus}: hilfreiche Orientierung fuer {publishing_audience}"
        if not any(_keyword_similarity(_normalize_keyword_phrase(fallback), _normalize_keyword_phrase(existing)) >= 0.82 for existing in candidates):
            candidates.append(fallback)
        else:
            candidates.append(f"{target_focus}: alltagsnahe Einordnung fuer {publishing_audience}")
    return candidates[:PAIR_FIT_CANDIDATE_COUNT]


def _pair_fit_score_candidate(
    topic: str,
    *,
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    target_business_intent: str,
) -> Dict[str, Any]:
    topic_tokens = set(_pair_fit_tokens_from_text(topic))
    publishing_tokens = {token for term in publishing_terms for token in _pair_fit_tokens_from_text(term)}
    target_tokens = {token for term in target_terms for token in _pair_fit_tokens_from_text(term)}
    shared_contexts = set(publishing_contexts) & set(target_contexts)
    publishing_overlap = len(topic_tokens & publishing_tokens)
    target_overlap = len(topic_tokens & target_tokens)
    overlap_bonus = len(topic_tokens & set(overlap_terms))
    shared_context_count = len(shared_contexts)
    bridge_evidence_score = shared_context_count * 3 + overlap_bonus * 2
    if publishing_overlap >= 2:
        bridge_evidence_score += 1
    if target_overlap >= 2:
        bridge_evidence_score += 1
    publishing_site_relevance = 2 + publishing_overlap * 2 + min(2, shared_context_count)
    target_site_relevance = 2 + target_overlap * 2 + min(2, shared_context_count) + min(1, overlap_bonus)
    if shared_context_count == 0 and overlap_bonus == 0:
        publishing_site_relevance -= 1
        target_site_relevance -= 1
    publishing_site_relevance = max(1, min(10, publishing_site_relevance))
    target_site_relevance = max(1, min(10, target_site_relevance))
    informational_value = 5 + sum(1 for cue in PAIR_FIT_INFORMATIONAL_CUES if cue in _normalize_keyword_phrase(topic))
    if "vergleich" in _normalize_keyword_phrase(topic) or "kriterien" in _normalize_keyword_phrase(topic):
        informational_value += 1
    informational_value = max(1, min(10, informational_value))
    backlink_naturalness = 2 + shared_context_count * 2 + min(2, overlap_bonus) + min(1, publishing_overlap) + min(1, target_overlap)
    if target_business_intent == "commercial" and shared_context_count == 0:
        backlink_naturalness -= 1
    if shared_context_count == 0 and overlap_bonus == 0:
        backlink_naturalness -= 2
    backlink_naturalness = max(1, min(10, backlink_naturalness))
    spam_risk = 1 + sum(1 for token in topic_tokens if token in PAIR_FIT_PROMO_TOKENS)
    if target_business_intent == "commercial":
        spam_risk += 1
    if shared_context_count == 0:
        spam_risk += 2
    if overlap_bonus == 0:
        spam_risk += 1
    if publishing_overlap == 0:
        spam_risk += 2
    if target_overlap == 0:
        spam_risk += 1
    if publishing_overlap <= 1 and target_overlap <= 1 and shared_context_count == 0:
        spam_risk += 1
    spam_risk = max(0, min(10, spam_risk))
    non_spamminess = max(1, min(10, 10 - spam_risk))
    seo_plausibility = 2 + publishing_overlap + target_overlap + min(3, shared_context_count + overlap_bonus)
    if shared_context_count == 0 and overlap_bonus == 0:
        seo_plausibility -= 1
    seo_plausibility = max(1, min(10, seo_plausibility))
    total_score = publishing_site_relevance + backlink_naturalness + informational_value + seo_plausibility + non_spamminess
    backlink_angle = (
        "Die Zielseite dient als weiterfuehrende Ressource innerhalb eines informativen Hauptthemas."
        if backlink_naturalness >= 6
        else "Die Zielseite kann nur vorsichtig und klar nachrangig als Zusatzquelle eingebunden werden."
    )
    return {
        "topic": topic,
        "publishing_site_relevance": publishing_site_relevance,
        "target_site_relevance": target_site_relevance,
        "backlink_naturalness": backlink_naturalness,
        "informational_value": informational_value,
        "seo_plausibility": seo_plausibility,
        "non_spamminess": non_spamminess,
        "spam_risk": spam_risk,
        "total_score": max(0, min(50, total_score)),
        "backlink_angle": backlink_angle,
        "score_breakdown": {
            "publishing_site_relevance": publishing_site_relevance,
            "target_site_relevance": target_site_relevance,
            "informational_value": informational_value,
            "backlink_naturalness": backlink_naturalness,
            "spam_risk": spam_risk,
            "publishing_overlap": publishing_overlap,
            "target_overlap": target_overlap,
            "shared_context_count": shared_context_count,
            "overlap_term_matches": overlap_bonus,
            "bridge_evidence_score": bridge_evidence_score,
        },
    }


def _pair_fit_reject_reason(final_match_decision: str, best_candidate: Dict[str, Any], overlap_terms: List[str]) -> str:
    if final_match_decision == "accepted":
        return ""
    if final_match_decision == "weak_fit":
        if overlap_terms:
            return "Es gibt eine grundsaetzlich denkbare Verbindung, aber sie bleibt noch schwach und braucht eine sehr vorsichtige redaktionelle Einbettung."
        return "Die Verbindung ist nur lose erkennbar und wirkt ohne starke redaktionelle Fuehrung schnell erzwungen."
    if overlap_terms:
        return "Zwischen beiden Seiten fehlt ein ausreichend natuerlicher Informationsraum; gemeinsame Signale reichen nicht fuer einen glaubwuerdigen Hauptartikel mit sinnvoller Zusatzressource."
    return "Die beiden Seiten teilen keinen tragfaehigen inhaltlichen Kontext fuer einen natuerlichen informativen Brueckenartikel."


def _pair_fit_overlap_reason(final_match_decision: str, overlap_terms: List[str], shared_contexts: List[str]) -> str:
    overlap_text = ", ".join(overlap_terms[:4])
    context_text = ", ".join(shared_contexts[:3])
    if final_match_decision == "accepted":
        return f"Gemeinsame Kontexte ({context_text}) und passende Signale ({overlap_text}) erlauben einen informativen Hauptartikel ohne werbliche Schwerpunktverschiebung.".strip()
    if final_match_decision == "weak_fit":
        return f"Es gibt gewisse Beruehrungspunkte ueber {context_text or 'einzelne Kontexte'}, aber die Verbindung bleibt redaktionell empfindlich.".strip()
    return f"Die Kontexte ueberlappen zu wenig ({context_text or 'keine tragfaehigen Kontexte'}) und die gemeinsamen Signale ({overlap_text or 'kaum relevante Signale'}) tragen keinen natuerlichen Artikel.".strip()


def _pair_fit_llm_input_payload(
    *,
    requested_topic: str,
    exclude_topics: List[str],
    target_site_url: str,
    publishing_site_url: str,
    target_profile: Dict[str, Any],
    publishing_profile: Dict[str, Any],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    heuristic_candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    shared_contexts = _dedupe_preserve_order([context for context in publishing_contexts if context in set(target_contexts)])
    return {
        "requested_topic": requested_topic,
        "exclude_topics": exclude_topics[:8],
        "publishing_site_url": publishing_site_url,
        "target_site_url": target_site_url,
        "publishing_profile": _compact_pair_fit_profile(publishing_profile, site_kind="publishing"),
        "target_profile": _compact_pair_fit_profile(target_profile, site_kind="target"),
        "derived_signals": {
            "publishing_topics": publishing_terms[:8],
            "target_topics": target_terms[:8],
            "publishing_contexts": publishing_contexts[:8],
            "target_contexts": target_contexts[:8],
            "shared_contexts": shared_contexts,
            "overlap_terms": overlap_terms[:12],
            "seed_bridge_topics": [
                {
                    "topic": str(item.get("topic") or "").strip(),
                    "heuristic_total_score": int(item.get("total_score") or 0),
                    "heuristic_breakdown": dict(item.get("score_breakdown") or {}),
                }
                for item in heuristic_candidates[:PAIR_FIT_CANDIDATE_COUNT]
            ],
        },
    }


def _pair_fit_llm_prompts(input_payload: Dict[str, Any]) -> tuple[str, str]:
    system_prompt = (
        "Du bist ein redaktioneller Match-Analyst fuer Backlink-geeignete Informationsartikel. "
        "Beurteile, ob auf der Publishing-Seite ein natuerlicher informativer Artikel entstehen kann, "
        "der genau einen kontextuellen Link zur Zielseite enthaelt. "
        "Nutze semantische Kontexte, Zielgruppe, redaktionelle Glaubwuerdigkeit und Informationswert. "
        "Lehne kommerzielle Ziele nicht automatisch ab. "
        "Antworte ausschliesslich mit gueltigem JSON."
    )
    user_prompt = (
        "Bewerte die Passung zwischen Publishing-Seite und Zielseite auf Basis der strukturierten Profildaten.\n"
        "Arbeitsregeln:\n"
        "- Das Hauptthema muss zuerst natuerlich zur Publishing-Seite passen.\n"
        "- Der Link zur Zielseite darf nur eine nachrangige, kontextuelle Ressource sein.\n"
        "- Match auf Kontext- und Zielgruppenebene, nicht nur ueber exakte Keywords.\n"
        "- Nutze die Seed-Topics als Startpunkt, darfst sie aber verbessern oder ersetzen.\n"
        "- Erzeuge genau 5 Kandidaten in deutscher Sprache.\n"
        "- Kandidaten bewerten auf einer Skala 0-10 fuer publishing_site_relevance, target_site_relevance, informational_value, backlink_naturalness, spam_risk.\n"
        "- total_score ist 0-50.\n"
        "- final_match_decision ist genau eines von: accepted, weak_fit, hard_reject.\n"
        "- accepted: mindestens ein Kandidat ist klar natuerlich und redaktionell glaubwuerdig.\n"
        "- weak_fit: eine Verbindung ist denkbar, aber empfindlich.\n"
        "- hard_reject: kein natuerlicher Brueckenartikel erkennbar.\n"
        "- Gib nur JSON mit diesem Schema zurueck:\n"
        "{\n"
        '  "topic_candidates": [\n'
        "    {\n"
        '      "topic": "string",\n'
        '      "publishing_site_relevance": 0,\n'
        '      "target_site_relevance": 0,\n'
        '      "informational_value": 0,\n'
        '      "backlink_naturalness": 0,\n'
        '      "spam_risk": 0,\n'
        '      "total_score": 0,\n'
        '      "backlink_angle": "string"\n'
        "    }\n"
        "  ],\n"
        '  "final_article_topic": "string",\n'
        '  "final_match_decision": "accepted|weak_fit|hard_reject",\n'
        '  "why_this_topic_was_chosen": "string",\n'
        '  "best_overlap_reason": "string",\n'
        '  "reject_reason": "string",\n'
        '  "fit_score": 0\n'
        "}\n\n"
        "Input:\n"
        f"{json.dumps(input_payload, ensure_ascii=False, sort_keys=True, indent=2)}"
    )
    return system_prompt, user_prompt


def _pair_fit_normalize_llm_payload(
    *,
    llm_payload: Dict[str, Any],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    requested_topic: str,
) -> Dict[str, Any]:
    candidates = _coerce_pair_fit_topic_candidates(llm_payload.get("topic_candidates"))
    if len(candidates) != PAIR_FIT_CANDIDATE_COUNT:
        raise CreatorError(f"Pair fit returned invalid candidate count:{len(candidates)}")
    for candidate in candidates:
        seo_plausibility = max(
            1,
            min(
                10,
                candidate["publishing_site_relevance"]
                + candidate["target_site_relevance"]
                + candidate["informational_value"]
                - candidate["spam_risk"],
            ),
        )
        candidate["seo_plausibility"] = seo_plausibility
        candidate["non_spamminess"] = max(1, min(10, 10 - candidate["spam_risk"]))
        score_breakdown = candidate.get("score_breakdown") if isinstance(candidate.get("score_breakdown"), dict) else {}
        candidate["score_breakdown"] = {
            **score_breakdown,
            "publishing_site_relevance": candidate["publishing_site_relevance"],
            "target_site_relevance": candidate["target_site_relevance"],
            "informational_value": candidate["informational_value"],
            "backlink_naturalness": candidate["backlink_naturalness"],
            "spam_risk": candidate["spam_risk"],
        }
    final_match_decision = str(llm_payload.get("final_match_decision") or "").strip().lower()
    if final_match_decision not in {"accepted", "weak_fit", "hard_reject"}:
        raise CreatorError(f"Pair fit returned invalid final_match_decision:{final_match_decision or 'missing'}")
    final_topic = str(llm_payload.get("final_article_topic") or "").strip()
    if not final_topic:
        final_topic = str(candidates[0].get("topic") or "").strip()
    if not final_topic and requested_topic.strip():
        final_topic = requested_topic.strip()
    if not final_topic:
        raise CreatorError("Pair fit returned no final_article_topic.")
    requested_normalized = _normalize_keyword_phrase(requested_topic)
    if requested_normalized:
        requested_candidate = next(
            (item for item in candidates if _keyword_similarity(_normalize_keyword_phrase(str(item.get("topic") or "")), requested_normalized) >= 0.88),
            None,
        )
        if requested_candidate is not None and final_match_decision != "hard_reject":
            final_topic = str(requested_candidate.get("topic") or "").strip() or final_topic
    best_candidate = next(
        (item for item in candidates if _keyword_similarity(_normalize_keyword_phrase(str(item.get("topic") or "")), _normalize_keyword_phrase(final_topic)) >= 0.88),
        candidates[0],
    )
    shared_contexts = _dedupe_preserve_order([context for context in publishing_contexts if context in set(target_contexts)])
    why_this_topic_was_chosen = str(llm_payload.get("why_this_topic_was_chosen") or "").strip()
    if not why_this_topic_was_chosen:
        why_this_topic_was_chosen = (
            "Das Thema bleibt klar informativ, passt zum Publishing-Kontext und nutzt die Zielseite nur als nachrangige Zusatzressource."
            if final_match_decision == "accepted"
            else "Das Thema ist als Bruecke denkbar, braucht aber besondere redaktionelle Vorsicht, damit der Verweis nicht werblich wirkt."
        )
    best_overlap_reason = str(llm_payload.get("best_overlap_reason") or "").strip()
    if not best_overlap_reason:
        best_overlap_reason = _pair_fit_overlap_reason(final_match_decision, overlap_terms, shared_contexts)
    reject_reason = str(llm_payload.get("reject_reason") or llm_payload.get("rejection_reason") or "").strip()
    if final_match_decision == "accepted":
        reject_reason = ""
    elif not reject_reason:
        reject_reason = _pair_fit_reject_reason(final_match_decision, best_candidate, overlap_terms)
    fit_score = int(llm_payload.get("fit_score") or 0)
    if fit_score <= 0:
        fit_score = max(0, min(100, int(best_candidate.get("total_score") or 0) * 2))
    return {
        "publishing_site_topics": publishing_terms[:8],
        "target_site_topics": target_terms[:8],
        "publishing_site_contexts": publishing_contexts,
        "target_site_contexts": target_contexts,
        "intersection_contexts": shared_contexts,
        "overlap_terms": overlap_terms,
        "generated_bridge_topics": [dict(item) for item in candidates],
        "score_breakdown": {
            "best_candidate": dict(best_candidate),
            "shared_context_count": len(shared_contexts),
            "overlap_term_count": len(overlap_terms),
            "match_engine": "llm_hybrid",
        },
        "best_overlap_reason": best_overlap_reason,
        "topic_candidates": candidates,
        "final_article_topic": final_topic,
        "why_this_topic_was_chosen": why_this_topic_was_chosen,
        "backlink_fit_ok": final_match_decision == "accepted",
        "fit_score": max(0, min(100, fit_score)),
        "decision": "accepted" if final_match_decision == "accepted" else "rejected",
        "final_match_decision": final_match_decision,
        "rejection_reason": reject_reason,
        "reject_reason": reject_reason,
    }


def _run_pair_fit_reasoning(
    *,
    requested_topic: str,
    exclude_topics: List[str],
    target_site_url: str,
    publishing_site_url: str,
    target_profile: Dict[str, Any],
    publishing_profile: Dict[str, Any],
    llm_api_key: str,
    llm_base_url: str,
    planning_model: str,
    timeout_seconds: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]],
) -> Dict[str, Any]:
    publishing_terms = _pair_fit_ranked_terms(publishing_profile, site_kind="publishing")
    target_terms = _pair_fit_ranked_terms(target_profile, site_kind="target")
    publishing_contexts = _pair_fit_expand_contexts(publishing_profile, ranked_terms=publishing_terms)
    target_contexts = _pair_fit_expand_contexts(target_profile, ranked_terms=target_terms)
    overlap_terms = _pair_fit_overlap_terms(publishing_terms, target_terms)
    generated_topics = _pair_fit_generate_bridge_topics(
        requested_topic=requested_topic,
        exclude_topics=exclude_topics,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
    )
    heuristic_candidates = [
        _pair_fit_score_candidate(
            topic,
            publishing_terms=publishing_terms,
            target_terms=target_terms,
            publishing_contexts=publishing_contexts,
            target_contexts=target_contexts,
            overlap_terms=overlap_terms,
            target_business_intent=str(target_profile.get("business_intent") or "").strip().lower(),
        )
        for topic in generated_topics
    ]
    heuristic_candidates = sorted(
        heuristic_candidates,
        key=lambda item: (
            -int(item.get("total_score") or 0),
            -int(item.get("publishing_site_relevance") or 0),
            -int(item.get("target_site_relevance") or 0),
            int(item.get("spam_risk") or 10),
            str(item.get("topic") or ""),
        ),
    )[:PAIR_FIT_CANDIDATE_COUNT]
    if len(heuristic_candidates) < PAIR_FIT_CANDIDATE_COUNT:
        raise CreatorError(f"Pair fit returned invalid candidate count:{len(heuristic_candidates)}")
    input_payload = _pair_fit_llm_input_payload(
        requested_topic=requested_topic,
        exclude_topics=exclude_topics,
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
        overlap_terms=overlap_terms,
        heuristic_candidates=heuristic_candidates,
    )
    system_prompt, user_prompt = _pair_fit_llm_prompts(input_payload)
    llm_payload = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=planning_model,
        timeout_seconds=timeout_seconds,
        max_tokens=3000,
        temperature=0.1,
        request_label="phase3_pair_fit",
        usage_collector=usage_collector,
    )
    return _pair_fit_normalize_llm_payload(
        llm_payload=llm_payload,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
        overlap_terms=overlap_terms,
        requested_topic=requested_topic,
    )


def run_pair_fit_pipeline(
    *,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    requested_topic: Optional[str],
    exclude_topics: Optional[List[str]],
    target_profile_payload: Optional[Dict[str, Any]],
    target_profile_content_hash: Optional[str],
    publishing_profile_payload: Optional[Dict[str, Any]],
    publishing_profile_content_hash: Optional[str],
) -> Dict[str, Any]:
    target_profile = _coerce_site_profile_payload(target_profile_payload)
    publishing_profile = _coerce_site_profile_payload(publishing_profile_payload)
    if not target_profile or not publishing_profile:
        raise CreatorError("Pair fit requires target_profile and publishing_profile.")

    explicit_llm_key = os.getenv("CREATOR_LLM_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    explicit_shared_model = os.getenv("CREATOR_LLM_MODEL", "").strip()
    explicit_planning_model = os.getenv("CREATOR_LLM_MODEL_PLANNING", "").strip()
    explicit_writing_model = os.getenv("CREATOR_LLM_MODEL_WRITING", "").strip()
    planning_model = explicit_planning_model or explicit_shared_model
    writing_model = explicit_writing_model or explicit_shared_model
    explicit_base_url = os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    if not planning_model:
        planning_model = DEFAULT_ANTHROPIC_PLANNING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if not writing_model:
        writing_model = DEFAULT_ANTHROPIC_WRITING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if explicit_base_url:
        llm_base_url = explicit_base_url
    elif anthropic_key and _model_prefers_anthropic(planning_model, writing_model):
        llm_base_url = "https://api.anthropic.com/v1"
    elif anthropic_key and not openai_key:
        llm_base_url = "https://api.anthropic.com/v1"
    else:
        llm_base_url = DEFAULT_LLM_BASE_URL
    if "anthropic" in llm_base_url.lower():
        llm_api_key = explicit_llm_key or anthropic_key or openai_key
    else:
        llm_api_key = explicit_llm_key or openai_key or anthropic_key
    if not llm_api_key:
        raise CreatorError("No LLM API key configured for pair fit.")

    target_profile_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile, sort_keys=True, ensure_ascii=False)
    )
    publishing_profile_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile, sort_keys=True, ensure_ascii=False)
    )
    pair_fit = _run_pair_fit_reasoning(
        requested_topic=(requested_topic or "").strip(),
        exclude_topics=list(exclude_topics or []),
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        planning_model=planning_model,
        timeout_seconds=max(1, _read_int_env("CREATOR_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)),
        usage_collector=None,
    )
    return {
        "ok": True,
        "cached": False,
        "pair_fit": pair_fit,
        "publishing_profile_hash": publishing_profile_hash,
        "target_profile_hash": target_profile_hash,
        "prompt_version": PAIR_FIT_PROMPT_VERSION,
        "model_name": planning_model,
    }


def _infer_meta_description(html: str) -> str:
    excerpt = ""
    match = re.search(r"<p[^>]*>(.*?)</p>", html or "", flags=re.IGNORECASE | re.DOTALL)
    if match:
        excerpt = re.sub(r"<[^>]+>", "", match.group(1)).strip()
    return excerpt[:160]


def _derive_slug(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    return cleaned.strip("-")[:80]


def _fill_article_metadata(article_payload: Dict[str, Any], fallback_title: str) -> Dict[str, Any]:
    html = str(article_payload.get("article_html") or "").strip()
    excerpt = str(article_payload.get("excerpt") or "").strip()
    if not excerpt:
        excerpt = _infer_meta_description(html)[:200]
    meta_title = str(article_payload.get("meta_title") or "").strip() or fallback_title
    meta_description = str(article_payload.get("meta_description") or "").strip() or _infer_meta_description(html)
    slug = str(article_payload.get("slug") or "").strip() or _derive_slug(meta_title or fallback_title)
    article_payload["meta_title"] = meta_title
    article_payload["meta_description"] = meta_description
    article_payload["slug"] = slug
    article_payload["excerpt"] = excerpt
    return article_payload


def _build_deterministic_image_prompts(topic: str) -> Dict[str, Any]:
    cleaned_topic = (topic or "").strip() or "Industry insights"
    return {
        "featured_prompt": f"Editorial hero image illustrating: {cleaned_topic}",
        "featured_alt": cleaned_topic,
        "include_in_content": False,
        "in_content_prompt": "",
        "in_content_alt": "",
    }


def _extract_keywords(text: str, max_terms: int = 10) -> List[str]:
    words = re.findall(r"\b[a-zA-Z][a-zA-Z-]{2,}\b", (text or "").lower())
    counts: Dict[str, int] = {}
    for word in words:
        if word in STOPWORDS:
            continue
        counts[word] = counts.get(word, 0) + 1
    sorted_terms = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _ in sorted_terms[:max_terms]]


def _guess_brand_name(target_url: str, html: str) -> str:
    meta = extract_meta_content(html, [("property", "og:site_name"), ("name", "application-name")])
    if meta:
        return meta
    title = extract_page_title(html)
    if title:
        for sep in ["|", "-", "–", ":"]:
            if sep in title:
                return title.split(sep)[0].strip()
        return title.strip()
    host = urlparse(target_url).netloc
    return host.replace("www.", "").split(".")[0].replace("-", " ").title()


def _pick_backlink_url(target_url: str, html: str) -> str:
    canonical = extract_canonical_link(html)
    if canonical:
        try:
            target = urlparse(target_url)
            canon = urlparse(canonical)
            if target.netloc and canon.netloc and target.netloc == canon.netloc and target.path == canon.path:
                return canonical
        except Exception:
            pass
    return target_url


def _is_anchor_safe(anchor: Optional[str]) -> bool:
    if not anchor:
        return False
    cleaned = anchor.strip()
    if len(cleaned) < 2 or len(cleaned) > 80:
        return False
    if re.search(r"https?://", cleaned):
        return False
    lowered = cleaned.lower()
    if any(term in lowered for term in ["visit our", "buy now", "click here", "limited time"]):
        return False
    return True


def _build_anchor_text(anchor_type: str, brand_name: str, keyword_cluster: List[str]) -> str:
    if anchor_type == "brand" and brand_name:
        return brand_name
    if anchor_type == "partial_match" and keyword_cluster:
        return " ".join(keyword_cluster[:3]).title()
    return "this resource"


def _strip_code_fences(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:html)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value or "", flags=re.IGNORECASE | re.DOTALL)


def _tokenize_words(text: str) -> List[str]:
    return re.findall(r"\b[a-zA-ZäöüÄÖÜß]{2,}\b", (text or "").lower())


def _looks_english_heavy(text: str) -> bool:
    words = _tokenize_words(text)
    if len(words) < 120:
        return False
    de_hits = sum(1 for word in words if word in GERMAN_FUNCTION_WORDS)
    en_hits = sum(1 for word in words if word in ENGLISH_FUNCTION_WORDS)
    return en_hits >= 10 and en_hits > int(de_hits * 1.2)


def _extract_h2_headings(html: str) -> List[str]:
    headings: List[str] = []
    for match in re.finditer(r"<h2[^>]*>(.*?)</h2>", html or "", flags=re.IGNORECASE | re.DOTALL):
        heading = _strip_html_tags(match.group(1)).strip()
        if heading:
            headings.append(heading)
    return headings


def _extract_h2_section_html(html: str, heading_name: str) -> str:
    matches = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", html or "", flags=re.IGNORECASE | re.DOTALL))
    if not matches:
        return ""
    normalized_heading = _normalize_keyword_phrase(heading_name)
    document = html or ""
    for index, match in enumerate(matches):
        heading = _normalize_keyword_phrase(_strip_html_tags(match.group(1)))
        if heading != normalized_heading:
            continue
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(document)
        return document[start:end].strip()
    return ""


def _extract_h2_section_text(html: str, heading_name: str) -> str:
    return _strip_html_tags(_extract_h2_section_html(html, heading_name)).strip()


def _topic_keywords(topic: str, *, max_terms: int = 5) -> List[str]:
    words = _tokenize_words(topic)
    out: List[str] = []
    seen: set[str] = set()
    for word in words:
        if len(word) < 4:
            continue
        if word in STOPWORDS or word in GERMAN_FUNCTION_WORDS or word in ENGLISH_FUNCTION_WORDS:
            continue
        if word in seen:
            continue
        seen.add(word)
        out.append(word)
        if len(out) >= max_terms:
            break
    return out


def _normalize_keyword_phrase(value: str) -> str:
    cleaned = re.sub(r"[^\wäöüÄÖÜß\s-]", " ", (value or "").strip().lower())
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _keyword_token_set(value: str) -> set[str]:
    tokens = _tokenize_words(_normalize_keyword_phrase(value))
    return {
        token
        for token in tokens
        if token not in STOPWORDS
        and token not in GERMAN_FUNCTION_WORDS
        and token not in ENGLISH_FUNCTION_WORDS
    }


def _is_valid_keyword_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return False
    words = normalized.split()
    if not (KEYWORD_MIN_WORDS <= len(words) <= KEYWORD_MAX_WORDS):
        return False
    if len(normalized) < 6 or len(normalized) > 90:
        return False
    if any(token.isdigit() for token in words):
        return False
    return len(_keyword_token_set(normalized)) >= 2


def _is_low_signal_keyword_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return True
    tokens = [token for token in _keyword_token_set(normalized) if token]
    if not tokens:
        return True
    low_signal_hits = sum(1 for token in tokens if token in KEYWORD_LOW_SIGNAL_TOKENS)
    if low_signal_hits >= len(tokens):
        return True
    if len(tokens) >= 3 and low_signal_hits >= len(tokens) - 1:
        return True
    return False


def _keyword_similarity(a: str, b: str) -> float:
    ta = _keyword_token_set(a)
    tb = _keyword_token_set(b)
    if not ta or not tb:
        return 0.0
    intersection = len(ta & tb)
    union = len(ta | tb)
    if union == 0:
        return 0.0
    return intersection / union


def _dedupe_keyword_phrases(values: List[str]) -> List[str]:
    out: List[str] = []
    for item in values:
        normalized = _normalize_keyword_phrase(item)
        if not _is_valid_keyword_phrase(normalized):
            continue
        if _is_low_signal_keyword_phrase(normalized):
            continue
        if any(_keyword_similarity(normalized, existing) >= 0.75 for existing in out):
            continue
        out.append(normalized)
    return out


def _build_topic_phrase(topic: str) -> str:
    normalized = _normalize_keyword_phrase(topic)
    words = normalized.split()
    if len(words) > KEYWORD_MAX_WORDS:
        normalized = " ".join(words[:KEYWORD_MAX_WORDS])
    return normalized


def _extract_candidate_phrases_from_topics(topics: List[str], *, max_phrases: int = 16) -> List[str]:
    out: List[str] = []
    for topic in topics:
        normalized = _normalize_keyword_phrase(topic)
        if not normalized:
            continue
        words = normalized.split()
        if len(words) > KEYWORD_MAX_WORDS:
            out.append(" ".join(words[:KEYWORD_MAX_WORDS]))
        out.append(normalized)
        if len(out) >= max_phrases:
            break
    return out[:max_phrases]


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        normalized = _normalize_keyword_phrase(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _build_keyword_query_variants(
    *,
    topic: str,
    primary_hint: str,
    allowed_topics: List[str],
    max_queries: int = 8,
) -> List[str]:
    base_phrases = _dedupe_preserve_order(
        [topic, primary_hint] + _extract_candidate_phrases_from_topics(allowed_topics, max_phrases=2)
    )
    if not base_phrases:
        return []

    queries: List[str] = []
    primary_base = base_phrases[0]
    queries.append(primary_base)
    for modifier in GERMAN_KEYWORD_MODIFIERS[:2]:
        queries.append(f"{primary_base} {modifier}")
    queries.extend(
        [
            f"was ist {primary_base}",
            f"wie {primary_base}",
            f"wann {primary_base}",
            f"warum {primary_base}",
        ]
    )
    for base in base_phrases[1:2]:
        queries.append(base)
        for modifier in GERMAN_KEYWORD_MODIFIERS[:2]:
            queries.append(f"{base} {modifier}")
    for modifier in GERMAN_KEYWORD_MODIFIERS[2:4]:
        queries.append(f"{primary_base} {modifier}")

    deduped = _dedupe_preserve_order(queries)
    return deduped[:max_queries]


def _derive_trend_query_family(value: str) -> str:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return ""
    words = normalized.split()
    if len(words) >= 2 and any(normalized.startswith(f"{prefix} ") for prefix in GERMAN_QUESTION_PREFIXES):
        words = words[2:] if words[0] in {"was", "wie", "wann", "warum", "welche", "welcher", "welches", "wo", "woran", "kann", "darf"} else words[1:]
    words = [word for word in words if word not in GERMAN_KEYWORD_MODIFIERS]
    if len(words) > 4:
        words = words[:4]
    return " ".join(words).strip()


def _get_cached_google_suggestions(query: str) -> Optional[List[str]]:
    normalized_query = _normalize_keyword_phrase(query)
    if not normalized_query:
        return None
    cached = GOOGLE_SUGGEST_CACHE.get(normalized_query)
    if not cached:
        return None
    expires_at = float(cached.get("expires_at") or 0)
    if expires_at <= time.time():
        GOOGLE_SUGGEST_CACHE.pop(normalized_query, None)
        return None
    results = cached.get("results")
    if not isinstance(results, list):
        return None
    return [str(item).strip() for item in results if str(item).strip()]


def _set_cached_google_suggestions(query: str, results: List[str]) -> None:
    normalized_query = _normalize_keyword_phrase(query)
    if not normalized_query:
        return
    if len(GOOGLE_SUGGEST_CACHE) >= GOOGLE_SUGGEST_CACHE_MAX_ENTRIES:
        oldest_key = min(
            GOOGLE_SUGGEST_CACHE,
            key=lambda key: float(GOOGLE_SUGGEST_CACHE[key].get("stored_at") or 0),
        )
        GOOGLE_SUGGEST_CACHE.pop(oldest_key, None)
    GOOGLE_SUGGEST_CACHE[normalized_query] = {
        "results": [str(item).strip() for item in results if str(item).strip()],
        "stored_at": time.time(),
        "expires_at": time.time() + GOOGLE_SUGGEST_CACHE_TTL_SECONDS,
    }


def _trend_entry_is_fresh(entry: Dict[str, Any]) -> bool:
    fetched_at_raw = str(entry.get("fetched_at") or "").strip()
    expires_at_raw = str(entry.get("expires_at") or "").strip()
    if expires_at_raw:
        try:
            expires_at = datetime.datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
            return expires_at > datetime.datetime.now(datetime.timezone.utc)
        except ValueError:
            return False
    if fetched_at_raw:
        try:
            fetched_at = datetime.datetime.fromisoformat(fetched_at_raw.replace("Z", "+00:00"))
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=datetime.timezone.utc)
            age = datetime.datetime.now(datetime.timezone.utc) - fetched_at
            return age.total_seconds() <= DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS
        except ValueError:
            return False
    return False


def _fetch_google_de_suggestions_live(query: str, *, timeout_seconds: int) -> List[str]:
    cleaned = _normalize_keyword_phrase(query)
    if not cleaned:
        return []
    url = "https://suggestqueries.google.com/complete/search"
    try:
        response = requests.get(
            url,
            params={"client": "firefox", "hl": "de", "gl": "de", "q": cleaned},
            headers={"User-Agent": "creator-service/1.0"},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    if not isinstance(payload, list) or len(payload) < 2:
        return []
    suggestions_raw = payload[1]
    if not isinstance(suggestions_raw, list):
        return []
    out: List[str] = []
    for item in suggestions_raw:
        if isinstance(item, str) and item.strip():
            out.append(_normalize_keyword_phrase(item))
    return out


def _fetch_google_de_suggestions(
    query: str,
    *,
    timeout_seconds: int,
    trend_cache_ttl_seconds: int,
    cache_metadata_collector: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    cleaned = _normalize_keyword_phrase(query)
    if not cleaned:
        return []
    query_family = _derive_trend_query_family(cleaned)
    cached = _get_cached_google_suggestions(cleaned)
    if cached is not None:
        if cache_metadata_collector is not None:
            cache_metadata_collector.append({"query": cleaned, "source": "memory", "status": "fresh"})
        return cached

    db_entry = get_keyword_trend_cache_entry(cleaned)
    db_payload = db_entry.get("payload") if isinstance(db_entry, dict) else None
    db_results = []
    if isinstance(db_payload, dict):
        raw = db_payload.get("suggestions")
        if isinstance(raw, list):
            db_results = [str(item).strip() for item in raw if str(item).strip()]
    db_is_fresh = isinstance(db_entry, dict) and _trend_entry_is_fresh(db_entry)
    if db_results and db_is_fresh:
        _set_cached_google_suggestions(cleaned, db_results)
        record_keyword_trend_cache_hit(cleaned)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db",
                    "status": "fresh",
                    "fetched_at": str(db_entry.get("fetched_at") or ""),
                }
            )
        return db_results

    family_entries = get_keyword_trend_cache_family_entries(query_family) if query_family else []
    family_suggestions = _merge_string_lists(
        *[
            [
                str(item).strip()
                for item in ((entry.get("payload") or {}).get("suggestions") or [])
                if str(item).strip()
            ]
            for entry in family_entries
            if _trend_entry_is_fresh(entry)
        ],
        max_items=12,
    )
    if family_suggestions:
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db_family",
                    "status": "fresh_family_support",
                    "query_family": query_family,
                }
            )

    live_results = _fetch_google_de_suggestions_live(cleaned, timeout_seconds=timeout_seconds)
    if live_results:
        merged_live_results = _merge_string_lists(live_results, family_suggestions, max_items=12)
        _set_cached_google_suggestions(cleaned, merged_live_results)
        upsert_keyword_trend_cache_entry(
            seed_query=query,
            normalized_seed_query=cleaned,
            query_family=query_family,
            payload={"suggestions": merged_live_results},
            ttl_seconds=trend_cache_ttl_seconds,
        )
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "live",
                    "status": "refreshed" if db_results else "miss_refreshed",
                }
            )
        return merged_live_results

    if db_results:
        _set_cached_google_suggestions(cleaned, db_results)
        record_keyword_trend_cache_hit(cleaned)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db",
                    "status": "stale_fallback",
                    "fetched_at": str(db_entry.get("fetched_at") or ""),
                }
            )
        return db_results

    if family_suggestions:
        _set_cached_google_suggestions(cleaned, family_suggestions)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db_family",
                    "status": "fresh_family_fallback",
                    "query_family": query_family,
                }
            )
        return family_suggestions

    if cache_metadata_collector is not None:
        cache_metadata_collector.append({"query": cleaned, "source": "none", "status": "empty"})
    return []


def _looks_like_question_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return False
    return any(normalized.startswith(f"{prefix} ") for prefix in GERMAN_QUESTION_PREFIXES)


def _rank_keyword_candidates(
    candidates: List[str],
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    allowed_tokens: set[str],
    trend_tokens: set[str],
    max_items: int,
) -> List[str]:
    ranked = sorted(
        _dedupe_keyword_phrases(candidates),
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ),
        reverse=True,
    )
    return ranked[:max_items]


def _discover_keyword_candidates(
    *,
    topic: str,
    primary_hint: str,
    keyword_cluster: List[str],
    allowed_topics: List[str],
    timeout_seconds: int,
    max_terms: int,
    trend_cache_ttl_seconds: int = DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
) -> Dict[str, Any]:
    query_variants = _build_keyword_query_variants(
        topic=topic,
        primary_hint=primary_hint,
        allowed_topics=allowed_topics,
    )
    raw_suggestions: List[str] = []
    trend_cache_events: List[Dict[str, Any]] = []
    for query in query_variants:
        raw_suggestions.extend(
            _fetch_google_de_suggestions(
                query,
                timeout_seconds=timeout_seconds,
                trend_cache_ttl_seconds=trend_cache_ttl_seconds,
                cache_metadata_collector=trend_cache_events,
            )
        )

    suggestion_pool = _dedupe_preserve_order(raw_suggestions)
    keyword_candidates = [item for item in suggestion_pool if not _looks_like_question_phrase(item)]
    faq_candidates = [item for item in suggestion_pool if _looks_like_question_phrase(item)]
    if len(faq_candidates) < 3:
        faq_candidates.extend(item for item in query_variants if _looks_like_question_phrase(item))

    topic_tokens = _keyword_token_set(topic)
    cluster_tokens = {_normalize_keyword_phrase(item) for item in keyword_cluster if _normalize_keyword_phrase(item)}
    allowed_tokens = _keyword_token_set(" ".join(allowed_topics))
    trend_tokens = _keyword_token_set(" ".join(keyword_candidates))

    ranked_keywords = _rank_keyword_candidates(
        keyword_candidates or query_variants,
        topic_tokens=topic_tokens,
        cluster_tokens=cluster_tokens,
        allowed_tokens=allowed_tokens,
        trend_tokens=trend_tokens,
        max_items=max_terms,
    )
    ranked_faqs = _rank_keyword_candidates(
        faq_candidates,
        topic_tokens=topic_tokens,
        cluster_tokens=cluster_tokens,
        allowed_tokens=allowed_tokens,
        trend_tokens=trend_tokens,
        max_items=KEYWORD_MAX_FAQ,
    )
    return {
        "query_variants": query_variants,
        "trend_candidates": ranked_keywords,
        "faq_candidates": ranked_faqs,
        "trend_cache_events": trend_cache_events,
    }


def _fetch_keyword_trend_candidates(
    *,
    topic: str,
    primary_hint: str,
    keyword_cluster: List[str],
    allowed_topics: List[str],
    timeout_seconds: int,
    max_terms: int,
) -> List[str]:
    discovery = _discover_keyword_candidates(
        topic=topic,
        primary_hint=primary_hint,
        keyword_cluster=keyword_cluster,
        allowed_topics=allowed_topics,
        timeout_seconds=timeout_seconds,
        max_terms=max_terms,
    )
    return discovery["trend_candidates"]


def _score_keyword_candidate(
    candidate: str,
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    allowed_tokens: set[str],
    trend_tokens: set[str],
) -> float:
    candidate_tokens = _keyword_token_set(candidate)
    if not candidate_tokens:
        return -1.0
    score = 0.0
    score += 3.0 * len(candidate_tokens & topic_tokens)
    score += 1.5 * len(candidate_tokens & cluster_tokens)
    score += 1.0 * len(candidate_tokens & allowed_tokens)
    score += 2.0 * len(candidate_tokens & trend_tokens)
    score += min(1.5, len(candidate_tokens) * 0.3)
    return score


def _keyword_candidate_has_relevance(
    candidate: str,
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    trend_tokens: set[str],
) -> bool:
    candidate_tokens = _keyword_token_set(candidate)
    if not candidate_tokens:
        return False
    return bool(candidate_tokens & (topic_tokens | cluster_tokens | trend_tokens))


def _select_keywords(
    *,
    topic: str,
    llm_primary: str,
    llm_secondary: List[str],
    keyword_cluster: List[str],
    allowed_topics: List[str],
    trend_candidates: List[str],
    faq_candidates: List[str],
) -> Dict[str, Any]:
    topic_phrase = _build_topic_phrase(topic)
    topic_tokens = _keyword_token_set(topic_phrase)
    cluster_tokens = _keyword_token_set(" ".join(keyword_cluster))
    allowed_tokens = _keyword_token_set(" ".join(allowed_topics))
    trend_tokens = _keyword_token_set(" ".join(trend_candidates))

    primary_pool = _dedupe_keyword_phrases(
        [llm_primary, topic_phrase] + trend_candidates + _extract_candidate_phrases_from_topics(allowed_topics, max_phrases=8)
    )
    if not primary_pool and _is_valid_keyword_phrase(topic_phrase):
        primary_pool = [topic_phrase]
    if not primary_pool:
        fallback = _normalize_keyword_phrase(topic) or "branchen einblicke"
        primary_pool = [fallback]
    primary_ranked = sorted(
        primary_pool,
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ),
        reverse=True,
    )
    primary_keyword = primary_ranked[0]

    secondary_pool = _dedupe_keyword_phrases(
        llm_secondary
        + trend_candidates
        + _extract_candidate_phrases_from_topics(allowed_topics)
        + [topic_phrase]
    )
    ranked_secondary = sorted(
        [
            candidate
            for candidate in secondary_pool
            if _keyword_similarity(candidate, primary_keyword) < 0.8
            and _keyword_candidate_has_relevance(
                candidate,
                topic_tokens=topic_tokens,
                cluster_tokens=cluster_tokens,
                trend_tokens=trend_tokens,
            )
        ],
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ),
        reverse=True,
    )
    secondary_keywords = ranked_secondary[:KEYWORD_MAX_SECONDARY]
    if len(secondary_keywords) < KEYWORD_MIN_SECONDARY:
        fallback_secondary = _dedupe_keyword_phrases(
            [f"{primary_keyword} tipps", f"{primary_keyword} ratgeber", f"{primary_keyword} auswirkungen"]
        )
        for candidate in fallback_secondary:
            if len(secondary_keywords) >= KEYWORD_MIN_SECONDARY:
                break
            if any(_keyword_similarity(candidate, existing) >= 0.75 for existing in secondary_keywords):
                continue
            if _keyword_similarity(candidate, primary_keyword) >= 0.8:
                continue
            secondary_keywords.append(candidate)

    return {
        "primary_keyword": primary_keyword,
        "secondary_keywords": secondary_keywords[:KEYWORD_MAX_SECONDARY],
        "trend_candidates": trend_candidates,
        "faq_candidates": _rank_keyword_candidates(
            faq_candidates,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
            max_items=KEYWORD_MAX_FAQ,
        ),
    }


def _coerce_internal_link_inventory(items: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    out: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        categories = item.get("categories")
        out.append(
            {
                "url": url,
                "title": str(item.get("title") or "").strip(),
                "excerpt": str(item.get("excerpt") or "").strip(),
                "slug": str(item.get("slug") or "").strip(),
                "categories": [str(value).strip() for value in categories if str(value).strip()] if isinstance(categories, list) else [],
                "published_at": str(item.get("published_at") or "").strip(),
            }
        )
    return out


def _score_internal_link_inventory_item(
    item: Dict[str, Any],
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
) -> float:
    topic_tokens = _keyword_token_set(topic)
    primary_tokens = _keyword_token_set(primary_keyword)
    secondary_tokens = _keyword_token_set(" ".join(secondary_keywords))
    title_tokens = _keyword_token_set(str(item.get("title") or ""))
    excerpt_tokens = _keyword_token_set(str(item.get("excerpt") or ""))
    slug_tokens = _keyword_token_set(str(item.get("slug") or ""))
    category_tokens = _keyword_token_set(" ".join(item.get("categories") or []))
    combined = title_tokens | excerpt_tokens | slug_tokens | category_tokens
    if not combined:
        return 0.0
    score = 0.0
    score += 4.0 * len(combined & topic_tokens)
    score += 3.0 * len(combined & primary_tokens)
    score += 1.5 * len(combined & secondary_tokens)
    score += 1.0 * _keyword_similarity(str(item.get("title") or ""), primary_keyword)
    score += 0.8 * _keyword_similarity(str(item.get("title") or ""), topic)
    score += min(1.0, len(title_tokens) * 0.2)
    if str(item.get("published_at") or "").strip():
        score += 0.3
    return score


def _rank_internal_link_inventory(
    items: List[Dict[str, Any]],
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    publishing_site_url: str,
    backlink_url: str,
    max_items: int,
) -> List[Dict[str, Any]]:
    normalized_items = _coerce_internal_link_inventory(items)
    filtered: List[Dict[str, Any]] = []
    for item in normalized_items:
        url = str(item.get("url") or "").strip()
        if not _is_internal_href(url, publishing_site_url):
            continue
        if _normalize_url(url) == _normalize_url(backlink_url):
            continue
        filtered.append(item)
    ranked = sorted(
        filtered,
        key=lambda item: _score_internal_link_inventory_item(
            item,
            topic=topic,
            primary_keyword=primary_keyword,
            secondary_keywords=secondary_keywords,
        ),
        reverse=True,
    )
    return ranked[:max_items]


def _normalize_text_for_keyword_search(value: str) -> str:
    normalized = _normalize_keyword_phrase(value)
    return f" {normalized} " if normalized else " "


def _keyword_present(text: str, keyword_phrase: str) -> bool:
    normalized_text = _normalize_text_for_keyword_search(text)
    normalized_keyword = _normalize_keyword_phrase(keyword_phrase)
    if not normalized_keyword:
        return False
    return f" {normalized_keyword} " in normalized_text


def _keyword_token_approx_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if min(len(a), len(b)) < 5:
        return False
    prefix = 0
    for left, right in zip(a, b):
        if left != right:
            break
        prefix += 1
    return prefix >= (min(len(a), len(b)) - 2)


def _keyword_present_relaxed(text: str, keyword_phrase: str) -> bool:
    if _keyword_present(text, keyword_phrase):
        return True
    keyword_tokens = list(_keyword_token_set(keyword_phrase))
    text_tokens = list(_keyword_token_set(text))
    if not keyword_tokens or not text_tokens:
        return False
    matched = 0
    for keyword_token in keyword_tokens:
        if any(_keyword_token_approx_match(keyword_token, text_token) for text_token in text_tokens):
            matched += 1
    return matched >= max(1, len(keyword_tokens) - 1)


def _count_keyword_occurrences(text: str, keyword_phrase: str) -> int:
    normalized_text = _normalize_keyword_phrase(text)
    normalized_keyword = _normalize_keyword_phrase(keyword_phrase)
    if not normalized_text or not normalized_keyword:
        return 0
    pattern = r"(?<!\w)" + r"\s+".join(re.escape(token) for token in normalized_keyword.split()) + r"(?!\w)"
    return len(re.findall(pattern, normalized_text))


def _extract_h1_text(html: str) -> str:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html or "", flags=re.IGNORECASE | re.DOTALL)
    return _strip_html_tags(match.group(1)).strip() if match else ""


def _extract_first_paragraph_text(html: str) -> str:
    match = re.search(r"<p[^>]*>(.*?)</p>", html or "", flags=re.IGNORECASE | re.DOTALL)
    return _strip_html_tags(match.group(1)).strip() if match else ""


def _ensure_primary_keyword_in_intro(html: str, primary_keyword: str) -> str:
    keyword = _normalize_keyword_phrase(primary_keyword)
    if not keyword:
        return html or ""
    article_html = html or ""
    intro_text = _extract_first_paragraph_text(article_html)
    if _keyword_present(intro_text, keyword):
        return article_html

    sentence = f"{_format_title_case(primary_keyword)} ist dabei ein zentraler Aspekt."

    def _inject(match: re.Match[str]) -> str:
        inner = match.group(1)
        stripped = _strip_html_tags(inner).strip()
        if not stripped:
            return f"<p>{sentence}</p>"
        return f"<p>{sentence} {inner.strip()}</p>"

    if re.search(r"<p[^>]*>.*?</p>", article_html, flags=re.IGNORECASE | re.DOTALL):
        return re.sub(r"<p[^>]*>(.*?)</p>", _inject, article_html, count=1, flags=re.IGNORECASE | re.DOTALL)
    return f"<p>{sentence}</p>{article_html}"


def _trim_article_to_word_limit(html: str, max_words: int) -> str:
    article_html = html or ""
    if word_count_from_html(article_html) <= max_words:
        return article_html

    soup = BeautifulSoup(article_html, "lxml")
    body = soup.body or soup
    paragraphs = body.find_all("p")
    if not paragraphs:
        return article_html

    first_paragraph = paragraphs[0] if paragraphs else None
    current_count = word_count_from_html(str(body))
    candidates = [p for p in paragraphs if p is not first_paragraph and not p.find("a")]
    if not candidates:
        candidates = [p for p in paragraphs if p is not first_paragraph]

    for paragraph in reversed(candidates):
        if current_count <= max_words:
            break
        text = re.sub(r"\s+", " ", paragraph.get_text(" ")).strip()
        words = re.findall(r"\b\w+\b", text)
        if len(words) <= 28:
            continue
        overflow = current_count - max_words
        target_words = max(28, len(words) - overflow)
        trimmed_text = " ".join(words[:target_words]).strip()
        if not trimmed_text:
            continue
        if trimmed_text[-1] not in ".!?":
            trimmed_text += "."
        paragraph.clear()
        paragraph.append(trimmed_text)
        current_count = word_count_from_html(str(body))

    result = body.decode_contents() if getattr(body, "decode_contents", None) else str(body)
    return result


def _validate_keyword_coverage(article_html: str, primary_keyword: str, secondary_keywords: List[str]) -> List[str]:
    errors: List[str] = []
    primary = _normalize_keyword_phrase(primary_keyword)
    secondaries = _dedupe_keyword_phrases(secondary_keywords)[:KEYWORD_MAX_SECONDARY]
    if not _is_valid_keyword_phrase(primary):
        errors.append("primary_keyword_invalid")
        return errors

    h1_text = _extract_h1_text(article_html)
    intro_text = _extract_first_paragraph_text(article_html)
    h2_text = " ".join(_extract_h2_headings(article_html))
    plain_text = _strip_html_tags(article_html)

    if not _keyword_present(h1_text, primary):
        errors.append("primary_keyword_missing_h1")
    if not _keyword_present(intro_text, primary):
        errors.append("primary_keyword_missing_intro")
    if not _keyword_present(h2_text, primary):
        errors.append("primary_keyword_missing_h2")

    required_secondaries = secondaries[:KEYWORD_MIN_SECONDARY]
    missing_secondaries = [kw for kw in required_secondaries if not _keyword_present_relaxed(plain_text, kw)]
    if missing_secondaries:
        errors.append("secondary_keywords_missing:" + ",".join(missing_secondaries[:3]))

    words = max(1, word_count_from_html(article_html))
    max_occurrences = max(3, int((words / 300.0) * 3))
    for keyword in [primary] + required_secondaries:
        occurrences = _count_keyword_occurrences(plain_text, keyword)
        if occurrences > max_occurrences:
            errors.append(f"keyword_overused:{keyword}:{occurrences}")

    return errors


def _extract_internal_anchor_texts(article_html: str, *, backlink_url: str, publishing_site_url: str) -> List[str]:
    anchors: List[str] = []
    for href, inner in re.findall(
        r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        article_html or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            continue
        if not _is_internal_href(absolute, publishing_site_url):
            continue
        anchor_text = _strip_html_tags(inner).strip()
        if anchor_text:
            anchors.append(anchor_text)
    return anchors


def _validate_internal_anchor_texts(article_html: str, *, backlink_url: str, publishing_site_url: str) -> List[str]:
    anchors = _extract_internal_anchor_texts(article_html, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    if not anchors:
        return []
    errors: List[str] = []
    unique_normalized = {_normalize_keyword_phrase(anchor) for anchor in anchors if _normalize_keyword_phrase(anchor)}
    if len(unique_normalized) < min(INTERNAL_LINK_ANCHOR_MIN_UNIQUE, len(anchors)):
        errors.append("internal_anchor_diversity_too_low")
    generic_count = sum(1 for anchor in anchors if _normalize_keyword_phrase(anchor) in {"mehr dazu", "siehe auch", "passend dazu", "weiterfuehrende informationen"})
    if generic_count >= len(anchors):
        errors.append("internal_anchor_too_generic")
    return errors


def _validate_structured_content(article_html: str, structured_mode: str) -> List[str]:
    mode = (structured_mode or "").strip().lower()
    if mode == "list":
        if not re.search(r"<(?:ul|ol)\b", article_html or "", flags=re.IGNORECASE):
            return ["structured_list_missing"]
    if mode == "table":
        if not re.search(r"<table\b", article_html or "", flags=re.IGNORECASE):
            return ["structured_table_missing"]
    return []


def _validate_seo_metadata(
    *,
    article_html: str,
    primary_keyword: str,
    required_h1: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    structured_mode: str,
) -> List[str]:
    errors: List[str] = []
    if len((meta_title or "").strip()) < SEO_TITLE_MIN_CHARS or len((meta_title or "").strip()) > SEO_TITLE_MAX_CHARS:
        errors.append(f"meta_title_length_invalid:{len((meta_title or '').strip())}")
    if len((meta_description or "").strip()) < SEO_DESCRIPTION_MIN_CHARS or len((meta_description or "").strip()) > SEO_DESCRIPTION_MAX_CHARS:
        errors.append(f"meta_description_length_invalid:{len((meta_description or '').strip())}")
    if not _keyword_present_relaxed(meta_title, primary_keyword):
        errors.append("meta_title_missing_primary_keyword")
    normalized_slug = _derive_slug(slug or "")
    if normalized_slug != (slug or "").strip():
        errors.append("slug_format_invalid")
    slug_tokens = _keyword_token_set(slug or "")
    primary_tokens = _keyword_token_set(primary_keyword)
    if len(slug_tokens & primary_tokens) < min(2, max(1, len(primary_tokens))):
        errors.append("slug_missing_primary_keyword")
    h1_text = _extract_h1_text(article_html)
    if _normalize_keyword_phrase(h1_text) != _normalize_keyword_phrase(required_h1):
        errors.append("h1_not_required_title")
    errors.extend(_validate_structured_content(article_html, structured_mode))
    return errors


def _score_seo_output(
    *,
    article_html: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    required_h1: str,
    structured_mode: str,
    backlink_url: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
    topic: str,
) -> Dict[str, Any]:
    checks = {
        "keyword_coverage": _validate_keyword_coverage(article_html, primary_keyword, secondary_keywords),
        "language_conclusion": _validate_language_and_conclusion(article_html, topic),
        "link_strategy": _validate_link_strategy(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
            min_internal_links=min_internal_links,
            max_internal_links=max_internal_links,
        ),
        "anchor_quality": _validate_internal_anchor_texts(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
        ),
        "metadata": _validate_seo_metadata(
            article_html=article_html,
            primary_keyword=primary_keyword,
            required_h1=required_h1,
            meta_title=meta_title,
            meta_description=meta_description,
            slug=slug,
            structured_mode=structured_mode,
        ),
    }
    error_count = sum(len(value) for value in checks.values())
    score = max(0, 100 - (error_count * 8))
    return {"score": score, "checks": checks}


def _collect_article_validation_errors(
    *,
    article_html: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    required_h1: str,
    structured_mode: str,
    backlink_url: str,
    backlink_placement: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
) -> List[str]:
    errors: List[str] = []
    for check in (
        validate_word_count(article_html, ARTICLE_MIN_WORDS, ARTICLE_MAX_WORDS),
        validate_backlink_placement(article_html, backlink_url, backlink_placement),
    ):
        if check:
            errors.append(check)
    errors.extend(
        _validate_link_strategy(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
            min_internal_links=min_internal_links,
            max_internal_links=max_internal_links,
        )
    )
    errors.extend(
        _validate_internal_anchor_texts(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
        )
    )
    if not (ARTICLE_MIN_H2 <= count_h2(article_html) <= ARTICLE_MAX_H2):
        errors.append("h2_count_invalid")
    errors.extend(_validate_language_and_conclusion(article_html, topic))
    errors.extend(_validate_keyword_coverage(article_html, primary_keyword, secondary_keywords))
    errors.extend(
        _validate_seo_metadata(
            article_html=article_html,
            primary_keyword=primary_keyword,
            required_h1=required_h1,
            meta_title=meta_title,
            meta_description=meta_description,
            slug=slug,
            structured_mode=structured_mode,
        )
    )
    return errors


def _contains_generic_conclusion(text: str) -> bool:
    lowered = (text or "").lower()
    return any(phrase in lowered for phrase in GENERIC_CONCLUSION_PHRASES)


def _format_faq_question(question: str) -> str:
    raw = re.sub(r"\s+", " ", str(question or "").strip())
    normalized = _normalize_keyword_phrase(raw)
    if not normalized:
        return ""
    formatted = normalized[:1].upper() + normalized[1:]
    if (raw.endswith("?") or _looks_like_question_phrase(normalized)) and not formatted.endswith("?"):
        formatted += "?"
    return formatted


def _dedupe_faq_questions(values: List[str], *, max_items: int = FAQ_MIN_QUESTIONS) -> List[str]:
    out: List[str] = []
    for item in values:
        formatted = _format_faq_question(item)
        if not formatted:
            continue
        normalized = _normalize_keyword_phrase(formatted)
        if any(_keyword_similarity(normalized, _normalize_keyword_phrase(existing)) >= 0.7 for existing in out):
            continue
        out.append(formatted)
        if len(out) >= max_items:
            break
    return out


def _ensure_faq_candidates(topic: str, faq_candidates: List[str]) -> List[str]:
    normalized_faqs = _dedupe_faq_questions(faq_candidates, max_items=FAQ_MIN_QUESTIONS)
    if len(normalized_faqs) >= FAQ_MIN_QUESTIONS:
        return normalized_faqs[:FAQ_MIN_QUESTIONS]

    topic_phrase = _build_topic_phrase(topic) or "dieses thema"
    fallback_questions = [
        f"Was ist {topic_phrase}?",
        f"Welche Ursachen hat {topic_phrase}?",
        f"Wann ist Hilfe bei {topic_phrase} sinnvoll?",
    ]
    normalized_faqs = _dedupe_faq_questions(normalized_faqs + fallback_questions, max_items=FAQ_MIN_QUESTIONS)
    return normalized_faqs[:FAQ_MIN_QUESTIONS]


def _build_article_faq_queries(
    *,
    topic: str,
    primary_keyword: str,
    article_html: str,
    max_queries: int = 6,
) -> List[str]:
    h2_headings = [
        heading for heading in _extract_h2_headings(article_html)
        if _normalize_keyword_phrase(heading) not in {"fazit", "faq"}
    ]
    return _build_keyword_query_variants(
        topic=topic,
        primary_hint=primary_keyword,
        allowed_topics=h2_headings[:2],
        max_queries=max_queries,
    )


def _coerce_generated_faqs(value: Any) -> List[Dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        question = str(item.get("question") or "").strip()
        answer_html = str(item.get("answer_html") or "").strip()
        search_reason = str(item.get("search_reason") or "").strip()
        if not question or not answer_html:
            continue
        if not question.endswith("?"):
            question = f"{question.rstrip('.!?')}?"
        answer_html = _strip_code_fences(answer_html)
        answer_html = re.sub(r"<h[1-6][^>]*>.*?</h[1-6]>", "", answer_html, flags=re.IGNORECASE | re.DOTALL)
        answer_html = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", answer_html, flags=re.IGNORECASE | re.DOTALL)
        if not re.search(r"<(?:p|ul|ol|table)\b", answer_html, flags=re.IGNORECASE):
            answer_html = _wrap_paragraphs(answer_html)
        if not answer_html.strip():
            continue
        out.append(
            {
                "question": question,
                "answer_html": answer_html.strip(),
                "search_reason": search_reason,
            }
        )
    return out[:KEYWORD_MAX_FAQ]


def _render_faq_section_html(faqs: List[Dict[str, str]]) -> str:
    normalized_questions = _dedupe_faq_questions([item.get("question") or "" for item in faqs], max_items=len(faqs))
    rendered: List[str] = []
    for question in normalized_questions:
        item = next((faq for faq in faqs if _keyword_similarity(faq.get("question", ""), question) >= 0.8), None)
        if item is None:
            continue
        rendered.append(f"<h3>{question}</h3>{item['answer_html']}")
    return "".join(rendered)


def _replace_faq_section(article_html: str, faq_html: str) -> str:
    html = article_html or ""
    match = re.search(r"(<h2[^>]*>\s*FAQ\s*</h2>)(.*)$", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return html[:match.start()] + match.group(1) + faq_html
    return f"{html}<h2>FAQ</h2>{faq_html}"


def _generate_search_informed_faqs(
    *,
    article_html: str,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    current_faq_candidates: List[str],
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    timeout_seconds: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    queries = _build_article_faq_queries(
        topic=topic,
        primary_keyword=primary_keyword,
        article_html=article_html,
    )
    search_questions: List[str] = []
    for query in queries:
        suggestions = _fetch_google_de_suggestions(
            query,
            timeout_seconds=timeout_seconds,
            trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
        )
        search_questions.extend(item for item in suggestions if _looks_like_question_phrase(item))
    normalized_search_questions = _ensure_faq_candidates(
        topic,
        _dedupe_faq_questions(search_questions + current_faq_candidates, max_items=KEYWORD_MAX_FAQ),
    )
    system_prompt = (
        "Create a German FAQ section for a finished SEO article. "
        "Use the article, the primary/secondary keywords, and Germany-focused search questions. "
        "Questions must be natural, specific, and useful for readers. "
        "Answers must be concise HTML, 35-60 words each, with no links and no markdown. "
        "Return JSON only."
    )
    article_text = _strip_html_tags(article_html)
    article_text = re.sub(r"\s+", " ", article_text).strip()[:6000]
    user_prompt = (
        f"Topic: {topic}\n"
        f"Primary keyword: {primary_keyword}\n"
        f"Secondary keywords: {secondary_keywords}\n"
        f"Germany search questions: {normalized_search_questions[:KEYWORD_MAX_FAQ]}\n"
        f"Existing FAQ candidates: {current_faq_candidates[:KEYWORD_MAX_FAQ]}\n"
        f"Article text: {article_text}\n"
        "Return JSON: "
        "{\"faqs\":[{\"question\":\"...?\",\"answer_html\":\"<p>...</p>\",\"search_reason\":\"...\"}]}"
    )
    llm_out = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=llm_model,
        timeout_seconds=timeout_seconds,
        max_tokens=1800,
        temperature=0.2,
        request_label="phase5_faq_enrichment",
        usage_collector=usage_collector,
    )
    faqs = _coerce_generated_faqs(llm_out.get("faqs"))
    if len(faqs) < FAQ_MIN_QUESTIONS:
        raise CreatorError(f"FAQ enrichment returned too few items:{len(faqs)}")
    faq_html = _render_faq_section_html(faqs)
    if not faq_html.strip():
        raise CreatorError("FAQ enrichment returned empty FAQ html.")
    return {
        "faqs": faqs[:KEYWORD_MAX_FAQ],
        "faq_html": faq_html,
        "search_questions": normalized_search_questions[:KEYWORD_MAX_FAQ],
        "queries": queries,
    }


def _inject_faq_section(outline_items: List[Any], faq_candidates: List[str], topic: str) -> List[Any]:
    if not isinstance(outline_items, list):
        return outline_items

    faq_section: Optional[Dict[str, Any]] = None
    fazit_section: Optional[Dict[str, Any]] = None
    core_sections: List[Dict[str, Any]] = []
    normalized_faqs = _ensure_faq_candidates(topic, faq_candidates)

    for item in outline_items:
        section = item if isinstance(item, dict) else {"h2": str(item), "h3": []}
        h2_value = str(section.get("h2") or "").strip()
        heading_normalized = _normalize_keyword_phrase(h2_value)
        if heading_normalized == "faq":
            faq_section = {"h2": "FAQ", "h3": normalized_faqs}
            continue
        if heading_normalized == "fazit":
            fazit_section = {"h2": "Fazit", "h3": []}
            continue
        core_sections.append({"h2": h2_value, "h3": section.get("h3") or []})

    if fazit_section is None:
        fazit_section = {"h2": "Fazit", "h3": []}
    if faq_section is None:
        faq_section = {"h2": "FAQ", "h3": normalized_faqs}

    trimmed_core = core_sections[: max(0, ARTICLE_MAX_H2 - 2)]
    return trimmed_core + [fazit_section, faq_section]


def _format_outline_heading(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ""
    return cleaned[:1].upper() + cleaned[1:]


def _build_deterministic_outline(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    faq_candidates: List[str],
    structured_mode: str,
    anchor_text_final: str,
) -> Dict[str, Any]:
    topic_phrase = _build_topic_phrase(topic) or _build_topic_phrase(primary_keyword) or "dieses Thema"
    primary_heading = _format_outline_heading(primary_keyword or topic_phrase)
    secondary_pool = [
        _format_outline_heading(item)
        for item in secondary_keywords
        if _format_outline_heading(item)
        and _keyword_similarity(_normalize_keyword_phrase(item), _normalize_keyword_phrase(primary_heading)) < 0.85
    ]

    if structured_mode == "list":
        first_heading = f"{primary_heading}: Checkliste und wichtigste Schritte"
    elif structured_mode == "table":
        first_heading = f"{primary_heading}: Vergleich und Ueberblick"
    else:
        first_heading = f"{primary_heading}: Das Wichtigste im Ueberblick"

    core_sections: List[Dict[str, Any]] = [{"h2": first_heading, "h3": []}]

    fallback_headings = [
        f"Ursachen und Hintergruende zu {topic_phrase}",
        f"Auswirkungen und typische Herausforderungen bei {topic_phrase}",
        f"Praktische Tipps und Unterstuetzung zu {topic_phrase}",
        f"Haeufige Fehler und sinnvolle naechste Schritte bei {topic_phrase}",
    ]
    for heading in secondary_pool:
        core_sections.append({"h2": heading, "h3": []})
        if len(core_sections) >= ARTICLE_MAX_H2 - 2:
            break
    for heading in fallback_headings:
        if len(core_sections) >= ARTICLE_MAX_H2 - 2:
            break
        normalized = _normalize_keyword_phrase(heading)
        if any(_keyword_similarity(_normalize_keyword_phrase(item.get("h2") or ""), normalized) >= 0.8 for item in core_sections):
            continue
        core_sections.append({"h2": heading, "h3": []})

    while len(core_sections) < ARTICLE_MIN_H2 - 2:
        core_sections.append({"h2": f"Weitere wichtige Aspekte zu {topic_phrase}", "h3": []})

    outline_items = _inject_faq_section(core_sections, faq_candidates, topic)
    return {
        "outline": outline_items,
        "backlink_placement": "intro",
        "anchor_text_final": anchor_text_final,
    }


def _validate_language_and_conclusion(article_html: str, topic: str) -> List[str]:
    errors: List[str] = []
    plain_text = _strip_html_tags(article_html)
    if _looks_english_heavy(plain_text):
        errors.append("language_not_german")

    headings = _extract_h2_headings(article_html)
    normalized_headings = [_normalize_keyword_phrase(item) for item in headings]
    if len(normalized_headings) < ARTICLE_MIN_H2 or len(normalized_headings) > ARTICLE_MAX_H2:
        errors.append("h2_count_invalid")
    if not normalized_headings or normalized_headings[-1] != "faq":
        errors.append("final_h2_not_faq")
    if len(normalized_headings) < 2 or normalized_headings[-2] != "fazit":
        errors.append("penultimate_h2_not_fazit")

    conclusion_text = _extract_h2_section_text(article_html, "Fazit")
    if conclusion_text:
        if _contains_generic_conclusion(conclusion_text):
            errors.append("conclusion_generic")
        topic_terms = _topic_keywords(topic)
        if topic_terms:
            lowered = conclusion_text.lower()
            if not any(term in lowered for term in topic_terms):
                errors.append("conclusion_not_topic_specific")

    faq_html = _extract_h2_section_html(article_html, "FAQ")
    faq_text = _strip_html_tags(faq_html).strip()
    if not faq_text:
        errors.append("faq_missing")
    else:
        faq_questions = [
            _strip_html_tags(match.group(1)).strip()
            for match in re.finditer(r"<h3[^>]*>(.*?)</h3>", faq_html, flags=re.IGNORECASE | re.DOTALL)
            if _strip_html_tags(match.group(1)).strip()
        ]
        if len(faq_questions) < FAQ_MIN_QUESTIONS:
            errors.append(f"faq_question_count_too_low:{len(faq_questions)}")
        unique_questions = _dedupe_faq_questions(faq_questions, max_items=max(FAQ_MIN_QUESTIONS, len(faq_questions)))
        if len(unique_questions) < len(faq_questions):
            errors.append("faq_questions_not_unique")
        if any(not question.endswith("?") for question in faq_questions):
            errors.append("faq_question_format_invalid")
        if word_count_from_html(faq_html) < FAQ_MIN_WORDS:
            errors.append(f"faq_answers_too_thin:{word_count_from_html(faq_html)}")

    return errors


def _wrap_paragraphs(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if "<p" in cleaned or "<h2" in cleaned:
        return cleaned
    parts = [part.strip() for part in re.split(r"\n{2,}", cleaned) if part.strip()]
    if not parts:
        return ""
    return "".join(f"<p>{part}</p>" for part in parts)


def _normalize_section_html(h2: str, h3s: List[str], raw: str) -> str:
    cleaned = _strip_code_fences(raw)
    existing_h3_blocks = list(
        re.finditer(
            r"<h3[^>]*>(.*?)</h3>(.*?)(?=<h3[^>]*>|$)",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )
    cleaned = re.sub(r"<h[1-2][^>]*>.*?</h[1-2]>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    body = cleaned if re.search(r"<(?:p|ul|ol|table)\b", cleaned, flags=re.IGNORECASE) else _wrap_paragraphs(cleaned)
    html = f"<h2>{h2}</h2>"
    if h3s:
        if existing_h3_blocks:
            rendered = []
            for block in existing_h3_blocks:
                heading = _strip_html_tags(block.group(1)).strip()
                content = (block.group(2) or "").strip()
                if not heading:
                    continue
                if content and not re.search(r"<(?:p|ul|ol|table)\b", content, flags=re.IGNORECASE):
                    content = _wrap_paragraphs(content)
                rendered.append((heading, content))
            for idx, h3 in enumerate(h3s):
                html += f"<h3>{h3}</h3>"
                matched = next((content for heading, content in rendered if _keyword_similarity(heading, h3) >= 0.75), "")
                if not matched and idx < len(rendered):
                    matched = rendered[idx][1]
                html += matched or "<p></p>"
            return html

        plain_text = re.sub(r"\s+", " ", _strip_html_tags(body)).strip()
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", plain_text) if part.strip()]
        chunks: List[str] = []
        if sentences:
            chunk_size = max(1, math.ceil(len(sentences) / max(1, len(h3s))))
            for idx in range(len(h3s)):
                chunk = " ".join(sentences[idx * chunk_size:(idx + 1) * chunk_size]).strip()
                chunks.append(chunk)
        while len(chunks) < len(h3s):
            chunks.append("")
        for idx, h3 in enumerate(h3s):
            html += f"<h3>{h3}</h3>"
            answer = chunks[idx].strip()
            html += _wrap_paragraphs(answer) or "<p></p>"
        return html

    if body:
        html += body
    return html


def _strip_h1_tags(html: str) -> str:
    return re.sub(r"<h1[^>]*>.*?</h1>", "", html, flags=re.IGNORECASE | re.DOTALL)


def _ensure_required_h1(html: str, required_h1: str) -> str:
    body = _strip_h1_tags(html or "").strip()
    heading = _strip_html_tags(required_h1 or "").strip()
    if not heading:
        return body
    return f"<h1>{heading}</h1>{body}"


def _strip_empty_blocks(html: str) -> str:
    cleaned = html or ""
    empty_block = r"(?:\s|&nbsp;|&#160;|<br\s*/?>)*"
    for tag in ("p", "h1", "h2", "h3"):
        cleaned = re.sub(
            rf"<{tag}[^>]*>{empty_block}</{tag}>",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
    return cleaned


def _strip_leading_empty_blocks(html: str) -> str:
    cleaned = (html or "").lstrip()
    pattern = (
        r"^(?:\s*(?:"
        r"<p[^>]*>(?:\s|&nbsp;|&#160;|<br\s*/?>)*</p>"
        r"|<br\s*/?>"
        r"|<h1[^>]*>(?:\s|&nbsp;|&#160;)*</h1>"
        r"))+"
    )
    return re.sub(pattern, "", cleaned, flags=re.IGNORECASE)


def _host_variants(url: str) -> set[str]:
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


def _absolutize_url(href: str, base_url: str) -> str:
    raw = (href or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    parsed_base = urlparse((base_url or "").strip())
    host = (parsed_base.netloc or "").strip()
    scheme = (parsed_base.scheme or "https").strip()
    if not host:
        return raw
    if raw.startswith("/"):
        return f"{scheme}://{host}{raw}"
    return raw


def _is_internal_href(href: str, publishing_site_url: str) -> bool:
    absolute = _absolutize_url(href, publishing_site_url)
    parsed = urlparse(absolute)
    if absolute.startswith("/") and not parsed.netloc:
        return True
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return False
    return host in _host_variants(publishing_site_url)


def _is_backlink_href(href: str, backlink_url: str) -> bool:
    if not backlink_url:
        return False
    return _normalize_url(_absolutize_url(href, backlink_url)) == _normalize_url(backlink_url)


def _normalize_internal_link_candidates(
    links: List[str],
    *,
    publishing_site_url: str,
    backlink_url: str,
    max_items: int,
) -> List[str]:
    out: List[str] = []
    backlink_norm = _normalize_url(backlink_url)
    for href in links:
        absolute = _absolutize_url(str(href), publishing_site_url)
        if not absolute:
            continue
        if not _is_internal_href(absolute, publishing_site_url):
            continue
        if _normalize_url(absolute) == backlink_norm:
            continue
        parsed = urlparse(absolute)
        if not parsed.scheme or not parsed.netloc:
            continue
        cleaned = absolute.strip()
        if cleaned in out:
            continue
        out.append(cleaned)
        if len(out) >= max_items:
            break
    return out


def _internal_anchor_text(url: str, anchor_map: Optional[Dict[str, str]] = None) -> str:
    normalized_url = _normalize_url(url)
    if anchor_map and normalized_url in anchor_map:
        raw_value = anchor_map[normalized_url]
        if isinstance(raw_value, (list, tuple)):
            for candidate in raw_value:
                preferred = re.sub(r"\s+", " ", str(candidate or "").strip())
                if preferred:
                    words = preferred.split()
                    return " ".join(words[:8])
        preferred = re.sub(r"\s+", " ", str(raw_value or "").strip())
        if preferred:
            words = preferred.split()
            return " ".join(words[:8])
    parsed = urlparse((url or "").strip())
    tail = (parsed.path or "").strip("/").split("/")[-1] if parsed.path else ""
    cleaned = re.sub(r"[-_]+", " ", tail).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) >= 3:
        return cleaned.capitalize()
    return "Weiterfuehrende Informationen"


def _build_internal_anchor_variants(item: Dict[str, Any]) -> List[str]:
    variants: List[str] = []
    title = str(item.get("title") or "").strip()
    slug = str(item.get("slug") or "").strip()
    categories = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
    if title:
        variants.append(title)
        title_words = title.split()
        if len(title_words) > 4:
            variants.append(" ".join(title_words[:4]))
    if categories and title:
        variants.append(f"{categories[0]}: {title}")
    if slug:
        slug_variant = re.sub(r"[-_]+", " ", slug).strip()
        if slug_variant:
            variants.append(_format_title_case(slug_variant))
    return _merge_string_lists(variants, max_items=4)


def _extract_link_stats(article_html: str, *, backlink_url: str, publishing_site_url: str) -> Dict[str, Any]:
    links = re.findall(r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", article_html or "", flags=re.IGNORECASE | re.DOTALL)
    backlink_count = 0
    internal_count = 0
    external_count = 0
    internal_urls: List[str] = []
    for href, _inner in links:
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            backlink_count += 1
            continue
        if _is_internal_href(absolute, publishing_site_url):
            internal_count += 1
            norm = _normalize_url(absolute)
            if norm and norm not in internal_urls:
                internal_urls.append(norm)
            continue
        external_count += 1
    return {
        "backlink_count": backlink_count,
        "internal_count": internal_count,
        "external_count": external_count,
        "internal_unique_count": len(internal_urls),
    }


def _validate_link_strategy(
    article_html: str,
    *,
    backlink_url: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
) -> List[str]:
    stats = _extract_link_stats(article_html, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    errors: List[str] = []
    if stats["backlink_count"] != 1:
        errors.append(f"backlink_count_invalid:{stats['backlink_count']}")
    if stats["external_count"] != 0:
        errors.append(f"external_link_count_invalid:{stats['external_count']}")
    if stats["internal_count"] < min_internal_links:
        errors.append(f"internal_link_count_too_low:{stats['internal_count']}")
    if stats["internal_count"] > max_internal_links:
        errors.append(f"internal_link_count_too_high:{stats['internal_count']}")
    if stats["internal_unique_count"] < min_internal_links:
        errors.append(f"internal_link_uniqueness_too_low:{stats['internal_unique_count']}")
    return errors


def _strip_disallowed_links(html: str, *, backlink_url: str, publishing_site_url: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        href = match.group(1) or ""
        inner = match.group(2) or ""
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            return match.group(0)
        if _is_internal_href(absolute, publishing_site_url):
            return match.group(0)
        return inner

    return re.sub(
        r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        replacer,
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )


def _insert_backlink(html: str, backlink_url: str, anchor_text: str, placement: str) -> str:
    anchor_html = f'<a href="{backlink_url}">{anchor_text}</a>'
    if placement == "intro":
        match = re.search(r"</p>", html, flags=re.IGNORECASE)
        if match:
            return html[:match.start()] + f" {anchor_html}" + html[match.start():]
        return anchor_html + html

    index = 0
    try:
        index = max(0, int(placement.split("_")[1]) - 1)
    except Exception:
        index = 0

    matches = list(re.finditer(r"<h2[^>]*>", html, flags=re.IGNORECASE))
    if not matches:
        return html + anchor_html

    if index >= len(matches):
        index = len(matches) - 1

    start = matches[index].end()
    after = html[start:]
    p_match = re.search(r"</p>", after, flags=re.IGNORECASE)
    if p_match:
        insert_at = start + p_match.start()
        return html[:insert_at] + f" {anchor_html}" + html[insert_at:]
    return html[:start] + anchor_html + html[start:]


def _insert_internal_links(
    html: str,
    *,
    internal_links: List[str],
    target_internal_count: int,
    anchor_map: Optional[Dict[str, str]] = None,
) -> str:
    if target_internal_count <= 0 or not internal_links:
        return html
    working = html or ""
    h2_matches = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", working, flags=re.IGNORECASE | re.DOTALL))
    usable_section_indexes = []
    for idx, match in enumerate(h2_matches):
        heading_text = _strip_html_tags(match.group(1)).strip().lower()
        if "fazit" in heading_text or "faq" in heading_text:
            continue
        usable_section_indexes.append(idx)
    if not usable_section_indexes:
        usable_section_indexes = [0]

    for idx in range(min(target_internal_count, len(internal_links))):
        href = internal_links[idx]
        anchor_text = _internal_anchor_text(href, anchor_map=anchor_map)
        link_html = f'<a href="{href}">{anchor_text}</a>'
        section_idx = usable_section_indexes[idx % len(usable_section_indexes)]
        lead = ["Siehe auch", "Passend dazu", "Vertiefend", "Hilfreich ist auch"][idx % 4]

        section_starts = list(re.finditer(r"<h2[^>]*>", working, flags=re.IGNORECASE))
        if section_starts:
            section_start = section_starts[min(section_idx, len(section_starts) - 1)].end()
            tail = working[section_start:]
            p_match = re.search(r"</p>", tail, flags=re.IGNORECASE)
            if p_match:
                insert_at = section_start + p_match.start()
                working = working[:insert_at] + f" {lead}: {link_html}." + working[insert_at:]
                continue

        match = re.search(r"</p>", working, flags=re.IGNORECASE)
        if match:
            working = working[:match.start()] + f" {lead}: {link_html}." + working[match.start():]
        else:
            working += f"<p>{lead}: {link_html}.</p>"

    return working


def _new_token_bucket() -> Dict[str, int]:
    return {
        "calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
    }


def _as_non_negative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _phase_from_request_label(label: str) -> str:
    cleaned = (label or "").strip().lower()
    if cleaned.startswith("phase2"):
        return "phase2"
    if cleaned.startswith("phase3"):
        return "phase3"
    if cleaned.startswith("phase4"):
        return "phase4"
    if cleaned.startswith("phase5"):
        return "phase5"
    if cleaned.startswith("phase7"):
        return "phase7"
    return "unknown"


def _is_link_only_error(error: str) -> bool:
    value = (error or "").strip()
    return (
        value.startswith("backlink_missing")
        or value.startswith("backlink_wrong_placement")
        or value.startswith("backlink_count_invalid")
        or value.startswith("internal_link_count_too_low")
        or value.startswith("internal_link_count_too_high")
        or value.startswith("internal_link_uniqueness_too_low")
        or value.startswith("external_link_count_invalid")
    )


def _repair_link_constraints(
    *,
    article_html: str,
    backlink_url: str,
    publishing_site_url: str,
    internal_links: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]] = None,
    min_internal_links: int,
    max_internal_links: int,
    backlink_placement: str,
    anchor_text: str,
    required_h1: str = "",
) -> str:
    # Remove all hyperlinks and then insert the required backlink + internal links.
    repaired = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", article_html or "", flags=re.IGNORECASE | re.DOTALL)
    if backlink_url and anchor_text:
        repaired = _insert_backlink(repaired, backlink_url, anchor_text, backlink_placement)
    normalized_internal = _normalize_internal_link_candidates(
        internal_links,
        publishing_site_url=publishing_site_url,
        backlink_url=backlink_url,
        max_items=max_internal_links,
    )
    target_internal_count = min(max_internal_links, len(normalized_internal))
    if target_internal_count < min_internal_links:
        target_internal_count = len(normalized_internal)
    repaired = _insert_internal_links(
        repaired,
        internal_links=normalized_internal,
        target_internal_count=target_internal_count,
        anchor_map=internal_link_anchor_map,
    )
    repaired = _strip_disallowed_links(repaired, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    repaired = _strip_empty_blocks(repaired)
    repaired = _strip_leading_empty_blocks(repaired)
    repaired = _ensure_required_h1(repaired, required_h1)
    return repaired


def _generate_article_by_sections(
    *,
    phase4: Dict[str, Any],
    phase3: Dict[str, Any],
    backlink_url: str,
    publishing_site_url: str,
    internal_link_candidates: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]],
    min_internal_links: int,
    max_internal_links: int,
    faq_candidates: List[str],
    structured_mode: str,
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    http_timeout: int,
    expand_passes: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Optional[Dict[str, Any]]:
    outline_items = phase4.get("outline") or []
    if not isinstance(outline_items, list) or not outline_items:
        return None

    h2_count = len(outline_items)
    intro_target = 100
    target_total = 700
    per_section = max(85, int((target_total - intro_target) / max(1, h2_count)))
    per_min = max(70, per_section - 15)
    per_max = min(150, per_section + 15)

    backlink_placement = phase4.get("backlink_placement") or "intro"
    anchor_text = phase4.get("anchor_text_final") or "this resource"
    internal_links_prompt = internal_link_candidates[:max_internal_links]

    intro_system = "Write a short introduction paragraph in German (de-DE) in HTML. Return only HTML."
    intro_user = (
        f"Topic: {phase3.get('final_article_topic','')}\n"
        f"H1: {phase4.get('h1','')}\n"
        f"Primary keyword: {phase3.get('primary_keyword','')}\n"
        f"Length: {intro_target - 15}-{intro_target + 15} words.\n"
        "Do not include links unless explicitly requested. Language: German (de-DE). "
        "Include the primary keyword naturally in this first paragraph."
    )
    if backlink_placement == "intro":
        intro_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."

    try:
        intro_raw = call_llm_text(
            system_prompt=intro_system,
            user_prompt=intro_user,
            api_key=llm_api_key,
            base_url=llm_base_url,
            model=llm_model,
            timeout_seconds=http_timeout,
            max_tokens=3000,
            temperature=0.2,
            request_label="phase5_section_intro",
            usage_collector=usage_collector,
        )
    except LLMError:
        intro_raw = ""
    intro_html = _wrap_paragraphs(intro_raw) or "<p></p>"
    intro_html = _ensure_primary_keyword_in_intro(intro_html, phase3.get("primary_keyword", ""))
    intro_html = _ensure_required_h1(intro_html, phase4.get("h1", ""))

    sections_html: List[str] = []
    for index, item in enumerate(outline_items, start=1):
        h2 = (item.get("h2") or "").strip() if isinstance(item, dict) else str(item)
        h3s = item.get("h3") if isinstance(item, dict) else []
        h3s_list = [str(h3).strip() for h3 in (h3s or []) if str(h3).strip()]
        placement_index = None
        if backlink_placement.startswith("section_"):
            try:
                placement_index = int(backlink_placement.split("_")[1]) - 1
            except Exception:
                placement_index = None
        include_backlink = placement_index == (index - 1)

        section_system = "Write HTML for a single H2 section of a submitted article in German (de-DE). Return only HTML."
        section_user = (
            f"H2: {h2}\n"
            f"H3s: {h3s_list}\n"
            f"Primary keyword: {phase3.get('primary_keyword','')}\n"
            f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
            f"Length: {per_min}-{per_max} words.\n"
            "Write in a neutral authoritative tone in German (de-DE). Do not use bullet lists unless necessary."
            "\nDo not include links unless explicitly requested."
        )
        if "faq" in h2.lower():
            section_user += (
                f"\nThis is the FAQ section. Answer these questions clearly and directly: {faq_candidates[:3]}. "
                "Use the H3 questions as subheadings, avoid duplicate questions, and write 35-55 words per answer in German."
            )
        elif structured_mode == "list" and index == 1:
            section_user += "\nInclude a meaningful HTML list (<ul> or <ol>) in this section."
        elif structured_mode == "table" and index == 1:
            section_user += "\nInclude a meaningful HTML table in this section."
        if "fazit" in h2.lower():
            section_user += (
                f"\nThis is the final 'Fazit' section. Summarize concrete takeaways for topic: "
                f"{phase3.get('final_article_topic','')}. Avoid generic statements."
            )
        if include_backlink:
            section_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."
        if internal_links_prompt:
            section_user += (
                f"\nUse up to one internal link from this allowed list when contextually relevant: {internal_links_prompt}. "
                "Do not use external links."
            )

        try:
            raw = call_llm_text(
                system_prompt=section_system,
                user_prompt=section_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=3000,
                temperature=0.2,
                request_label=f"phase5_section_{index}",
                usage_collector=usage_collector,
            )
        except LLMError:
            raw = ""

        sections_html.append(_normalize_section_html(h2, h3s_list, raw))

    article_html = intro_html + "".join(sections_html)
    article_html = _repair_link_constraints(
        article_html=article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        internal_links=internal_link_candidates,
        internal_link_anchor_map=internal_link_anchor_map,
        min_internal_links=min_internal_links,
        max_internal_links=max_internal_links,
        backlink_placement=backlink_placement,
        anchor_text=anchor_text,
        required_h1=phase4.get("h1", ""),
    )
    article_html = _strip_empty_blocks(article_html)
    article_html = _strip_leading_empty_blocks(article_html)
    article_html = _ensure_required_h1(article_html, phase4.get("h1", ""))
    article_html = _trim_article_to_word_limit(article_html, ARTICLE_MAX_WORDS)

    word_count = word_count_from_html(article_html)
    for _expand_pass in range(max(0, expand_passes)):
        if word_count >= ARTICLE_MIN_WORDS:
            break
        expand_system = "Write an additional paragraph for a German (de-DE) blog post in HTML. Return only HTML."
        expand_user = (
            f"Topic: {phase3.get('final_article_topic','')}\n"
            f"Primary keyword: {phase3.get('primary_keyword','')}\n"
            f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
            f"Current word count: {word_count}. Need at least {ARTICLE_MIN_WORDS} words.\n"
            f"Write one additional paragraph of 80-120 words that fits the article. "
            "No hyperlinks. Language: German (de-DE)."
        )
        try:
            extra = call_llm_text(
                system_prompt=expand_system,
                user_prompt=expand_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=3000,
                temperature=0.2,
                request_label="phase5_section_expand",
                usage_collector=usage_collector,
            )
            article_html += _wrap_paragraphs(extra)
            word_count = word_count_from_html(article_html)
        except LLMError:
            break
    article_html = _ensure_required_h1(article_html, phase4.get("h1", ""))
    article_html = _trim_article_to_word_limit(article_html, ARTICLE_MAX_WORDS)

    meta_title = phase4.get("h1") or ""
    excerpt = ""
    match = re.search(r"<p[^>]*>(.*?)</p>", article_html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        excerpt = re.sub(r"<[^>]+>", "", match.group(1)).strip()[:200]

    return {
        "meta_title": meta_title,
        "meta_description": "",
        "slug": "",
        "excerpt": excerpt,
        "article_html": article_html,
    }


def _call_leonardo(
    *,
    prompt: str,
    api_key: str,
    base_url: str,
    model_id: str,
    width: int,
    height: int,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
) -> str:
    if not api_key:
        raise CreatorError("LEONARDO_API_KEY is required for image generation.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    create_url = base_url.rstrip("/") + "/generations"
    payload = {
        "prompt": prompt,
        "modelId": model_id,
        "width": width,
        "height": height,
        "num_images": 1,
        "negative_prompt": NEGATIVE_PROMPT,
        "guidance_scale": 6,
    }
    try:
        response = requests.post(create_url, headers=headers, json=payload, timeout=timeout_seconds)
    except Exception as exc:  # pragma: no cover - network
        raise CreatorError(f"Leonardo request failed: {exc}") from exc
    if response.status_code >= 400:
        raise CreatorError(f"Leonardo HTTP {response.status_code}: {response.text[:300]}")
    body = response.json()
    generation_id = body.get("sdGenerationJob", {}).get("generationId") or body.get("generationId")
    if not generation_id:
        raise CreatorError("Leonardo response missing generationId.")

    poll_url = base_url.rstrip("/") + f"/generations/{generation_id}"
    deadline = time.time() + poll_timeout_seconds
    while time.time() < deadline:
        try:
            poll = requests.get(poll_url, headers=headers, timeout=timeout_seconds)
        except Exception as exc:  # pragma: no cover - network
            raise CreatorError(f"Leonardo poll failed: {exc}") from exc
        if poll.status_code >= 400:
            raise CreatorError(f"Leonardo poll HTTP {poll.status_code}: {poll.text[:300]}")
        data = poll.json()
        generations = data.get("generations") or data.get("sdGenerationJob", {}).get("generations") or []
        if generations:
            url = generations[0].get("url") or generations[0].get("imageUrl")
            if url:
                return url
        time.sleep(poll_interval_seconds)
    raise CreatorError("Leonardo generation timed out.")


PHASE_LABELS: List[str] = [
    "",                          # index 0 unused
    "Analyzing target site",     # phase 1
    "Analyzing publishing site", # phase 2
    "Selecting topic",           # phase 3
    "Creating outline",          # phase 4
    "Writing article",           # phase 5
    "Generating images",         # phase 6
    "Final SEO checks",          # phase 7
]

def _noop_progress(phase: int, label: str, percent: int) -> None:
    pass


def run_creator_pipeline(
    *,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    anchor: Optional[str],
    topic: Optional[str],
    exclude_topics: Optional[List[str]] = None,
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
    phase1_cache_payload: Optional[Dict[str, Any]] = None,
    phase1_cache_content_hash: Optional[str] = None,
    phase2_cache_payload: Optional[Dict[str, Any]] = None,
    phase2_cache_content_hash: Optional[str] = None,
    target_profile_payload: Optional[Dict[str, Any]] = None,
    target_profile_content_hash: Optional[str] = None,
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    publishing_profile_content_hash: Optional[str] = None,
    dry_run: bool,
    on_progress: Optional[Callable[[int, str, int], None]] = None,
) -> Dict[str, Any]:
    progress = on_progress or _noop_progress
    warnings: List[str] = []
    phase_names = [f"phase{i}" for i in range(1, 8)]
    tokens_by_phase: Dict[str, Dict[str, int]] = {phase: _new_token_bucket() for phase in phase_names}
    tokens_by_label: Dict[str, Dict[str, int]] = {}
    debug: Dict[str, Any] = {
        "dry_run": dry_run,
        "timings_ms": {},
        "fetched_pages": [],
        "tokens_by_phase": tokens_by_phase,
        "tokens_by_label": tokens_by_label,
    }
    current_year = datetime.datetime.now().year

    http_timeout = _read_int_env("CREATOR_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    http_retries = _read_non_negative_int_env("CREATOR_HTTP_RETRIES", DEFAULT_HTTP_RETRIES)
    site_analysis_max_pages = max(1, _read_int_env("CREATOR_SITE_ANALYSIS_MAX_PAGES", DEFAULT_SITE_ANALYSIS_MAX_PAGES))
    phase2_prompt_chars = _read_int_env("CREATOR_PHASE2_PROMPT_CHARS", 2500)
    phase2_max_tokens = _read_int_env("CREATOR_PHASE2_MAX_TOKENS", 3000)
    phase4_max_attempts = 1
    phase4_max_tokens = _read_int_env("CREATOR_PHASE4_MAX_TOKENS", 3000)
    phase5_max_attempts = 1
    phase5_max_tokens_attempt1 = _read_int_env("CREATOR_PHASE5_MAX_TOKENS_ATTEMPT1", 3000)
    phase5_max_tokens_retry = _read_int_env("CREATOR_PHASE5_MAX_TOKENS_RETRY", 3000)
    phase5_fallback_expand_passes = 0
    phase7_repair_attempts = 0
    internal_link_min = max(0, _read_int_env("CREATOR_INTERNAL_LINK_MIN", DEFAULT_INTERNAL_LINK_MIN))
    internal_link_max = max(internal_link_min, _read_int_env("CREATOR_INTERNAL_LINK_MAX", DEFAULT_INTERNAL_LINK_MAX))
    internal_link_candidates_max = max(internal_link_max, _read_int_env("CREATOR_INTERNAL_LINK_CANDIDATES_MAX", 10))
    keyword_trends_enabled = _read_bool_env("CREATOR_KEYWORD_TRENDS_ENABLED", True)
    keyword_trends_timeout = max(1, _read_int_env("CREATOR_KEYWORD_TRENDS_TIMEOUT_SECONDS", 4))
    keyword_trends_max_terms = max(4, min(20, _read_int_env("CREATOR_KEYWORD_TRENDS_MAX_TERMS", 10)))
    keyword_trend_cache_ttl_seconds = max(
        3600,
        _read_int_env("CREATOR_KEYWORD_TREND_CACHE_TTL_SECONDS", DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS),
    )
    explicit_llm_key = os.getenv("CREATOR_LLM_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    explicit_shared_model = os.getenv("CREATOR_LLM_MODEL", "").strip()
    explicit_planning_model = os.getenv("CREATOR_LLM_MODEL_PLANNING", "").strip()
    explicit_writing_model = os.getenv("CREATOR_LLM_MODEL_WRITING", "").strip()
    planning_model = explicit_planning_model or explicit_shared_model
    writing_model = explicit_writing_model or explicit_shared_model
    explicit_base_url = os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    if not planning_model:
        planning_model = DEFAULT_ANTHROPIC_PLANNING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if not writing_model:
        writing_model = DEFAULT_ANTHROPIC_WRITING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL

    if explicit_base_url:
        llm_base_url = explicit_base_url
    elif anthropic_key and _model_prefers_anthropic(planning_model, writing_model):
        llm_base_url = "https://api.anthropic.com/v1"
    elif anthropic_key and not openai_key:
        llm_base_url = "https://api.anthropic.com/v1"
    else:
        llm_base_url = DEFAULT_LLM_BASE_URL

    if "anthropic" in llm_base_url.lower():
        llm_api_key = explicit_llm_key or anthropic_key or openai_key
    else:
        llm_api_key = explicit_llm_key or openai_key or anthropic_key

    debug["keyword_trends_enabled"] = keyword_trends_enabled
    debug["keyword_trend_cache_ttl_seconds"] = keyword_trend_cache_ttl_seconds
    provided_internal_link_inventory = _coerce_internal_link_inventory(internal_link_inventory)
    target_profile = _coerce_site_profile_payload(target_profile_payload)
    publishing_profile = _coerce_site_profile_payload(publishing_profile_payload)
    if not target_profile or not publishing_profile:
        raise CreatorError(
            "Creator requires deterministic target_profile and publishing_profile for profile-first topic selection."
        )
    debug["internal_link_inventory_count"] = len(provided_internal_link_inventory)

    def _collect_llm_usage(record: Dict[str, Any]) -> None:
        label = str(record.get("label") or "unspecified")
        phase_key = _phase_from_request_label(label)
        bucket = tokens_by_phase.get(phase_key)
        if bucket is None:
            bucket = _new_token_bucket()
            tokens_by_phase[phase_key] = bucket

        label_bucket = tokens_by_label.get(label)
        if label_bucket is None:
            label_bucket = _new_token_bucket()
            tokens_by_label[label] = label_bucket

        for target_bucket in (bucket, label_bucket):
            target_bucket["calls"] += 1
            target_bucket["prompt_tokens"] += _as_non_negative_int(record.get("prompt_tokens"))
            target_bucket["completion_tokens"] += _as_non_negative_int(record.get("completion_tokens"))
            target_bucket["total_tokens"] += _as_non_negative_int(record.get("total_tokens"))
            target_bucket["cache_creation_input_tokens"] += _as_non_negative_int(record.get("cache_creation_input_tokens"))
            target_bucket["cache_read_input_tokens"] += _as_non_negative_int(record.get("cache_read_input_tokens"))

    progress(1, PHASE_LABELS[1], 0)
    phase_start = time.time()
    logger.info("creator.phase1.start target=%s", target_site_url)
    normalized_target_url = _normalize_url(target_site_url)
    target_content_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile, sort_keys=True, ensure_ascii=False)
    )
    target_snapshot = {
        "pages": [],
        "sample_urls": list(target_profile.get("sample_urls") or []),
        "sample_page_titles": list(target_profile.get("sample_page_titles") or []),
        "combined_text": " ".join(
            _merge_string_lists(
                target_profile.get("topics") or [],
                target_profile.get("repeated_keywords") or [],
                target_profile.get("visible_headings") or [],
                max_items=24,
            )
        ),
        "content_hash": target_content_hash,
    }
    phase1 = _phase1_from_target_profile(target_profile, target_site_url=target_site_url)
    phase1_cache_hit = False
    phase1_cache_warm = False
    phase1_cache_meta = {
        "normalized_url": normalized_target_url,
        "content_hash": target_content_hash,
        "prompt_version": PHASE1_CACHE_PROMPT_VERSION,
        "generator_mode": "deterministic",
        "model_name": "",
        "cache_hit": False,
        "cacheable": True,
        "snapshot_page_count": len(target_snapshot.get("pages") or []),
        "sample_urls": list(target_snapshot.get("sample_urls") or []),
    }

    brand_name = str(phase1.get("brand_name") or "").strip()
    backlink_url = str(phase1.get("backlink_url") or "").strip() or (target_site_url or "")
    anchor_type = str(phase1.get("anchor_type") or "").strip()
    keyword_cluster = [str(item).strip() for item in (phase1.get("keyword_cluster") or []) if str(item).strip()]
    if anchor_type not in {"brand", "contextual_generic", "partial_match"}:
        anchor_type = "partial_match" if (not brand_name and keyword_cluster) else ("brand" if brand_name else "contextual_generic")
    phase1 = {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
        "site_summary": str(phase1.get("site_summary") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (phase1.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (phase1.get("sample_urls") or []) if str(item).strip()],
    }

    debug["phase1_cache_hit"] = phase1_cache_hit
    debug["phase1_snapshot"] = {
        "page_count": len(target_snapshot.get("pages") or []),
        "sample_urls": phase1.get("sample_urls") or [],
        "cache_fallback_used": phase1_cache_warm,
    }
    debug["timings_ms"]["phase1"] = int((time.time() - phase_start) * 1000)
    progress(1, PHASE_LABELS[1], 14)

    progress(2, PHASE_LABELS[2], 14)
    phase_start = time.time()
    logger.info("creator.phase2.start publishing=%s", publishing_site_url)
    normalized_publishing_url = _normalize_url(publishing_site_url)
    inventory_topic_insights = _build_inventory_topic_insights(provided_internal_link_inventory)
    publishing_content_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile, sort_keys=True, ensure_ascii=False)
    )
    publishing_snapshot = {
        "pages": [],
        "sample_urls": list(publishing_profile.get("sample_urls") or []),
        "sample_page_titles": list(publishing_profile.get("sample_page_titles") or []),
        "combined_text": " ".join(
            _merge_string_lists(
                publishing_profile.get("topics") or [],
                publishing_profile.get("topic_clusters") or [],
                publishing_profile.get("visible_headings") or [],
                max_items=24,
            )
        ),
        "content_hash": publishing_content_hash,
    }
    internal_link_candidates: List[str] = []
    effective_internal_min = 0
    effective_internal_max = 0
    phase2 = _phase2_from_publishing_profile(publishing_profile)
    phase2_cache_hit = False
    phase2_cache_warm = False
    phase2_cache_meta = {
        "normalized_url": normalized_publishing_url,
        "content_hash": publishing_content_hash,
        "prompt_version": PHASE2_CACHE_PROMPT_VERSION,
        "generator_mode": "deterministic",
        "model_name": "",
        "cache_hit": False,
        "cacheable": True,
        "snapshot_page_count": len(publishing_snapshot.get("pages") or []),
        "sample_urls": list(publishing_snapshot.get("sample_urls") or []),
    }
    phase2 = _merge_phase2_analysis(
        phase2,
        None,
        inventory_categories=inventory_topic_insights.get("site_categories") or [],
    )
    debug["phase2_cache_hit"] = phase2_cache_hit
    debug["phase2_snapshot"] = {
        "page_count": len(publishing_snapshot.get("pages") or []),
        "sample_urls": phase2.get("sample_urls") or [],
        "inventory_category_count": len(phase2.get("site_categories") or []),
        "cache_fallback_used": phase2_cache_warm,
    }
    debug["timings_ms"]["phase2"] = int((time.time() - phase_start) * 1000)
    progress(2, PHASE_LABELS[2], 28)

    progress(3, PHASE_LABELS[3], 28)
    phase_start = time.time()
    logger.info("creator.phase3.start")
    safe_exclude = list(exclude_topics or [])
    requested_topic = (topic or "").strip()
    target_profile_for_fit = target_profile
    publishing_profile_for_fit = publishing_profile
    target_profile_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile_for_fit, sort_keys=True, ensure_ascii=False)
    )
    publishing_profile_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile_for_fit, sort_keys=True, ensure_ascii=False)
    )
    pair_fit = None
    try:
        pair_fit = _run_pair_fit_reasoning(
            requested_topic=requested_topic,
            exclude_topics=safe_exclude,
            target_site_url=target_site_url,
            publishing_site_url=publishing_site_url,
            target_profile=target_profile_for_fit,
            publishing_profile=publishing_profile_for_fit,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            planning_model=planning_model,
            timeout_seconds=http_timeout,
            usage_collector=_collect_llm_usage,
        )
    except LLMError as exc:
        raise CreatorError(f"Pair fit reasoning failed: {exc}") from exc
    final_match_decision = str(pair_fit.get("final_match_decision") or "").strip().lower() or (
        "accepted" if bool(pair_fit.get("backlink_fit_ok")) else "hard_reject"
    )
    allow_rejected_pairs_for_testing = _read_bool_env("ALLOW_REJECTED_PAIRS_FOR_TESTING", False)
    if final_match_decision != "accepted":
        rejection_reason = str(
            pair_fit.get("reject_reason")
            or pair_fit.get("rejection_reason")
            or pair_fit.get("best_overlap_reason")
            or "no_natural_semantic_fit"
        ).strip()
        if not allow_rejected_pairs_for_testing:
            raise CreatorError(f"Pair fit rejected: {rejection_reason}")
        warnings.append(f"pair_fit_override_enabled:{final_match_decision}")
    elif final_match_decision == "accepted":
        pair_fit["backlink_fit_ok"] = True

    resolved_topic = str(pair_fit.get("final_article_topic") or requested_topic or "").strip()
    if not resolved_topic:
        raise CreatorError("Pair fit returned no final_article_topic.")
    phase3 = {
        "final_article_topic": resolved_topic,
        "search_intent_type": "informational",
        "primary_keyword": requested_topic or resolved_topic,
        "secondary_keywords": keyword_cluster[1:3] if len(keyword_cluster) > 1 else [],
        "pair_fit": pair_fit,
    }

    keyword_discovery: Dict[str, Any] = {"query_variants": [], "trend_candidates": [], "faq_candidates": []}
    if keyword_trends_enabled and phase3.get("final_article_topic"):
        keyword_discovery = _discover_keyword_candidates(
            topic=phase3.get("final_article_topic", ""),
            primary_hint=phase3.get("primary_keyword", ""),
            keyword_cluster=keyword_cluster,
            allowed_topics=phase2.get("allowed_topics") or [],
            timeout_seconds=keyword_trends_timeout,
            max_terms=keyword_trends_max_terms,
            trend_cache_ttl_seconds=keyword_trend_cache_ttl_seconds,
        )
    keyword_selection = _select_keywords(
        topic=phase3.get("final_article_topic", ""),
        llm_primary=phase3.get("primary_keyword", ""),
        llm_secondary=phase3.get("secondary_keywords") or [],
        keyword_cluster=keyword_cluster,
        allowed_topics=phase2.get("allowed_topics") or [],
        trend_candidates=keyword_discovery.get("trend_candidates") or [],
        faq_candidates=keyword_discovery.get("faq_candidates") or [],
    )
    phase3["primary_keyword"] = keyword_selection["primary_keyword"]
    phase3["secondary_keywords"] = keyword_selection["secondary_keywords"]
    phase3["faq_candidates"] = _ensure_faq_candidates(
        phase3.get("final_article_topic", ""),
        keyword_selection.get("faq_candidates") or [],
    )
    phase3["structured_content_mode"] = _structured_content_mode(
        phase3.get("final_article_topic", ""),
        phase3.get("primary_keyword", ""),
        phase3.get("search_intent_type", ""),
    )
    title_package = _build_deterministic_title_package(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        search_intent_type=phase3.get("search_intent_type", ""),
        structured_mode=phase3.get("structured_content_mode", "none"),
        current_year=current_year,
    )
    phase3["title_package"] = title_package
    ranked_internal_link_inventory = _rank_internal_link_inventory(
        provided_internal_link_inventory,
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        publishing_site_url=publishing_site_url,
        backlink_url=backlink_url,
        max_items=internal_link_candidates_max,
    )
    if ranked_internal_link_inventory:
        internal_link_candidates = _normalize_internal_link_candidates(
            [str(item.get("url") or "").strip() for item in ranked_internal_link_inventory],
            publishing_site_url=publishing_site_url,
            backlink_url=backlink_url,
            max_items=internal_link_candidates_max,
        )
        internal_link_source = "inventory"
        internal_link_anchor_map = {
            _normalize_url(str(item.get("url") or "").strip()): _build_internal_anchor_variants(item)
            for item in ranked_internal_link_inventory
            if str(item.get("url") or "").strip()
        }
        internal_links_prompt_entries = [
            f"{(str(item.get('title') or '').strip() or _internal_anchor_text(str(item.get('url') or '')))} -> {str(item.get('url') or '').strip()}"
            for item in ranked_internal_link_inventory
            if str(item.get("url") or "").strip()
        ]
    else:
        internal_link_candidates = homepage_internal_link_candidates
        internal_link_source = "homepage"
        internal_link_anchor_map = {
            _normalize_url(url): _internal_anchor_text(url)
            for url in internal_link_candidates
            if url
        }
        internal_links_prompt_entries = list(internal_link_candidates)
    effective_internal_min = min(internal_link_min, len(internal_link_candidates))
    effective_internal_max = min(internal_link_max, len(internal_link_candidates))
    if effective_internal_max < effective_internal_min:
        effective_internal_max = effective_internal_min
    debug["keyword_selection"] = {
        "primary_keyword": phase3["primary_keyword"],
        "secondary_keywords": phase3["secondary_keywords"],
        "trend_candidates": keyword_selection.get("trend_candidates") or [],
        "faq_candidates": keyword_selection.get("faq_candidates") or [],
        "query_variants": keyword_discovery.get("query_variants") or [],
        "trend_cache_events": keyword_discovery.get("trend_cache_events") or [],
        "structured_content_mode": phase3.get("structured_content_mode", "none"),
        "title_package": title_package,
        "pair_fit": pair_fit,
    }
    debug["internal_linking"] = {
        "configured_min": internal_link_min,
        "configured_max": internal_link_max,
        "effective_min": effective_internal_min,
        "effective_max": effective_internal_max,
        "candidate_count": len(internal_link_candidates),
        "candidate_source": internal_link_source,
        "candidates": internal_link_candidates[:8],
        "inventory_matches": ranked_internal_link_inventory[:5],
    }
    debug["timings_ms"]["phase3"] = int((time.time() - phase_start) * 1000)
    progress(3, PHASE_LABELS[3], 42)

    progress(4, PHASE_LABELS[4], 42)
    phase_start = time.time()
    logger.info("creator.phase4.start")
    anchor_safe = _is_anchor_safe(anchor)
    outline = None
    phase4 = {}
    outline_errors: List[str] = []
    faq_candidates = _ensure_faq_candidates(phase3.get("final_article_topic", ""), phase3.get("faq_candidates") or [])
    for attempt in range(1, phase4_max_attempts + 1):
        system_prompt = (
            f"Create a German (de-DE) SEO article outline using the REQUIRED H1 exactly. Provide {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections, optional H3. "
            "The penultimate H2 must be titled 'Fazit'. "
            "The final H2 must be titled 'FAQ'. "
            "Ensure keyword intent mapping: include the primary keyword in H1 and in at least one H2; "
            "cover secondary keywords naturally across remaining H2/H3 headings. "
            "When a structured content mode is provided, make room for that structure naturally in the outline. "
            f"If H1 includes a year, it must be {current_year} (no other years in titles). "
            "Ensure H3 headings only appear under their respective H2 parents (no orphan H3). "
            "Choose backlink placement as intro or one specific section (section_2..section_5). "
            "Return JSON only."
        )
        user_prompt = (
            f"Topic: {phase3['final_article_topic']}\n"
            f"Required H1: {phase3['title_package']['h1']}\n"
            f"Allowed topics: {phase2['allowed_topics']}\n"
            f"Primary keyword: {phase3['primary_keyword']}\n"
            f"Secondary keywords: {phase3['secondary_keywords']}\n"
            f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
            f"FAQ candidates: {faq_candidates[:3]}\n"
            f"Anchor provided: {anchor or ''}\n"
            f"Anchor safe: {anchor_safe}\n"
            "Language: German (de-DE).\n"
            "Return JSON: {\"outline\":[{\"h2\":\"...\",\"h3\":[\"...\"]}],"
            "\"backlink_placement\":\"intro|section_2|section_3|section_4|section_5\",\"anchor_text_final\":\"...\"}"
        )
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=planning_model,
                timeout_seconds=http_timeout,
                max_tokens=phase4_max_tokens,
                temperature=0.1,
                request_label="phase4",
                usage_collector=_collect_llm_usage,
            )
        except LLMError as exc:
            outline_errors.append(str(exc))
            continue

        h1 = phase3["title_package"]["h1"]
        outline_items = llm_out.get("outline") or []
        backlink_placement = (llm_out.get("backlink_placement") or "").strip()
        anchor_text_final = (llm_out.get("anchor_text_final") or "").strip()
        outline_items = _inject_faq_section(outline_items, faq_candidates, phase3.get("final_article_topic", ""))
        if not h1 or not isinstance(outline_items, list) or not (ARTICLE_MIN_H2 <= len(outline_items) <= ARTICLE_MAX_H2):
            outline_errors.append("invalid_outline_structure")
            continue
        if backlink_placement not in {"intro", "section_2", "section_3", "section_4", "section_5"}:
            outline_errors.append("invalid_backlink_placement")
            continue
        primary_keyword_phase3 = _normalize_keyword_phrase(phase3.get("primary_keyword", ""))
        h1_lower = _normalize_keyword_phrase(h1)
        outline_h2_combined = " ".join(
            _normalize_keyword_phrase(item.get("h2", "") if isinstance(item, dict) else str(item))
            for item in outline_items
        )
        if primary_keyword_phase3 and not _keyword_present(outline_h2_combined, primary_keyword_phase3):
            for idx, outline_item in enumerate(outline_items):
                if not isinstance(outline_item, dict):
                    continue
                current_h2 = str(outline_item.get("h2") or "").strip()
                normalized_h2 = _normalize_keyword_phrase(current_h2)
                if normalized_h2 in {"fazit", "faq"}:
                    continue
                outline_items[idx] = {
                    **outline_item,
                    "h2": f"{_format_title_case(phase3['primary_keyword'])}: {current_h2}",
                }
                break
            outline_h2_combined = " ".join(
                _normalize_keyword_phrase(item.get("h2", "") if isinstance(item, dict) else str(item))
                for item in outline_items
            )
            if not _keyword_present(outline_h2_combined, primary_keyword_phase3):
                outline_errors.append("primary_keyword_missing_in_outline")
                continue
        if _normalize_keyword_phrase(str(outline_items[-1].get("h2") or "") if isinstance(outline_items[-1], dict) else str(outline_items[-1])) != "faq":
            outline_errors.append("final_h2_not_faq")
            continue
        penultimate_item = outline_items[-2] if len(outline_items) >= 2 else {}
        penultimate_h2 = str(penultimate_item.get("h2") or "").strip() if isinstance(penultimate_item, dict) else str(penultimate_item).strip()
        if _normalize_keyword_phrase(penultimate_h2) != "fazit":
            outline_errors.append("penultimate_h2_not_fazit")
            continue
        if not anchor_text_final:
            anchor_text_final = anchor if anchor_safe else _build_anchor_text(anchor_type, brand_name, keyword_cluster)
        outline = {
            "h1": h1,
            "outline": outline_items,
            "backlink_placement": backlink_placement,
            "anchor_text_final": anchor_text_final,
        }
        break

    if not outline:
        raise CreatorError(f"Phase 4 outline generation failed: {outline_errors}")

    phase4 = outline
    debug["faq_generation"] = {
        "faq_enabled": True,
        "faq_candidates": faq_candidates[:3],
        "faq_in_outline": True,
    }
    debug["timings_ms"]["phase4"] = int((time.time() - phase_start) * 1000)
    progress(4, PHASE_LABELS[4], 56)

    progress(5, PHASE_LABELS[5], 56)
    phase_start = time.time()
    logger.info("creator.phase5.start")
    article_payload = None
    errors: List[str] = []
    backlink_url = phase1["backlink_url"]
    last_article_html = ""
    last_validation_errors: List[str] = []
    internal_links_prompt_text = internal_links_prompt_entries[:effective_internal_max]
    faq_prompt_text = phase3.get("faq_candidates") or []
    for attempt in range(1, phase5_max_attempts + 1):
        if attempt == 1:
            article_payload = _generate_article_by_sections(
                phase4=phase4,
                phase3=phase3,
                backlink_url=backlink_url,
                publishing_site_url=publishing_site_url,
                internal_link_candidates=internal_link_candidates,
                internal_link_anchor_map=internal_link_anchor_map,
                min_internal_links=effective_internal_min,
                max_internal_links=effective_internal_max,
                faq_candidates=faq_candidates,
                structured_mode=phase3.get("structured_content_mode", "none"),
                llm_api_key=llm_api_key,
                llm_base_url=llm_base_url,
                llm_model=writing_model,
                http_timeout=http_timeout,
                expand_passes=max(0, phase5_max_attempts - 1),
                usage_collector=_collect_llm_usage,
            )
            if not article_payload:
                errors.append("section_generation_failed")
                continue
            article_html = (article_payload.get("article_html") or "").strip()
            if not article_html:
                errors.append("missing_article_html")
                continue

            wc = word_count_from_html(article_html)
            logger.info("creator.phase5.attempt attempt=%s mode=sectioned word_count=%s", attempt, wc)

            validation_errors = _collect_article_validation_errors(
                article_html=article_html,
                meta_title=phase3["title_package"]["meta_title"],
                meta_description=_build_deterministic_meta_description(
                    topic=phase3["final_article_topic"],
                    primary_keyword=phase3["primary_keyword"],
                    secondary_keywords=phase3.get("secondary_keywords") or [],
                    structured_mode=phase3.get("structured_content_mode", "none"),
                ),
                slug=phase3["title_package"]["slug"],
                topic=phase3["final_article_topic"],
                primary_keyword=phase3.get("primary_keyword", ""),
                secondary_keywords=phase3.get("secondary_keywords") or [],
                required_h1=phase4["h1"],
                structured_mode=phase3.get("structured_content_mode", "none"),
                backlink_url=backlink_url,
                backlink_placement=phase4["backlink_placement"],
                publishing_site_url=publishing_site_url,
                min_internal_links=effective_internal_min,
                max_internal_links=effective_internal_max,
            )

            if validation_errors:
                errors.extend(validation_errors)
                last_article_html = article_html
                last_validation_errors = validation_errors
                continue

            article_payload = {
                "meta_title": phase4["h1"],
                "meta_description": "",
                "slug": "",
                "excerpt": article_payload.get("excerpt") or "",
                "article_html": article_html,
            }
            break
        elif last_article_html:
            system_prompt = (
                "Fix or rewrite the HTML to satisfy all constraints. Do not return markdown fences. "
                f"If H1 or meta_title includes a year, it must be {current_year} (no other years in titles). "
                "Keep the penultimate H2 titled 'Fazit' and the final H2 titled 'FAQ'. "
                "Maintain strict heading hierarchy: H3 headings must follow and belong to their H2 parents. "
                "If the outline includes an FAQ section, answer each FAQ H3 directly, avoid duplicate questions, and keep each answer concise but useful. "
                "Keep language strictly German (de-DE). Keep the final 'Fazit' topic-specific, not generic. "
                "Enforce keyword contract: primary in H1+intro+>=1 H2, and 4-6 secondary keywords covered naturally. "
                "Preserve the required meta title and slug unless they violate a hard validation rule. "
                "If structured content mode is 'list', include at least one meaningful HTML list. "
                "If structured content mode is 'table', include at least one meaningful HTML table. "
                "Enforce link contract: exactly one backlink to Backlink URL, "
                f"{effective_internal_min}-{effective_internal_max} internal links from allowed list, no other external links. "
                "Return only the fixed article HTML. Do not return JSON. Do not return markdown fences."
            )
            user_prompt = (
                f"Current article_html:\n{last_article_html}\n\n"
                f"Issues: {last_validation_errors}\n"
                f"Required H1: {phase4['h1']}\n"
                f"Required meta_title: {phase3['title_package']['meta_title']}\n"
                f"Required slug: {phase3['title_package']['slug']}\n"
                f"Required outline: {phase4['outline']}\n"
                f"Constraints: {ARTICLE_MIN_WORDS}-{ARTICLE_MAX_WORDS} words, H1 + {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections.\n"
                f"Backlink URL: {backlink_url}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Anchor text (use exactly): {phase4['anchor_text_final']}\n"
                f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
                f"FAQ candidates: {faq_prompt_text[:3]}\n"
                f"Allowed internal links (publishing site only): {internal_links_prompt_text}\n"
                f"Internal link rule: min {effective_internal_min}, max {effective_internal_max}\n"
                f"Topic for topic-specific Fazit: {phase3['final_article_topic']}\n"
                "Language: German (de-DE).\n"
                "Keyword rules: primary in H1+intro+>=1 H2, each secondary >=1 mention, natural density.\n"
                "Return only the fixed article HTML."
            )
            model_for_attempt = writing_model
            max_tokens = phase5_max_tokens_retry
            temperature = 0.2
        else:
            system_prompt = (
                f"Write a NEW German (de-DE) article from scratch. CRITICAL: the article body MUST be {ARTICLE_MIN_WORDS}-{ARTICLE_MAX_WORDS} words "
                "(aim for about 800 words). Each H2 section needs 1-2 substantial paragraphs. "
                "The penultimate H2 must be titled 'Fazit' and the final H2 must be titled 'FAQ'. "
                "Include exactly one backlink to the provided Backlink URL, plus internal links to the publishing site. "
                "No external links beyond the backlink. "
                "Keyword contract: primary keyword must appear in H1, first paragraph, and at least one H2. "
                "Use 4-6 secondary keywords naturally in the body at least once each. Avoid keyword stuffing. "
                "Follow the required meta title and slug exactly unless they violate a hard validation rule. "
                f"If H1 or meta_title includes a year, it must be {current_year} (no other years in titles). "
                "In body content, historical years or specific dates only when necessary for factual accuracy. "
                "Maintain strict heading hierarchy: H3 headings must follow and belong to their H2 parents. "
                "If structured content mode is 'list', include at least one meaningful HTML list. "
                "If structured content mode is 'table', include at least one meaningful HTML table. "
                "If the outline includes an FAQ section, answer each FAQ H3 directly, avoid duplicate questions, and keep each answer concise but useful. "
                "The final 'Fazit' must summarize the specific article topic (not generic text). "
                "Return only the final article HTML. Do not return JSON. Do not return markdown fences."
            )
            user_prompt = (
                f"H1: {phase4['h1']}\n"
                f"Required meta_title: {phase3['title_package']['meta_title']}\n"
                f"Required slug: {phase3['title_package']['slug']}\n"
                f"Outline: {phase4['outline']}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Backlink URL: {backlink_url}\n"
                f"Anchor text (use exactly): {phase4['anchor_text_final']}\n"
                f"Allowed internal links (publishing site only): {internal_links_prompt_text}\n"
                f"Internal link rule: min {effective_internal_min}, max {effective_internal_max}\n"
                f"Constraints: {ARTICLE_MIN_WORDS}-{ARTICLE_MAX_WORDS} words (aim for about 800), H1 + {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections, "
                "neutral authoritative tone, no CTA spam, no 'visit our site' language.\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
                f"FAQ candidates: {faq_prompt_text[:3]}\n"
                f"Topic for topic-specific Fazit: {phase3['final_article_topic']}\n"
                "Language: German (de-DE).\n"
                "Keyword rules: primary in H1+intro+>=1 H2, each secondary >=1 mention, natural density.\n"
                "Return only the final article HTML."
            )
            model_for_attempt = writing_model
            max_tokens = phase5_max_tokens_retry
            temperature = 0.2
        try:
            article_html = call_llm_text(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=model_for_attempt,
                timeout_seconds=http_timeout,
                max_tokens=max_tokens,
                temperature=temperature,
                request_label=f"phase5_attempt_{attempt}",
                usage_collector=_collect_llm_usage,
            )
        except LLMError as exc:
            errors.append(str(exc))
            continue

        article_html = (article_html or "").strip()
        if not article_html:
            errors.append("missing_article_html")
            continue

        wc = word_count_from_html(article_html)
        logger.info(
            "creator.phase5.attempt attempt=%s word_count=%s",
            attempt, wc,
        )

        validation_errors = _collect_article_validation_errors(
            article_html=article_html,
            meta_title=phase3["title_package"]["meta_title"],
            meta_description=_build_deterministic_meta_description(
                topic=phase3["final_article_topic"],
                primary_keyword=phase3["primary_keyword"],
                secondary_keywords=phase3.get("secondary_keywords") or [],
                structured_mode=phase3.get("structured_content_mode", "none"),
            ),
            slug=phase3["title_package"]["slug"],
            topic=phase3["final_article_topic"],
            primary_keyword=phase3.get("primary_keyword", ""),
            secondary_keywords=phase3.get("secondary_keywords") or [],
            required_h1=phase4["h1"],
            structured_mode=phase3.get("structured_content_mode", "none"),
            backlink_url=backlink_url,
            backlink_placement=phase4["backlink_placement"],
            publishing_site_url=publishing_site_url,
            min_internal_links=effective_internal_min,
            max_internal_links=effective_internal_max,
        )

        if validation_errors:
            errors.extend(validation_errors)
            last_article_html = article_html
            last_validation_errors = validation_errors
            continue

        article_payload = {
            "meta_title": phase4["h1"],
            "meta_description": "",
            "slug": "",
            "excerpt": "",
            "article_html": article_html,
        }
        break

    if not article_payload:
        raise CreatorError(f"Article generation failed: {errors}")

    art_html = (article_payload.get("article_html") or "").strip()
    art_html = _strip_empty_blocks(art_html)
    art_html = _strip_leading_empty_blocks(art_html)
    article_payload["article_html"] = art_html
    article_payload["meta_title"] = phase3["title_package"]["meta_title"]
    article_payload["slug"] = phase3["title_package"]["slug"]
    article_payload["meta_description"] = _build_deterministic_meta_description(
        topic=phase3["final_article_topic"],
        primary_keyword=phase3["primary_keyword"],
        secondary_keywords=phase3.get("secondary_keywords") or [],
        structured_mode=phase3.get("structured_content_mode", "none"),
    )
    article_payload = _fill_article_metadata(article_payload, phase4["h1"])
    faq_enrichment_debug: Dict[str, Any] = {"enabled": True, "applied": False, "queries": [], "search_questions": [], "items": []}
    try:
        faq_enrichment = _generate_search_informed_faqs(
            article_html=article_payload["article_html"],
            topic=phase3["final_article_topic"],
            primary_keyword=phase3["primary_keyword"],
            secondary_keywords=phase3.get("secondary_keywords") or [],
            current_faq_candidates=phase3.get("faq_candidates") or [],
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=writing_model,
            timeout_seconds=http_timeout,
            usage_collector=_collect_llm_usage,
        )
        article_payload["article_html"] = _replace_faq_section(
            article_payload["article_html"],
            faq_enrichment["faq_html"],
        )
        faq_enrichment_debug = {
            "enabled": True,
            "applied": True,
            "queries": faq_enrichment["queries"],
            "search_questions": faq_enrichment["search_questions"],
            "items": faq_enrichment["faqs"],
        }
    except (CreatorError, LLMError) as exc:
        warnings.append(f"phase5_faq_enrichment_failed:{exc}")
        faq_enrichment_debug["error"] = str(exc)

    phase5 = article_payload
    debug["faq_enrichment"] = faq_enrichment_debug
    debug["timings_ms"]["phase5"] = int((time.time() - phase_start) * 1000)
    progress(5, PHASE_LABELS[5], 70)

    progress(6, PHASE_LABELS[6], 70)
    phase_start = time.time()
    logger.info("creator.phase6.start")
    phase6 = {
        "image_model": "Leonardo Flux Schnell",
        "featured_image": {},
        "in_content_image": {},
    }

    image_prompts = _build_deterministic_image_prompts(phase3["final_article_topic"])

    featured_prompt = (image_prompts.get("featured_prompt") or "").strip()
    featured_alt = (image_prompts.get("featured_alt") or "").strip()
    include_in_content = bool(image_prompts.get("include_in_content"))
    in_content_prompt = (image_prompts.get("in_content_prompt") or "").strip()
    in_content_alt = (image_prompts.get("in_content_alt") or "").strip()

    if not featured_prompt:
        featured_prompt = f"Editorial photo illustrating: {phase3['final_article_topic']}"
    if not featured_alt:
        featured_alt = phase3["final_article_topic"]

    leonardo_api_key = os.getenv("LEONARDO_API_KEY", "").strip()
    leonardo_base_url = os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip()
    width = _read_int_env("CREATOR_IMAGE_WIDTH", DEFAULT_IMAGE_WIDTH)
    height = _read_int_env("CREATOR_IMAGE_HEIGHT", DEFAULT_IMAGE_HEIGHT)
    poll_timeout = _read_int_env("CREATOR_IMAGE_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS)
    poll_interval = _read_int_env("CREATOR_IMAGE_POLL_INTERVAL_SECONDS", DEFAULT_POLL_SECONDS)
    image_generation_enabled = _read_bool_env("CREATOR_IMAGE_GENERATION_ENABLED", False)
    image_required = _read_bool_env("CREATOR_IMAGE_REQUIRED", False)

    featured_image_url = ""
    in_content_image_url = ""
    if not image_generation_enabled:
        warnings.append("phase6_image_generation_disabled")
        logger.info("creator.phase6.skip reason=image_generation_disabled")
        include_in_content = False
    elif not dry_run:
        try:
            featured_image_url = _call_leonardo(
                prompt=featured_prompt,
                api_key=leonardo_api_key,
                base_url=leonardo_base_url,
                model_id=DEFAULT_LEONARDO_MODEL_ID,
                width=width,
                height=height,
                timeout_seconds=http_timeout,
                poll_timeout_seconds=poll_timeout,
                poll_interval_seconds=poll_interval,
            )
        except CreatorError as exc:
            warnings.append(f"phase6_featured_image_failed:{exc}")
            featured_image_url = ""
            include_in_content = False

        if include_in_content and in_content_prompt:
            try:
                in_content_image_url = _call_leonardo(
                    prompt=in_content_prompt,
                    api_key=leonardo_api_key,
                    base_url=leonardo_base_url,
                    model_id=DEFAULT_LEONARDO_MODEL_ID,
                    width=width,
                    height=height,
                    timeout_seconds=http_timeout,
                    poll_timeout_seconds=poll_timeout,
                    poll_interval_seconds=poll_interval,
                )
            except CreatorError as exc:
                warnings.append(f"phase6_in_content_image_failed:{exc}")
                in_content_image_url = ""

    if image_required and image_generation_enabled and not featured_image_url and not dry_run:
        raise CreatorError("Featured image generation failed.")

    phase6["featured_image"] = {
        "prompt": featured_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": featured_alt,
    }
    phase6["in_content_image"] = {
        "prompt": in_content_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": in_content_alt,
    }
    debug["timings_ms"]["phase6"] = int((time.time() - phase_start) * 1000)
    progress(6, PHASE_LABELS[6], 85)

    progress(7, PHASE_LABELS[7], 85)
    phase_start = time.time()
    p7_wc = word_count_from_html(phase5["article_html"])
    logger.info("creator.phase7.start word_count=%s", p7_wc)
    phase7_errors: List[str] = []
    allowed_topics = [t.lower() for t in phase2.get("allowed_topics") or [] if isinstance(t, str)]
    if allowed_topics:
        topic_lower = (phase3["final_article_topic"] or "").lower()
        if not any(topic in topic_lower for topic in allowed_topics):
            warnings.append("topic_not_in_allowed_topics")
    phase7_errors = _collect_article_validation_errors(
        article_html=phase5["article_html"],
        meta_title=phase5.get("meta_title") or phase3["title_package"]["meta_title"],
        meta_description=phase5.get("meta_description") or _build_deterministic_meta_description(
            topic=phase3["final_article_topic"],
            primary_keyword=phase3["primary_keyword"],
            secondary_keywords=phase3.get("secondary_keywords") or [],
            structured_mode=phase3.get("structured_content_mode", "none"),
        ),
        slug=phase5.get("slug") or phase3["title_package"]["slug"],
        topic=phase3["final_article_topic"],
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        required_h1=phase4["h1"],
        structured_mode=phase3.get("structured_content_mode", "none"),
        backlink_url=backlink_url,
        backlink_placement=phase4["backlink_placement"],
        publishing_site_url=publishing_site_url,
        min_internal_links=effective_internal_min,
        max_internal_links=effective_internal_max,
    )

    if phase7_errors:
        current_wc = word_count_from_html(phase5["article_html"])
        logger.info("creator.phase7.issues errors=%s word_count=%s", phase7_errors, current_wc)
        for repair_attempt in range(phase7_repair_attempts):
            wc_ok = ARTICLE_MIN_WORDS <= current_wc <= ARTICLE_MAX_WORDS
            if wc_ok:
                wc_instruction = (
                    f"The word count ({current_wc}) is fine - do NOT add or remove content. "
                    "Only fix the specific issues listed below."
                )
            else:
                wc_instruction = (
                    f"The article currently has {current_wc} words. "
                    f"Adjust it to be between {ARTICLE_MIN_WORDS} and {ARTICLE_MAX_WORDS} words."
                )
            system_prompt = (
                "Fix the HTML article to satisfy SEO checks. "
                f"{wc_instruction} "
                f"Keep {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections. "
                "The penultimate H2 must be 'Fazit' and the final H2 must be 'FAQ'. "
                "Enforce link contract: exactly one backlink to Backlink URL, "
                f"{effective_internal_min}-{effective_internal_max} internal links from allowed list, no other external links. "
                f"If H1 or meta_title includes a year, it must be {current_year} (no other years in titles). "
                "Maintain strict heading hierarchy: H3 headings must follow and belong to their H2 parents. "
                "Keep the required meta title and slug aligned with the SEO contract. "
                "If structured content mode is 'list', include at least one meaningful HTML list. "
                "If structured content mode is 'table', include at least one meaningful HTML table. "
                "If the outline includes an FAQ section, answer the FAQ H3 headings directly, avoid duplicate questions, and keep each answer concise but useful. "
                "Language must be strictly German (de-DE). Keep the final 'Fazit' topic-specific and non-generic. "
                "Keyword contract: primary in H1+intro+>=1 H2 and 4-6 secondary keywords covered naturally. "
                "Return JSON only."
            )
            user_prompt = (
                f"Article_html: {phase5['article_html']}\n"
                f"Issues to fix: {phase7_errors}\n"
                f"Current word count: {current_wc}\n"
                f"Required meta_title: {phase3['title_package']['meta_title']}\n"
                f"Required slug: {phase3['title_package']['slug']}\n"
                f"Backlink URL: {backlink_url}\n"
                f"Placement: {phase4['backlink_placement']}\n"
                f"Anchor text: {phase4['anchor_text_final']}\n"
                f"FAQ candidates: {(phase3.get('faq_candidates') or [])[:3]}\n"
                f"Allowed internal links (publishing site only): {internal_links_prompt_text}\n"
                f"Internal link rule: min {effective_internal_min}, max {effective_internal_max}\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
                f"Topic for topic-specific Fazit: {phase3['final_article_topic']}\n"
                "Language: German (de-DE).\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            try:
                llm_out = call_llm_json(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    api_key=llm_api_key,
                    base_url=llm_base_url,
                    model=planning_model,
                    timeout_seconds=http_timeout,
                    max_tokens=3000,
                    allow_html_fallback=False,
                    request_label="phase7_repair",
                    usage_collector=_collect_llm_usage,
                )
                fixed_html = (llm_out.get("article_html") or "").strip()
                fixed_wc = word_count_from_html(fixed_html) if fixed_html else 0
                logger.info("creator.phase7.fix_result attempt=%s before=%s after=%s", repair_attempt + 1, current_wc, fixed_wc)
                if fixed_html and ARTICLE_MIN_WORDS <= fixed_wc <= ARTICLE_MAX_WORDS:
                    phase5["article_html"] = fixed_html
                elif fixed_html and fixed_wc > 0 and not wc_ok:
                    phase5["article_html"] = fixed_html
                phase5["article_html"] = _repair_link_constraints(
                    article_html=phase5["article_html"],
                    backlink_url=backlink_url,
                    publishing_site_url=publishing_site_url,
                    internal_links=internal_link_candidates,
                    internal_link_anchor_map=internal_link_anchor_map,
                    min_internal_links=effective_internal_min,
                    max_internal_links=effective_internal_max,
                    backlink_placement=phase4["backlink_placement"],
                    anchor_text=phase4["anchor_text_final"],
                    required_h1=phase4["h1"],
                )
                phase5["meta_title"] = llm_out.get("meta_title") or phase5["meta_title"]
                phase5["meta_description"] = llm_out.get("meta_description") or phase5["meta_description"]
                phase5["slug"] = llm_out.get("slug") or phase5["slug"]
                phase5["excerpt"] = llm_out.get("excerpt") or phase5["excerpt"]
                phase5["meta_title"] = phase3["title_package"]["meta_title"]
                phase5["slug"] = phase3["title_package"]["slug"]
                phase5["meta_description"] = _build_deterministic_meta_description(
                    topic=phase3["final_article_topic"],
                    primary_keyword=phase3["primary_keyword"],
                    secondary_keywords=phase3.get("secondary_keywords") or [],
                    structured_mode=phase3.get("structured_content_mode", "none"),
                )
                phase5 = _fill_article_metadata(phase5, phase4["h1"])
                phase7_errors = _collect_article_validation_errors(
                    article_html=phase5["article_html"],
                    meta_title=phase5.get("meta_title") or phase3["title_package"]["meta_title"],
                    meta_description=phase5.get("meta_description") or "",
                    slug=phase5.get("slug") or phase3["title_package"]["slug"],
                    topic=phase3["final_article_topic"],
                    primary_keyword=phase3.get("primary_keyword", ""),
                    secondary_keywords=phase3.get("secondary_keywords") or [],
                    required_h1=phase4["h1"],
                    structured_mode=phase3.get("structured_content_mode", "none"),
                    backlink_url=backlink_url,
                    backlink_placement=phase4["backlink_placement"],
                    publishing_site_url=publishing_site_url,
                    min_internal_links=effective_internal_min,
                    max_internal_links=effective_internal_max,
                )
                current_wc = word_count_from_html(phase5["article_html"])
                if not phase7_errors:
                    break
            except LLMError as exc:
                phase7_errors.append(f"phase7_fix_failed:{exc}")
                break

    if phase7_errors:
        raise CreatorError(f"Final SEO checks failed: {phase7_errors}")

    seo_evaluation = _score_seo_output(
        article_html=phase5["article_html"],
        meta_title=phase5.get("meta_title") or "",
        meta_description=phase5.get("meta_description") or "",
        slug=phase5.get("slug") or "",
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        required_h1=phase4["h1"],
        structured_mode=phase3.get("structured_content_mode", "none"),
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        min_internal_links=effective_internal_min,
        max_internal_links=effective_internal_max,
        topic=phase3["final_article_topic"],
    )
    debug["seo_evaluation"] = seo_evaluation

    debug["timings_ms"]["phase7"] = int((time.time() - phase_start) * 1000)
    progress(7, PHASE_LABELS[7], 100)

    images: List[Dict[str, str]] = []
    if featured_image_url:
        images.append({"type": "featured", "id_or_url": featured_image_url})
    if in_content_image_url:
        images.append({"type": "in_content", "id_or_url": in_content_image_url})

    debug["token_phase_ranking"] = sorted(
        [{"phase": phase_name, **tokens_by_phase.get(phase_name, _new_token_bucket())} for phase_name in phase_names],
        key=lambda item: (
            -item["total_tokens"],
            -item["prompt_tokens"],
            -item["completion_tokens"],
            -item["calls"],
        ),
    )
    debug["llm_tokens_total"] = {
        "calls": sum(bucket.get("calls", 0) for bucket in tokens_by_phase.values()),
        "prompt_tokens": sum(bucket.get("prompt_tokens", 0) for bucket in tokens_by_phase.values()),
        "completion_tokens": sum(bucket.get("completion_tokens", 0) for bucket in tokens_by_phase.values()),
        "total_tokens": sum(bucket.get("total_tokens", 0) for bucket in tokens_by_phase.values()),
        "cache_creation_input_tokens": sum(bucket.get("cache_creation_input_tokens", 0) for bucket in tokens_by_phase.values()),
        "cache_read_input_tokens": sum(bucket.get("cache_read_input_tokens", 0) for bucket in tokens_by_phase.values()),
    }

    return {
        "ok": True,
        "target_site_url": target_site_url,
        "host_site_url": publishing_site_url,
        "phase1": phase1,
        "phase1_cache_meta": phase1_cache_meta,
        "phase2": phase2,
        "phase2_cache_meta": phase2_cache_meta,
        "phase3": phase3,
        "phase4": phase4,
        "phase5": phase5,
        "phase6": phase6,
        "seo_evaluation": seo_evaluation,
        "images": images,
        "warnings": warnings,
        "debug": debug,
    }
