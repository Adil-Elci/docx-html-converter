from __future__ import annotations

import datetime
import hashlib
import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import requests

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
DEFAULT_HTTP_RETRIES = 2
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1024
DEFAULT_IMAGE_HEIGHT = 576
DEFAULT_POLL_SECONDS = 2
DEFAULT_POLL_TIMEOUT_SECONDS = 90
PHASE1_CACHE_PROMPT_VERSION = "v3"
PHASE2_CACHE_PROMPT_VERSION = "v3"
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
        validate_word_count(article_html, 600, 850),
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
    normalized = _normalize_keyword_phrase(question)
    if not normalized:
        return ""
    formatted = normalized[:1].upper() + normalized[1:]
    if _looks_like_question_phrase(normalized) and not formatted.endswith("?"):
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
    cleaned = re.sub(r"<h1[^>]*>.*?</h1>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if "<h2" in cleaned:
        return cleaned
    body = _wrap_paragraphs(cleaned)
    html = f"<h2>{h2}</h2>"
    if h3s:
        for h3 in h3s:
            html += f"<h3>{h3}</h3>"
    if body:
        html += body
    return html


def _strip_h1_tags(html: str) -> str:
    return re.sub(r"<h1[^>]*>.*?</h1>", "", html, flags=re.IGNORECASE | re.DOTALL)


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
        index = max(0, int(placement.split("_")[1]) - 2)
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
    repaired = _strip_h1_tags(repaired)
    repaired = _strip_empty_blocks(repaired)
    repaired = _strip_leading_empty_blocks(repaired)
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
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Optional[Dict[str, Any]]:
    outline_items = phase4.get("outline") or []
    if not isinstance(outline_items, list) or not outline_items:
        return None

    h2_count = len(outline_items)
    intro_target = 100
    target_total = 750
    per_section = max(100, int((target_total - intro_target) / max(1, h2_count)))
    per_min = max(80, per_section - 20)
    per_max = min(200, per_section + 30)

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
            max_tokens=320,
            temperature=0.2,
            request_label="phase5_fallback_intro",
            usage_collector=usage_collector,
        )
    except LLMError:
        intro_raw = ""
    intro_html = _wrap_paragraphs(intro_raw) or "<p></p>"

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
                "Use the H3 questions as subheadings, avoid duplicate questions, and write 35-60 words per answer in German."
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
                max_tokens=750,
                temperature=0.2,
                request_label=f"phase5_fallback_section_{index}",
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
    )
    article_html = _strip_h1_tags(article_html)
    article_html = _strip_empty_blocks(article_html)
    article_html = _strip_leading_empty_blocks(article_html)

    word_count = word_count_from_html(article_html)
    for _expand_pass in range(3):
        if word_count >= 650:
            break
        expand_system = "Write an additional paragraph for a German (de-DE) blog post in HTML. Return only HTML."
        expand_user = (
            f"Topic: {phase3.get('final_article_topic','')}\n"
            f"Primary keyword: {phase3.get('primary_keyword','')}\n"
            f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
            f"Current word count: {word_count}. Need at least 650 words.\n"
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
                max_tokens=220,
                temperature=0.2,
                request_label="phase5_fallback_expand",
                usage_collector=usage_collector,
            )
            article_html += _wrap_paragraphs(extra)
            word_count = word_count_from_html(article_html)
        except LLMError:
            break

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
    anchor: Optional[str],
    topic: Optional[str],
    exclude_topics: Optional[List[str]] = None,
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
    phase1_cache_payload: Optional[Dict[str, Any]] = None,
    phase1_cache_content_hash: Optional[str] = None,
    phase2_cache_payload: Optional[Dict[str, Any]] = None,
    phase2_cache_content_hash: Optional[str] = None,
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
    http_retries = _read_int_env("CREATOR_HTTP_RETRIES", DEFAULT_HTTP_RETRIES)
    site_analysis_max_pages = max(1, _read_int_env("CREATOR_SITE_ANALYSIS_MAX_PAGES", DEFAULT_SITE_ANALYSIS_MAX_PAGES))
    phase2_prompt_chars = _read_int_env("CREATOR_PHASE2_PROMPT_CHARS", 2500)
    phase2_max_tokens = _read_int_env("CREATOR_PHASE2_MAX_TOKENS", 400)
    phase5_max_attempts = max(1, min(2, _read_int_env("CREATOR_PHASE5_MAX_ATTEMPTS", 2)))
    phase5_max_tokens_attempt1 = _read_int_env("CREATOR_PHASE5_MAX_TOKENS_ATTEMPT1", 1800)
    phase5_max_tokens_retry = _read_int_env("CREATOR_PHASE5_MAX_TOKENS_RETRY", 1200)
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
    target_html = fetch_url(
        target_site_url,
        purpose="target_home",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
    )
    target_seed_links = extract_internal_links(target_html, target_site_url, limit=max(4, site_analysis_max_pages * 2))
    target_snapshot = _build_site_snapshot(
        site_url=target_site_url,
        homepage_html=target_html,
        candidate_urls=target_seed_links,
        purpose_prefix="target_snapshot",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
        max_pages=site_analysis_max_pages,
    )
    target_text = target_snapshot["combined_text"]
    target_content_hash = target_snapshot["content_hash"]
    normalized_target_url = _normalize_url(target_site_url)
    brand_name = _guess_brand_name(target_site_url, target_html)
    keyword_cluster = _extract_keywords(target_text, max_terms=10)
    backlink_url = _pick_backlink_url(target_site_url, target_html) or target_site_url
    anchor_type = "brand" if brand_name else "contextual_generic"
    if not brand_name and keyword_cluster:
        anchor_type = "partial_match"
    if not keyword_cluster:
        warnings.append("target_keywords_missing")
    phase1 = {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
        "site_summary": str(target_snapshot.get("site_summary") or "").strip(),
        "sample_page_titles": list(target_snapshot.get("sample_page_titles") or []),
        "sample_urls": list(target_snapshot.get("sample_urls") or []),
    }
    phase1_cache_hit = False
    phase1_cache_warm = False
    phase1_cache_meta = {
        "normalized_url": normalized_target_url,
        "content_hash": target_content_hash,
        "prompt_version": PHASE1_CACHE_PROMPT_VERSION,
        "generator_mode": "deterministic",
        "model_name": "",
        "cache_hit": False,
        "cacheable": bool(target_text),
        "snapshot_page_count": len(target_snapshot.get("pages") or []),
        "sample_urls": list(target_snapshot.get("sample_urls") or []),
    }
    cached_phase1 = _coerce_phase1_payload(phase1_cache_payload)
    if target_text and cached_phase1 and (phase1_cache_content_hash or "").strip() == target_content_hash:
        phase1 = cached_phase1
        phase1_cache_hit = True
        phase1_cache_meta["cache_hit"] = True
    elif cached_phase1 and not target_text:
        phase1 = cached_phase1
        phase1_cache_warm = True
        warnings.append("phase1_cache_fallback_used")
    elif cached_phase1:
        phase1["keyword_cluster"] = _merge_string_lists(
            phase1.get("keyword_cluster") or [],
            cached_phase1.get("keyword_cluster") or [],
            max_items=10,
        )
        phase1["sample_page_titles"] = _merge_string_lists(
            phase1.get("sample_page_titles") or [],
            cached_phase1.get("sample_page_titles") or [],
            max_items=8,
        )
        phase1["sample_urls"] = _merge_string_lists(
            phase1.get("sample_urls") or [],
            cached_phase1.get("sample_urls") or [],
            max_items=8,
        )
        phase1["site_summary"] = str(phase1.get("site_summary") or "").strip() or str(cached_phase1.get("site_summary") or "").strip()

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
    publishing_html = fetch_url(
        publishing_site_url,
        purpose="host_home",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
    )
    normalized_publishing_url = _normalize_url(publishing_site_url)
    inventory_topic_insights = _build_inventory_topic_insights(provided_internal_link_inventory)
    raw_internal_links = extract_internal_links(
        publishing_html,
        publishing_site_url,
        limit=max(internal_link_candidates_max, site_analysis_max_pages * 2),
    )
    inventory_seed_urls = [str(item.get("url") or "").strip() for item in provided_internal_link_inventory[: max(0, site_analysis_max_pages - 1)]]
    publishing_snapshot = _build_site_snapshot(
        site_url=publishing_site_url,
        homepage_html=publishing_html,
        candidate_urls=inventory_seed_urls + raw_internal_links,
        purpose_prefix="publishing_snapshot",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
        max_pages=site_analysis_max_pages,
    )
    publishing_text = publishing_snapshot["combined_text"]
    publishing_content_hash = publishing_snapshot["content_hash"]
    homepage_internal_link_candidates = _normalize_internal_link_candidates(
        raw_internal_links,
        publishing_site_url=publishing_site_url,
        backlink_url=backlink_url,
        max_items=internal_link_candidates_max,
    )
    internal_link_candidates: List[str] = []
    effective_internal_min = 0
    effective_internal_max = 0
    if not publishing_text:
        warnings.append("publishing_site_fetch_empty")
    phase2 = {
        "allowed_topics": [],
        "content_style_constraints": [],
        "internal_linking_opportunities": [],
        "site_summary": str(publishing_snapshot.get("site_summary") or "").strip(),
        "site_categories": inventory_topic_insights.get("site_categories") or [],
        "topic_clusters": inventory_topic_insights.get("topic_clusters") or [],
        "prominent_titles": inventory_topic_insights.get("prominent_titles") or [],
        "sample_page_titles": list(publishing_snapshot.get("sample_page_titles") or []),
        "sample_urls": list(publishing_snapshot.get("sample_urls") or []),
    }
    phase2_cache_hit = False
    phase2_cache_warm = False
    phase2_cache_meta = {
        "normalized_url": normalized_publishing_url,
        "content_hash": publishing_content_hash,
        "prompt_version": PHASE2_CACHE_PROMPT_VERSION,
        "generator_mode": "llm",
        "model_name": planning_model,
        "cache_hit": False,
        "cacheable": True,
        "snapshot_page_count": len(publishing_snapshot.get("pages") or []),
        "sample_urls": list(publishing_snapshot.get("sample_urls") or []),
    }
    if publishing_text:
        cached_phase2 = _coerce_phase2_payload(phase2_cache_payload)
        if cached_phase2 and (phase2_cache_content_hash or "").strip() == publishing_content_hash:
            phase2 = cached_phase2
            phase2_cache_hit = True
            phase2_cache_meta["cache_hit"] = True
            phase2_cache_meta["cacheable"] = True
        else:
            cached_context = ""
            if cached_phase2:
                phase2_cache_warm = True
                cached_context = (
                    f"Vorherige gecachte Zusammenfassung: {cached_phase2.get('site_summary') or ''}\n"
                    f"Vorherige gecachte Themen: {cached_phase2.get('allowed_topics') or []}\n"
                    f"Vorherige gecachte Kategorien: {cached_phase2.get('site_categories') or []}\n"
                    f"Vorherige gecachte Themencluster: {cached_phase2.get('topic_clusters') or []}\n"
                    f"Vorherige gecachte Seitentitel: {cached_phase2.get('sample_page_titles') or []}\n"
                )
            system_prompt = (
                "You analyze publishing site content for safe submitted article topics. "
                "Use only the provided site text. Return JSON with allowed_topics (5-10), "
                "content_style_constraints (3-6), internal_linking_opportunities (optional, internal only), "
                "site_summary (1 short sentence in German), site_categories (up to 8), topic_clusters (up to 8), "
                "prominent_titles (up to 6), sample_page_titles (up to 6). "
                "All returned natural-language text must be in German (de-DE)."
            )
            user_prompt = (
                "Publishing site snapshot text:\n"
                f"{publishing_text[:phase2_prompt_chars]}\n\n"
                f"Bekannte Kategorien aus dem internen Inventar: {phase2.get('site_categories') or []}\n"
                f"Bekannte Themencluster aus dem Inventar: {phase2.get('topic_clusters') or []}\n"
                f"Bekannte prominente Titel aus dem Inventar: {phase2.get('prominent_titles') or []}\n"
                f"{cached_context}"
                "Return JSON: {\"allowed_topics\":[...],\"content_style_constraints\":[...],\"internal_linking_opportunities\":[...],"
                "\"site_summary\":\"...\",\"site_categories\":[...],\"topic_clusters\":[...],\"prominent_titles\":[...],\"sample_page_titles\":[...]}."
            )
            try:
                llm_out = call_llm_json(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    api_key=llm_api_key,
                    base_url=llm_base_url,
                    model=planning_model,
                    timeout_seconds=http_timeout,
                    max_tokens=phase2_max_tokens,
                    request_label="phase2",
                    usage_collector=_collect_llm_usage,
                )
                phase2["allowed_topics"] = llm_out.get("allowed_topics") or []
                phase2["content_style_constraints"] = llm_out.get("content_style_constraints") or []
                phase2["internal_linking_opportunities"] = llm_out.get("internal_linking_opportunities") or []
                phase2["site_summary"] = str(llm_out.get("site_summary") or phase2.get("site_summary") or "").strip()
                phase2["site_categories"] = [
                    str(item).strip() for item in (llm_out.get("site_categories") or []) if str(item).strip()
                ]
                phase2["topic_clusters"] = [
                    str(item).strip() for item in (llm_out.get("topic_clusters") or []) if str(item).strip()
                ]
                phase2["prominent_titles"] = [
                    str(item).strip() for item in (llm_out.get("prominent_titles") or []) if str(item).strip()
                ]
                phase2["sample_page_titles"] = [
                    str(item).strip() for item in (llm_out.get("sample_page_titles") or []) if str(item).strip()
                ]
            except LLMError as exc:
                warnings.append(f"phase2_llm_failed:{exc}")
                phase2["allowed_topics"] = _extract_keywords(publishing_text, max_terms=8)
                phase2["content_style_constraints"] = ["Neutraler, fachlich-serioeser Ton", "Werbliche Sprache vermeiden"]
                phase2_cache_meta["generator_mode"] = "deterministic"
                phase2_cache_meta["model_name"] = ""
            phase2 = _merge_phase2_analysis(
                phase2,
                cached_phase2,
                inventory_categories=inventory_topic_insights.get("site_categories") or [],
            )
    else:
        cached_phase2 = _coerce_phase2_payload(phase2_cache_payload)
        if cached_phase2:
            phase2 = _merge_phase2_analysis(
                cached_phase2,
                None,
                inventory_categories=inventory_topic_insights.get("site_categories") or [],
            )
            phase2_cache_warm = True
            warnings.append("phase2_cache_fallback_used")
        else:
            phase2["allowed_topics"] = []
            phase2["content_style_constraints"] = []
            phase2_cache_meta["generator_mode"] = "deterministic"
            phase2_cache_meta["model_name"] = ""

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
    if requested_topic:
        phase3 = {
            "final_article_topic": requested_topic,
            "search_intent_type": "informational",
            "primary_keyword": requested_topic,
            "secondary_keywords": keyword_cluster[1:3] if len(keyword_cluster) > 1 else [],
        }
    else:
        system_prompt = (
            "You select a submitted article topic that fits publishing site authority and allows a natural backlink. "
            "Avoid promotional topics and exact match money keywords. "
            "You MUST choose a unique topic that is clearly different from any previously used topics listed below. "
            "All returned natural-language fields must be in German (de-DE). Return JSON only."
        )
        exclude_block = ""
        if safe_exclude:
            exclude_block = (
                "Previously used topics (DO NOT reuse or closely paraphrase these):\n"
                + "\n".join(f"- {t}" for t in safe_exclude)
                + "\n\n"
            )
        user_prompt = (
            f"{exclude_block}"
            f"Allowed topics: {phase2['allowed_topics']}\n"
            f"Publishing site summary: {phase2.get('site_summary') or ''}\n"
            f"Publishing site categories: {phase2.get('site_categories') or []}\n"
            f"Publishing site topic clusters: {phase2.get('topic_clusters') or []}\n"
            f"Publishing site prominent titles: {(phase2.get('prominent_titles') or [])[:6]}\n"
            f"Existing publishing page titles: {(phase2.get('sample_page_titles') or [])[:6]}\n"
            f"Target keyword cluster: {keyword_cluster}\n"
            "Return JSON: {\"final_article_topic\":\"...\",\"search_intent_type\":\"informational|commercial|navigational\","
            "\"primary_keyword\":\"...\",\"secondary_keywords\":[\"...\",\"...\"]}"
        )
        phase3_temperature = 0.7 if safe_exclude else 0.3
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=planning_model,
                timeout_seconds=http_timeout,
                max_tokens=300,
                temperature=phase3_temperature,
                request_label="phase3",
                usage_collector=_collect_llm_usage,
            )
            resolved_topic = llm_out.get("final_article_topic") or ""
            resolved_primary = llm_out.get("primary_keyword") or (keyword_cluster[0] if keyword_cluster else "")
            phase3 = {
                "final_article_topic": resolved_topic,
                "search_intent_type": llm_out.get("search_intent_type") or "informational",
                "primary_keyword": resolved_primary,
                "secondary_keywords": llm_out.get("secondary_keywords") or [],
            }
        except LLMError as exc:
            warnings.append(f"phase3_llm_failed:{exc}")
            fallback_topic = phase2["allowed_topics"][0] if phase2["allowed_topics"] else "Industry insights"
            phase3 = {
                "final_article_topic": fallback_topic,
                "search_intent_type": "informational",
                "primary_keyword": keyword_cluster[0] if keyword_cluster else fallback_topic,
                "secondary_keywords": keyword_cluster[1:3] if len(keyword_cluster) > 1 else [],
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
    for attempt in range(1, 3):
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
                max_tokens=500,
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
        if primary_keyword_phase3 and not (
            _keyword_present(h1_lower, primary_keyword_phase3)
            or _keyword_present(outline_h2_combined, primary_keyword_phase3)
        ):
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
        raise CreatorError(f"Outline validation failed: {outline_errors}")

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
            system_prompt = (
                "Write a German (de-DE) SEO blog post in clean HTML. CRITICAL: the article body MUST be 650-800 words "
                "(aim for 750 words). Use neutral authoritative tone, "
                "Include exactly one backlink to the provided Backlink URL, plus internal links to the publishing site. "
                "No external links beyond the backlink, no CTA spam, no 'visit our site' language. "
                f"Include H1 and {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections. "
                "The penultimate H2 must be titled 'Fazit' and the final H2 must be titled 'FAQ'. "
                "Each section should have 1-2 substantial paragraphs. "
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
                "Return JSON only."
            )
            user_prompt = (
                f"H1: {phase4['h1']}\n"
                f"Required meta_title: {phase3['title_package']['meta_title']}\n"
                f"Required slug: {phase3['title_package']['slug']}\n"
                f"Target meta_description: {_build_deterministic_meta_description(topic=phase3['final_article_topic'], primary_keyword=phase3['primary_keyword'], secondary_keywords=phase3['secondary_keywords'], structured_mode=phase3.get('structured_content_mode','none'))}\n"
                f"Outline: {phase4['outline']}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Backlink URL: {backlink_url}\n"
                f"Anchor text: {phase4['anchor_text_final']}\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
                f"FAQ candidates: {faq_prompt_text[:3]}\n"
                f"Allowed internal links (publishing site only): {internal_links_prompt_text}\n"
                f"Internal link rule: min {effective_internal_min}, max {effective_internal_max}\n"
                f"Topic for topic-specific Fazit: {phase3['final_article_topic']}\n"
                "Language: German (de-DE).\n"
                "Keyword rules: primary in H1+intro+>=1 H2, each secondary >=1 mention, natural density.\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            model_for_attempt = planning_model
            max_tokens = phase5_max_tokens_attempt1
            temperature = 0.3
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
                "Return JSON only."
            )
            user_prompt = (
                f"Current article_html:\n{last_article_html}\n\n"
                f"Issues: {last_validation_errors}\n"
                f"Required H1: {phase4['h1']}\n"
                f"Required meta_title: {phase3['title_package']['meta_title']}\n"
                f"Required slug: {phase3['title_package']['slug']}\n"
                f"Required outline: {phase4['outline']}\n"
                f"Constraints: 650-800 words, H1 + {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections.\n"
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
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            model_for_attempt = writing_model
            max_tokens = phase5_max_tokens_retry
            temperature = 0.2
        else:
            system_prompt = (
                "Write a NEW German (de-DE) article from scratch. CRITICAL: the article body MUST be 650-800 words "
                "(aim for 750 words). Each H2 section needs 1-2 substantial paragraphs. "
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
                "Do not return markdown fences. Return JSON only."
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
                f"Constraints: 650-800 words (aim for 750), H1 + {ARTICLE_MIN_H2}-{ARTICLE_MAX_H2} H2 sections, "
                "neutral authoritative tone, no CTA spam, no 'visit our site' language.\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                f"Structured content mode: {phase3.get('structured_content_mode', 'none')}\n"
                f"FAQ candidates: {faq_prompt_text[:3]}\n"
                f"Topic for topic-specific Fazit: {phase3['final_article_topic']}\n"
                "Language: German (de-DE).\n"
                "Keyword rules: primary in H1+intro+>=1 H2, each secondary >=1 mention, natural density.\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            model_for_attempt = writing_model
            max_tokens = phase5_max_tokens_retry
            temperature = 0.2
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=model_for_attempt,
                timeout_seconds=http_timeout,
                max_tokens=max_tokens,
                temperature=temperature,
                allow_html_fallback=True,
                request_label=f"phase5_attempt_{attempt}",
                usage_collector=_collect_llm_usage,
            )
        except LLMError as exc:
            errors.append(str(exc))
            continue

        html_fallback = bool(llm_out.pop("_html_fallback", False))
        article_html = (llm_out.get("article_html") or "").strip()
        if not article_html:
            errors.append("missing_article_html")
            continue

        wc = word_count_from_html(article_html)
        logger.info(
            "creator.phase5.attempt attempt=%s word_count=%s html_fallback=%s",
            attempt, wc, html_fallback,
        )

        if html_fallback:
            warnings.append("llm_html_fallback")

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
            if all(_is_link_only_error(err) for err in validation_errors):
                repaired_html = _repair_link_constraints(
                    article_html=article_html,
                    backlink_url=backlink_url,
                    publishing_site_url=publishing_site_url,
                    internal_links=internal_link_candidates,
                    internal_link_anchor_map=internal_link_anchor_map,
                    min_internal_links=effective_internal_min,
                    max_internal_links=effective_internal_max,
                    backlink_placement=phase4["backlink_placement"],
                    anchor_text=phase4["anchor_text_final"],
                )
                repaired_errors = _collect_article_validation_errors(
                    article_html=repaired_html,
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
                if not repaired_errors:
                    warnings.append("phase5_link_constraints_repaired_deterministically")
                    article_payload = {
                        "meta_title": llm_out.get("meta_title") or phase4["h1"],
                        "meta_description": llm_out.get("meta_description") or "",
                        "slug": llm_out.get("slug") or "",
                        "excerpt": llm_out.get("excerpt") or "",
                        "article_html": repaired_html,
                    }
                    break
                validation_errors = repaired_errors
                article_html = repaired_html

            errors.extend(validation_errors)
            last_article_html = article_html
            last_validation_errors = validation_errors
            continue

        article_payload = {
            "meta_title": llm_out.get("meta_title") or phase4["h1"],
            "meta_description": llm_out.get("meta_description") or "",
            "slug": llm_out.get("slug") or "",
            "excerpt": llm_out.get("excerpt") or "",
            "article_html": article_html,
        }
        break

    if not article_payload:
        fallback_payload = _generate_article_by_sections(
            phase4=phase4,
            phase3=phase3,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
            internal_link_candidates=internal_link_candidates,
            internal_link_anchor_map=internal_link_anchor_map,
            min_internal_links=effective_internal_min,
            max_internal_links=effective_internal_max,
            faq_candidates=faq_prompt_text,
            structured_mode=phase3.get("structured_content_mode", "none"),
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=planning_model,
            http_timeout=http_timeout,
            usage_collector=_collect_llm_usage,
        )
        if fallback_payload:
            article_html = (fallback_payload.get("article_html") or "").strip()
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
            if not validation_errors:
                article_payload = fallback_payload
            else:
                errors.extend(validation_errors)

    if not article_payload:
        raise CreatorError(f"Article generation failed: {errors}")

    # ── post-generation repairs ──────────────────────────────────────
    art_html = (article_payload.get("article_html") or "").strip()
    art_html = _repair_link_constraints(
        article_html=art_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        internal_links=internal_link_candidates,
        internal_link_anchor_map=internal_link_anchor_map,
        min_internal_links=effective_internal_min,
        max_internal_links=effective_internal_max,
        backlink_placement=phase4["backlink_placement"],
        anchor_text=phase4.get("anchor_text_final") or "this resource",
    )
    art_html = _strip_h1_tags(art_html)
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

    phase5 = article_payload
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
    image_required = _read_bool_env("CREATOR_IMAGE_REQUIRED", False)

    featured_image_url = ""
    in_content_image_url = ""
    if not dry_run:
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

    if image_required and not featured_image_url and not dry_run:
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
        # one fix pass
        current_wc = word_count_from_html(phase5["article_html"])
        logger.info("creator.phase7.issues errors=%s word_count=%s", phase7_errors, current_wc)
        wc_ok = 600 <= current_wc <= 850
        if wc_ok:
            wc_instruction = (
                f"The word count ({current_wc}) is fine - do NOT add or remove content. "
                "Only fix the specific issues listed below."
            )
        else:
            wc_instruction = (
                f"The article currently has {current_wc} words. "
                "Adjust it to be between 650 and 800 words."
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
                max_tokens=1600,
                allow_html_fallback=True,
                request_label="phase7_repair",
                usage_collector=_collect_llm_usage,
            )
            fixed_html = (llm_out.get("article_html") or "").strip()
            fixed_wc = word_count_from_html(fixed_html) if fixed_html else 0
            logger.info("creator.phase7.fix_result before=%s after=%s", current_wc, fixed_wc)
            # Accept the fix only if it stays within bounds
            if fixed_html and 600 <= fixed_wc <= 850:
                phase5["article_html"] = fixed_html
            elif fixed_html and fixed_wc > 0:
                # Fix went out of bounds; keep original if it was in bounds
                if not wc_ok:
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
        except LLMError as exc:
            phase7_errors.append(f"phase7_fix_failed:{exc}")

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
