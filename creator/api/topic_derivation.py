"""Derive a target keyword from a target backlink URL when the webhook didn't
provide one.

Pipeline:
1. Fetch the target page (HTML).
2. Detect language: ``<html lang>`` -> TLD heuristic -> Haiku content classification.
   Reject if the resulting language is not in the allowed set (currently de/fr).
3. Extract candidate keywords deterministically from <title>, <h1>, <h2>,
   OG/Twitter meta, URL slug.
4. If deterministic candidates are too thin, augment with a single Haiku call.
5. Rank candidates by DataForSEO search volume + recent trend momentum (last
   3 months vs prior 3 months from monthly_searches history). The trend signal
   is intentionally a soft boost, not a hard filter -- evergreen B2B keywords
   like ``steuerberater hamburg`` legitimately have flat trends.
6. Cache the result in ``seo_research_cache`` (cache_kind='derived_topic') so
   re-runs against the same URL skip the spend.

Hard-fails (raise ``TopicDerivationError``):
- target URL unreachable / non-HTML / no extractable text
- detected language not in allowed_languages

Soft-fails (return result with a note):
- all candidates have search_volume == 0 -> still return highest-ranked one
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from .dataforseo import (
    DataForSEOClient,
    DataForSEOError,
    KeywordMetric,
    MonthlySearchVolume,
)
from .llm import LLMError, call_llm_json

logger = logging.getLogger("creator.topic_derivation")

DERIVATION_VERSION = "v1"
CACHE_KIND = "derived_topic"
CACHE_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days -- target pages rarely change topic

DEFAULT_ALLOWED_LANGUAGES: Tuple[str, ...] = ("de", "fr")
LANGUAGE_TO_LOCATION: Dict[str, Tuple[int, str]] = {
    # ISO 639-1 -> (DataForSEO location_code, DataForSEO language_code)
    "de": (2276, "de"),  # Germany
    "fr": (2250, "fr"),  # France
}
TLD_LANGUAGE_HINTS: Dict[str, str] = {
    "de": "de",
    "at": "de",
    "ch": "de",
    "fr": "fr",
    "be": "fr",
}

DEFAULT_FETCH_TIMEOUT_SECONDS = 15
DEFAULT_FETCH_RETRIES = 2
DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_LLM_TIMEOUT_SECONDS = 30
MAX_PAGE_TEXT_CHARS = 4000
MAX_CANDIDATES_TO_RANK = 8
TREND_RECENT_MONTHS = 3
TREND_PRIOR_MONTHS = 3


class TopicDerivationError(RuntimeError):
    """Raised when derivation cannot produce a usable keyword + locale.

    Carries a stable ``code`` so the portal can render a clean admin error.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class CandidateScore:
    keyword: str
    source: str  # "title" | "h1" | "h2" | "og" | "slug" | "haiku"
    search_volume: int = 0
    trend_ratio: float = 1.0  # recent_avg / prior_avg; 1.0 = flat
    score: float = 0.0


@dataclass
class DerivedTopic:
    target_url: str
    target_keyword: str
    language_code: str  # ISO 639-1
    location_code: int  # DataForSEO
    alternates: List[str] = field(default_factory=list)
    candidates: List[CandidateScore] = field(default_factory=list)
    confidence: float = 0.0
    notes: List[str] = field(default_factory=list)
    cost_usd: float = 0.0
    cache_hit: bool = False


# ---- HTTP fetch -----------------------------------------------------------


def _fetch_html(url: str, *, timeout_seconds: int, retries: int) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        raise TopicDerivationError("url_missing", "target_url is empty.")
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"

    last_error: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = requests.get(
                cleaned,
                timeout=timeout_seconds,
                allow_redirects=True,
                headers={
                    "User-Agent": "creator-service/topic-derivation/1.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
        except requests.RequestException as exc:
            last_error = str(exc)
            continue
        if response.status_code >= 400:
            last_error = f"http_{response.status_code}"
            continue
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "html" not in content_type and "xml" not in content_type:
            raise TopicDerivationError(
                "non_html_response",
                f"Target URL returned content-type {content_type!r}; expected HTML.",
            )
        text = response.text or ""
        if not text.strip():
            raise TopicDerivationError("empty_response", "Target URL returned an empty body.")
        return text

    raise TopicDerivationError(
        "fetch_failed",
        f"Could not fetch target URL after {retries} attempts: {last_error or 'unknown error'}.",
    )


# ---- Language detection ---------------------------------------------------


def _detect_language(html: str, target_url: str) -> Optional[str]:
    """Returns ISO 639-1 lowercase, or None if undetectable."""

    soup = BeautifulSoup(html or "", "lxml")
    html_tag = soup.find("html")
    if html_tag is not None:
        lang_attr = (html_tag.get("lang") or html_tag.get("xml:lang") or "").strip().lower()
        if lang_attr:
            primary = lang_attr.split("-")[0].split("_")[0]
            if len(primary) == 2 and primary.isalpha():
                return primary

    parsed = urlparse(target_url)
    host = (parsed.netloc or "").lower()
    if host:
        tld = host.rsplit(".", 1)[-1]
        hint = TLD_LANGUAGE_HINTS.get(tld)
        if hint:
            return hint

    return None


# ---- Page text + deterministic candidates --------------------------------


def _extract_page_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for tag in soup(("script", "style", "noscript", "template")):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_PAGE_TEXT_CHARS]


def _normalize_keyword(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "")).strip().lower()
    cleaned = re.sub(r"[‐-―]", "-", cleaned)  # unicode hyphens
    # Trim site-name suffixes commonly appended to <title>: " | Brand", " - Brand"
    cleaned = re.split(r"\s+[\|\-–—]\s+", cleaned)[0].strip()
    cleaned = re.sub(r"[^\w\s\-'äöüßàâçéèêëîïôûùüÿœæ]", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _slug_to_keyword(url: str) -> str:
    parsed = urlparse(url)
    path = (parsed.path or "").strip("/")
    if not path:
        return ""
    last_segment = path.rsplit("/", 1)[-1]
    last_segment = re.sub(r"\.(html?|php|aspx?)$", "", last_segment, flags=re.IGNORECASE)
    return _normalize_keyword(last_segment.replace("-", " ").replace("_", " "))


def _extract_deterministic_candidates(html: str, target_url: str) -> List[Tuple[str, str]]:
    """Returns [(keyword, source)] in priority order, deduped."""

    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[str, str]] = []
    seen: set[str] = set()

    def push(value: str, source: str) -> None:
        normalized = _normalize_keyword(value)
        if not normalized or len(normalized) < 3:
            return
        if normalized in seen:
            return
        seen.add(normalized)
        out.append((normalized, source))

    if soup.title and soup.title.string:
        push(str(soup.title.string), "title")

    for tag_name, source_label in (("h1", "h1"), ("h2", "h2")):
        for tag in soup.find_all(tag_name)[:3]:
            push(tag.get_text(" ", strip=True), source_label)

    for attr, value in (
        ("property", "og:title"),
        ("name", "twitter:title"),
    ):
        meta = soup.find("meta", attrs={attr: value})
        if meta and meta.get("content"):
            push(str(meta.get("content")), "og")

    slug = _slug_to_keyword(target_url)
    if slug:
        push(slug, "slug")

    return out[:MAX_CANDIDATES_TO_RANK]


# ---- Haiku fallback for keyword extraction --------------------------------


_HAIKU_SYSTEM_PROMPT = (
    "You extract one commercial-intent SEO keyword from a web page. "
    "Return JSON: {\"primary\": <string>, \"alternates\": [<string>, <string>]}. "
    "Rules: keyword MUST be in the requested language; 2-5 words; "
    "noun-phrase only (no questions, no full sentences); lowercase; "
    "must reflect the page's main commercial topic, not boilerplate."
)


def _haiku_extract_keyword(
    *,
    page_text: str,
    language_code: str,
    api_key: str,
    model: str = DEFAULT_HAIKU_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
) -> Tuple[List[str], float]:
    """Returns ([primary, *alternates], approx_cost_usd)."""

    user_prompt = (
        f"Language: {language_code}\n\n"
        f"Page text (truncated to {MAX_PAGE_TEXT_CHARS} chars):\n"
        f"---\n{page_text}\n---\n\n"
        "Return only the JSON object."
    )
    payload = call_llm_json(
        system_prompt=_HAIKU_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_tokens=200,
        temperature=0.2,
        request_label="topic_derivation_haiku",
    )

    primary = _normalize_keyword(str(payload.get("primary") or ""))
    alternates_raw = payload.get("alternates") or []
    alternates: List[str] = []
    if isinstance(alternates_raw, list):
        for item in alternates_raw[:3]:
            if isinstance(item, str):
                normalized = _normalize_keyword(item)
                if normalized and normalized != primary and normalized not in alternates:
                    alternates.append(normalized)

    out: List[str] = []
    if primary:
        out.append(primary)
    out.extend(alternates)

    # Approx cost: Haiku 4.5 ~$1/MTok in, ~$5/MTok out. We send ~4K chars in
    # (~1K tokens), get ~50 tokens out. Round to a conservative $0.001.
    approx_cost = 0.001
    return out, approx_cost


# ---- Trend + volume ranking ----------------------------------------------


def _trend_ratio(monthly: Sequence[MonthlySearchVolume]) -> float:
    """recent_avg / prior_avg over the last (recent + prior) months. 1.0 = flat.

    Returns 1.0 when history is missing or prior_avg is 0 (avoids div-by-zero
    and avoids ranking a brand-new keyword as 'infinite trend').
    """

    if len(monthly) < (TREND_RECENT_MONTHS + TREND_PRIOR_MONTHS):
        return 1.0
    sorted_history = sorted(monthly, key=lambda b: (b.year, b.month))
    recent = sorted_history[-TREND_RECENT_MONTHS:]
    prior = sorted_history[-(TREND_RECENT_MONTHS + TREND_PRIOR_MONTHS): -TREND_RECENT_MONTHS]
    recent_avg = sum(b.search_volume for b in recent) / max(1, len(recent))
    prior_avg = sum(b.search_volume for b in prior) / max(1, len(prior))
    if prior_avg <= 0:
        return 1.0
    return recent_avg / prior_avg


def _score_candidate(*, search_volume: int, trend_ratio: float) -> float:
    """Higher = better. Combines log-volume with a soft trend boost.

    - search_volume contributes log10(1 + volume) so 1k vs 10k matters but the
      gap saturates (we don't want a 100k seasonal spike to crush a 5k stable
      B2B term).
    - trend_ratio is bounded to [0.5, 2.0] then translated to a multiplier in
      [0.85, 1.25]. So a rising keyword gets a +25% boost, a declining one a
      -15% penalty -- noticeable but not enough to override raw volume.
    """

    import math

    volume_score = math.log10(1 + max(0, int(search_volume)))
    bounded_trend = max(0.5, min(2.0, trend_ratio))
    # Map [0.5, 2.0] -> [0.85, 1.25] linearly
    trend_multiplier = 0.85 + (bounded_trend - 0.5) * (0.25 - (-0.15)) / (2.0 - 0.5)
    return volume_score * trend_multiplier


def _rank_candidates(
    *,
    keywords: Sequence[str],
    location_code: int,
    language_code: str,
    client: DataForSEOClient,
) -> Tuple[List[CandidateScore], float]:
    """Calls DataForSEO once for the whole batch; returns scored list + cost."""

    cleaned = [kw for kw in keywords if kw and kw.strip()]
    if not cleaned:
        return [], 0.0

    try:
        metrics = client.keyword_volume(
            cleaned,
            location_code=location_code,
            language_code=language_code,
        )
    except DataForSEOError as exc:
        logger.warning("topic_derivation.dataforseo_failed err=%s", exc)
        return [], 0.0

    by_keyword: Dict[str, KeywordMetric] = {m.keyword.lower(): m for m in metrics if m.keyword}
    scored: List[CandidateScore] = []
    for kw in cleaned:
        metric = by_keyword.get(kw.lower())
        volume = int(metric.search_volume or 0) if metric else 0
        trend = _trend_ratio(metric.monthly_searches) if metric else 1.0
        scored.append(CandidateScore(
            keyword=kw,
            source="",
            search_volume=volume,
            trend_ratio=trend,
            score=_score_candidate(search_volume=volume, trend_ratio=trend),
        ))
    scored.sort(key=lambda c: c.score, reverse=True)
    cost = float(metrics[0].cost) if metrics else 0.0
    return scored, cost


# ---- Cache ---------------------------------------------------------------


def _build_cache_key(target_url: str, allowed_languages: Sequence[str]) -> str:
    normalized_url = (target_url or "").strip().lower()
    langs = ",".join(sorted(set(lang.lower() for lang in allowed_languages)))
    return f"{normalized_url}|langs={langs}|ver={DERIVATION_VERSION}"


def _build_cache_locale(language_code: str, location_code: int) -> str:
    return f"{language_code.lower()}-{int(location_code)}"


def _serialize_for_cache(result: DerivedTopic) -> Dict[str, Any]:
    return {
        "target_url": result.target_url,
        "target_keyword": result.target_keyword,
        "language_code": result.language_code,
        "location_code": result.location_code,
        "alternates": list(result.alternates),
        "candidates": [
            {
                "keyword": c.keyword,
                "source": c.source,
                "search_volume": c.search_volume,
                "trend_ratio": c.trend_ratio,
                "score": c.score,
            }
            for c in result.candidates
        ],
        "confidence": result.confidence,
        "notes": list(result.notes),
        "cost_usd": result.cost_usd,
    }


def _hydrate_from_cache(payload: Dict[str, Any]) -> DerivedTopic:
    return DerivedTopic(
        target_url=str(payload.get("target_url") or ""),
        target_keyword=str(payload.get("target_keyword") or ""),
        language_code=str(payload.get("language_code") or ""),
        location_code=int(payload.get("location_code") or 0),
        alternates=[str(a) for a in (payload.get("alternates") or []) if isinstance(a, str)],
        candidates=[
            CandidateScore(
                keyword=str(c.get("keyword") or ""),
                source=str(c.get("source") or ""),
                search_volume=int(c.get("search_volume") or 0),
                trend_ratio=float(c.get("trend_ratio") or 1.0),
                score=float(c.get("score") or 0.0),
            )
            for c in (payload.get("candidates") or [])
            if isinstance(c, dict)
        ],
        confidence=float(payload.get("confidence") or 0.0),
        notes=[str(n) for n in (payload.get("notes") or []) if isinstance(n, str)],
        cost_usd=0.0,  # cache hit -> no spend
        cache_hit=True,
    )


# ---- Orchestrator --------------------------------------------------------


def derive_topic(
    target_url: str,
    *,
    allowed_languages: Sequence[str] = DEFAULT_ALLOWED_LANGUAGES,
    language_override: Optional[str] = None,
    use_cache: bool = True,
    fetch_timeout_seconds: int = DEFAULT_FETCH_TIMEOUT_SECONDS,
    fetch_retries: int = DEFAULT_FETCH_RETRIES,
    api_key: Optional[str] = None,
    dataforseo_client: Optional[DataForSEOClient] = None,
) -> DerivedTopic:
    """Derive ``target_keyword`` + locale from a backlink target URL.

    Raises ``TopicDerivationError`` on URL-fetch / language-rejection failures.
    Successful returns always have a non-empty ``target_keyword``.
    """

    cleaned_url = (target_url or "").strip()
    if not cleaned_url:
        raise TopicDerivationError("url_missing", "target_url is required.")

    allowed_set = {lang.lower() for lang in allowed_languages}
    if not allowed_set:
        raise ValueError("allowed_languages must not be empty.")

    cache_key = _build_cache_key(cleaned_url, allowed_languages)

    if use_cache:
        try:
            from .topic_derivation_cache import get_cached_derived_topic  # local import to keep tests light
        except ImportError:
            get_cached_derived_topic = None  # type: ignore
        if get_cached_derived_topic is not None:
            cached_payload = get_cached_derived_topic(
                lookup_key=cache_key,
            )
            if cached_payload:
                logger.info("topic_derivation.cache_hit url=%s", cleaned_url)
                return _hydrate_from_cache(cached_payload)

    html = _fetch_html(cleaned_url, timeout_seconds=fetch_timeout_seconds, retries=fetch_retries)

    if language_override:
        detected_lang = language_override.strip().lower()
    else:
        detected_lang = _detect_language(html, cleaned_url) or ""

    if not detected_lang or detected_lang not in allowed_set:
        raise TopicDerivationError(
            "language_not_allowed",
            (
                f"Detected language {detected_lang!r} is not in the allowed set "
                f"{sorted(allowed_set)}. Topic derivation currently supports German "
                f"and French target sites only."
            ),
        )

    if detected_lang not in LANGUAGE_TO_LOCATION:
        raise TopicDerivationError(
            "language_unmapped",
            f"No DataForSEO location mapping for language {detected_lang!r}.",
        )
    location_code, language_code = LANGUAGE_TO_LOCATION[detected_lang]

    deterministic = _extract_deterministic_candidates(html, cleaned_url)
    notes: List[str] = []
    cost_usd = 0.0

    needs_haiku = len(deterministic) < 2 or all(len(kw) < 4 for kw, _ in deterministic)
    haiku_keywords: List[str] = []
    if needs_haiku:
        resolved_api_key = (api_key or os.getenv("ANTHROPIC_API_KEY") or "").strip()
        if not resolved_api_key:
            notes.append("haiku_skipped_no_api_key")
        else:
            page_text = _extract_page_text(html)
            try:
                haiku_keywords, haiku_cost = _haiku_extract_keyword(
                    page_text=page_text,
                    language_code=language_code,
                    api_key=resolved_api_key,
                )
                cost_usd += haiku_cost
                if haiku_keywords:
                    notes.append("used_haiku_fallback")
            except LLMError as exc:
                logger.warning("topic_derivation.haiku_failed err=%s", exc)
                notes.append(f"haiku_failed:{str(exc)[:80]}")

    # Build merged candidate list, preserving source labels.
    merged: List[Tuple[str, str]] = list(deterministic)
    seen = {kw for kw, _ in merged}
    for kw in haiku_keywords:
        if kw and kw not in seen:
            merged.append((kw, "haiku"))
            seen.add(kw)

    if not merged:
        raise TopicDerivationError(
            "no_candidates",
            "Could not extract any keyword candidates from the target page.",
        )

    merged = merged[:MAX_CANDIDATES_TO_RANK]

    client = dataforseo_client or DataForSEOClient()
    scored, dataforseo_cost = _rank_candidates(
        keywords=[kw for kw, _ in merged],
        location_code=location_code,
        language_code=language_code,
        client=client,
    )
    cost_usd += dataforseo_cost

    source_by_kw = {kw: src for kw, src in merged}
    for cand in scored:
        cand.source = source_by_kw.get(cand.keyword, cand.source)

    if not scored:
        # DataForSEO failed entirely; fall back to deterministic order.
        scored = [
            CandidateScore(keyword=kw, source=src, search_volume=0, trend_ratio=1.0, score=0.0)
            for kw, src in merged
        ]
        notes.append("dataforseo_unavailable")

    primary = scored[0]
    if primary.search_volume == 0:
        notes.append("zero_volume_primary")

    alternates = [c.keyword for c in scored[1:4] if c.keyword != primary.keyword]

    # Confidence: cheap heuristic. High when primary has real volume + clear
    # gap to the next candidate; low when everything is zero-volume.
    if scored and scored[0].search_volume > 0:
        next_score = scored[1].score if len(scored) > 1 else 0.0
        gap = max(0.0, scored[0].score - next_score)
        confidence = min(1.0, 0.5 + gap * 0.25)
    else:
        confidence = 0.2

    result = DerivedTopic(
        target_url=cleaned_url,
        target_keyword=primary.keyword,
        language_code=language_code,
        location_code=location_code,
        alternates=alternates,
        candidates=scored,
        confidence=confidence,
        notes=notes,
        cost_usd=cost_usd,
        cache_hit=False,
    )

    if use_cache:
        try:
            from .topic_derivation_cache import upsert_derived_topic
            upsert_derived_topic(
                lookup_key=cache_key,
                locale=_build_cache_locale(language_code, location_code),
                payload=_serialize_for_cache(result),
                ttl_seconds=CACHE_TTL_SECONDS,
            )
        except ImportError:
            pass

    return result


def page_content_hash(html: str) -> str:
    """Stable hash of page text for invalidating cache when content changes."""

    return hashlib.sha256(_extract_page_text(html).encode("utf-8")).hexdigest()
