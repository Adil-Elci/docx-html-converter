"""Orchestrate the research layer for a single target keyword.

Combines:
- DataForSEO SERP organic + PAA + related searches
- DataForSEO keyword volume (primary only by default)
- DataForSEO related keywords (optional, can be skipped to save spend)
- HTML body scraping for the top-N competitors
- Claude Haiku entity extraction across competitor bodies
- Deterministic helpers: word-count median, common H2 themes, topical gap

The output is a single immutable ``ResearchPayload`` consumed by the contract
generator. Cost is summed across DataForSEO endpoints; LLM cost is tracked
separately by the LLM module's usage collector.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from statistics import median
from typing import Iterable, List, Optional

from dataclasses import asdict
from typing import Any, Dict

from .dataforseo import (
    DEFAULT_LANGUAGE_CODE,
    DEFAULT_LOCATION_CODE,
    DataForSEOClient,
    KeywordMetric,
    OrganicResult,
    RelatedKeyword,
)
from .entity_extract import ExtractedEntity, extract_entities_from_competitors
from . import research_cache
from .serp_scrape import (
    DEFAULT_MAX_BODY_CHARS,
    DEFAULT_TIMEOUT_SECONDS as SCRAPE_DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    ScrapedCompetitor,
    scrape_top_results,
)

logger = logging.getLogger("creator.research")

DEFAULT_TOP_N_COMPETITORS = 5
DEFAULT_RELATED_KEYWORD_LIMIT = 30
DEFAULT_HIGH_COVERAGE_THRESHOLD = 0.6
DEFAULT_COMMON_H2_MIN_COMPETITORS = 2
RESEARCH_VERSION = "v1"


@dataclass
class ResearchPayload:
    target_keyword: str
    location_code: int
    language_code: str

    # SERP
    organic: List[OrganicResult] = field(default_factory=list)
    paa_questions: List[str] = field(default_factory=list)
    related_searches: List[str] = field(default_factory=list)

    # Keyword metrics
    primary_volume: Optional[KeywordMetric] = None
    related_keywords: List[RelatedKeyword] = field(default_factory=list)

    # Competitor analysis
    competitors: List[ScrapedCompetitor] = field(default_factory=list)
    competitor_word_count_median: Optional[int] = None
    common_h2_themes: List[str] = field(default_factory=list)

    # Entities
    entities: List[ExtractedEntity] = field(default_factory=list)
    high_coverage_entities: List[ExtractedEntity] = field(default_factory=list)

    # Provenance
    research_version: str = RESEARCH_VERSION
    total_cost_usd: float = 0.0

    @property
    def successful_competitor_count(self) -> int:
        return sum(1 for c in self.competitors if c.fetch_status == "ok")


# ---- helpers ----------------------------------------------------------------


def _normalize_h2(text: str) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", (text or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _find_common_h2_themes(
    competitors: Iterable[ScrapedCompetitor],
    *,
    min_competitors: int = DEFAULT_COMMON_H2_MIN_COMPETITORS,
    limit: int = 20,
) -> List[str]:
    """H2 headings whose normalized form appears in N or more competitors.

    The first raw form encountered is preserved as the display string.
    """

    counter: Counter[str] = Counter()
    representative: dict[str, str] = {}
    for competitor in competitors:
        seen_in_this: set[str] = set()
        for raw in competitor.h2s:
            normalized = _normalize_h2(raw)
            if not normalized or normalized in seen_in_this:
                continue
            seen_in_this.add(normalized)
            counter[normalized] += 1
            representative.setdefault(normalized, raw.strip())
    return [
        representative[norm]
        for norm, count in counter.most_common()
        if count >= min_competitors
    ][:limit]


def _word_count_median(competitors: Iterable[ScrapedCompetitor]) -> Optional[int]:
    counts = [c.word_count for c in competitors if c.fetch_status == "ok" and c.word_count > 0]
    if not counts:
        return None
    return int(median(counts))


def topical_gap(
    research: ResearchPayload,
    *,
    article_text: str,
    coverage_threshold: float = DEFAULT_HIGH_COVERAGE_THRESHOLD,
) -> List[ExtractedEntity]:
    """Entities present in coverage_threshold of competitors but missing from the article."""

    if not article_text:
        return [e for e in research.entities if e.coverage >= coverage_threshold]
    lowered = article_text.lower()
    missing: List[ExtractedEntity] = []
    for entity in research.entities:
        if entity.coverage < coverage_threshold:
            continue
        if entity.name.lower() not in lowered:
            missing.append(entity)
    return missing


# ---- cache serialisation ----------------------------------------------------


def _payload_to_dict(payload: ResearchPayload) -> Dict[str, Any]:
    """Round-trippable jsonable dict for ``seo_research_cache.payload`` storage."""

    return asdict(payload)


def _payload_from_dict(data: Dict[str, Any]) -> ResearchPayload:
    """Inverse of ``_payload_to_dict``: rebuild dataclasses from a cached dict."""

    primary_volume_raw = data.get("primary_volume")
    primary_volume = KeywordMetric(**primary_volume_raw) if isinstance(primary_volume_raw, dict) else None
    return ResearchPayload(
        target_keyword=data["target_keyword"],
        location_code=int(data["location_code"]),
        language_code=str(data["language_code"]),
        organic=[OrganicResult(**item) for item in (data.get("organic") or [])],
        paa_questions=list(data.get("paa_questions") or []),
        related_searches=list(data.get("related_searches") or []),
        primary_volume=primary_volume,
        related_keywords=[RelatedKeyword(**item) for item in (data.get("related_keywords") or [])],
        competitors=[ScrapedCompetitor(**item) for item in (data.get("competitors") or [])],
        competitor_word_count_median=data.get("competitor_word_count_median"),
        common_h2_themes=list(data.get("common_h2_themes") or []),
        entities=[ExtractedEntity(**item) for item in (data.get("entities") or [])],
        high_coverage_entities=[ExtractedEntity(**item) for item in (data.get("high_coverage_entities") or [])],
        research_version=str(data.get("research_version") or RESEARCH_VERSION),
        # Cached payloads were already paid for in a prior run.
        total_cost_usd=0.0,
    )


# ---- orchestrator -----------------------------------------------------------


def run_research(
    *,
    target_keyword: str,
    dataforseo: Optional[DataForSEOClient] = None,
    top_n_competitors: int = DEFAULT_TOP_N_COMPETITORS,
    related_keyword_limit: int = DEFAULT_RELATED_KEYWORD_LIMIT,
    high_coverage_threshold: float = DEFAULT_HIGH_COVERAGE_THRESHOLD,
    skip_related_keywords: bool = False,
    skip_entity_extraction: bool = False,
    location_code: int = DEFAULT_LOCATION_CODE,
    language_code: str = DEFAULT_LANGUAGE_CODE,
    anthropic_api_key: Optional[str] = None,
    scrape_timeout_seconds: int = SCRAPE_DEFAULT_TIMEOUT,
    scrape_max_body_chars: int = DEFAULT_MAX_BODY_CHARS,
    scrape_user_agent: str = DEFAULT_USER_AGENT,
    use_cache: bool = True,
) -> ResearchPayload:
    """Run the full research pipeline for a target keyword.

    Spend, per call (live):
    - SERP organic: ~$0.002
    - Keyword volume (primary only): ~$0.00005
    - Related keywords (when not skipped): ~$0.01
    - Haiku entity extraction (when not skipped): ~$0.01–0.03
    Total: ~$0.03–0.05.

    Cached payloads (within the seo_research_cache TTL) skip every spend
    item above and return ``total_cost_usd=0.0``.
    """

    cache_key = research_cache.build_lookup_key(
        target_keyword=target_keyword,
        skip_related_keywords=skip_related_keywords,
        skip_entity_extraction=skip_entity_extraction,
        research_version=RESEARCH_VERSION,
    )
    cache_locale = research_cache.build_locale(language_code, location_code)
    if use_cache:
        cached = research_cache.get_cached_research_payload(
            lookup_key=cache_key,
            locale=cache_locale,
        )
        if cached is not None:
            try:
                return _payload_from_dict(cached)
            except Exception:
                logger.warning(
                    "research.cache_hydrate_failed keyword=%s — falling through to live run",
                    target_keyword,
                    exc_info=True,
                )

    client = dataforseo or DataForSEOClient()
    cost_running = 0.0

    serp = client.serp_organic(
        target_keyword,
        location_code=location_code,
        language_code=language_code,
    )
    cost_running += serp.cost

    primary_volume: Optional[KeywordMetric] = None
    volumes = client.keyword_volume(
        [target_keyword],
        location_code=location_code,
        language_code=language_code,
    )
    if volumes:
        primary_volume = volumes[0]
        cost_running += primary_volume.cost

    related_keywords: List[RelatedKeyword] = []
    if not skip_related_keywords:
        related_result = client.related_keywords(
            target_keyword,
            location_code=location_code,
            language_code=language_code,
            limit=related_keyword_limit,
        )
        related_keywords = related_result.items
        cost_running += related_result.cost

    competitors = scrape_top_results(
        serp.organic,
        top_n=top_n_competitors,
        timeout_seconds=scrape_timeout_seconds,
        max_body_chars=scrape_max_body_chars,
        user_agent=scrape_user_agent,
    )

    successful = [c for c in competitors if c.fetch_status == "ok"]
    if len(successful) < len(competitors):
        logger.info(
            "research.competitor_fetch_partial keyword=%s ok=%s total=%s",
            target_keyword,
            len(successful),
            len(competitors),
        )

    word_count_median = _word_count_median(successful)
    common_h2s = _find_common_h2_themes(successful)

    entities: List[ExtractedEntity] = []
    if not skip_entity_extraction:
        try:
            entity_result = extract_entities_from_competitors(
                successful,
                topic=target_keyword,
                api_key=anthropic_api_key,
            )
            entities = entity_result.entities
        except Exception as exc:
            logger.warning("research.entity_extraction_failed keyword=%s err=%s", target_keyword, exc)

    high_coverage = [e for e in entities if e.coverage >= high_coverage_threshold]

    payload = ResearchPayload(
        target_keyword=target_keyword,
        location_code=location_code,
        language_code=language_code,
        organic=serp.organic,
        paa_questions=serp.people_also_ask,
        related_searches=serp.related_searches,
        primary_volume=primary_volume,
        related_keywords=related_keywords,
        competitors=competitors,
        competitor_word_count_median=word_count_median,
        common_h2_themes=common_h2s,
        entities=entities,
        high_coverage_entities=high_coverage,
        total_cost_usd=cost_running,
    )
    if use_cache:
        try:
            research_cache.upsert_research_payload(
                lookup_key=cache_key,
                locale=cache_locale,
                payload=_payload_to_dict(payload),
            )
        except Exception:
            logger.warning(
                "research.cache_upsert_failed keyword=%s — payload is fresh but won't cache",
                target_keyword,
                exc_info=True,
            )
    return payload
