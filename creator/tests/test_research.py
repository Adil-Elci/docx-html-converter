from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from creator.api import research
from creator.api.dataforseo import (
    KeywordMetric,
    OrganicResult,
    RelatedKeyword,
    RelatedKeywordsResult,
    SerpResult,
)
from creator.api.entity_extract import EntityExtractionResult, ExtractedEntity
from creator.api.research import (
    ResearchPayload,
    _find_common_h2_themes,
    _word_count_median,
    run_research,
    topical_gap,
)
from creator.api.serp_scrape import ScrapedCompetitor


def _organic(rank: int, url: str) -> OrganicResult:
    return OrganicResult(rank=rank, url=url, title=f"Title {rank}", description="", domain=url)


def _serp(keyword: str, organics: list, paa: list = None, related: list = None, cost: float = 0.002) -> SerpResult:
    return SerpResult(
        keyword=keyword,
        organic=organics,
        people_also_ask=paa or [],
        related_searches=related or [],
        cost=cost,
    )


def _competitor(url: str, *, body: str = "default body text", h2s: list = None, status: str = "ok", word_count: int = 800) -> ScrapedCompetitor:
    return ScrapedCompetitor(
        url=url,
        final_url=url,
        fetch_status=status,
        body_text=body,
        word_count=word_count,
        h2s=h2s or [],
    )


def _make_client() -> MagicMock:
    client = MagicMock()
    client.serp_organic.return_value = _serp(
        "steuerberater hamburg",
        organics=[_organic(1, "https://a.de"), _organic(2, "https://b.de")],
        paa=["Was kostet ein Steuerberater?"],
        related=["steuerberater hamburg altona"],
    )
    client.keyword_volume.return_value = [
        KeywordMetric(keyword="steuerberater hamburg", search_volume=9900, competition=0.45, cpc=4.32, cost=0.0001)
    ]
    client.related_keywords.return_value = RelatedKeywordsResult(
        seed_keyword="steuerberater hamburg",
        items=[RelatedKeyword(keyword="steuerberater hamburg altona", search_volume=480)],
        cost=0.01,
    )
    return client


def test_run_research_aggregates_serp_keyword_and_entities(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    competitors = [
        _competitor("https://a.de", body="DATEV blah", h2s=["Vorteile", "Kosten"]),
        _competitor("https://b.de", body="DATEV more", h2s=["Vorteile", "Honorare"]),
    ]
    entity_result = EntityExtractionResult(
        entities=[ExtractedEntity(name="DATEV", type="ORGANIZATION", n_competitors=2, coverage=1.0)],
        competitor_count=2,
    )
    with patch("creator.api.research.scrape_top_results", return_value=competitors), \
         patch("creator.api.research.extract_entities_from_competitors", return_value=entity_result):
        payload = run_research(target_keyword="steuerberater hamburg", dataforseo=client)
    assert payload.target_keyword == "steuerberater hamburg"
    assert len(payload.organic) == 2
    assert payload.paa_questions == ["Was kostet ein Steuerberater?"]
    assert payload.primary_volume is not None and payload.primary_volume.search_volume == 9900
    assert payload.related_keywords[0].keyword == "steuerberater hamburg altona"
    assert payload.entities[0].name == "DATEV"
    assert payload.high_coverage_entities[0].name == "DATEV"
    assert payload.competitor_word_count_median == 800


def test_run_research_sums_costs_across_endpoints(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=0)
        payload = run_research(target_keyword="x", dataforseo=client)
    assert payload.total_cost_usd == pytest.approx(0.002 + 0.0001 + 0.01)


def test_run_research_skip_related_keywords_skips_call(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=0)
        payload = run_research(target_keyword="x", dataforseo=client, skip_related_keywords=True)
    client.related_keywords.assert_not_called()
    assert payload.related_keywords == []
    assert payload.total_cost_usd == pytest.approx(0.002 + 0.0001)


def test_run_research_skip_entity_extraction_no_llm_call(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        run_research(target_keyword="x", dataforseo=client, skip_entity_extraction=True)
    mock_entities.assert_not_called()


def test_run_research_handles_partial_competitor_failures(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    competitors = [
        _competitor("https://a.de", word_count=900),
        _competitor("https://b.de", status="forbidden", word_count=0),
        _competitor("https://c.de", word_count=700),
    ]
    with patch("creator.api.research.scrape_top_results", return_value=competitors), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=2)
        payload = run_research(target_keyword="x", dataforseo=client)
    assert payload.successful_competitor_count == 2
    # Median of [700, 900] = 800
    assert payload.competitor_word_count_median == 800


def test_run_research_swallows_entity_extraction_failure(monkeypatch):
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[_competitor("https://a.de")]), \
         patch("creator.api.research.extract_entities_from_competitors", side_effect=RuntimeError("boom")):
        payload = run_research(target_keyword="x", dataforseo=client)
    assert payload.entities == []
    assert payload.high_coverage_entities == []


def test_find_common_h2_themes_keeps_themes_above_threshold():
    competitors = [
        _competitor("a", h2s=["Vorteile", "Kosten und Honorare", "Kontakt"]),
        _competitor("b", h2s=["Vorteile", "Kosten und Honorare"]),
        _competitor("c", h2s=["Auswahlkriterien"]),
    ]
    themes = _find_common_h2_themes(competitors, min_competitors=2)
    # "Vorteile" and "Kosten und Honorare" appear in 2 of 3 → kept.
    # "Kontakt", "Auswahlkriterien" appear in 1 of 3 → dropped.
    assert "Vorteile" in themes
    assert "Kosten und Honorare" in themes
    assert "Kontakt" not in themes


def test_find_common_h2_themes_normalizes_punctuation_and_case():
    competitors = [
        _competitor("a", h2s=["Vorteile!", "Kosten - Honorare"]),
        _competitor("b", h2s=["vorteile", "kosten honorare"]),
    ]
    themes = _find_common_h2_themes(competitors, min_competitors=2)
    # First raw form is preserved as the display string
    assert themes[0] in {"Vorteile!", "Kosten - Honorare"}


def test_word_count_median_handles_empty():
    assert _word_count_median([]) is None
    assert _word_count_median([_competitor("a", word_count=0)]) is None


def test_word_count_median_excludes_failed_fetches():
    competitors = [
        _competitor("a", word_count=500),
        _competitor("b", word_count=0, status="timeout"),
        _competitor("c", word_count=900),
    ]
    assert _word_count_median(competitors) == 700  # median of [500, 900]


def test_topical_gap_flags_missing_high_coverage_entities():
    payload = ResearchPayload(
        target_keyword="t",
        location_code=2276,
        language_code="de",
        entities=[
            ExtractedEntity(name="DATEV", type="ORG", n_competitors=4, coverage=0.8),
            ExtractedEntity(name="ELSTER", type="ORG", n_competitors=2, coverage=0.4),
            ExtractedEntity(name="DSGVO", type="LAW", n_competitors=4, coverage=0.8),
        ],
    )
    article = "Wir verwenden DATEV in unserer Kanzlei."
    missing = topical_gap(payload, article_text=article, coverage_threshold=0.6)
    # DSGVO is high-coverage and missing → flagged.
    # ELSTER is below threshold → not flagged regardless.
    # DATEV is high-coverage and present → not flagged.
    assert [e.name for e in missing] == ["DSGVO"]


def test_topical_gap_handles_empty_article():
    payload = ResearchPayload(
        target_keyword="t",
        location_code=2276,
        language_code="de",
        entities=[
            ExtractedEntity(name="DATEV", type="ORG", n_competitors=4, coverage=0.8),
        ],
    )
    missing = topical_gap(payload, article_text="", coverage_threshold=0.6)
    assert [e.name for e in missing] == ["DATEV"]
