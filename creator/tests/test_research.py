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
    _sanitize_seo_keyword,
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


# ---- research cache --------------------------------------------------------


def _hydratable_payload_dict() -> dict:
    return {
        "target_keyword": "steuerberater hamburg",
        "location_code": 2276,
        "language_code": "de",
        "organic": [{"rank": 1, "url": "https://a.de", "title": "T", "description": "", "domain": "a.de"}],
        "paa_questions": ["q?"],
        "related_searches": ["x"],
        "primary_volume": {"keyword": "steuerberater hamburg", "search_volume": 9900, "competition": 0.45, "cpc": 4.32, "cost": 0.0, "monthly_searches": []},
        "related_keywords": [{"keyword": "steuerberater hamburg altona", "search_volume": 480, "cpc": None, "competition": None}],
        "competitors": [{
            "url": "https://a.de", "final_url": "https://a.de", "fetch_status": "ok",
            "http_status": 200, "title": "T", "h1": "H", "h2s": ["A"], "h3s": [], "body_text": "b",
            "word_count": 800, "internal_link_count": 0, "external_link_count": 0,
            "schema_types": [], "has_faq_schema": False, "has_article_schema": False,
        }],
        "competitor_word_count_median": 800,
        "common_h2_themes": ["A"],
        "entities": [{"name": "DATEV", "type": "ORG", "n_competitors": 2, "coverage": 1.0}],
        "high_coverage_entities": [{"name": "DATEV", "type": "ORG", "n_competitors": 2, "coverage": 1.0}],
        "research_version": "v1",
        "total_cost_usd": 0.05,
    }


def test_payload_serialisation_round_trip():
    cached = _hydratable_payload_dict()
    payload = research._payload_from_dict(cached)
    assert payload.target_keyword == "steuerberater hamburg"
    assert payload.organic[0].rank == 1
    assert payload.related_keywords[0].keyword == "steuerberater hamburg altona"
    assert payload.competitors[0].word_count == 800
    assert payload.entities[0].name == "DATEV"
    # Cached hydration always reports zero spend — the original cost was paid before.
    assert payload.total_cost_usd == 0.0

    redumped = research._payload_to_dict(payload)
    # All fields except total_cost_usd should round-trip identically.
    cached_for_compare = dict(cached)
    cached_for_compare["total_cost_usd"] = 0.0
    assert redumped == cached_for_compare


def test_run_research_returns_cached_payload_without_calling_dataforseo(monkeypatch):
    monkeypatch.setattr(
        "creator.api.research.research_cache.get_cached_research_payload",
        lambda **kwargs: _hydratable_payload_dict(),
    )
    upsert_calls = []
    monkeypatch.setattr(
        "creator.api.research.research_cache.upsert_research_payload",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    client = MagicMock()  # If touched, we'd see calls — assertions below catch that.
    payload = run_research(target_keyword="steuerberater hamburg", dataforseo=client)
    assert payload.target_keyword == "steuerberater hamburg"
    assert payload.entities[0].name == "DATEV"
    assert payload.total_cost_usd == 0.0
    client.serp_organic.assert_not_called()
    client.keyword_volume.assert_not_called()
    client.related_keywords.assert_not_called()
    assert upsert_calls == []  # Cache hit must not re-write.


def test_run_research_writes_fresh_payload_to_cache_on_miss(monkeypatch):
    upsert_calls = []
    monkeypatch.setattr(
        "creator.api.research.research_cache.get_cached_research_payload",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "creator.api.research.research_cache.upsert_research_payload",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=0)
        run_research(target_keyword="x", dataforseo=client)
    assert len(upsert_calls) == 1
    assert "x" in upsert_calls[0]["lookup_key"]
    assert upsert_calls[0]["locale"] == "de-2276"
    # The serialised payload must round-trip back through _payload_from_dict.
    rebuilt = research._payload_from_dict(upsert_calls[0]["payload"])
    assert rebuilt.target_keyword == "x"


def test_run_research_skips_cache_when_use_cache_false(monkeypatch):
    monkeypatch.setattr(
        "creator.api.research.research_cache.get_cached_research_payload",
        lambda **kwargs: pytest.fail("get_cached_research_payload should not be called when use_cache=False"),
    )
    monkeypatch.setattr(
        "creator.api.research.research_cache.upsert_research_payload",
        lambda **kwargs: pytest.fail("upsert_research_payload should not be called when use_cache=False"),
    )
    client = _make_client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=0)
        run_research(target_keyword="y", dataforseo=client, use_cache=False)


def test_build_lookup_key_distinguishes_skip_flags():
    from creator.api import research_cache

    base = research_cache.build_lookup_key(
        target_keyword="Steuerberater Hamburg",
        skip_related_keywords=False,
        skip_entity_extraction=False,
        research_version="v1",
    )
    other = research_cache.build_lookup_key(
        target_keyword="steuerberater hamburg",  # different case — should normalise to same key
        skip_related_keywords=False,
        skip_entity_extraction=False,
        research_version="v1",
    )
    assert base == other
    skip_changed = research_cache.build_lookup_key(
        target_keyword="steuerberater hamburg",
        skip_related_keywords=True,
        skip_entity_extraction=False,
        research_version="v1",
    )
    assert skip_changed != base


def test_build_locale_combines_language_and_location():
    from creator.api import research_cache

    assert research_cache.build_locale("de", 2276) == "de-2276"
    assert research_cache.build_locale("EN", 2840) == "en-2840"


# ---- _sanitize_seo_keyword ------------------------------------------------


class TestSanitizeSeoKeyword:
    def test_passthrough_for_clean_keyword(self):
        assert _sanitize_seo_keyword("kinderbrillen kaufen") == "kinderbrillen kaufen"

    def test_strips_after_colon(self):
        # The brillenhaus regression: brainstorm returned a title-shaped string.
        out = _sanitize_seo_keyword(
            "Augengesundheit und Sehhilfen: Wie die richtige Brille zu deinem Lifestyle passt"
        )
        assert out == "Augengesundheit und Sehhilfen"

    def test_strips_after_em_dash(self):
        assert _sanitize_seo_keyword("brille kind — der ratgeber") == "brille kind"

    def test_strips_after_pipe(self):
        assert _sanitize_seo_keyword("brille | shop") == "brille"

    def test_strips_after_question_mark(self):
        assert _sanitize_seo_keyword("was kostet eine brille? alle fakten") == "was kostet eine brille"

    def test_truncates_long_keyword_to_word_cap(self):
        out = _sanitize_seo_keyword("a b c d e f g h i j k l m")
        assert len(out.split()) <= 8

    def test_returns_empty_on_empty_input(self):
        assert _sanitize_seo_keyword("") == ""
        assert _sanitize_seo_keyword("   ") == ""

    def test_collapses_whitespace(self):
        assert _sanitize_seo_keyword("  brille    kaufen  ") == "brille kaufen"


def test_run_research_sanitizes_title_shaped_keyword(monkeypatch):
    """Regression: brainstorm/selector occasionally hand us a title-shaped
    string; research must sanitize before calling DataForSEO so we don't hit
    status 40501 'Keyword text has too many words'."""

    client = _make_client()
    captured_keyword = {"value": None}
    original_serp = client.serp_organic

    def capture_serp(keyword, **kwargs):
        captured_keyword["value"] = keyword
        return original_serp.return_value

    client.serp_organic.side_effect = capture_serp
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.research.scrape_top_results", return_value=[]), \
         patch("creator.api.research.extract_entities_from_competitors") as mock_entities:
        mock_entities.return_value = EntityExtractionResult(entities=[], competitor_count=0)
        payload = run_research(
            target_keyword="Augengesundheit und Sehhilfen: Wie die richtige Brille zu deinem Lifestyle passt",
            dataforseo=client,
            use_cache=False,
        )
    # DataForSEO saw the sanitized form, not the title.
    assert captured_keyword["value"] == "Augengesundheit und Sehhilfen"
    # Payload reflects the sanitized keyword too.
    assert payload.target_keyword == "Augengesundheit und Sehhilfen"


def test_run_research_raises_on_unusable_keyword():
    with pytest.raises(ValueError, match="unusable after sanitization"):
        run_research(target_keyword="   ", use_cache=False)
