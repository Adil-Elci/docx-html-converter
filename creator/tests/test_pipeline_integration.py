"""Integration test for the research -> contract -> evaluate handoff.

Fully mocked: no live API calls. Verifies that the data shapes flow correctly
between modules so that any signature drift surfaces here, not in production.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from creator.api.contract_generator import generate_contract
from creator.api.dataforseo import KeywordMetric, OrganicResult, RelatedKeyword, RelatedKeywordsResult, SerpResult
from creator.api.entity_extract import EntityExtractionResult, ExtractedEntity
from creator.api.eval_harness import evaluate
from creator.api.eval_judge import JudgeAxisResult, JudgeScores
from creator.api.research import run_research
from creator.api.serp_scrape import ScrapedCompetitor


def _research_dependencies():
    """Mock the three external dependencies of run_research."""

    serp = SerpResult(
        keyword="steuerberater hamburg",
        organic=[
            OrganicResult(rank=1, url="https://a.de", title="Steuerberater A", description="", domain="a.de"),
            OrganicResult(rank=2, url="https://b.de", title="Steuerberater B", description="", domain="b.de"),
        ],
        people_also_ask=["Was kostet ein Steuerberater?"],
        related_searches=["steuerberater hamburg altona"],
        cost=0.002,
    )
    client = MagicMock()
    client.serp_organic.return_value = serp
    client.keyword_volume.return_value = [
        KeywordMetric(keyword="steuerberater hamburg", search_volume=9900, competition=0.45, cpc=4.32, cost=0.0001)
    ]
    client.related_keywords.return_value = RelatedKeywordsResult(
        seed_keyword="steuerberater hamburg",
        items=[RelatedKeyword(keyword="steuerberater hamburg altona", search_volume=480)],
        cost=0.01,
    )

    competitors = [
        ScrapedCompetitor(
            url="https://a.de",
            final_url="https://a.de",
            fetch_status="ok",
            body_text="DATEV ist relevant. Steuerberatergebührenverordnung gilt.",
            word_count=950,
            h2s=["Auswahlkriterien", "Kosten und Honorare"],
        ),
        ScrapedCompetitor(
            url="https://b.de",
            final_url="https://b.de",
            fetch_status="ok",
            body_text="DATEV nutzen wir. Steuerberatergebührenverordnung beachten.",
            word_count=920,
            h2s=["Auswahlkriterien", "Honorare"],
        ),
    ]
    entity_result = EntityExtractionResult(
        entities=[
            ExtractedEntity(name="DATEV", type="ORGANIZATION", n_competitors=2, coverage=1.0),
            ExtractedEntity(name="Steuerberatergebührenverordnung", type="LAW", n_competitors=2, coverage=1.0),
        ],
        competitor_count=2,
    )
    return client, competitors, entity_result


def _contract_payload() -> dict:
    return {
        "target_keyword": "steuerberater hamburg",
        "secondary_keywords": ["steuerberater hamburg altona", "günstiger steuerberater hamburg"],
        "intent": "transactional",
        "tone": "sie",
        "target_audience": "Hamburger Unternehmer und Selbstständige",
        "word_count_target": 940,
        "h1": "Steuerberater Hamburg: So finden Sie den richtigen Berater",
        "meta_title": "Steuerberater Hamburg finden: Tipps für Unternehmer",
        "meta_description": "Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen, Honorare und Spezialisierungen. Unser Leitfaden hilft bei der Auswahl heute.",
        "slug": "steuerberater-hamburg-finden",
        "sections": [
            {"h2": "Warum ein Hamburger Steuerberater wichtig ist", "mandate": "Erkläre Vorteile fuer Hamburger Unternehmer.", "target_word_count": 200, "required_subheadings": [], "required_elements": []},
            {"h2": "Auswahlkriterien für Ihren Steuerberater", "mandate": "Liste relevante Kriterien fuer die Auswahl auf.", "target_word_count": 280, "required_subheadings": [], "required_elements": ["list"]},
            {"h2": "Kosten und Honorare im Überblick", "mandate": "Erkläre typische Honorarstrukturen mit Spannen.", "target_word_count": 240, "required_subheadings": [], "required_elements": ["table"]},
            {"h2": "DATEV und digitale Tools", "mandate": "Beschreibe wie moderne Berater DATEV einsetzen.", "target_word_count": 200, "required_subheadings": [], "required_elements": []},
        ],
        "faq_items": [
            {"question": "Was kostet ein Steuerberater?", "answer_outline": "100-300 Euro pro Stunde."}
        ],
        "required_entities": [
            {"name": "DATEV", "placement_hint": "in section 4"},
            {"name": "Steuerberatergebührenverordnung", "placement_hint": "in section 3"},
        ],
        "link_plan": [
            {
                "target_url": "https://client.de/steuerberatung",
                "anchor_strategy": "partial_match",
                "section_index": 1,
                "surrounding_context_requirements": "Im Kontext einer Empfehlung in der Sektion zu Auswahlkriterien.",
                "link_type": "backlink",
            }
        ],
        "schema_spec": {"article": True, "faq_page": True},
        "ai_tell_blocklist": [
            "Darüber hinaus", "Es ist wichtig zu beachten", "Zusammenfassend",
            "In der heutigen Zeit", "Letztendlich", "Abschließend",
            "In diesem Artikel werden wir", "Im Folgenden", "wie bereits erwähnt",
            "ohne Zweifel", "selbstverständlich", "essenziell",
        ],
        "competitor_top_urls": ["https://a.de", "https://b.de"],
        "contract_version": "v1",
    }


def test_research_to_contract_pipeline_smokes_clean(monkeypatch):
    """Run research, hand its output to contract generator, verify shapes line up."""

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    client, competitors, entity_result = _research_dependencies()

    with patch("creator.api.research.scrape_top_results", return_value=competitors), \
         patch("creator.api.research.extract_entities_from_competitors", return_value=entity_result):
        research = run_research(target_keyword="steuerberater hamburg", dataforseo=client)

    # Sanity on research handoff data:
    assert research.competitor_word_count_median == 935  # median of [950, 920]
    assert "DATEV" in {e.name for e in research.high_coverage_entities}
    assert research.paa_questions == ["Was kostet ein Steuerberater?"]

    # Now feed it to the contract generator (LLM mocked).
    def fake_caller(**kwargs):
        # Verify that research signals reached the prompt.
        assert "DATEV" in kwargs["user_prompt"]
        assert "Steuerberatergebührenverordnung" in kwargs["user_prompt"]
        assert "Was kostet ein Steuerberater?" in kwargs["user_prompt"]
        assert "935 Wörter" in kwargs["user_prompt"]
        return json.dumps(_contract_payload(), ensure_ascii=False)

    contract = generate_contract(
        research,
        target_backlink_url="https://client.de/steuerberatung",
        anchor_hint="partial_match",
        llm_caller=fake_caller,
    )

    # Contract reflects research:
    assert contract.target_keyword == research.target_keyword
    assert any(e.name == "DATEV" for e in contract.required_entities)
    assert contract.link_plan[0].target_url == "https://client.de/steuerberatung"

    # Now run the harness end-to-end to confirm signatures still align.
    article_html = (
        "<h1>Steuerberater Hamburg: So finden Sie den richtigen Berater</h1>"
        "<h2>Warum ein Hamburger Steuerberater wichtig ist</h2><p>" + ("Steuerberater Hamburg " * 30) + "</p>"
        "<h2>Auswahlkriterien für Ihren Steuerberater</h2>"
        "<p>Wir empfehlen den <a href='https://client.de/steuerberatung'>spezialisierten Steuerberater</a> für Unternehmen.</p>"
        "<h2>Kosten und Honorare im Überblick</h2>"
        "<p>Was kostet ein Steuerberater? Ein Steuerberater kostet zwischen 100 und 300 Euro pro Stunde. "
        "Die Steuerberatergebührenverordnung regelt Honorare.</p>"
        "<h2>DATEV und digitale Tools</h2><p>DATEV-Software ist Standard.</p>"
    )

    judge_scores = JudgeScores(
        intent_match=JudgeAxisResult(score=8, rationale="ok", threshold=7),
        backlink_anchor_naturalness=JudgeAxisResult(score=7, rationale="ok", threshold=7),
        eeat_signal_density=JudgeAxisResult(score=6, rationale="ok", threshold=6),
    )

    report = evaluate(
        article_html=article_html,
        contract=contract,
        host_domain="client.de",
        meta_title=contract.meta_title,
        meta_description=contract.meta_description,
        research=research,
        judge_scores=judge_scores,
    )

    # All three previously-stubbed checks should now be real (not stub-text).
    by_name = {r.name: r for r in report.llm_judged}
    assert {"intent_match", "backlink_anchor_naturalness", "eeat_signal_density"} == set(by_name.keys())
    for axis in ("intent_match", "backlink_anchor_naturalness", "eeat_signal_density"):
        assert "STUB" not in (by_name[axis].detail or "")

    # Research-driven deterministic checks should also be real (not "no entities to enforce").
    det_by_name = {r.name: r for r in report.deterministic}
    assert det_by_name["topical_entity_coverage"].detail.startswith("2/2")
    assert "1/1 questions addressed" in det_by_name["paa_coverage"].detail
