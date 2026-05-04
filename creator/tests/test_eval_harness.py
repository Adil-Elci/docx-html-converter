from __future__ import annotations

import pytest

from creator.api.contract import ContentContract, GermanTone, SearchIntent, SectionPlan
from creator.api.entity_extract import ExtractedEntity
from creator.api.eval_harness import (
    check_paa_coverage,
    check_topical_entity_coverage,
    evaluate,
)
from creator.api.eval_judge import JudgeAxisResult, JudgeScores
from creator.api.research import ResearchPayload


def _entity(name: str, coverage: float, type_: str = "CONCEPT") -> ExtractedEntity:
    return ExtractedEntity(name=name, type=type_, n_competitors=int(coverage * 5), coverage=coverage)


def _contract() -> ContentContract:
    return ContentContract(
        target_keyword="steuerberater hamburg",
        intent=SearchIntent.TRANSACTIONAL,
        target_audience="Hamburger Unternehmer und Selbstständige",
        word_count_target=900,
        h1="Steuerberater Hamburg: Wie Sie den richtigen Berater finden",
        meta_title="Steuerberater Hamburg finden: Tipps für Unternehmer 2026",
        meta_description="Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen, Honorare und Spezialisierungen mit unserem Leitfaden zur Auswahl.",
        slug="steuerberater-hamburg",
        sections=[
            SectionPlan(h2="Warum Hamburg", mandate="Erklaere die Vorteile fuer Hamburger Unternehmer ausfuehrlich", target_word_count=200),
            SectionPlan(h2="Auswahlkriterien", mandate="Liste relevante Kriterien fuer die Auswahl auf", target_word_count=300),
            SectionPlan(h2="Kosten und Honorare", mandate="Erklaere typische Honorarstrukturen und Spannen", target_word_count=200),
        ],
    )


# ---- topical entity coverage -----------------------------------------------


def test_topical_entity_coverage_passes_when_all_entities_present():
    entities = [_entity("DATEV", 0.8), _entity("ELSTER", 0.8), _entity("DSGVO", 0.6)]
    article = "Die DATEV-Software unterstützt Steuerberater. ELSTER ist die Online-Plattform. DSGVO regelt den Datenschutz."
    result = check_topical_entity_coverage(article, entities)
    assert result.passed is True
    assert result.value == pytest.approx(1.0)


def test_topical_entity_coverage_fails_below_threshold():
    entities = [_entity("DATEV", 0.8), _entity("ELSTER", 0.8), _entity("DSGVO", 0.6)]
    article = "Wir nutzen nur DATEV."
    result = check_topical_entity_coverage(article, entities)
    assert result.passed is False
    assert result.value == pytest.approx(1 / 3)


def test_topical_entity_coverage_passes_with_no_entities():
    result = check_topical_entity_coverage("any article", [])
    assert result.passed is True
    assert result.value == 1.0


def test_topical_entity_coverage_is_case_insensitive():
    entities = [_entity("DATEV", 0.8)]
    article = "Wir verwenden datev-Software."  # lowercase variant
    result = check_topical_entity_coverage(article, entities)
    assert result.passed is True


# ---- PAA coverage ----------------------------------------------------------


def test_paa_coverage_passes_when_questions_addressed():
    paa = [
        "Was kostet ein Steuerberater in Hamburg?",
        "Welche Steuerberater sind die besten?",
        "Wie finde ich einen Steuerberater?",
    ]
    article = (
        "Ein Steuerberater in Hamburg kostet zwischen 100 und 300 Euro. "
        "Die besten Steuerberater sind branchenspezialisiert. "
        "Steuerberater finde ich über Empfehlungen und Online-Recherche."
    )
    result = check_paa_coverage(article, paa)
    assert result.passed is True
    assert result.value == pytest.approx(1.0)


def test_paa_coverage_fails_when_questions_unaddressed():
    paa = [
        "Was kostet ein Steuerberater in Hamburg?",
        "Welche Versicherungen brauche ich?",
        "Wie hoch ist der Mindestlohn?",
    ]
    article = "Steuerberater Hamburg sind teuer."
    result = check_paa_coverage(article, paa)
    assert result.passed is False
    assert result.value < 0.8


def test_paa_coverage_passes_with_no_questions():
    result = check_paa_coverage("article", [])
    assert result.passed is True


def test_paa_coverage_uses_content_words_only():
    # Question filled with stopwords; the only content word is "Steuerberater".
    paa = ["Wie ist der Steuerberater?"]
    article = "Steuerberater sind hilfreich."
    result = check_paa_coverage(article, paa)
    assert result.passed is True


# ---- evaluate() integration ------------------------------------------------


def test_evaluate_includes_research_driven_checks_when_payload_provided():
    research = ResearchPayload(
        target_keyword="steuerberater hamburg",
        location_code=2276,
        language_code="de",
        paa_questions=["Was kostet ein Steuerberater in Hamburg?"],
        entities=[_entity("DATEV", 0.8)],
        high_coverage_entities=[_entity("DATEV", 0.8)],
        competitor_word_count_median=900,
    )
    article = (
        "<h1>Steuerberater Hamburg: Wie Sie den richtigen Berater finden</h1>"
        "<p>Steuerberater Hamburg helfen mit Steuern. " + "Steuerberater Hamburg " * 50 + "</p>"
    )
    report = evaluate(
        article_html=article,
        contract=_contract(),
        host_domain="example.de",
        meta_title=_contract().meta_title,
        meta_description=_contract().meta_description,
        research=research,
    )
    names = {r.name for r in report.deterministic}
    assert "topical_entity_coverage" in names
    assert "paa_coverage" in names
    # No more topical_entity_coverage stub in llm_judged
    llm_names = {r.name for r in report.llm_judged}
    assert "topical_entity_coverage" not in llm_names
    assert "paa_coverage" not in llm_names


def test_evaluate_research_driven_checks_passthrough_when_payload_missing():
    article = "<h1>Steuerberater Hamburg</h1><p>Steuerberater Hamburg.</p>"
    report = evaluate(
        article_html=article,
        contract=_contract(),
        host_domain="example.de",
        meta_title=_contract().meta_title,
        meta_description=_contract().meta_description,
    )
    # Without research, both research-driven checks default to "true / nothing to enforce"
    by_name = {r.name: r for r in report.deterministic}
    assert by_name["topical_entity_coverage"].passed is True
    assert by_name["paa_coverage"].passed is True


# ---- judge_scores wiring ---------------------------------------------------


def _judge_scores(intent: int = 9, backlink: int = 8, eeat: int = 7) -> JudgeScores:
    return JudgeScores(
        intent_match=JudgeAxisResult(score=intent, rationale="ok", threshold=7),
        backlink_anchor_naturalness=JudgeAxisResult(score=backlink, rationale="ok", threshold=7),
        eeat_signal_density=JudgeAxisResult(score=eeat, rationale="ok", threshold=6),
    )


def test_evaluate_uses_judge_scores_when_provided():
    article = "<h1>Steuerberater Hamburg</h1><p>Steuerberater Hamburg.</p>"
    report = evaluate(
        article_html=article,
        contract=_contract(),
        host_domain="example.de",
        meta_title=_contract().meta_title,
        meta_description=_contract().meta_description,
        judge_scores=_judge_scores(intent=9, backlink=8, eeat=7),
    )
    by_name = {r.name: r for r in report.llm_judged}
    assert by_name["intent_match"].passed is True
    assert by_name["intent_match"].value == 9.0
    assert by_name["backlink_anchor_naturalness"].passed is True
    assert by_name["eeat_signal_density"].passed is True
    assert "STUB" not in (by_name["intent_match"].detail or "")


def test_evaluate_judge_scores_below_threshold_fail():
    article = "<h1>Steuerberater Hamburg</h1><p>X.</p>"
    report = evaluate(
        article_html=article,
        contract=_contract(),
        host_domain="example.de",
        meta_title=_contract().meta_title,
        meta_description=_contract().meta_description,
        judge_scores=_judge_scores(intent=4, backlink=3, eeat=2),
    )
    by_name = {r.name: r for r in report.llm_judged}
    assert by_name["intent_match"].passed is False
    assert by_name["backlink_anchor_naturalness"].passed is False
    assert by_name["eeat_signal_density"].passed is False


def test_evaluate_falls_back_to_stubs_without_judge_scores():
    article = "<h1>Steuerberater Hamburg</h1><p>X.</p>"
    report = evaluate(
        article_html=article,
        contract=_contract(),
        host_domain="example.de",
        meta_title=_contract().meta_title,
        meta_description=_contract().meta_description,
    )
    by_name = {r.name: r for r in report.llm_judged}
    # Stubs default to passed=True (no judgment without data)
    assert by_name["intent_match"].passed is True
    assert "STUB" in by_name["intent_match"].detail
