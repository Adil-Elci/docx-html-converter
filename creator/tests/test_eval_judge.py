from __future__ import annotations

from unittest.mock import patch

import pytest

from creator.api.contract import ContentContract, GermanTone, LinkTarget, SearchIntent, SectionPlan
from creator.api.dataforseo import OrganicResult
from creator.api.eval_judge import (
    BACKLINK_NATURALNESS_MIN_SCORE,
    EEAT_DENSITY_MIN_SCORE,
    INTENT_MATCH_MIN_SCORE,
    JudgeAxisResult,
    JudgeScores,
    _coerce_score,
    build_user_prompt,
    judge_article,
)
from creator.api.llm import LLMError
from creator.api.research import ResearchPayload


def _contract_with_backlink() -> ContentContract:
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
            SectionPlan(h2="Warum Hamburg", mandate="Erkläre die Vorteile spezifisch für Hamburger Unternehmer.", target_word_count=200),
            SectionPlan(h2="Auswahlkriterien", mandate="Liste relevante Kriterien für die Auswahl auf.", target_word_count=300),
            SectionPlan(h2="Kosten und Honorare", mandate="Erkläre typische Honorarstrukturen.", target_word_count=200),
        ],
        link_plan=[
            LinkTarget(
                target_url="https://client.de/steuerberatung",
                anchor_strategy="partial_match",
                section_index=1,
                surrounding_context_requirements="Im Kontext einer Empfehlung in der Sektion zu Auswahlkriterien.",
                link_type="backlink",
            )
        ],
    )


def _research() -> ResearchPayload:
    return ResearchPayload(
        target_keyword="steuerberater hamburg",
        location_code=2276,
        language_code="de",
        organic=[
            OrganicResult(rank=1, url="https://a.de", title="Steuerberater Hamburg A", description="", domain="a.de"),
            OrganicResult(rank=2, url="https://b.de", title="Beste Steuerberater Hamburg", description="", domain="b.de"),
        ],
    )


# ---- prompt assembly -------------------------------------------------------


def test_build_user_prompt_includes_contract_and_competitor_signals():
    prompt = build_user_prompt(
        article_html="<h1>Test</h1>",
        contract=_contract_with_backlink(),
        research=_research(),
    )
    assert "steuerberater hamburg" in prompt
    assert "transactional" in prompt
    assert "https://client.de/steuerberatung" in prompt
    assert "partial_match" in prompt
    assert "Steuerberater Hamburg A" in prompt


def test_build_user_prompt_handles_no_research():
    prompt = build_user_prompt(article_html="<h1>X</h1>", contract=_contract_with_backlink())
    assert "(keine Wettbewerber-Daten verfügbar)" in prompt


def test_build_user_prompt_handles_no_link_plan():
    contract = _contract_with_backlink().model_copy(update={"link_plan": []})
    prompt = build_user_prompt(article_html="<h1>X</h1>", contract=contract)
    assert "(kein Backlink im Vertrag definiert)" in prompt


def test_build_user_prompt_truncates_long_articles():
    article = "<p>" + ("Wort " * 5000) + "</p>"
    prompt = build_user_prompt(article_html=article, contract=_contract_with_backlink(), article_max_chars=200)
    assert len(prompt) < 2000  # truncation kicked in
    assert " ..." in prompt


# ---- coercion --------------------------------------------------------------


def test_coerce_score_clamps_int_to_0_10():
    assert _coerce_score(7) == 7
    assert _coerce_score(11) == 10
    assert _coerce_score(-3) == 0


def test_coerce_score_handles_string_int():
    assert _coerce_score("8") == 8
    assert _coerce_score(" 5 ") == 5


def test_coerce_score_returns_zero_for_garbage():
    assert _coerce_score("not a number") == 0
    assert _coerce_score(None) == 0
    assert _coerce_score(True) == 0  # booleans treated as garbage


def test_coerce_score_rounds_floats():
    assert _coerce_score(7.6) == 8
    assert _coerce_score(7.4) == 7


# ---- judge_article ---------------------------------------------------------


def _good_judge_payload() -> dict:
    return {
        "intent_match": 9,
        "intent_match_rationale": "Artikel adressiert transaktionalen Intent korrekt.",
        "backlink_anchor_naturalness": 8,
        "backlink_anchor_naturalness_rationale": "Anker liest sich natürlich im Kontext.",
        "eeat_signal_density": 7,
        "eeat_signal_density_rationale": "Konkrete Zahlen und Gesetzesverweise vorhanden.",
    }


def test_judge_article_parses_payload(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return _good_judge_payload()

    scores = judge_article(
        article_html="<h1>X</h1>",
        contract=_contract_with_backlink(),
        research=_research(),
        llm_caller=fake_caller,
    )
    assert scores.intent_match.score == 9
    assert scores.intent_match.passed is True
    assert scores.backlink_anchor_naturalness.score == 8
    assert scores.eeat_signal_density.score == 7
    assert all(axis.passed for axis in [scores.intent_match, scores.backlink_anchor_naturalness, scores.eeat_signal_density])


def test_judge_article_marks_axes_below_threshold_as_failed(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return {
            "intent_match": 5,
            "intent_match_rationale": "Mittelmäßig.",
            "backlink_anchor_naturalness": 4,
            "backlink_anchor_naturalness_rationale": "Anker wirkt erzwungen.",
            "eeat_signal_density": 3,
            "eeat_signal_density_rationale": "Wenig Signale.",
        }

    scores = judge_article(
        article_html="<h1>X</h1>",
        contract=_contract_with_backlink(),
        llm_caller=fake_caller,
    )
    assert scores.intent_match.passed is False
    assert scores.backlink_anchor_naturalness.passed is False
    assert scores.eeat_signal_density.passed is False


def test_judge_article_handles_missing_fields_gracefully(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return {"intent_match": 8}  # missing other axes

    scores = judge_article(
        article_html="<h1>X</h1>",
        contract=_contract_with_backlink(),
        llm_caller=fake_caller,
    )
    assert scores.intent_match.score == 8
    assert scores.backlink_anchor_naturalness.score == 0
    assert scores.eeat_signal_density.score == 0
    assert scores.intent_match.rationale == ""


def test_judge_article_rejects_non_dict_response(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return "not a dict"

    with pytest.raises(LLMError, match="non-dict"):
        judge_article(
            article_html="<h1>X</h1>",
            contract=_contract_with_backlink(),
            llm_caller=fake_caller,
        )


def test_judge_article_requires_api_key_in_production(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        judge_article(article_html="<h1>X</h1>", contract=_contract_with_backlink())


def test_judge_article_threshold_constants_match_min_scores():
    # Sanity check that the thresholds from the spec match the constants.
    assert INTENT_MATCH_MIN_SCORE == 7
    assert BACKLINK_NATURALNESS_MIN_SCORE == 7
    assert EEAT_DENSITY_MIN_SCORE == 6
