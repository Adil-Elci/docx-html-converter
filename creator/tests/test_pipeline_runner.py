from __future__ import annotations

from unittest.mock import DEFAULT, patch

import pytest

from creator.api.article_assembler import AssembledArticle
from creator.api.contract import (
    ContentContract,
    EntityRequirement,
    LinkTarget,
    SearchIntent,
    SectionPlan,
)
from creator.api.eval_harness import CheckResult, QualityReport
from creator.api.eval_judge import JudgeAxisResult, JudgeScores
from creator.api.pipeline_runner import (
    PipelineError,
    PipelineRun,
    _host_from_url,
    run_pipeline,
)
from creator.api.research import ResearchPayload
from creator.api.section_writer import SectionDraft


def _research() -> ResearchPayload:
    return ResearchPayload(
        target_keyword="steuerberater hamburg",
        location_code=2276,
        language_code="de",
        competitor_word_count_median=900,
        total_cost_usd=0.05,
    )


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
            SectionPlan(h2="Warum Hamburg", mandate="Vorteile fuer Hamburger Unternehmer ausfuehrlich.", target_word_count=200),
            SectionPlan(h2="Auswahlkriterien", mandate="Liste relevante Kriterien fuer die Auswahl auf.", target_word_count=300),
            SectionPlan(h2="Kosten und Honorare", mandate="Erklaere typische Honorarstrukturen.", target_word_count=200),
        ],
        link_plan=[
            LinkTarget(
                target_url="https://client.de/leistungen",
                anchor_strategy="partial_match",
                section_index=1,
                surrounding_context_requirements="Im Kontext.",
                link_type="backlink",
            )
        ],
    )


def _drafts() -> list[SectionDraft]:
    return [
        SectionDraft(section_index=i, h2=h2, body_html=f"<p>Inhalt {i}</p>", word_count=200)
        for i, h2 in enumerate(["Warum Hamburg", "Auswahlkriterien", "Kosten und Honorare"])
    ]


def _assembled() -> AssembledArticle:
    return AssembledArticle(
        article_html="<h1>Steuerberater Hamburg</h1><p>Body</p>",
        schema_blocks=['<script type="application/ld+json">{"@type":"Article"}</script>'],
    )


def _judge_scores(intent: int = 9, backlink: int = 8, eeat: int = 7) -> JudgeScores:
    return JudgeScores(
        intent_match=JudgeAxisResult(score=intent, rationale="ok", threshold=7),
        backlink_anchor_naturalness=JudgeAxisResult(score=backlink, rationale="ok", threshold=7),
        eeat_signal_density=JudgeAxisResult(score=eeat, rationale="ok", threshold=6),
    )


def _quality_report(passed: bool = True) -> QualityReport:
    return QualityReport(
        contract_target_keyword="steuerberater hamburg",
        deterministic=[CheckResult(name="word_count_band", passed=passed, value=900.0)],
        llm_judged=[CheckResult(name="intent_match", passed=passed, value=9.0)],
    )


def _patch_steps():
    """Patch every external pipeline step. Yields a dict of mocks keyed by attribute name."""

    return patch.multiple(
        "creator.api.pipeline_runner",
        run_research=DEFAULT,
        generate_contract=DEFAULT,
        write_all_sections=DEFAULT,
        assemble_article=DEFAULT,
        refine_voice=DEFAULT,
        judge_article=DEFAULT,
        evaluate=DEFAULT,
    )


def _setup_default_returns(mocks: dict) -> dict:
    mocks["run_research"].return_value = _research()
    mocks["generate_contract"].return_value = _contract()
    mocks["write_all_sections"].return_value = _drafts()
    mocks["assemble_article"].return_value = _assembled()
    mocks["refine_voice"].return_value = "<h1>Refined</h1><p>Body</p>"
    mocks["judge_article"].return_value = _judge_scores()
    mocks["evaluate"].return_value = _quality_report()
    return mocks


# ---- happy path ------------------------------------------------------------


def test_run_pipeline_chains_all_phases_in_order():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        result = run_pipeline(
            target_keyword="steuerberater hamburg",
            target_backlink_url="https://client.de/leistungen",
            publishing_site_url="https://example.de",
        )
    assert isinstance(result, PipelineRun)
    assert result.research.target_keyword == "steuerberater hamburg"
    assert result.contract.target_keyword == "steuerberater hamburg"
    assert len(result.sections) == 3
    assert result.refined_article_html == "<h1>Refined</h1><p>Body</p>"
    assert "<h1>Refined</h1>" in result.final_html
    assert "Article" in result.final_html  # schema block re-attached
    assert result.judge_scores is not None
    assert result.quality_report.passed is True


def test_run_pipeline_extracts_publishing_host():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        result = run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://www.example.de/blog",
        )
    assert result.publishing_site_host == "example.de"


def test_run_pipeline_skip_voice_pass_uses_assembled_html():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        result = run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
            skip_voice_pass=True,
        )
    mocks["refine_voice"].assert_not_called()
    assert "<h1>Steuerberater Hamburg</h1>" in result.refined_article_html
    assert result.skipped_voice_pass is True
    assert "voice_pass skipped" in result.notes[0]


def test_run_pipeline_skip_judge_yields_none_scores():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        result = run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
            skip_judge=True,
        )
    mocks["judge_article"].assert_not_called()
    assert result.judge_scores is None
    assert result.skipped_judge is True


def test_run_pipeline_passes_anchor_hint_to_contract():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
            anchor_hint="branded",
        )
    mocks["generate_contract"].assert_called_once()
    kwargs = mocks["generate_contract"].call_args.kwargs
    assert kwargs["anchor_hint"] == "branded"


def test_run_pipeline_passes_canonical_url_to_assembler():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
            canonical_url="https://example.de/article-slug",
        )
    kwargs = mocks["assemble_article"].call_args.kwargs
    assert kwargs["canonical_url"] == "https://example.de/article-slug"


def test_run_pipeline_forwards_research_skip_flags():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        run_pipeline(
            target_keyword="x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
            skip_related_keywords=True,
            skip_entity_extraction=True,
        )
    kwargs = mocks["run_research"].call_args.kwargs
    assert kwargs["skip_related_keywords"] is True
    assert kwargs["skip_entity_extraction"] is True


# ---- failure paths ---------------------------------------------------------


def test_run_pipeline_wraps_research_failure_with_phase_label():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        mocks["run_research"].side_effect = RuntimeError("boom")
        with pytest.raises(PipelineError, match=r"\[research\]"):
            run_pipeline(
                target_keyword="x",
                target_backlink_url="https://client.de/y",
                publishing_site_url="https://example.de",
            )


def test_run_pipeline_wraps_contract_failure_with_phase_label():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        mocks["generate_contract"].side_effect = RuntimeError("contract crashed")
        with pytest.raises(PipelineError, match=r"\[contract\]"):
            run_pipeline(
                target_keyword="x",
                target_backlink_url="https://client.de/y",
                publishing_site_url="https://example.de",
            )


def test_run_pipeline_wraps_voice_pass_failure_with_phase_label():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        mocks["refine_voice"].side_effect = RuntimeError("voice failed")
        with pytest.raises(PipelineError, match=r"\[voice_pass\]"):
            run_pipeline(
                target_keyword="x",
                target_backlink_url="https://client.de/y",
                publishing_site_url="https://example.de",
            )


def test_run_pipeline_wraps_judge_failure_with_phase_label():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        mocks["judge_article"].side_effect = RuntimeError("judge failed")
        with pytest.raises(PipelineError, match=r"\[judge\]"):
            run_pipeline(
                target_keyword="x",
                target_backlink_url="https://client.de/y",
                publishing_site_url="https://example.de",
            )


def test_run_pipeline_eval_failure_carries_phase_label():
    with _patch_steps() as mocks:
        _setup_default_returns(mocks)
        mocks["evaluate"].side_effect = RuntimeError("eval crashed")
        with pytest.raises(PipelineError, match=r"\[eval\]"):
            run_pipeline(
                target_keyword="x",
                target_backlink_url="https://client.de/y",
                publishing_site_url="https://example.de",
            )


# ---- host extraction -------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://example.de/path", "example.de"),
        ("https://www.example.de/blog", "example.de"),
        ("example.de", "example.de"),
        ("https://EXAMPLE.de/", "example.de"),
        ("http://sub.example.de/x", "sub.example.de"),
    ],
)
def test_host_from_url_normalizes(url: str, expected: str):
    assert _host_from_url(url) == expected
