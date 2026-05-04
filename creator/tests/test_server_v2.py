from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from creator.api.article_assembler import AssembledArticle
from creator.api.contract import (
    ContentContract,
    LinkTarget,
    SearchIntent,
    SectionPlan,
)
from creator.api.eval_harness import CheckResult, QualityReport
from creator.api.eval_judge import JudgeAxisResult, JudgeScores
from creator.api.pipeline_runner import PipelineError, PipelineRun
from creator.api.research import ResearchPayload
from creator.api.section_writer import SectionDraft
from creator.api.server import app


def _client() -> TestClient:
    return TestClient(app)


def _pipeline_run() -> PipelineRun:
    return PipelineRun(
        target_keyword="steuerberater hamburg",
        target_backlink_url="https://client.de/leistungen",
        publishing_site_host="example.de",
        research=ResearchPayload(
            target_keyword="steuerberater hamburg",
            location_code=2276,
            language_code="de",
            competitor_word_count_median=900,
            total_cost_usd=0.05,
        ),
        contract=ContentContract(
            target_keyword="steuerberater hamburg",
            intent=SearchIntent.TRANSACTIONAL,
            target_audience="Hamburger Unternehmer",
            word_count_target=900,
            h1="Steuerberater Hamburg: Wie Sie den richtigen Berater finden",
            meta_title="Steuerberater Hamburg finden: Tipps für Unternehmer",
            meta_description="Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen und Honorare in unserem Leitfaden.",
            slug="steuerberater-hamburg",
            sections=[
                SectionPlan(h2="Warum Hamburg", mandate="Vorteile fuer Unternehmer.", target_word_count=200),
                SectionPlan(h2="Auswahl", mandate="Kriterien auflisten.", target_word_count=300),
                SectionPlan(h2="Kosten", mandate="Honorare erklaeren.", target_word_count=200),
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
        ),
        sections=[
            SectionDraft(section_index=i, h2=h2, body_html=f"<p>{i}</p>", word_count=200)
            for i, h2 in enumerate(["Warum Hamburg", "Auswahl", "Kosten"])
        ],
        assembled=AssembledArticle(
            article_html="<h1>Test</h1><p>Body</p>",
            schema_blocks=['<script type="application/ld+json">{"@type":"Article"}</script>'],
        ),
        refined_article_html="<h1>Refined</h1><p>Body</p>",
        final_html='<h1>Refined</h1><p>Body</p>\n<script type="application/ld+json">{"@type":"Article"}</script>',
        judge_scores=JudgeScores(
            intent_match=JudgeAxisResult(score=8, rationale="ok", threshold=7),
            backlink_anchor_naturalness=JudgeAxisResult(score=7, rationale="ok", threshold=7),
            eeat_signal_density=JudgeAxisResult(score=6, rationale="ok", threshold=6),
        ),
        quality_report=QualityReport(
            contract_target_keyword="steuerberater hamburg",
            deterministic=[CheckResult(name="word_count_band", passed=True, value=900.0)],
            llm_judged=[CheckResult(name="intent_match", passed=True, value=8.0)],
        ),
    )


# ---- happy path ------------------------------------------------------------


def test_v2_run_pipeline_returns_serialized_run():
    with patch("creator.api.server.run_pipeline", return_value=_pipeline_run()) as mock_run:
        response = _client().post(
            "/v2/run-pipeline",
            json={
                "target_keyword": "steuerberater hamburg",
                "target_backlink_url": "https://client.de/leistungen",
                "publishing_site_url": "https://example.de",
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["target_keyword"] == "steuerberater hamburg"
    assert payload["publishing_site_host"] == "example.de"
    assert payload["contract"]["target_keyword"] == "steuerberater hamburg"
    assert len(payload["sections"]) == 3
    assert payload["article_html"]["final"].startswith("<h1>Refined</h1>")
    assert payload["judge_scores"]["intent_match"]["score"] == 8
    assert payload["quality_report"]["passed"] is True
    mock_run.assert_called_once()


def test_v2_run_pipeline_forwards_optional_kwargs():
    with patch("creator.api.server.run_pipeline", return_value=_pipeline_run()) as mock_run:
        _client().post(
            "/v2/run-pipeline",
            json={
                "target_keyword": "steuerberater hamburg",
                "target_backlink_url": "https://client.de/leistungen",
                "publishing_site_url": "https://example.de",
                "anchor_hint": "partial_match",
                "canonical_url": "https://example.de/post-slug",
                "skip_voice_pass": True,
                "skip_judge": True,
            },
        )
    kwargs = mock_run.call_args.kwargs
    assert kwargs["anchor_hint"] == "partial_match"
    assert kwargs["canonical_url"] == "https://example.de/post-slug"
    assert kwargs["skip_voice_pass"] is True
    assert kwargs["skip_judge"] is True


def test_v2_run_pipeline_omits_judge_scores_when_skipped():
    run = _pipeline_run()
    run.judge_scores = None
    run.skipped_judge = True
    with patch("creator.api.server.run_pipeline", return_value=run):
        response = _client().post(
            "/v2/run-pipeline",
            json={
                "target_keyword": "steuerberater hamburg",
                "target_backlink_url": "https://client.de/y",
                "publishing_site_url": "https://example.de",
                "skip_judge": True,
            },
        )
    payload = response.json()
    assert payload["judge_scores"] is None
    assert payload["skipped_judge"] is True


# ---- failure paths ---------------------------------------------------------


def test_v2_run_pipeline_returns_422_on_pipeline_error():
    err = PipelineError("contract", "Contract output failed schema validation")
    with patch("creator.api.server.run_pipeline", side_effect=err):
        response = _client().post(
            "/v2/run-pipeline",
            json={
                "target_keyword": "steuerberater hamburg",
                "target_backlink_url": "https://client.de/y",
                "publishing_site_url": "https://example.de",
            },
        )
    assert response.status_code == 422
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"] == "pipeline_failed"
    assert payload["phase"] == "contract"
    assert "Contract output failed schema validation" in payload["message"]


def test_v2_run_pipeline_validates_required_fields():
    response = _client().post(
        "/v2/run-pipeline",
        json={"target_keyword": "x"},  # missing other required fields
    )
    assert response.status_code == 422


def test_v2_run_pipeline_rejects_too_short_keyword():
    response = _client().post(
        "/v2/run-pipeline",
        json={
            "target_keyword": "a",
            "target_backlink_url": "https://client.de/y",
            "publishing_site_url": "https://example.de",
        },
    )
    assert response.status_code == 422
