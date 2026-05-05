from __future__ import annotations

from unittest.mock import patch

import pytest

from creator.api import publisher_fit as pf
from creator.api.publisher_fit import (
    FitVerdict,
    PublisherFitError,
    validate_or_refine_topic_for_publisher,
)


# ---- _summarise_publisher_profile ----------------------------------------


class TestSummarisePublisherProfile:
    def test_picks_high_signal_fields(self):
        out = pf._summarise_publisher_profile({
            "primary_context": "kids and family",
            "topics": [{"label": "parenting"}, {"label": "schule"}],
            "language": "de",
            "audience": "parents 30-45",
        })
        assert "primary context: kids and family" in out
        assert "topics: parenting, schule" in out
        assert "audience: parents 30-45" in out
        assert "language: de" in out

    def test_skips_empty_fields(self):
        out = pf._summarise_publisher_profile({
            "primary_context": "tax",
            "topics": [],
            "secondary_contexts": "",
        })
        assert "primary context: tax" in out
        assert "topics" not in out
        assert "secondary_contexts" not in out

    def test_empty_payload_returns_marker(self):
        out = pf._summarise_publisher_profile({})
        assert out.startswith("(") and out.endswith(")")

    def test_none_returns_marker(self):
        out = pf._summarise_publisher_profile(None)
        assert out.startswith("(")


# ---- validate_or_refine_topic_for_publisher ------------------------------


class TestValidateOrRefine:
    def test_soft_passes_with_no_profile(self):
        verdict = validate_or_refine_topic_for_publisher(
            target_keyword="kw",
            publishing_profile_payload=None,
            language="de",
        )
        assert verdict.refined_keyword == "kw"
        assert verdict.changed is False
        assert verdict.confidence == 0.5
        assert verdict.rationale == "publisher_profile_unavailable"

    def test_soft_passes_when_no_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            verdict = validate_or_refine_topic_for_publisher(
                target_keyword="kw",
                publishing_profile_payload={"primary_context": "kids"},
                language="de",
                api_key=None,
            )
        assert verdict.refined_keyword == "kw"
        assert verdict.confidence == 0.5
        assert verdict.rationale == "anthropic_api_key_not_configured"

    def test_returns_unchanged_when_llm_says_fits(self):
        with patch.object(pf, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "verdict": "fits",
                "refined_keyword": "kinderbrillen online kaufen",
                "confidence": 0.9,
                "rationale": "good fit",
            }
            verdict = validate_or_refine_topic_for_publisher(
                target_keyword="kinderbrillen online kaufen",
                publishing_profile_payload={"primary_context": "kids"},
                language="de",
                api_key="test",
            )
        assert verdict.refined_keyword == "kinderbrillen online kaufen"
        assert verdict.changed is False
        assert verdict.confidence == 0.9

    def test_refines_when_llm_says_refine(self):
        # The classic brillenhaus24 -> kidsblatt case.
        with patch.object(pf, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "verdict": "refine",
                "refined_keyword": "kinderbrillen online kaufen",
                "confidence": 0.75,
                "rationale": "publisher targets parents; refine to kids glasses",
            }
            verdict = validate_or_refine_topic_for_publisher(
                target_keyword="günstige brillen online kaufen",
                publishing_profile_payload={"primary_context": "kids and family"},
                language="de",
                api_key="test",
            )
        assert verdict.refined_keyword == "kinderbrillen online kaufen"
        assert verdict.original_keyword == "günstige brillen online kaufen"
        assert verdict.changed is True
        assert verdict.confidence == 0.75

    def test_hard_fails_when_no_fit(self):
        with patch.object(pf, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "verdict": "no_fit",
                "refined_keyword": "",
                "confidence": 0.1,
                "rationale": "adult product on a kids-only publisher",
            }
            with pytest.raises(PublisherFitError) as exc:
                validate_or_refine_topic_for_publisher(
                    target_keyword="adult product xyz",
                    publishing_profile_payload={"primary_context": "kids only"},
                    language="de",
                    api_key="test",
                )
        assert exc.value.code == "no_editorial_fit"
        assert "no editorial intersection" in str(exc.value).lower()

    def test_hard_fails_when_confidence_below_threshold(self):
        with patch.object(pf, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "verdict": "refine",
                "refined_keyword": "stretched fit keyword",
                "confidence": 0.25,
                "rationale": "weak overlap",
            }
            with pytest.raises(PublisherFitError) as exc:
                validate_or_refine_topic_for_publisher(
                    target_keyword="kw",
                    publishing_profile_payload={"primary_context": "x"},
                    language="de",
                    api_key="test",
                    min_confidence=0.4,
                )
        assert exc.value.code == "fit_below_threshold"

    def test_soft_passes_on_llm_failure(self):
        from creator.api.llm import LLMError

        with patch.object(pf, "call_llm_json", side_effect=LLMError("API down")):
            verdict = validate_or_refine_topic_for_publisher(
                target_keyword="kw",
                publishing_profile_payload={"primary_context": "x"},
                language="de",
                api_key="test",
            )
        assert verdict.refined_keyword == "kw"
        assert "llm_unavailable" in verdict.rationale

    def test_french_language_routes_to_french_prompt(self):
        captured = {}

        def fake_llm(**kwargs):
            captured.update(kwargs)
            return {"verdict": "fits", "refined_keyword": "kw", "confidence": 0.9, "rationale": "ok"}

        with patch.object(pf, "call_llm_json", side_effect=fake_llm):
            validate_or_refine_topic_for_publisher(
                target_keyword="kw",
                publishing_profile_payload={"primary_context": "x"},
                language="fr",
                api_key="test",
            )
        assert "stratège de contenu francophone" in captured["system_prompt"]


# ---- HTTP endpoint --------------------------------------------------------


class TestEndpoint:
    def test_endpoint_returns_200_on_fit(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        with patch("creator.api.server.validate_or_refine_topic_for_publisher") as mock_fn:
            mock_fn.return_value = FitVerdict(
                refined_keyword="kinderbrillen",
                original_keyword="brillen",
                changed=True,
                confidence=0.8,
                rationale="kids site refine",
                cost_usd=0.001,
            )
            response = TestClient(app).post(
                "/v2/refine-topic-for-publisher",
                json={
                    "target_keyword": "brillen",
                    "publishing_profile_payload": {"primary_context": "kids"},
                    "language": "de",
                },
            )
        assert response.status_code == 200
        body = response.json()
        assert body["ok"] is True
        assert body["refined_keyword"] == "kinderbrillen"
        assert body["changed"] is True
        assert body["confidence"] == 0.8

    def test_endpoint_returns_422_on_no_fit(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        with patch("creator.api.server.validate_or_refine_topic_for_publisher") as mock_fn:
            mock_fn.side_effect = PublisherFitError(
                "no_editorial_fit",
                "No editorial intersection.",
            )
            response = TestClient(app).post(
                "/v2/refine-topic-for-publisher",
                json={
                    "target_keyword": "adult product",
                    "publishing_profile_payload": {"primary_context": "kids"},
                    "language": "de",
                },
            )
        assert response.status_code == 422
        body = response.json()
        assert body["ok"] is False
        assert body["error"] == "publisher_fit_failed"
        assert body["code"] == "no_editorial_fit"
