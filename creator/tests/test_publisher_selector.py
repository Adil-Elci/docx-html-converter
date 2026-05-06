from __future__ import annotations

from unittest.mock import patch

import pytest

from creator.api import publisher_selector as ps
from creator.api.publisher_selector import (
    PublisherSelectorError,
    SelectionResult,
    select_best_publisher,
)


# ---- _summarise_target_profile -------------------------------------------


class TestSummariseTargetProfile:
    def test_picks_high_signal_fields(self):
        out = ps._summarise_target_profile(
            {
                "domain_level_topic": "eyewear ecommerce",
                "primary_context": "shopping",
                "topics": [{"label": "brillen"}, {"label": "kontaktlinsen"}],
                "audience": "adults shopping for prescription glasses",
            },
            target_url="https://brillenhaus24.de",
        )
        assert "url: https://brillenhaus24.de" in out
        assert "domain topic: eyewear ecommerce" in out
        assert "primary context: shopping" in out
        assert "topics: brillen, kontaktlinsen" in out
        assert "audience: adults shopping" in out

    def test_falls_through_when_profile_empty(self):
        out = ps._summarise_target_profile({}, target_url="https://x.de")
        assert "url: https://x.de" in out
        assert "(empty)" in out

    def test_handles_missing_profile(self):
        out = ps._summarise_target_profile(None, target_url="https://x.de")
        assert "url: https://x.de" in out
        assert "(not available)" in out


# ---- _summarise_candidate ------------------------------------------------


class TestSummariseCandidate:
    def test_renders_id_url_and_profile(self):
        block = ps._summarise_candidate(
            {
                "site_id": "abc-123",
                "site_url": "https://kidsblatt.de",
                "publishing_profile_payload": {
                    "primary_context": "family",
                    "topics": ["parenting", "schule"],
                    "audience": "parents",
                },
            },
            index=0,
        )
        assert "[1] site_id: abc-123" in block
        assert "site_url: https://kidsblatt.de" in block
        assert "primary context: family" in block
        assert "topics: parenting, schule" in block
        assert "audience: parents" in block

    def test_marks_general_publisher(self):
        block = ps._summarise_candidate(
            {
                "site_id": "g-1",
                "site_url": "https://allgemein.de",
                "publishing_profile_payload": {"primary_context": "lifestyle"},
                "is_general": True,
            },
            index=2,
        )
        assert "general-purpose" in block.lower() or "Allgemein" in block

    def test_tolerates_empty_profile(self):
        block = ps._summarise_candidate(
            {"site_id": "x", "site_url": "https://x.de"},
            index=0,
        )
        # No crash, no profile bullets, just id/url
        assert "site_id: x" in block
        assert "primary context" not in block


# ---- _parse_ranking + _resolve_best_pick ---------------------------------


class TestParseRanking:
    def test_orders_and_filters_by_known_ids(self):
        candidates = [
            {"site_id": "a", "site_url": "https://a.de"},
            {"site_id": "b", "site_url": "https://b.de"},
        ]
        ranking = ps._parse_ranking(
            [
                {"site_id": "b", "fit_score": 0.9, "rationale": "good"},
                {"site_id": "a", "fit_score": 0.4, "rationale": "weak"},
                {"site_id": "unknown", "fit_score": 0.5, "rationale": "ghost"},
            ],
            candidates,
        )
        assert [r.site_id for r in ranking] == ["b", "a", "unknown"]
        # Unknown ids still parse but get empty site_url -- caller falls back.
        assert ranking[0].site_url == "https://b.de"
        assert ranking[2].site_url == ""

    def test_clamps_fit_scores(self):
        ranking = ps._parse_ranking(
            [
                {"site_id": "a", "fit_score": 1.5, "rationale": ""},
                {"site_id": "b", "fit_score": -0.2, "rationale": ""},
            ],
            [{"site_id": "a", "site_url": "https://a"}, {"site_id": "b", "site_url": "https://b"}],
        )
        assert ranking[0].fit_score == 1.0
        assert ranking[1].fit_score == 0.0


class TestResolveBestPick:
    def test_no_fit_short_circuits(self):
        site_id, _, topic, conf, rationale, no_fit = ps._resolve_best_pick(
            payload={"no_fit": True, "best_pick": {"rationale": "no overlap"}},
            ranking=[],
            candidates=[],
            fallback_topic="kw",
        )
        assert no_fit is True
        assert site_id == ""
        assert topic == "kw"
        assert "no overlap" in rationale

    def test_uses_llm_best_pick_when_in_ranking(self):
        ranking = [
            ps.CandidateRanking(site_id="b", site_url="https://b.de", fit_score=0.9, rationale="r-b"),
            ps.CandidateRanking(site_id="a", site_url="https://a.de", fit_score=0.5, rationale="r-a"),
        ]
        site_id, site_url, topic, conf, rationale, no_fit = ps._resolve_best_pick(
            payload={
                "best_pick": {
                    "site_id": "b",
                    "refined_topic": "kinderbrillen",
                    "confidence": 0.85,
                    "rationale": "kids glasses on family magazine",
                },
            },
            ranking=ranking,
            candidates=[
                {"site_id": "a", "site_url": "https://a.de"},
                {"site_id": "b", "site_url": "https://b.de"},
            ],
            fallback_topic="brillen",
        )
        assert no_fit is False
        assert site_id == "b"
        assert site_url == "https://b.de"
        assert topic == "kinderbrillen"
        assert conf == 0.85
        assert "kids glasses" in rationale

    def test_falls_back_to_top_of_ranking_when_pick_unknown(self):
        ranking = [
            ps.CandidateRanking(site_id="b", site_url="https://b.de", fit_score=0.9, rationale="r-b"),
            ps.CandidateRanking(site_id="a", site_url="https://a.de", fit_score=0.5, rationale="r-a"),
        ]
        site_id, _, topic, conf, _, no_fit = ps._resolve_best_pick(
            payload={"best_pick": {"site_id": "ghost", "refined_topic": "x"}},
            ranking=ranking,
            candidates=[],
            fallback_topic="kw",
        )
        assert no_fit is False
        assert site_id == "b"  # top of ranking by fit_score
        assert conf == 0.9


# ---- select_best_publisher (top-level) -----------------------------------


class TestSelectBestPublisher:
    def test_raises_on_empty_keyword(self):
        with pytest.raises(PublisherSelectorError) as exc:
            select_best_publisher(
                target_url="https://x.de",
                target_keyword="",
                candidates=[{"site_id": "a", "site_url": "https://a.de"}],
            )
        assert exc.value.code == "missing_keyword"

    def test_raises_on_no_candidates(self):
        with pytest.raises(PublisherSelectorError) as exc:
            select_best_publisher(
                target_url="https://x.de",
                target_keyword="kw",
                candidates=[],
            )
        assert exc.value.code == "missing_candidates"

    def test_soft_passes_when_no_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            result = select_best_publisher(
                target_url="https://x.de",
                target_keyword="kw",
                candidates=[
                    {"site_id": "a", "site_url": "https://a.de", "publishing_profile_payload": {"primary_context": "x"}},
                    {"site_id": "b", "site_url": "https://b.de", "publishing_profile_payload": {"primary_context": "y"}},
                ],
                api_key=None,
            )
        assert result.soft_passed is True
        assert result.best_site_id == "a"  # first candidate wins on soft-pass
        assert result.confidence == 0.5

    def test_picks_winner_from_llm_response(self):
        with patch.object(ps, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "ranking": [
                    {"site_id": "kids", "fit_score": 0.92, "rationale": "family audience"},
                    {"site_id": "solar", "fit_score": 0.15, "rationale": "no overlap"},
                ],
                "best_pick": {
                    "site_id": "kids",
                    "refined_topic": "kinderbrillen",
                    "confidence": 0.9,
                    "rationale": "kids' eyewear fits family magazine",
                },
                "no_fit": False,
            }
            result = select_best_publisher(
                target_url="https://brillenhaus24.de",
                target_keyword="brillen",
                candidates=[
                    {"site_id": "solar", "site_url": "https://solar.de", "publishing_profile_payload": {"primary_context": "solar"}},
                    {"site_id": "kids", "site_url": "https://kids.de", "publishing_profile_payload": {"primary_context": "family"}},
                ],
                api_key="test",
            )
        assert result.no_fit is False
        assert result.soft_passed is False
        assert result.best_site_id == "kids"
        assert result.best_site_url == "https://kids.de"
        assert result.refined_topic == "kinderbrillen"
        assert result.confidence == 0.9
        assert [r.site_id for r in result.ranking] == ["kids", "solar"]

    def test_returns_no_fit_verdict_without_raising(self):
        with patch.object(ps, "call_llm_json") as mock_llm:
            mock_llm.return_value = {
                "ranking": [
                    {"site_id": "solar", "fit_score": 0.1, "rationale": "no overlap"},
                ],
                "best_pick": {"site_id": "", "refined_topic": "", "confidence": 0.1, "rationale": "nothing fits"},
                "no_fit": True,
            }
            result = select_best_publisher(
                target_url="https://brillenhaus24.de",
                target_keyword="brillen",
                candidates=[
                    {"site_id": "solar", "site_url": "https://solar.de", "publishing_profile_payload": {"primary_context": "solar"}},
                ],
                api_key="test",
            )
        assert result.no_fit is True
        assert result.best_site_id == ""
        assert result.refined_topic == "brillen"  # falls back to original keyword
        assert "nothing fits" in result.rationale

    def test_soft_passes_on_llm_failure(self):
        from creator.api.llm import LLMError

        with patch.object(ps, "call_llm_json", side_effect=LLMError("API down")):
            result = select_best_publisher(
                target_url="https://x.de",
                target_keyword="kw",
                candidates=[
                    {"site_id": "a", "site_url": "https://a.de", "publishing_profile_payload": {"primary_context": "x"}},
                ],
                api_key="test",
            )
        assert result.soft_passed is True
        assert "llm_unavailable" in result.rationale
        assert result.best_site_id == "a"

    def test_french_language_routes_to_french_prompt(self):
        captured: dict = {}

        def fake_llm(**kwargs):
            captured.update(kwargs)
            return {
                "ranking": [{"site_id": "a", "fit_score": 0.8, "rationale": "ok"}],
                "best_pick": {"site_id": "a", "refined_topic": "x", "confidence": 0.8, "rationale": "ok"},
                "no_fit": False,
            }

        with patch.object(ps, "call_llm_json", side_effect=fake_llm):
            select_best_publisher(
                target_url="https://x.fr",
                target_keyword="kw",
                candidates=[{"site_id": "a", "site_url": "https://a.fr", "publishing_profile_payload": {"primary_context": "x"}}],
                language="fr",
                api_key="test",
            )
        assert "strategiste de contenu francophone" in captured["system_prompt"]

    def test_caps_candidates_at_max(self):
        captured: dict = {}

        def fake_llm(**kwargs):
            captured.update(kwargs)
            return {
                "ranking": [{"site_id": "c0", "fit_score": 0.7, "rationale": "ok"}],
                "best_pick": {"site_id": "c0", "refined_topic": "x", "confidence": 0.7, "rationale": "ok"},
                "no_fit": False,
            }

        candidates = [
            {"site_id": f"c{i}", "site_url": f"https://c{i}.de", "publishing_profile_payload": {"primary_context": "x"}}
            for i in range(ps.MAX_CANDIDATES_IN_PROMPT + 5)
        ]
        with patch.object(ps, "call_llm_json", side_effect=fake_llm):
            select_best_publisher(
                target_url="https://x.de",
                target_keyword="kw",
                candidates=candidates,
                api_key="test",
            )
        # The prompt should mention the cap, not the full list size.
        assert f"({ps.MAX_CANDIDATES_IN_PROMPT})" in captured["user_prompt"]
        # And not the over-cap count.
        assert f"({len(candidates)})" not in captured["user_prompt"]
