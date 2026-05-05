from __future__ import annotations

from unittest.mock import patch

import pytest

from creator.api import topic_brainstorm as tb
from creator.api.topic_brainstorm import (
    BrainstormResult,
    EditorialAngle,
    TopicBrainstormError,
    brainstorm_editorial_angles,
)


# ---- _summarise_publisher_profile -----------------------------------------


class TestSummariseProfile:
    def test_picks_high_signal_fields(self):
        out = tb._summarise_publisher_profile({
            "primary_context": "kids and family",
            "topics": [{"label": "schule"}, "freizeit"],
            "language": "de",
        })
        assert "kids and family" in out
        assert "schule" in out and "freizeit" in out
        assert "language: de" in out

    def test_empty_profile(self):
        assert tb._summarise_publisher_profile({}).startswith("(")
        assert tb._summarise_publisher_profile(None).startswith("(")


# ---- brainstorm_editorial_angles -----------------------------------------


def _angles_payload() -> dict:
    return {
        "angles": [
            {
                "title": "Kurzsichtigkeit bei Kindern: Warum immer mehr Grundschüler eine Brille brauchen",
                "target_keyword": "kurzsichtigkeit kinder",
                "hook": "Studien zeigen: jedes zweite Kind in Asien hat Myopie; auch in Deutschland steigt die Rate.",
                "rationale": "Trend-Story passt perfekt zur Eltern-Audience eines Kindermagazins.",
            },
            {
                "title": "Bildschirmzeit & Augengesundheit",
                "target_keyword": "bildschirmzeit kinder augen",
                "hook": "Smartphones, Tablets, Schul-Laptops: was tun, wenn die Augen müde werden?",
                "rationale": "Aktuelles Thema, hoher Bezug zur Audience.",
            },
        ],
    }


class TestBrainstorm:
    def test_returns_empty_without_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                api_key=None,
            )
        assert result.angles == []
        assert result.cost_usd == 0.0

    def test_returns_empty_on_llm_failure(self):
        from creator.api.llm import LLMError

        with patch.object(tb, "call_llm_json", side_effect=LLMError("boom")):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                api_key="test",
            )
        assert result.angles == []

    def test_parses_llm_angles(self):
        with patch.object(tb, "call_llm_json", return_value=_angles_payload()):
            result = brainstorm_editorial_angles(
                target_url="https://www.brillenhaus24.de",
                target_keyword="brillen online kaufen",
                publishing_profile_payload={"primary_context": "kids and family"},
                language="de",
                current_year=2026,
                api_key="test",
            )
        assert len(result.angles) == 2
        assert result.angles[0].title.startswith("Kurzsichtigkeit")
        assert result.angles[0].target_keyword == "kurzsichtigkeit kinder"
        assert result.cost_usd == 0.02

    def test_caps_at_num_angles(self):
        big_payload = {
            "angles": [
                {"title": f"Title {i}", "target_keyword": f"kw{i}", "hook": "h", "rationale": "r"}
                for i in range(10)
            ]
        }
        with patch.object(tb, "call_llm_json", return_value=big_payload):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                api_key="test",
                num_angles=3,
            )
        assert len(result.angles) == 3

    def test_drops_malformed_angles(self):
        bad_payload = {
            "angles": [
                {"title": "Good", "target_keyword": "good kw", "hook": "h", "rationale": "r"},
                {"title": "", "target_keyword": "missing title"},  # dropped
                "not a dict",                                        # dropped
                {"target_keyword": "missing title also"},            # dropped
            ]
        }
        with patch.object(tb, "call_llm_json", return_value=bad_payload):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                api_key="test",
            )
        assert len(result.angles) == 1
        assert result.angles[0].title == "Good"

    def test_french_routes_to_french_prompt(self):
        captured: dict = {}

        def fake(**kwargs):
            captured.update(kwargs)
            return _angles_payload()

        with patch.object(tb, "call_llm_json", side_effect=fake):
            brainstorm_editorial_angles(
                target_url="https://cabinet.fr",
                target_keyword="expert-comptable paris",
                publishing_profile_payload={"primary_context": "business news"},
                language="fr",
                api_key="test",
            )
        assert "rédacteur en chef" in captured["system_prompt"].lower() \
            or "rédacteur" in captured["system_prompt"].lower()

    def test_hard_fails_on_missing_inputs(self):
        with pytest.raises(TopicBrainstormError):
            brainstorm_editorial_angles(target_url="", target_keyword="kw")
        with pytest.raises(TopicBrainstormError):
            brainstorm_editorial_angles(target_url="https://x.de", target_keyword="")


# ---- exclude_topics post-filter ------------------------------------------


class TestExcludeTopicsFilter:
    def test_filter_drops_substring_match_in_keyword(self):
        angles = [
            tb.EditorialAngle(title="A", target_keyword="kinderbrillen online", hook="", rationale=""),
            tb.EditorialAngle(title="B", target_keyword="bildschirmzeit kinder", hook="", rationale=""),
        ]
        kept, dropped = tb._filter_against_excludes(angles, ["kinderbrillen"])
        assert dropped == 1
        assert len(kept) == 1
        assert kept[0].target_keyword == "bildschirmzeit kinder"

    def test_filter_drops_substring_match_in_title(self):
        angles = [
            tb.EditorialAngle(title="Erste Kinderbrille kaufen", target_keyword="brille", hook="", rationale=""),
            tb.EditorialAngle(title="Bildschirmzeit", target_keyword="zeit", hook="", rationale=""),
        ]
        kept, _ = tb._filter_against_excludes(angles, ["erste kinderbrille"])
        assert len(kept) == 1
        assert kept[0].title == "Bildschirmzeit"

    def test_filter_passes_when_no_excludes(self):
        angles = [tb.EditorialAngle(title="A", target_keyword="a kw", hook="", rationale="")]
        kept, dropped = tb._filter_against_excludes(angles, None)
        assert dropped == 0
        assert kept == angles

    def test_brainstorm_post_filters_haiku_output(self):
        with patch.object(tb, "call_llm_json", return_value=_angles_payload()):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                api_key="test",
                exclude_topics=["bildschirmzeit"],  # drops the second angle
            )
        assert len(result.angles) == 1
        assert result.angles[0].title.startswith("Kurzsichtigkeit")
        assert result.excluded_count == 1


# ---- cache integration ---------------------------------------------------


class TestBrainstormCache:
    def test_cache_hit_skips_llm(self):
        cached = {
            "angles": [
                {"title": "Cached A", "target_keyword": "cached a kw", "hook": "h", "rationale": "r"},
                {"title": "Cached B", "target_keyword": "cached b kw", "hook": "h", "rationale": "r"},
            ]
        }
        with patch("creator.api.topic_brainstorm_cache.get_cached_brainstorm", return_value=cached), \
             patch.object(tb, "call_llm_json") as mock_llm:
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                publisher_url="https://kidsblatt.de",
                api_key="test",
            )
        assert result.cache_hit is True
        assert result.cost_usd == 0.0
        assert len(result.angles) == 2
        mock_llm.assert_not_called()

    def test_cache_miss_writes_back(self):
        with patch("creator.api.topic_brainstorm_cache.get_cached_brainstorm", return_value=None), \
             patch("creator.api.topic_brainstorm_cache.upsert_brainstorm") as mock_upsert, \
             patch.object(tb, "call_llm_json", return_value=_angles_payload()):
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                publisher_url="https://kidsblatt.de",
                api_key="test",
            )
        assert result.cache_hit is False
        assert mock_upsert.call_count == 1
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["lookup_key"].startswith("brainstorm|")
        assert kwargs["locale"].startswith("de-")
        assert len(kwargs["payload"]["angles"]) == 2

    def test_cache_exhausted_when_all_excluded_triggers_refresh(self):
        cached = {
            "angles": [
                {"title": "Old A", "target_keyword": "old keyword a", "hook": "h", "rationale": "r"},
                {"title": "Old B", "target_keyword": "old keyword b", "hook": "h", "rationale": "r"},
            ]
        }
        with patch("creator.api.topic_brainstorm_cache.get_cached_brainstorm", return_value=cached), \
             patch("creator.api.topic_brainstorm_cache.upsert_brainstorm") as mock_upsert, \
             patch.object(tb, "call_llm_json", return_value=_angles_payload()) as mock_llm:
            result = brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                publisher_url="https://kidsblatt.de",
                api_key="test",
                exclude_topics=["old keyword a", "old keyword b"],
            )
        # Cache was exhausted, so we re-brainstormed.
        assert result.cache_hit is False
        mock_llm.assert_called_once()
        # And we wrote the FRESH angles back to cache.
        assert mock_upsert.call_count == 1

    def test_no_publisher_url_skips_cache_entirely(self):
        with patch("creator.api.topic_brainstorm_cache.get_cached_brainstorm") as mock_get, \
             patch("creator.api.topic_brainstorm_cache.upsert_brainstorm") as mock_upsert, \
             patch.object(tb, "call_llm_json", return_value=_angles_payload()):
            brainstorm_editorial_angles(
                target_url="https://x.de",
                target_keyword="kw",
                publisher_url=None,
                api_key="test",
            )
        mock_get.assert_not_called()
        mock_upsert.assert_not_called()


# ---- HTTP endpoint --------------------------------------------------------


class TestEndpoint:
    def test_endpoint_returns_200_with_angles(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        with patch("creator.api.server.brainstorm_editorial_angles") as mock_fn:
            mock_fn.return_value = BrainstormResult(
                angles=[
                    EditorialAngle(
                        title="Kurzsichtigkeit bei Kindern",
                        target_keyword="kurzsichtigkeit kinder",
                        hook="Trend",
                        rationale="ok",
                    ),
                ],
                cost_usd=0.02,
            )
            response = TestClient(app).post(
                "/v2/brainstorm-topics",
                json={
                    "target_url": "https://www.brillenhaus24.de",
                    "target_keyword": "brillen online kaufen",
                    "publishing_profile_payload": {"primary_context": "kids"},
                    "language": "de",
                },
            )
        assert response.status_code == 200
        body = response.json()
        assert body["ok"] is True
        assert len(body["angles"]) == 1
        assert body["angles"][0]["target_keyword"] == "kurzsichtigkeit kinder"

    def test_endpoint_validates_required_fields(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        response = TestClient(app).post(
            "/v2/brainstorm-topics",
            json={"target_url": "https://x.de"},  # missing target_keyword
        )
        assert response.status_code == 422
