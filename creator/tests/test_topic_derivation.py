from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from creator.api import topic_derivation as td
from creator.api.dataforseo import KeywordMetric, MonthlySearchVolume
from creator.api.topic_derivation import (
    CandidateScore,
    DerivedTopic,
    TopicDerivationError,
    derive_topic,
)


# ---- HTML fixtures --------------------------------------------------------


def _html(
    *,
    lang: str = "de",
    title: str = "Steuerberater Hamburg | Beispiel",
    h1: str = "Steuerberater in Hamburg finden",
    body: str = "Wir sind Steuerberater in Hamburg und beraten mittelständische Unternehmer.",
    extras: str = "",
) -> str:
    return f"""<!doctype html>
<html lang="{lang}">
<head>
  <title>{title}</title>
  <meta property="og:title" content="{title}">
</head>
<body>
  <h1>{h1}</h1>
  <p>{body}</p>
  {extras}
</body>
</html>"""


def _http_response(*, status_code: int = 200, body: str = "", content_type: str = "text/html; charset=utf-8") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    response.headers = {"Content-Type": content_type}
    return response


# ---- Pure helpers ---------------------------------------------------------


class TestNormalizeKeyword:
    def test_strips_brand_suffix(self):
        assert td._normalize_keyword("Steuerberater Hamburg | Beispiel GmbH") == "steuerberater hamburg"

    def test_strips_dash_suffix(self):
        assert td._normalize_keyword("Tax Advisor - Acme") == "tax advisor"

    def test_keeps_german_umlauts(self):
        assert td._normalize_keyword("Müller Söhne GmbH") == "müller söhne gmbh"

    def test_keeps_french_diacritics(self):
        assert td._normalize_keyword("L'expert-comptable à Paris") == "l'expert-comptable à paris"

    def test_collapses_whitespace(self):
        assert td._normalize_keyword("  foo   bar  ") == "foo bar"


class TestSlugToKeyword:
    def test_basic_slug(self):
        assert td._slug_to_keyword("https://client.de/leistungen/steuerberatung") == "steuerberatung"

    def test_html_extension_stripped(self):
        assert td._slug_to_keyword("https://client.de/services.html") == "services"

    def test_empty_path(self):
        assert td._slug_to_keyword("https://client.de/") == ""


class TestDetectLanguage:
    def test_html_lang_attribute_de(self):
        assert td._detect_language(_html(lang="de"), "https://client.fr/x") == "de"

    def test_html_lang_attribute_fr(self):
        assert td._detect_language(_html(lang="fr"), "https://client.de/x") == "fr"

    def test_strips_region_suffix(self):
        assert td._detect_language(_html(lang="de-AT"), "https://example.com") == "de"

    def test_falls_back_to_tld(self):
        no_lang = "<!doctype html><html><head></head><body><p>Hi</p></body></html>"
        assert td._detect_language(no_lang, "https://client.de/x") == "de"
        assert td._detect_language(no_lang, "https://client.fr/x") == "fr"

    def test_returns_none_when_undetectable(self):
        no_lang = "<!doctype html><html><head></head><body><p>Hi</p></body></html>"
        assert td._detect_language(no_lang, "https://client.com/x") is None


class TestExtractDeterministicCandidates:
    def test_pulls_title_h1_h2_og_slug(self):
        html = _html(
            title="Steuerberater Hamburg | Beispiel",
            h1="Steuerberater in Hamburg finden",
            extras="<h2>Unsere Leistungen</h2>",
        )
        candidates = td._extract_deterministic_candidates(html, "https://client.de/leistungen/steuerberater")
        kws = [kw for kw, _ in candidates]
        assert "steuerberater hamburg" in kws
        assert "steuerberater in hamburg finden" in kws
        assert "unsere leistungen" in kws
        # Slug last
        assert any("steuerberater" in kw for kw in kws)

    def test_dedupes_across_sources(self):
        html = _html(title="Foo Bar", h1="Foo Bar")
        candidates = td._extract_deterministic_candidates(html, "https://x.de/foo-bar")
        assert len([kw for kw, _ in candidates if kw == "foo bar"]) == 1

    def test_ignores_too_short(self):
        html = _html(title="A", h1="Hi")
        candidates = td._extract_deterministic_candidates(html, "https://x.de/")
        assert candidates == []


class TestTrendRatio:
    def test_flat_when_history_short(self):
        history = [MonthlySearchVolume(year=2026, month=m, search_volume=100) for m in range(1, 5)]
        assert td._trend_ratio(history) == 1.0

    def test_rising_trend(self):
        # Prior 3 months: 100 each. Recent 3 months: 200 each. Ratio = 2.0.
        history = (
            [MonthlySearchVolume(year=2025, month=m, search_volume=100) for m in (10, 11, 12)]
            + [MonthlySearchVolume(year=2026, month=m, search_volume=200) for m in (1, 2, 3)]
        )
        assert td._trend_ratio(history) == 2.0

    def test_declining_trend(self):
        history = (
            [MonthlySearchVolume(year=2025, month=m, search_volume=200) for m in (10, 11, 12)]
            + [MonthlySearchVolume(year=2026, month=m, search_volume=100) for m in (1, 2, 3)]
        )
        assert td._trend_ratio(history) == 0.5

    def test_zero_prior_returns_flat(self):
        history = (
            [MonthlySearchVolume(year=2025, month=m, search_volume=0) for m in (10, 11, 12)]
            + [MonthlySearchVolume(year=2026, month=m, search_volume=500) for m in (1, 2, 3)]
        )
        assert td._trend_ratio(history) == 1.0


class TestScoreCandidate:
    def test_higher_volume_wins_when_trend_equal(self):
        a = td._score_candidate(search_volume=1000, trend_ratio=1.0)
        b = td._score_candidate(search_volume=10000, trend_ratio=1.0)
        assert b > a

    def test_rising_trend_boosts_score(self):
        flat = td._score_candidate(search_volume=1000, trend_ratio=1.0)
        rising = td._score_candidate(search_volume=1000, trend_ratio=2.0)
        assert rising > flat

    def test_declining_trend_penalises(self):
        flat = td._score_candidate(search_volume=1000, trend_ratio=1.0)
        declining = td._score_candidate(search_volume=1000, trend_ratio=0.5)
        assert declining < flat

    def test_volume_dominates_extreme_trend(self):
        # 100x volume gap should still win even against a 2x trend on the smaller term.
        big_flat = td._score_candidate(search_volume=100000, trend_ratio=1.0)
        small_rising = td._score_candidate(search_volume=1000, trend_ratio=2.0)
        assert big_flat > small_rising


# ---- Orchestrator end-to-end ---------------------------------------------


def _ranked_metrics(values: dict[str, int]) -> list[KeywordMetric]:
    return [
        KeywordMetric(keyword=kw, search_volume=vol, cost=0.001)
        for kw, vol in values.items()
    ]


class TestDeriveTopic:
    def test_happy_path_german_url(self):
        html = _html(lang="de", title="Steuerberater Hamburg | Kanzlei")
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({
            "steuerberater hamburg": 5400,
            "steuerberater in hamburg finden": 90,
        })
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            result = derive_topic(
                "https://client.de/steuerberater-hamburg",
                use_cache=False,
                dataforseo_client=client,
            )
        assert isinstance(result, DerivedTopic)
        assert result.target_keyword == "steuerberater hamburg"
        assert result.language_code == "de"
        assert result.location_code == 2276
        assert result.cache_hit is False
        assert "steuerberater hamburg" not in result.alternates  # primary not duplicated

    def test_french_url_routes_to_paris(self):
        html = _html(lang="fr", title="Expert-comptable Paris | Cabinet", h1="Expert-comptable à Paris")
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({
            "expert-comptable paris": 4400,
            "expert-comptable à paris": 1200,
        })
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            result = derive_topic(
                "https://cabinet.fr/expert-comptable-paris",
                use_cache=False,
                dataforseo_client=client,
            )
        assert result.language_code == "fr"
        assert result.location_code == 2250
        assert result.target_keyword in {"expert-comptable paris", "expert-comptable à paris"}

    def test_rejects_english_target(self):
        html = _html(lang="en", title="Tax Advisor London", h1="Tax Advisor")
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            with pytest.raises(TopicDerivationError) as exc:
                derive_topic(
                    "https://client.co.uk/tax",
                    use_cache=False,
                    dataforseo_client=MagicMock(),
                )
        assert exc.value.code == "language_not_allowed"

    def test_rejects_unreachable_url(self):
        with patch.object(td.requests, "get", side_effect=td.requests.RequestException("connection refused")):
            with pytest.raises(TopicDerivationError) as exc:
                derive_topic(
                    "https://does-not-exist.de/x",
                    use_cache=False,
                    dataforseo_client=MagicMock(),
                )
        assert exc.value.code == "fetch_failed"

    def test_rejects_non_html_response(self):
        with patch.object(
            td.requests,
            "get",
            return_value=_http_response(body="{}", content_type="application/json"),
        ):
            with pytest.raises(TopicDerivationError) as exc:
                derive_topic(
                    "https://api.client.de/x",
                    use_cache=False,
                    dataforseo_client=MagicMock(),
                )
        assert exc.value.code == "non_html_response"

    def test_rejects_http_error(self):
        with patch.object(
            td.requests,
            "get",
            return_value=_http_response(status_code=404, body="Not Found"),
        ):
            with pytest.raises(TopicDerivationError) as exc:
                derive_topic(
                    "https://client.de/missing",
                    use_cache=False,
                    fetch_retries=1,
                    dataforseo_client=MagicMock(),
                )
        assert exc.value.code == "fetch_failed"

    def test_zero_volume_primary_still_returns_with_note(self):
        html = _html(lang="de", title="Sehr Spezielles Nischenthema XYZ")
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({
            "sehr spezielles nischenthema xyz": 0,
        })
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            result = derive_topic(
                "https://client.de/nische",
                use_cache=False,
                dataforseo_client=client,
            )
        assert result.target_keyword == "sehr spezielles nischenthema xyz"
        assert "zero_volume_primary" in result.notes

    def test_dataforseo_failure_falls_back_to_deterministic_order(self):
        from creator.api.dataforseo import DataForSEOError

        html = _html(lang="de")
        client = MagicMock()
        client.keyword_volume.side_effect = DataForSEOError("boom")
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            result = derive_topic(
                "https://client.de/steuerberater",
                use_cache=False,
                dataforseo_client=client,
            )
        # Falls back to first deterministic candidate when ranking is unavailable.
        assert result.target_keyword
        assert "dataforseo_unavailable" in result.notes

    def test_haiku_fallback_invoked_when_deterministic_thin(self):
        # Page with no useful title/h1/og -> needs Haiku.
        html = '<html lang="de"><head></head><body><p>Some prose without real headings.</p></body></html>'
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({"haiku-derived keyword": 1000})

        def fake_haiku(*, page_text, language_code, api_key, model=None, base_url=None, timeout_seconds=None):
            return ["haiku-derived keyword", "alt one"], 0.001

        with patch.object(td.requests, "get", return_value=_http_response(body=html)), \
             patch.object(td, "_haiku_extract_keyword", side_effect=fake_haiku), \
             patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            result = derive_topic(
                "https://client.de/page",
                use_cache=False,
                dataforseo_client=client,
            )
        assert result.target_keyword == "haiku-derived keyword"
        assert "used_haiku_fallback" in result.notes

    def test_haiku_skipped_without_api_key(self):
        html = '<html lang="de"><head></head><body><p>X</p></body></html>'
        client = MagicMock()
        client.keyword_volume.return_value = []
        with patch.object(td.requests, "get", return_value=_http_response(body=html)), \
             patch.dict("os.environ", {}, clear=True):
            with pytest.raises(TopicDerivationError) as exc:
                derive_topic(
                    "https://client.de/x",
                    use_cache=False,
                    dataforseo_client=client,
                )
        assert exc.value.code == "no_candidates"

    def test_language_override_bypasses_detection(self):
        html = _html(lang="en", title="Tax Advisor London")
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({"tax advisor london": 1000})
        with patch.object(td.requests, "get", return_value=_http_response(body=html)):
            result = derive_topic(
                "https://client.com/x",
                allowed_languages=("de",),
                language_override="de",
                use_cache=False,
                dataforseo_client=client,
            )
        assert result.language_code == "de"
        assert result.location_code == 2276


class TestCacheIntegration:
    def test_cache_hit_skips_fetch_and_dataforseo(self):
        cached_payload = {
            "target_url": "https://client.de/x",
            "target_keyword": "cached keyword",
            "language_code": "de",
            "location_code": 2276,
            "alternates": ["alt one"],
            "candidates": [
                {"keyword": "cached keyword", "source": "title", "search_volume": 800, "trend_ratio": 1.1, "score": 3.2}
            ],
            "confidence": 0.7,
            "notes": [],
            "cost_usd": 0.05,
        }
        client = MagicMock()
        with patch.object(td.requests, "get") as mock_get, \
             patch("creator.api.topic_derivation_cache.get_cached_derived_topic", return_value=cached_payload):
            result = derive_topic(
                "https://client.de/x",
                dataforseo_client=client,
            )
        assert result.cache_hit is True
        assert result.target_keyword == "cached keyword"
        assert result.cost_usd == 0.0  # cache hit -> no spend recorded
        mock_get.assert_not_called()
        client.keyword_volume.assert_not_called()

    def test_cache_miss_writes_back(self):
        html = _html(lang="de")
        client = MagicMock()
        client.keyword_volume.return_value = _ranked_metrics({"steuerberater hamburg": 5400})
        with patch.object(td.requests, "get", return_value=_http_response(body=html)), \
             patch("creator.api.topic_derivation_cache.get_cached_derived_topic", return_value=None), \
             patch("creator.api.topic_derivation_cache.upsert_derived_topic") as mock_upsert:
            derive_topic("https://client.de/x", dataforseo_client=client)
        assert mock_upsert.call_count == 1
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["lookup_key"]
        assert kwargs["locale"] == "de-2276"
        assert kwargs["payload"]["target_keyword"]


# ---- HTTP endpoint -------------------------------------------------------


class TestEndpoint:
    def test_endpoint_returns_200_on_success(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        with patch("creator.api.server.derive_topic") as mock_derive:
            mock_derive.return_value = DerivedTopic(
                target_url="https://client.de/x",
                target_keyword="steuerberater hamburg",
                language_code="de",
                location_code=2276,
                alternates=["steuerberater"],
                candidates=[CandidateScore(keyword="steuerberater hamburg", source="title", search_volume=5400, trend_ratio=1.1, score=4.2)],
                confidence=0.78,
                notes=[],
                cost_usd=0.001,
                cache_hit=False,
            )
            response = TestClient(app).post(
                "/v2/derive-topic",
                json={"target_url": "https://client.de/x"},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["ok"] is True
        assert body["target_keyword"] == "steuerberater hamburg"
        assert body["language_code"] == "de"
        assert body["location_code"] == 2276
        assert body["candidates"][0]["search_volume"] == 5400

    def test_endpoint_returns_422_on_language_rejection(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        with patch("creator.api.server.derive_topic") as mock_derive:
            mock_derive.side_effect = TopicDerivationError(
                "language_not_allowed",
                "Detected language 'en' is not in the allowed set ['de', 'fr'].",
            )
            response = TestClient(app).post(
                "/v2/derive-topic",
                json={"target_url": "https://client.com/x"},
            )
        assert response.status_code == 422
        body = response.json()
        assert body["ok"] is False
        assert body["error"] == "topic_derivation_failed"
        assert body["code"] == "language_not_allowed"
        assert "not in the allowed set" in body["message"]

    def test_endpoint_validation_error_for_missing_url(self):
        from fastapi.testclient import TestClient

        from creator.api.server import app

        response = TestClient(app).post("/v2/derive-topic", json={})
        assert response.status_code == 422
