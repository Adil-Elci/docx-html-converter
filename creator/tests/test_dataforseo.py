from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from creator.api import dataforseo
from creator.api.dataforseo import (
    DataForSEOClient,
    DataForSEOConfig,
    DataForSEOError,
    DataForSEOInsufficientFunds,
)


def _config() -> DataForSEOConfig:
    return DataForSEOConfig(login="test", password="secret", retries=0)


def _mock_response(status_code: int = 200, json_body: dict = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body or {}
    response.raise_for_status = MagicMock()
    return response


def _serp_payload() -> dict:
    return {
        "version": "0.1.20240101",
        "status_code": 20000,
        "status_message": "Ok.",
        "cost": 0.0006,
        "tasks": [
            {
                "id": "task-1",
                "status_code": 20000,
                "status_message": "Ok.",
                "cost": 0.0006,
                "result": [
                    {
                        "items": [
                            {
                                "type": "organic",
                                "rank_group": 1,
                                "rank_absolute": 1,
                                "title": "Steuerberater Hamburg | Beispiel",
                                "url": "https://example.de/steuerberater",
                                "description": "Wir helfen Hamburger Unternehmern bei Steuern.",
                                "domain": "example.de",
                            },
                            {
                                "type": "organic",
                                "rank_group": 2,
                                "rank_absolute": 2,
                                "title": "Steuerberatung in Hamburg",
                                "url": "https://kanzlei.de/steuerberatung",
                                "description": "Persönliche Beratung in Hamburg.",
                                "domain": "kanzlei.de",
                            },
                            {
                                "type": "people_also_ask",
                                "items": [
                                    {"title": "Was kostet ein Steuerberater in Hamburg?"},
                                    {"title": "Welche Steuerberater sind die besten?"},
                                ],
                            },
                            {
                                "type": "related_searches",
                                "items": [
                                    "steuerberater hamburg altona",
                                    "günstiger steuerberater hamburg",
                                ],
                            },
                        ]
                    }
                ],
            }
        ],
    }


def _keyword_volume_payload() -> dict:
    return {
        "status_code": 20000,
        "cost": 0.00005,
        "tasks": [
            {
                "status_code": 20000,
                "result": [
                    {"keyword": "steuerberater hamburg", "search_volume": 9900, "competition": 0.45, "cpc": 4.32},
                    {"keyword": "steuerberatung hamburg", "search_volume": 2400, "competition": 0.32, "cpc": 3.18},
                ],
            }
        ],
    }


def _related_keywords_payload() -> dict:
    return {
        "status_code": 20000,
        "cost": 0.01,
        "tasks": [
            {
                "status_code": 20000,
                "result": [
                    {
                        "items": [
                            {
                                "keyword_data": {
                                    "keyword": "steuerberater hamburg altona",
                                    "keyword_info": {"search_volume": 480, "competition": 0.31, "cpc": 3.02},
                                }
                            },
                            {
                                "keyword_data": {
                                    "keyword": "steuerberater hamburg eppendorf",
                                    "keyword_info": {"search_volume": 210, "competition": 0.27, "cpc": 2.41},
                                }
                            },
                        ]
                    }
                ],
            }
        ],
    }


def test_load_config_from_env_requires_credentials(monkeypatch):
    monkeypatch.delenv("DATAFORSEO_LOGIN", raising=False)
    monkeypatch.delenv("DATAFORSEO_PASSWORD", raising=False)
    with pytest.raises(DataForSEOError, match="DATAFORSEO_LOGIN"):
        dataforseo.load_config_from_env()


def test_load_config_from_env_reads_credentials(monkeypatch):
    monkeypatch.setenv("DATAFORSEO_LOGIN", "user@example.de")
    monkeypatch.setenv("DATAFORSEO_PASSWORD", "secret")
    config = dataforseo.load_config_from_env()
    assert config.login == "user@example.de"
    assert config.password == "secret"
    assert config.base_url == dataforseo.DEFAULT_BASE_URL


def test_serp_organic_parses_organic_paa_and_related():
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=_mock_response(200, _serp_payload())):
        result = client.serp_organic("steuerberater hamburg")
    assert result.keyword == "steuerberater hamburg"
    assert len(result.organic) == 2
    assert result.organic[0].url == "https://example.de/steuerberater"
    assert result.organic[0].rank == 1
    assert result.organic[0].domain == "example.de"
    assert result.people_also_ask == [
        "Was kostet ein Steuerberater in Hamburg?",
        "Welche Steuerberater sind die besten?",
    ]
    assert "steuerberater hamburg altona" in result.related_searches
    assert result.cost == pytest.approx(0.0006)


def test_serp_organic_pins_germany_and_german_by_default():
    client = DataForSEOClient(config=_config())
    captured = {}

    def fake_post(url, json, auth, timeout, headers):
        captured["url"] = url
        captured["json"] = json
        captured["auth"] = auth
        return _mock_response(200, _serp_payload())

    with patch("creator.api.dataforseo.requests.post", side_effect=fake_post):
        client.serp_organic("test keyword")

    assert captured["url"].endswith("/v3/serp/google/organic/live/advanced")
    assert captured["json"][0]["location_code"] == 2276
    assert captured["json"][0]["language_code"] == "de"
    assert captured["auth"] == ("test", "secret")


def test_keyword_volume_parses_metrics():
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=_mock_response(200, _keyword_volume_payload())):
        metrics = client.keyword_volume(["steuerberater hamburg", "steuerberatung hamburg"])
    assert len(metrics) == 2
    assert metrics[0].keyword == "steuerberater hamburg"
    assert metrics[0].search_volume == 9900
    assert metrics[0].cpc == pytest.approx(4.32)


def test_keyword_volume_returns_empty_for_empty_input():
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post") as mocked:
        result = client.keyword_volume([])
    assert result == []
    mocked.assert_not_called()


def test_related_keywords_parses_nested_keyword_data():
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=_mock_response(200, _related_keywords_payload())):
        result = client.related_keywords("steuerberater hamburg")
    assert result.seed_keyword == "steuerberater hamburg"
    assert len(result.items) == 2
    assert result.items[0].keyword == "steuerberater hamburg altona"
    assert result.items[0].search_volume == 480
    assert result.cost == pytest.approx(0.01)


def test_insufficient_funds_raises_specific_exception():
    payload = {"status_code": 40202, "status_message": "Money limit reached.", "tasks": []}
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=_mock_response(200, payload)):
        with pytest.raises(DataForSEOInsufficientFunds, match="insufficient funds"):
            client.serp_organic("steuerberater hamburg")


def test_task_level_failure_raises_data_for_seo_error():
    payload = {
        "status_code": 20000,
        "tasks": [
            {"status_code": 40400, "status_message": "Not Found.", "result": []}
        ],
    }
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=_mock_response(200, payload)):
        with pytest.raises(DataForSEOError, match="task failed"):
            client.serp_organic("nonsense keyword")


def test_401_raises_descriptive_auth_error():
    response = MagicMock()
    response.status_code = 401
    response.json.return_value = {}
    client = DataForSEOClient(config=_config())
    with patch("creator.api.dataforseo.requests.post", return_value=response):
        with pytest.raises(DataForSEOError, match="authentication failed"):
            client.serp_organic("steuerberater hamburg")
