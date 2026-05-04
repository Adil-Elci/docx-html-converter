from __future__ import annotations

from unittest.mock import patch

import pytest

from creator.api import entity_extract
from creator.api.entity_extract import (
    EntityExtractionResult,
    ExtractedEntity,
    extract_entities_from_competitors,
)
from creator.api.llm import LLMError
from creator.api.serp_scrape import ScrapedCompetitor


def _competitor(body: str, *, status: str = "ok") -> ScrapedCompetitor:
    return ScrapedCompetitor(url="https://x.de", fetch_status=status, body_text=body)


def test_extract_entities_drops_below_threshold_competitors():
    result = extract_entities_from_competitors([_competitor("text")], topic="test")
    assert result.entities == []
    assert result.competitor_count == 1


def test_extract_entities_drops_failed_fetch_competitors():
    result = extract_entities_from_competitors(
        [_competitor("foo bar"), _competitor("", status="forbidden")],
        topic="test",
    )
    assert result.entities == []
    assert result.competitor_count == 1


def test_extract_entities_parses_llm_response(monkeypatch):
    bodies = [
        "Die DATEV unterstützt Steuerberater. Die Steuerberatergebührenverordnung regelt Honorare.",
        "DATEV ist führend in Deutschland. Die Steuerberatergebührenverordnung muss beachtet werden.",
        "Steuerberater nutzen DATEV-Software. Honorare folgen der Steuerberatergebührenverordnung.",
    ]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [
            {"name": "DATEV", "type": "ORGANIZATION", "n_competitors": 3},
            {"name": "Steuerberatergebührenverordnung", "type": "LAW", "n_competitors": 3},
            {"name": "GenericTerm", "type": "CONCEPT", "n_competitors": 0},  # below threshold
        ]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="Steuerberater")
    names = {e.name for e in result.entities}
    assert "DATEV" in names
    assert "Steuerberatergebührenverordnung" in names
    assert "GenericTerm" not in names
    assert all(e.coverage == pytest.approx(1.0) for e in result.entities)


def test_extract_entities_clamps_n_competitors_to_actual_count(monkeypatch):
    bodies = ["Foo Bar mention", "Foo Bar mention again", "no mention here"]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [
            {"name": "Foo Bar", "type": "CONCEPT", "n_competitors": 99},  # over-reports
        ]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert len(result.entities) == 1
    # regex should find Foo Bar in 2 of 3 bodies → n_competitors = 2
    assert result.entities[0].n_competitors == 2
    assert result.entities[0].coverage == pytest.approx(2 / 3)


def test_extract_entities_drops_hallucinated_entity(monkeypatch):
    bodies = ["real text only", "real text only too", "more real text"]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [
            {"name": "Phantasiebegriff", "type": "CONCEPT", "n_competitors": 3},  # not in bodies
        ]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert result.entities == []  # regex verification dropped it


def test_extract_entities_dedupes_case_insensitive(monkeypatch):
    bodies = ["DATEV is here", "datev is also here", "Datev makes software"]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [
            {"name": "DATEV", "type": "ORGANIZATION", "n_competitors": 3},
            {"name": "datev", "type": "ORGANIZATION", "n_competitors": 3},
        ]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert len(result.entities) == 1


def test_extract_entities_coerces_unknown_type(monkeypatch):
    bodies = ["DATEV is here", "DATEV is here too"]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [{"name": "DATEV", "type": "WEIRD_TYPE", "n_competitors": 2}]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert len(result.entities) == 1
    assert result.entities[0].type == "CONCEPT"


def test_extract_entities_sorts_by_n_competitors(monkeypatch):
    bodies = [
        "Alpha Beta Gamma here",
        "Alpha Beta Gamma here only",
        "only Alpha here",
    ]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {
        "entities": [
            {"name": "Alpha", "type": "CONCEPT", "n_competitors": 3},
            {"name": "Beta", "type": "CONCEPT", "n_competitors": 2},
            {"name": "Gamma", "type": "CONCEPT", "n_competitors": 2},
        ]
    }
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert [e.name for e in result.entities[:3]] == ["Alpha", "Beta", "Gamma"]
    assert result.entities[0].n_competitors == 3
    assert result.entities[1].n_competitors == 2


def test_extract_entities_raises_when_api_key_missing(monkeypatch):
    bodies = ["a a a", "b b b"]
    competitors = [_competitor(b) for b in bodies]
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        extract_entities_from_competitors(competitors, topic="t")


def test_extract_entities_handles_malformed_llm_response(monkeypatch):
    bodies = ["foo", "bar"]
    competitors = [_competitor(b) for b in bodies]
    fake_response = {"unexpected_key": []}
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", return_value=fake_response):
        result = extract_entities_from_competitors(competitors, topic="t")
    assert result.entities == []
    assert result.raw_response == fake_response


def test_extract_entities_truncates_long_bodies(monkeypatch):
    long_body = "DATEV " + ("filler " * 5000)
    competitors = [_competitor(long_body), _competitor(long_body)]
    captured_prompt: dict = {}

    def fake_call(*, user_prompt, **kwargs):
        captured_prompt["body"] = user_prompt
        return {"entities": []}

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with patch("creator.api.entity_extract.call_llm_json", side_effect=fake_call):
        extract_entities_from_competitors(competitors, topic="t", max_body_chars_per_competitor=500)

    # Each competitor body in the prompt should be ~500 chars (truncated)
    assert captured_prompt["body"].count("[Wettbewerber") == 2
    # The total prompt length should be far less than 2 * 5000 chars (5000 ≈ unbounded)
    assert len(captured_prompt["body"]) < 4000
