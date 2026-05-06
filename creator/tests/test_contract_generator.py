from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from creator.api import contract_generator
from creator.api.contract import ContentContract, GermanTone, SearchIntent
from creator.api.contract_generator import (
    build_system_prompt,
    build_user_prompt,
    call_with_thinking,
    generate_contract,
)
from creator.api.dataforseo import KeywordMetric, OrganicResult, RelatedKeyword
from creator.api.entity_extract import ExtractedEntity
from creator.api.llm import LLMError
from creator.api.prompt_registry import Prompt
from creator.api.research import ResearchPayload


def _research() -> ResearchPayload:
    return ResearchPayload(
        target_keyword="steuerberater hamburg",
        location_code=2276,
        language_code="de",
        organic=[
            OrganicResult(rank=1, url="https://example.de/a", title="Steuerberater Hamburg A", description="...", domain="example.de"),
            OrganicResult(rank=2, url="https://kanzlei.de/b", title="Beste Steuerberater Hamburg", description="...", domain="kanzlei.de"),
        ],
        paa_questions=[
            "Was kostet ein Steuerberater in Hamburg?",
            "Welche Steuerberater sind die besten?",
        ],
        related_searches=["steuerberater hamburg altona", "steuerberater hamburg günstig"],
        primary_volume=KeywordMetric(keyword="steuerberater hamburg", search_volume=9900, competition=0.45, cpc=4.32),
        related_keywords=[RelatedKeyword(keyword="steuerberater hamburg altona", search_volume=480)],
        competitor_word_count_median=950,
        common_h2_themes=["Auswahlkriterien", "Kosten und Honorare"],
        entities=[
            ExtractedEntity(name="DATEV", type="ORGANIZATION", n_competitors=4, coverage=0.8),
            ExtractedEntity(name="Steuerberatergebührenverordnung", type="LAW", n_competitors=3, coverage=0.6),
        ],
        high_coverage_entities=[
            ExtractedEntity(name="DATEV", type="ORGANIZATION", n_competitors=4, coverage=0.8),
            ExtractedEntity(name="Steuerberatergebührenverordnung", type="LAW", n_competitors=3, coverage=0.6),
        ],
    )


def _valid_contract_payload() -> dict:
    return {
        "target_keyword": "steuerberater hamburg",
        "secondary_keywords": ["steuerberater hamburg altona", "günstiger steuerberater hamburg"],
        "intent": "transactional",
        "tone": "sie",
        "target_audience": "Hamburger Unternehmer und Selbstständige",
        "word_count_target": 950,
        "h1": "Steuerberater Hamburg: So finden Sie den richtigen Berater",
        "meta_title": "Steuerberater Hamburg finden: Tipps für Unternehmer",
        "meta_description": "Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen, Honorare und Spezialisierungen. Unser Leitfaden hilft bei der Wahl.",
        "slug": "steuerberater-hamburg-finden",
        "sections": [
            {"h2": "Warum ein Hamburger Steuerberater wichtig ist", "mandate": "Erkläre die Vorteile spezifisch für Hamburger Unternehmer.", "target_word_count": 200, "required_subheadings": [], "required_elements": []},
            {"h2": "Auswahlkriterien für Ihren Steuerberater", "mandate": "Liste relevante Kriterien für die Auswahl detailliert auf.", "target_word_count": 280, "required_subheadings": [], "required_elements": ["list"]},
            {"h2": "Kosten und Honorare im Überblick", "mandate": "Erkläre typische Honorarstrukturen mit konkreten Spannen.", "target_word_count": 240, "required_subheadings": [], "required_elements": ["table"]},
            {"h2": "DATEV und digitale Tools", "mandate": "Beschreibe wie moderne Steuerberater DATEV und vergleichbare Software einsetzen.", "target_word_count": 200, "required_subheadings": [], "required_elements": []},
        ],
        "faq_items": [
            {"question": "Was kostet ein Steuerberater in Hamburg?", "answer_outline": "Stundensätze 120–250 Euro; abhängig von Leistung."},
            {"question": "Welche Steuerberater sind die besten?", "answer_outline": "Branchenspezialisierung als Kriterium."},
        ],
        "required_entities": [
            {"name": "DATEV", "placement_hint": "in section 4"},
            {"name": "Steuerberatergebührenverordnung", "placement_hint": "in section 3"},
        ],
        "link_plan": [
            {
                "target_url": "https://client.de/steuerberatung",
                "anchor_strategy": "partial_match",
                "section_index": 1,
                "surrounding_context_requirements": "In der Sektion über Auswahlkriterien, im Kontext einer Empfehlung.",
                "link_type": "backlink",
            }
        ],
        "schema_spec": {"article": True, "faq_page": True},
        "ai_tell_blocklist": [
            "Darüber hinaus", "Es ist wichtig zu beachten", "Zusammenfassend",
            "In der heutigen Zeit", "Letztendlich", "Abschließend",
            "In diesem Artikel werden wir", "Im Folgenden", "wie bereits erwähnt",
            "ohne Zweifel", "selbstverständlich", "essenziell",
        ],
        "competitor_top_urls": ["https://example.de/a", "https://kanzlei.de/b"],
        "contract_version": "v1",
    }


# ---- prompt assembly -------------------------------------------------------


def test_build_user_prompt_includes_research_signals():
    research = _research()
    prompt = build_user_prompt(research, target_backlink_url="https://client.de/x", anchor_hint="exact_match")
    assert "steuerberater hamburg" in prompt
    assert "https://client.de/x" in prompt
    assert "exact_match" in prompt
    assert "Was kostet ein Steuerberater in Hamburg?" in prompt
    assert "DATEV" in prompt
    assert "Steuerberatergebührenverordnung" in prompt
    assert "950 Wörter" in prompt
    assert "Auswahlkriterien" in prompt


def test_build_user_prompt_handles_anchor_hint_absent():
    prompt = build_user_prompt(_research(), target_backlink_url="https://x.de")
    assert "(frei wählbar)" in prompt


def test_build_user_prompt_handles_empty_research():
    empty = ResearchPayload(target_keyword="x", location_code=2276, language_code="de")
    prompt = build_user_prompt(empty, target_backlink_url="https://y.de")
    assert "(keine organischen Ergebnisse" in prompt
    assert "(keine Pflicht-Entitäten" in prompt
    assert "(keine Volumen-Daten)" in prompt


def test_build_user_prompt_tolerates_string_competition_label():
    # DataForSEO sometimes returns competition as "LOW" / "MEDIUM" / "HIGH"
    # instead of a 0-1 float. Formatting must not crash.
    research = _research()
    research.primary_volume = KeywordMetric(
        keyword="steuerberater hamburg",
        search_volume=9900,
        competition="HIGH",  # type: ignore[arg-type]  -- intentional API quirk
        cpc=4.32,
    )
    prompt = build_user_prompt(research, target_backlink_url="https://x.de")
    assert "HIGH" in prompt
    assert "4.32€" in prompt


def test_build_system_prompt_embeds_full_schema():
    fake_prompt = Prompt(name="contract_generator", version="v1", language="de", body="ROLE", metadata={})
    system = build_system_prompt(fake_prompt)
    assert "ROLE" in system
    assert "ContentContract" in system or "target_keyword" in system  # schema field present
    assert "ai_tell_blocklist" in system
    assert "link_plan" in system


# ---- generate_contract -----------------------------------------------------


def test_generate_contract_returns_validated_pydantic_model(monkeypatch):
    payload = _valid_contract_payload()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return json.dumps(payload, ensure_ascii=False)

    contract = generate_contract(
        _research(),
        target_backlink_url="https://client.de/steuerberatung",
        anchor_hint="partial_match",
        llm_caller=fake_caller,
    )
    assert isinstance(contract, ContentContract)
    assert contract.target_keyword == "steuerberater hamburg"
    assert contract.intent == SearchIntent.TRANSACTIONAL
    assert contract.tone == GermanTone.SIE
    assert len(contract.sections) >= 4
    assert any(link.link_type == "backlink" for link in contract.link_plan)
    assert len(contract.ai_tell_blocklist) >= 12


def test_generate_contract_rejects_invalid_schema(monkeypatch):
    bad_payload = _valid_contract_payload()
    bad_payload["sections"] = [{"h2": "X", "mandate": "Y", "target_word_count": 100}]  # min_length=3
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return json.dumps(bad_payload, ensure_ascii=False)

    with pytest.raises(LLMError, match="schema validation"):
        generate_contract(
            _research(),
            target_backlink_url="https://client.de/x",
            llm_caller=fake_caller,
        )


def test_generate_contract_rejects_non_json_response(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return "Sorry, I can't help with that."

    with pytest.raises(LLMError, match="non-JSON"):
        generate_contract(
            _research(),
            target_backlink_url="https://client.de/x",
            llm_caller=fake_caller,
        )


def test_generate_contract_requires_target_url(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with pytest.raises(ValueError, match="target_backlink_url"):
        generate_contract(_research(), target_backlink_url="   ")


def test_generate_contract_requires_api_key_in_production(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        generate_contract(_research(), target_backlink_url="https://client.de/x")


def test_generate_contract_passes_research_to_caller(monkeypatch):
    payload = _valid_contract_payload()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return json.dumps(payload, ensure_ascii=False)

    generate_contract(
        _research(),
        target_backlink_url="https://client.de/x",
        anchor_hint="branded",
        llm_caller=fake_caller,
    )
    assert "steuerberater hamburg" in captured["user_prompt"]
    assert "DATEV" in captured["user_prompt"]
    assert "branded" in captured["user_prompt"]
    assert "JSON-Schema" in captured["system_prompt"]


# ---- call_with_thinking ----------------------------------------------


def _thinking_response(text: str) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "content": [
            {"type": "thinking", "thinking": "internal reasoning ..."},
            {"type": "text", "text": text},
        ],
        "usage": {"input_tokens": 1200, "output_tokens": 800},
    }
    return response


def test_call_with_thinking_sends_thinking_param():
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _thinking_response('{"foo":"bar"}')

    with patch("creator.api.contract_generator.requests.post", side_effect=fake_post):
        text = call_with_thinking(
            system_prompt="sys",
            user_prompt="usr",
            api_key="test",
        )
    assert text == '{"foo":"bar"}'
    assert captured["json"]["thinking"]["type"] == "enabled"
    assert captured["json"]["thinking"]["budget_tokens"] == 4000
    assert captured["json"]["temperature"] == 1.0
    assert captured["url"].endswith("/messages")


def test_call_with_thinking_skips_thinking_blocks():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "content": [
            {"type": "thinking", "thinking": "should be ignored"},
            {"type": "text", "text": "first"},
            {"type": "text", "text": "second"},
        ],
        "usage": {},
    }
    with patch("creator.api.contract_generator.requests.post", return_value=response):
        text = call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert text == "first\nsecond"


def test_call_with_thinking_raises_on_http_error():
    response = MagicMock()
    response.status_code = 400
    response.text = "bad request"
    with patch("creator.api.contract_generator.requests.post", return_value=response):
        with pytest.raises(LLMError, match="HTTP 400"):
            call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")


def test_call_with_thinking_raises_when_no_text_blocks():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "content": [{"type": "thinking", "thinking": "only thinking"}],
        "usage": {},
    }
    with patch("creator.api.contract_generator.requests.post", return_value=response):
        with pytest.raises(LLMError, match="missing text"):
            call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")


def test_call_with_thinking_retries_on_529_overloaded():
    """Anthropic returns HTTP 529 when their servers are overloaded -- it's
    transient and identical in shape to a 503 from any other API. Failing
    the contract step on a single 529 wastes the ~$0.05 of research that
    just ran, so we retry with backoff."""

    overloaded = MagicMock()
    overloaded.status_code = 529
    overloaded.text = '{"type":"error","error":{"type":"overloaded_error"}}'

    success = _thinking_response('{"ok":true}')
    sleeps: list[float] = []

    with patch("creator.api.contract_generator.requests.post", side_effect=[overloaded, overloaded, success]), \
         patch("creator.api.contract_generator.time.sleep", side_effect=lambda s: sleeps.append(s)):
        text = call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert text == '{"ok":true}'
    # Two retries with exponential backoff (2s and 4s base) plus jitter
    # (0.8x-1.2x). Asserting bands rather than exact values because jitter
    # is non-deterministic by design.
    assert len(sleeps) == 2
    assert 1.6 <= sleeps[0] <= 2.4
    assert 3.2 <= sleeps[1] <= 4.8


def test_call_with_thinking_retries_on_503():
    overloaded = MagicMock()
    overloaded.status_code = 503
    overloaded.text = "Service Unavailable"
    success = _thinking_response('{"ok":1}')

    with patch("creator.api.contract_generator.requests.post", side_effect=[overloaded, success]), \
         patch("creator.api.contract_generator.time.sleep", lambda s: None):
        text = call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert text == '{"ok":1}'


def test_call_with_thinking_does_not_retry_4xx():
    """400/401/403 etc. are real client errors -- bad request, bad auth.
    They never fix themselves, so don't retry."""

    response = MagicMock()
    response.status_code = 401
    response.text = "Unauthorized"

    with patch("creator.api.contract_generator.requests.post", return_value=response) as mock_post, \
         patch("creator.api.contract_generator.time.sleep", lambda s: None):
        with pytest.raises(LLMError, match="HTTP 401"):
            call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert mock_post.call_count == 1


def test_call_with_thinking_raises_after_retry_exhaustion():
    overloaded = MagicMock()
    overloaded.status_code = 529
    overloaded.text = "Overloaded"

    with patch("creator.api.contract_generator.requests.post", return_value=overloaded) as mock_post, \
         patch("creator.api.contract_generator.time.sleep", lambda s: None):
        with pytest.raises(LLMError, match="after 5 attempts"):
            call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert mock_post.call_count == 5


def test_call_with_thinking_retries_on_connection_error():
    """Network blips (timeouts, DNS) get the same retry treatment as 5xx."""

    success = _thinking_response('{"ok":1}')

    side_effects = [
        __import__("requests").exceptions.ConnectionError("dns failure"),
        success,
    ]
    with patch("creator.api.contract_generator.requests.post", side_effect=side_effects), \
         patch("creator.api.contract_generator.time.sleep", lambda s: None):
        text = call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    assert text == '{"ok":1}'


def test_call_with_thinking_error_messages_use_model_name_not_opus():
    """Regression: error messages used to say 'Opus thinking ...' even when
    the actual model was Sonnet. Confusing in production logs."""

    response = MagicMock()
    response.status_code = 400
    response.text = "bad request"

    with patch("creator.api.contract_generator.requests.post", return_value=response):
        with pytest.raises(LLMError) as exc_info:
            call_with_thinking(system_prompt="s", user_prompt="u", api_key="k")
    # The default model is Sonnet 4.6 -- error message must reflect that.
    assert "claude-sonnet-4-6" in str(exc_info.value)
    assert "Opus" not in str(exc_info.value)
