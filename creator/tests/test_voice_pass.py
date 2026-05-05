from __future__ import annotations

import pytest

from creator.api.contract import (
    ContentContract,
    LinkTarget,
    SearchIntent,
    SectionPlan,
)
from creator.api.llm import LLMError
from creator.api.voice_pass import (
    VoicePassValidationError,
    _extract_urls,
    _looks_truncated,
    _strip_codeblock_wrapper,
    build_user_prompt,
    refine_voice,
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
                surrounding_context_requirements="Im Kontext einer Empfehlung.",
                link_type="backlink",
            )
        ],
        ai_tell_blocklist=["Darüber hinaus", "Es ist wichtig zu beachten", "extra-floskel"],
    )


def _article_with_link() -> str:
    return (
        "<h1>Steuerberater Hamburg</h1>\n"
        "<h2>Warum Hamburg</h2>\n"
        "<p>Hamburg ist ein wichtiger Wirtschaftsstandort.</p>\n"
        "<h2>Auswahlkriterien</h2>\n"
        '<p>Wir empfehlen einen <a href="https://client.de/leistungen">spezialisierten Berater</a>.</p>\n'
        "<h2>Kosten</h2>\n"
        "<p>Die Honorare richten sich nach der Verordnung.</p>\n"
    )


# ---- helpers ---------------------------------------------------------------


def test_extract_urls_finds_all_hrefs():
    html = '<a href="https://a.de/x">a</a> and <a href="https://b.de/y">b</a>'
    assert _extract_urls(html) == ["https://a.de/x", "https://b.de/y"]


def test_extract_urls_returns_empty_for_no_links():
    assert _extract_urls("<p>no links here</p>") == []


def test_strip_codeblock_wrapper_removes_html_fence():
    text = "```html\n<h1>Test</h1>\n```"
    assert _strip_codeblock_wrapper(text) == "<h1>Test</h1>"


def test_strip_codeblock_wrapper_removes_bare_fence():
    text = "```\n<h1>Test</h1>\n```"
    assert _strip_codeblock_wrapper(text) == "<h1>Test</h1>"


def test_strip_codeblock_wrapper_passes_through_when_absent():
    text = "<h1>Test</h1>"
    assert _strip_codeblock_wrapper(text) == "<h1>Test</h1>"


# ---- truncation detector --------------------------------------------------


def test_looks_truncated_flags_mid_word_cut():
    # Reproduces the regression: voice pass output ends mid-word.
    truncated = "<h1>X</h1>\n<h2>Y</h2>\n<p>Kinder verbringen mehr Zeit im Freien. Gläser ohne UV-400-"
    assert _looks_truncated(truncated) is True


def test_looks_truncated_flags_mid_sentence_cut():
    truncated = "<h1>X</h1>\n<h2>Y</h2>\n<p>Some content that ends without a closing tag mid-sentence,"
    assert _looks_truncated(truncated) is True


def test_looks_truncated_passes_complete_article():
    complete = "<h1>X</h1>\n<h2>Y</h2>\n<p>Vollständiger Absatz mit Punkt am Ende.</p>"
    assert _looks_truncated(complete) is False


def test_looks_truncated_accepts_jsonld_tail():
    with_schema = '<h1>X</h1>\n<p>Body.</p>\n<script type="application/ld+json">{"@type":"Article"}</script>'
    assert _looks_truncated(with_schema) is False


def test_looks_truncated_flags_empty_input():
    assert _looks_truncated("") is True
    assert _looks_truncated("   ") is True


def test_refine_voice_falls_back_when_truncated(monkeypatch):
    """When the LLM returns truncated HTML, voice_pass falls back to the
    assembled article instead of returning a clipped post."""

    truncated = "<h1>X</h1><h2>Y</h2><p>Hier endet der Text mitten im Satz, ohne"

    def fake_caller(**kwargs):
        return truncated

    article_html = "<h1>X</h1><h2>Y</h2><p>Komplette Version.</p>"
    refined = refine_voice(
        article_html=article_html,
        contract=_contract(),
        llm_caller=fake_caller,
    )
    assert refined == article_html  # fallback used


# ---- prompt assembly -------------------------------------------------------


def test_build_user_prompt_includes_contract_blocklist():
    prompt = build_user_prompt(article_html="<p>x</p>", contract=_contract())
    assert "extra-floskel" in prompt
    assert "Darüber hinaus" in prompt
    assert "Sie" in prompt


def test_build_user_prompt_handles_empty_blocklist():
    contract = _contract()
    contract.ai_tell_blocklist = []
    prompt = build_user_prompt(article_html="<p>x</p>", contract=contract)
    assert "(keine zusätzlichen vertragsspezifischen Floskeln)" in prompt


def test_build_user_prompt_includes_article():
    article = "<h1>Test</h1><p>Inhalt</p>"
    prompt = build_user_prompt(article_html=article, contract=_contract())
    assert article in prompt


# ---- refine_voice happy path ----------------------------------------------


def test_refine_voice_returns_refined_html(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    refined_output = _article_with_link().replace(
        "Hamburg ist ein wichtiger Wirtschaftsstandort.",
        "Hamburg zählt zu den wichtigsten Wirtschaftsstandorten Deutschlands.",
    )

    def fake_caller(**kwargs):
        return refined_output

    result = refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert "wichtigsten Wirtschaftsstandorten Deutschlands" in result
    assert "https://client.de/leistungen" in result


def test_refine_voice_strips_codeblock_wrapping(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    wrapped = "```html\n" + _article_with_link() + "\n```"

    def fake_caller(**kwargs):
        return wrapped

    result = refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert not result.startswith("```")
    assert result.startswith("<h1>")


def test_refine_voice_uses_cache_system(monkeypatch):
    """Voice pass system prompt is stable across articles within 5 min — cache it."""

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _article_with_link()

    refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert captured.get("cache_system") is True


def test_refine_voice_passes_correct_request_label(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _article_with_link()

    refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert captured["request_label"] == "voice_pass/v1"


def test_refine_voice_resolves_model_from_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    monkeypatch.setenv("CREATOR_VOICE_MODEL", "claude-haiku-4-5-20251001")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _article_with_link()

    refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert captured["model"] == "claude-haiku-4-5-20251001"


# ---- validation ------------------------------------------------------------


def test_refine_voice_raises_when_link_dropped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    output_without_link = (
        "<h1>Steuerberater Hamburg</h1>\n"
        "<h2>Warum Hamburg</h2>\n"
        "<p>Hamburg ist wichtig.</p>\n"
        "<h2>Auswahlkriterien</h2>\n"
        "<p>Wir empfehlen spezialisierte Berater.</p>\n"
    )

    def fake_caller(**kwargs):
        return output_without_link

    with pytest.raises(VoicePassValidationError, match="dropped 1 URL"):
        refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)


def test_refine_voice_skips_validation_when_disabled(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    output_without_link = "<h1>Test</h1><p>kein Link mehr.</p>"

    def fake_caller(**kwargs):
        return output_without_link

    result = refine_voice(
        article_html=_article_with_link(),
        contract=_contract(),
        llm_caller=fake_caller,
        validate_links=False,
    )
    assert "kein Link mehr" in result


def test_refine_voice_passes_when_all_links_preserved(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    # Reorder a paragraph but keep the link.
    output = (
        "<h1>Steuerberater Hamburg</h1>\n"
        "<h2>Warum Hamburg</h2>\n"
        "<p>Der Standort Hamburg bietet Vorteile.</p>\n"
        "<h2>Auswahlkriterien</h2>\n"
        '<p>Bei der Auswahl sollten Sie einen <a href="https://client.de/leistungen">spezialisierten Berater</a> bevorzugen.</p>\n'
        "<h2>Kosten</h2>\n"
        "<p>Honorare folgen der Verordnung.</p>\n"
    )

    def fake_caller(**kwargs):
        return output

    result = refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)
    assert "https://client.de/leistungen" in result


def test_refine_voice_handles_multiple_dropped_links(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    article_with_two_links = (
        '<p><a href="https://a.de/x">A</a> and <a href="https://b.de/y">B</a></p>'
    )

    def fake_caller(**kwargs):
        return "<p>nothing</p>"

    with pytest.raises(VoicePassValidationError, match="dropped 2 URL"):
        refine_voice(article_html=article_with_two_links, contract=_contract(), llm_caller=fake_caller)


# ---- error paths -----------------------------------------------------------


def test_refine_voice_raises_on_empty_input(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with pytest.raises(ValueError, match="article_html is empty"):
        refine_voice(article_html="   ", contract=_contract())


def test_refine_voice_raises_on_empty_response(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return ""

    with pytest.raises(LLMError, match="empty content"):
        refine_voice(article_html=_article_with_link(), contract=_contract(), llm_caller=fake_caller)


def test_refine_voice_requires_api_key_in_production(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        refine_voice(article_html=_article_with_link(), contract=_contract())
