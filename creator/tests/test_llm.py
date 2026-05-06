from unittest.mock import MagicMock, patch

import pytest

from creator.api.llm import (
    DEFAULT_LLM_BACKOFF_SECONDS,
    DEFAULT_LLM_RETRIES,
    LLMError,
    _call_anthropic,
    _extract_json,
    _is_retryable_error,
    _retry_sleep_seconds,
    call_llm_json,
    call_llm_text,
)


def test_extract_json_repairs_trailing_commas_and_bare_keys():
    payload = _extract_json(
        """
        ```json
        {
          outline: [
            {"h2": "Einleitung", "h3": [],},
            {"h2": "Fazit", "h3": [],},
            {"h2": "FAQ", "h3": ["Was ist wichtig?",],},
          ],
          backlink_placement: "intro",
          anchor_text_final: "Mehr erfahren",
        }
        ```
        """
    )

    assert payload["backlink_placement"] == "intro"
    assert payload["outline"][-1]["h2"] == "FAQ"


def test_extract_json_preserves_german_smart_quotes_inside_string_value():
    # Real-world failure mode: section_writer body_html contains German
    # typographic quotes („Foo“) which are valid inside a JSON string
    # value but were destroyed by an unconditional smart-quote -> ASCII
    # translation, which produced unescaped quotes inside the string and
    # broke parsing.
    payload = _extract_json(
        '{\n'
        '  "body_html": "<p>Der Titel „Steuerberater“ ist gesetzlich geschützt.</p>",\n'
        '  "links_inserted": [],\n'
        '  "word_count": 8\n'
        '}'
    )
    assert payload["word_count"] == 8
    assert "„Steuerberater“" in payload["body_html"]


def test_extract_json_heals_mixed_german_quotes_open_unicode_close_ascii():
    """Real failure: section_writer opens with „ but closes with bare ASCII "
    inside body_html, which terminates the JSON string prematurely.
    The heal rewrites the unmatched ASCII close as Unicode " before
    parsing so the section survives."""

    # Note: opening is „ (U+201E), closing is " (U+0022 — ASCII).
    raw = (
        '{\n'
        '  "body_html": "<p>Eltern sagen: „Brillen sind kostenlos." Doch das stimmt nicht.</p>",\n'
        '  "links_inserted": [],\n'
        '  "word_count": 12\n'
        '}'
    )
    payload = _extract_json(raw)
    assert payload["word_count"] == 12
    # Heal converted the bare ASCII close into a proper German typographic close.
    assert "„Brillen sind kostenlos.“" in payload["body_html"]
    # Surrounding prose is preserved (we didn't accidentally truncate).
    assert "Doch das stimmt nicht" in payload["body_html"]


def test_extract_json_heals_mixed_french_quotes_open_unicode_close_ascii():
    raw = (
        '{\n'
        '  "body_html": "<p>Les parents disent : «Les lunettes sont chères." Or c\'est faux.</p>",\n'
        '  "links_inserted": [],\n'
        '  "word_count": 13\n'
        '}'
    )
    payload = _extract_json(raw)
    assert payload["word_count"] == 13
    assert "«Les lunettes sont chères.»" in payload["body_html"]


def test_extract_json_leaves_correctly_paired_german_quotes_alone():
    """Heal must be idempotent: properly „..." paired strings shouldn't be touched."""

    raw = (
        '{\n'
        '  "body_html": "<p>Der Titel „Steuerberater“ ist geschützt.</p>",\n'
        '  "word_count": 5\n'
        '}'
    )
    payload = _extract_json(raw)
    assert "„Steuerberater“" in payload["body_html"]
    assert payload["body_html"].count("“") == 1  # no duplicate close added


def test_extract_json_still_handles_smart_quotes_at_structural_positions():
    # The complementary case: model used “ ” as the JSON quote
    # delimiters themselves (around keys / values). Original parse fails;
    # smart-quote translation fallback should rescue it.
    payload = _extract_json(
        '{\n'
        '  “outline”: [{“h2”: “Fazit”}]\n'
        '}'
    )
    assert payload["outline"][0]["h2"] == "Fazit"


def test_extract_json_accepts_raw_newlines_inside_string_value():
    # Real-world failure mode: section_writer's body_html field contains
    # multi-line HTML with literal newlines between <li> items. Standard
    # json.loads rejects raw control chars in strings; strict=False accepts.
    payload = _extract_json(
        '{\n'
        '  "body_html": "<ul>\n<li>Erstens</li>\n<li>Zweitens</li>\n</ul>",\n'
        '  "links_inserted": [],\n'
        '  "word_count": 4\n'
        '}'
    )
    assert payload["word_count"] == 4
    assert "<li>Erstens</li>" in payload["body_html"]
    assert "<li>Zweitens</li>" in payload["body_html"]


def test_extract_json_handles_surrounding_text_and_smart_quotes():
    payload = _extract_json(
        """
        Here is the requested JSON:
        {
          “outline”: [
            {“h2”: “Analyse”, “h3”: []},
            {“h2”: “Fazit”, “h3”: []},
            {“h2”: “FAQ”, “h3”: [“Was bedeutet das?”]}
          ],
          “backlink_placement”: “section_2”,
          “anchor_text_final”: “Zur Quelle”
        }
        """
    )

    assert payload["backlink_placement"] == "section_2"
    assert payload["anchor_text_final"] == "Zur Quelle"


def _anthropic_response(text: str = '{"foo":"bar"}', stop_reason: str = "end_turn") -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "content": [{"type": "text", "text": text}],
        "usage": {"input_tokens": 100, "output_tokens": 20},
        "stop_reason": stop_reason,
    }
    return response


def test_call_anthropic_sends_string_system_when_cache_disabled():
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured["json"] = json
        return _anthropic_response()

    with patch("creator.api.llm.requests.post", side_effect=fake_post):
        _call_anthropic(
            system_prompt="long stable prompt",
            user_prompt="usr",
            api_key="k",
            base_url="https://api.anthropic.com/v1",
            model="claude-sonnet-4-6",
            timeout_seconds=30,
            max_tokens=500,
            temperature=0.3,
            request_label="test",
            cache_system=False,
        )
    assert captured["json"]["system"] == "long stable prompt"


def test_call_anthropic_sends_list_with_cache_control_when_cache_enabled():
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured["json"] = json
        return _anthropic_response()

    with patch("creator.api.llm.requests.post", side_effect=fake_post):
        _call_anthropic(
            system_prompt="long stable prompt",
            user_prompt="usr",
            api_key="k",
            base_url="https://api.anthropic.com/v1",
            model="claude-sonnet-4-6",
            timeout_seconds=30,
            max_tokens=500,
            temperature=0.3,
            request_label="test",
            cache_system=True,
        )
    system_field = captured["json"]["system"]
    assert isinstance(system_field, list)
    assert len(system_field) == 1
    assert system_field[0]["type"] == "text"
    assert system_field[0]["text"] == "long stable prompt"
    assert system_field[0]["cache_control"] == {"type": "ephemeral"}


def test_is_retryable_error_includes_invalid_json_and_missing_content():
    assert _is_retryable_error(LLMError("LLM returned invalid JSON.")) is True
    assert _is_retryable_error(LLMError("LLM response missing content.")) is True
    assert _is_retryable_error(LLMError("LLM HTTP 503: x")) is True
    assert _is_retryable_error(LLMError("LLM HTTP 400: bad request")) is False


def test_is_retryable_error_includes_max_tokens_cap():
    """A max_tokens hit deserves a retry — sometimes the model briefly went
    verbose and a second attempt fits."""
    assert _is_retryable_error(LLMError("LLM hit max_tokens cap (4000) for x; output was clipped.")) is True


def test_call_anthropic_raises_distinct_error_on_max_tokens_stop():
    """When Anthropic returns stop_reason='max_tokens' the response is
    truncated; surface a clean, retryable error instead of a downstream
    'invalid JSON' parse failure on the clipped string."""

    def fake_post(url, headers, json, timeout):
        return _anthropic_response('{"foo": "ba', stop_reason="max_tokens")

    with patch("creator.api.llm.requests.post", side_effect=fake_post):
        with pytest.raises(LLMError, match="max_tokens cap"):
            _call_anthropic(
                system_prompt="x",
                user_prompt="y",
                api_key="k",
                base_url="https://api.anthropic.com/v1",
                model="claude-sonnet-4-6",
                timeout_seconds=30,
                max_tokens=500,
                temperature=0.3,
                request_label="test",
            )


def test_call_llm_json_retries_once_on_invalid_json(monkeypatch):
    responses = [
        _anthropic_response("not even close to json"),
        _anthropic_response('{"ok": true}'),
    ]
    call_count = {"n": 0}

    def fake_post(url, headers, json, timeout):
        idx = call_count["n"]
        call_count["n"] += 1
        return responses[idx]

    monkeypatch.setattr("creator.api.llm.requests.post", fake_post)
    monkeypatch.setattr("creator.api.llm.time.sleep", lambda _s: None)
    result = call_llm_json(
        system_prompt="s",
        user_prompt="u",
        api_key="k",
        base_url="https://api.anthropic.com/v1",
        model="claude-sonnet-4-6",
        timeout_seconds=30,
        max_tokens=400,
        backoff_seconds=0.0,
    )
    assert result == {"ok": True}
    assert call_count["n"] == 2


def test_call_llm_json_raises_after_exhausting_retries(monkeypatch):
    monkeypatch.setattr(
        "creator.api.llm.requests.post",
        lambda url, headers, json, timeout: _anthropic_response("garbage that wont parse"),
    )
    monkeypatch.setattr("creator.api.llm.time.sleep", lambda _s: None)
    with pytest.raises(LLMError, match="invalid JSON"):
        call_llm_json(
            system_prompt="s",
            user_prompt="u",
            api_key="k",
            base_url="https://api.anthropic.com/v1",
            model="claude-sonnet-4-6",
            timeout_seconds=30,
            max_tokens=400,
            retries=2,
            backoff_seconds=0.0,
        )


# ---- retry budget + jitter -----------------------------------------------


def _http_response(status_code: int, text: str) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests.exceptions import HTTPError

        def raise_for_status():
            raise HTTPError(f"{status_code}", response=response)

        response.raise_for_status = raise_for_status
    return response


def test_default_retries_is_overload_class():
    """Anthropic 529 (overloaded) clears in seconds-to-minutes. The default
    retry budget must cover at least the seconds-class events; one or two
    retries (the old defaults) was too tight."""

    assert DEFAULT_LLM_RETRIES >= 4
    assert DEFAULT_LLM_BACKOFF_SECONDS >= 1.5


def test_retry_sleep_seconds_applies_jitter():
    """Backoff is base * 2**attempt * uniform(0.8, 1.2). Multiple calls at
    the same attempt should land in the jitter band."""

    samples = [_retry_sleep_seconds(2.0, 0) for _ in range(50)]
    assert all(1.6 <= s <= 2.4 for s in samples), samples
    assert len(set(round(s, 3) for s in samples)) > 1  # jitter is actually random

    samples_attempt_2 = [_retry_sleep_seconds(2.0, 2) for _ in range(50)]
    # 2.0 * 2**2 = 8.0 base; jitter band 6.4 - 9.6
    assert all(6.4 <= s <= 9.6 for s in samples_attempt_2)


def test_call_llm_text_retries_on_529_overloaded(monkeypatch):
    """Regression: call_llm_text used to hardcode retries=0, so a single 529
    on the voice_pass step lost the entire ~$0.30 of upstream work."""

    overloaded = _http_response(529, '{"type":"error","error":{"type":"overloaded_error"}}')
    success = _anthropic_response("polished article body")

    call_count = {"n": 0}

    def fake_post(url, headers, json, timeout):
        idx = call_count["n"]
        call_count["n"] += 1
        return [overloaded, overloaded, success][idx]

    monkeypatch.setattr("creator.api.llm.requests.post", fake_post)
    monkeypatch.setattr("creator.api.llm.time.sleep", lambda _s: None)

    result = call_llm_text(
        system_prompt="s",
        user_prompt="u",
        api_key="k",
        base_url="https://api.anthropic.com/v1",
        model="claude-sonnet-4-6",
        timeout_seconds=30,
        max_tokens=2000,
    )
    assert result == "polished article body"
    assert call_count["n"] == 3  # two 529 retries + final success


def test_call_llm_text_does_not_retry_4xx(monkeypatch):
    """Bad request / bad auth should fail fast -- they never fix themselves."""

    response = _http_response(401, "Unauthorized")
    call_count = {"n": 0}

    def fake_post(*_args, **_kwargs):
        call_count["n"] += 1
        return response

    monkeypatch.setattr("creator.api.llm.requests.post", fake_post)
    monkeypatch.setattr("creator.api.llm.time.sleep", lambda _s: None)

    with pytest.raises(LLMError, match="HTTP 401"):
        call_llm_text(
            system_prompt="s",
            user_prompt="u",
            api_key="k",
            base_url="https://api.anthropic.com/v1",
            model="claude-sonnet-4-6",
            timeout_seconds=30,
            max_tokens=500,
        )
    assert call_count["n"] == 1


def test_is_retryable_error_includes_529():
    """529 must be in the retryable set (Anthropic 'overloaded')."""

    err = LLMError("LLM HTTP 529: overloaded_error")
    assert _is_retryable_error(err) is True


def test_call_llm_json_retries_on_529_overloaded(monkeypatch):
    overloaded = _http_response(529, "Overloaded")
    success = _anthropic_response('{"ok": true}')

    call_count = {"n": 0}

    def fake_post(url, headers, json, timeout):
        idx = call_count["n"]
        call_count["n"] += 1
        return [overloaded, overloaded, success][idx]

    monkeypatch.setattr("creator.api.llm.requests.post", fake_post)
    monkeypatch.setattr("creator.api.llm.time.sleep", lambda _s: None)

    result = call_llm_json(
        system_prompt="s",
        user_prompt="u",
        api_key="k",
        base_url="https://api.anthropic.com/v1",
        model="claude-sonnet-4-6",
        timeout_seconds=30,
        max_tokens=400,
    )
    assert result == {"ok": True}
    assert call_count["n"] == 3
