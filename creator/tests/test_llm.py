from unittest.mock import MagicMock, patch

from creator.api.llm import _call_anthropic, _extract_json


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


def _anthropic_response(text: str = '{"foo":"bar"}') -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "content": [{"type": "text", "text": text}],
        "usage": {"input_tokens": 100, "output_tokens": 20},
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
