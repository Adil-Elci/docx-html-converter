from creator.api.llm import _extract_json


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
