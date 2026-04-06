from __future__ import annotations

from creator.api import four_llm
from creator.api.four_llm_schemas import DraftArticleRequest, IntegrateLinksRequest, MetaTagsRequest, SiteUnderstandingRequest


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


def test_understand_target_site_appends_scraped_pages(monkeypatch) -> None:
    html = """
    <html>
      <head><title>Eigenheim Blog</title><meta name="description" content="Hausbau und Wohnen" /></head>
      <body>
        <h1>Hausbau verständlich erklärt</h1>
        <h2>Planung</h2>
        <a href="/garten">Garten</a>
      </body>
    </html>
    """
    monkeypatch.setattr(four_llm.requests, "get", lambda *args, **kwargs: _FakeResponse(html))
    monkeypatch.setattr(
        four_llm,
        "call_llm_json",
        lambda **kwargs: {
            "primary_niche": "Hausbau",
            "main_topic": "Eigenheim planen",
            "target_audience": "Bauherren",
            "seed_keywords": ["hausbau kosten", "eigenheim planen", "grundstück prüfen"],
            "content_tone": "informativ",
            "site_type": "blog",
            "language": "de",
        },
    )

    result = four_llm.understand_target_site(SiteUnderstandingRequest(target_site_url="https://example.com"))

    assert result.primary_niche == "Hausbau"
    assert result.scraped_pages
    assert result.scraped_pages[0].h1 == "Hausbau verständlich erklärt"


def test_draft_integrate_and_meta_use_structured_contracts(monkeypatch) -> None:
    monkeypatch.setattr(
        four_llm,
        "call_llm_text",
        lambda **kwargs: (
            "# Titel\n\n"
            "Einleitung mit [[INTERNAL_LINK_1]] und mehreren konkreten Sätzen zu Budget, Baukosten und Planung. "
            "Der Text ist absichtlich länger, damit das strukturierte Antwortmodell valide bleibt.\n\n"
            "## Kostenplanung\n\n"
            "Eine saubere Kostenplanung berücksichtigt Grundstück, Baukosten, Nebenkosten und Reserven. "
            "So bleibt der Artikel ausreichend lang und realistisch für den Test.\n\n"
            "## FAQ\n\n"
            "Antwort mit zusätzlichem Kontext zur Einordnung."
        ),
    )
    draft = four_llm.draft_article(
        DraftArticleRequest(
            content_brief={
                "target_keyword": "hausbau kosten",
                "secondary_keywords": ["grundstück prüfen"],
                "search_intent": "informational",
                "recommended_format": "guide",
                "recommended_word_count": 1200,
                "tone": "informativ",
                "target_audience": "Bauherren",
                "suggested_title": "Hausbau Kosten richtig einschätzen",
                "outline": ["Einleitung", "Kosten", "FAQ", "Fazit"],
                "key_topics_to_cover": ["Budget", "Nebenkosten"],
                "internal_link_candidates": [],
                "external_link_candidates": [],
                "competitor_references": [],
                "target_site_url": "https://target.example.com",
                "publishing_site_url": "https://publisher.example.com",
                "chosen_topic": "Hausbau Kosten",
            }
        )
    )
    assert "[[INTERNAL_LINK_1]]" in draft.markdown

    monkeypatch.setattr(
        four_llm,
        "call_llm_text",
        lambda **kwargs: (
            "# Titel\n\n"
            "Einleitung mit [Budgetplanung](https://publisher.example.com/budget) und genug Inhalt, "
            "damit die strukturierte Antwort nicht an der Mindestlänge scheitert. "
            "Der Absatz bleibt deshalb bewusst etwas ausführlicher und erläutert Budget, Baunebenkosten und Reserven.\n\n"
            "## Kostenplanung\n\n"
            "Budget, Nebenkosten und Puffer sollten sauber erläutert werden. "
            "Zusätzlich wird erklärt, warum frühe Entscheidungen den Finanzrahmen stabil halten.\n\n"
            "## FAQ\n\n"
            "Antwort mit weiterem Kontext und einer kurzen praktischen Einordnung."
        ),
    )
    linked = four_llm.integrate_links(
        IntegrateLinksRequest(
            article_markdown=draft.markdown,
            internal_links=[
                {
                    "url": "https://publisher.example.com/budget",
                    "title": "Budgetplanung",
                    "relevance_score": 0.9,
                    "link_type": "internal",
                    "target_kind": "owned_network",
                }
            ],
            external_links=[],
        )
    )
    assert linked.placed_links[0]["url"] == "https://publisher.example.com/budget"

    monkeypatch.setattr(
        four_llm,
        "call_llm_json",
        lambda **kwargs: {
            "meta_title": "Hausbau Kosten realistisch und sauber planen",
            "meta_description": "Hausbau Kosten realistisch planen: So behalten Bauherren Budget, Nebenkosten und Prioritäten von Anfang an sauber im Blick.",
            "tags": ["Hausbau", "Kosten", "Budget"],
        },
    )
    meta = four_llm.generate_meta(
        MetaTagsRequest(
            target_keyword="hausbau kosten",
            article_title="Hausbau Kosten realistisch planen",
            article_intro="Hausbau kostet mehr als nur den Rohbau. Wer früh sauber plant, behält Budget, Nebenkosten und Reserven im Blick.",
        )
    )
    assert "hausbau" in meta.meta_title.lower()
