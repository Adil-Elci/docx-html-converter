from __future__ import annotations

from portal_backend.api import automation_service
from portal_backend.api.four_llm_schemas import CompetitorReference, KeywordMetric


def test_run_create_article_pipeline_4llm_publishes_draft(monkeypatch) -> None:
    monkeypatch.setenv("CREATOR_PIPELINE_MODE", "4llm")

    monkeypatch.setattr(
        automation_service,
        "call_creator_site_understanding",
        lambda **kwargs: {
            "primary_niche": "Hausbau",
            "main_topic": "Hausbau planen",
            "target_audience": "Bauherren",
            "seed_keywords": ["hausbau kosten", "grundstück prüfen", "bauzeit planen"],
            "content_tone": "informativ",
            "site_type": "blog",
            "language": "de",
            "scraped_pages": [
                {
                    "url": "https://target.example.com/hausbau",
                    "title": "Hausbau",
                    "h1": "Hausbau planen",
                    "text_excerpt": "Hausbau Kosten, Budget und Bauablauf sauber planen.",
                }
            ],
        },
    )
    monkeypatch.setattr(
        automation_service,
        "_select_target_keyword",
        lambda *args, **kwargs: (
            KeywordMetric(
                keyword="hausbau kosten",
                search_volume=1000,
                keyword_difficulty=20.0,
                score=47.6,
                top_urls=["https://competitor.example.com/a"],
            ),
            [
                CompetitorReference(
                    url="https://competitor.example.com/a",
                    title="Hausbau Kosten richtig planen",
                    h1="Hausbau Kosten richtig planen",
                    h2s=["Budget aufstellen", "Nebenkosten einplanen"],
                    h3s=[],
                    word_count=1500,
                    content_format="guide",
                    key_topics=["Budget", "Nebenkosten"],
                )
            ],
        ),
    )
    monkeypatch.setattr(
        automation_service,
        "call_creator_draft_article",
        lambda **kwargs: {
            "markdown": "# Hausbau Kosten realistisch planen\n\nHausbau Kosten sauber einordnen [Budgetplanung](https://publisher.example.com/budget) und [Bauzeit](https://target.example.com/hausbau).\n\n## Hausbau Kosten: Wichtige Grundlagen\n\nHausbau Kosten beginnen mit Budget, Grundstück, Baunebenkosten und Reserven.\n\n## Hausbau Kosten: Praktische Kriterien\n\nHausbau Kosten hängen von Grundstück, Rohbau, Ausbau und Puffer ab.\n\n## FAQ\n\n### Welche Nebenkosten werden oft vergessen?\n\nNotar, Genehmigungen und Hausanschlüsse werden oft zu spät eingeplant.\n\n## Fazit\n\nHausbau Kosten lassen sich besser steuern, wenn Budget und Nebenkosten früh sauber geplant werden."
        },
    )
    monkeypatch.setattr(
        automation_service,
        "call_creator_integrate_links",
        lambda **kwargs: {"markdown": kwargs["article_markdown"]},
    )
    monkeypatch.setattr(
        automation_service,
        "call_creator_generate_meta",
        lambda **kwargs: {
            "meta_title": "Hausbau Kosten realistisch planen",
            "meta_description": "Hausbau Kosten realistisch planen: So behalten Bauherren Budget, Nebenkosten und Reserven von Anfang an belastbar im Blick.",
            "tags": ["Hausbau", "Kosten", "Budget"],
        },
    )
    monkeypatch.setattr(automation_service, "_validate_links", lambda urls, timeout_seconds: (True, []))
    monkeypatch.setattr(automation_service, "_run_copyscape_check", lambda content_text, timeout_seconds: 0.0)
    captured = {}

    def fake_create_post(**kwargs):
        captured["create_post"] = kwargs
        return {"id": 77, "link": "https://publisher.example.com/draft-77", "status": "draft"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create_post)

    result = automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher.example.com",
        publishing_site_id="site-1",
        client_target_site_id="target-1",
        anchor=None,
        topic=None,
        exclude_topics=[],
        recent_article_titles=[],
        internal_link_inventory=[
            {"url": "https://publisher.example.com/budget", "title": "Budgetplanung", "excerpt": "Budget und Baukosten planen"}
        ],
        publishing_candidates=[],
        phase1_cache_payload=None,
        phase1_cache_content_hash="",
        phase2_cache_payload=None,
        phase2_cache_content_hash="",
        target_profile_payload=None,
        target_profile_content_hash="",
        publishing_profile_payload=None,
        publishing_profile_content_hash="",
        on_phase=None,
        site_url="https://publisher.example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user",
        wp_app_password="pass",
        existing_wp_post_id=None,
        post_status="draft",
        author_id=3,
        category_ids=[9],
        category_candidates=[],
        timeout_seconds=5,
        creator_timeout_seconds=5,
        poll_timeout_seconds=5,
        poll_interval_seconds=1,
        image_width=1024,
        image_height=576,
        leonardo_api_key="",
        leonardo_base_url="",
        leonardo_model_id="",
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="",
        category_llm_model="",
        category_llm_max_categories=1,
        category_llm_confidence_threshold=0.5,
    )

    assert result["creator_output"]["pipeline_mode"] == "4llm"
    assert result["creator_output"]["pipeline_state"]["content_brief"]["target_keyword"] == "hausbau kosten"
    assert captured["create_post"]["site_url"] == "https://publisher.example.com"
    assert result["post_payload"]["id"] == 77
