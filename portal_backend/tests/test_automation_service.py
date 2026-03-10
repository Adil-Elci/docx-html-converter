from __future__ import annotations

from portal_backend.api import automation_service


def _creator_output_without_images() -> dict[str, object]:
    return {
        "phase5": {
            "meta_title": "Kinderbrille erkennen",
            "excerpt": "Kurzbeschreibung",
            "slug": "kinderbrille-erkennen",
            "article_html": "<p>Artikelinhalt</p>",
        },
        "phase6": {
            "featured_image": {
                "prompt": "Editorial photo illustrating: Kinderbrille erkennen",
                "alt_text": "Kinderbrille erkennen",
            }
        },
        "images": [],
    }


def test_run_create_article_pipeline_does_not_generate_portal_fallback_image_for_new_post(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        automation_service,
        "call_creator_service",
        lambda **_kwargs: _creator_output_without_images(),
    )

    def fake_create_post(**kwargs):
        calls["create_post"] = kwargs
        return {"id": 321, "link": "https://publisher.example.com/draft"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create_post)
    monkeypatch.setattr(
        automation_service,
        "generate_image_via_leonardo",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected fallback image generation")),
    )
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    result = automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher.example.com",
        publishing_site_id="site-id",
        client_target_site_id="target-id",
        anchor="Kinderbrille",
        topic=None,
        exclude_topics=[],
        internal_link_inventory=[],
        phase1_cache_payload=None,
        phase1_cache_content_hash="",
        phase2_cache_payload=None,
        phase2_cache_content_hash="",
        target_profile_payload=None,
        target_profile_content_hash="",
        publishing_profile_payload=None,
        publishing_profile_content_hash="",
        site_url="https://publisher.example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user",
        wp_app_password="pass",
        existing_wp_post_id=None,
        post_status="draft",
        author_id=7,
        category_ids=[9],
        category_candidates=[],
        timeout_seconds=5,
        creator_timeout_seconds=5,
        poll_timeout_seconds=5,
        poll_interval_seconds=1,
        image_width=1024,
        image_height=576,
        leonardo_api_key="configured-but-should-not-be-used",
        leonardo_base_url="https://leonardo.example.com",
        leonardo_model_id="model-id",
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="",
        category_llm_model="",
        category_llm_max_categories=1,
        category_llm_confidence_threshold=0.5,
    )

    assert result["image_url"] == ""
    assert result["media_payload"] == {}
    assert result["media_url"] is None
    assert calls["create_post"]["featured_media_id"] is None


def test_run_create_article_pipeline_clears_existing_featured_media_when_creator_returns_no_image(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        automation_service,
        "call_creator_service",
        lambda **_kwargs: _creator_output_without_images(),
    )

    def fake_update_post(**kwargs):
        calls["update_post"] = kwargs
        return {"id": 654, "link": "https://publisher.example.com/existing-draft"}

    monkeypatch.setattr(automation_service, "wp_update_post", fake_update_post)
    monkeypatch.setattr(
        automation_service,
        "generate_image_via_leonardo",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected fallback image generation")),
    )
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    result = automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher.example.com",
        publishing_site_id="site-id",
        client_target_site_id="target-id",
        anchor="Kinderbrille",
        topic=None,
        exclude_topics=[],
        internal_link_inventory=[],
        phase1_cache_payload=None,
        phase1_cache_content_hash="",
        phase2_cache_payload=None,
        phase2_cache_content_hash="",
        target_profile_payload=None,
        target_profile_content_hash="",
        publishing_profile_payload=None,
        publishing_profile_content_hash="",
        site_url="https://publisher.example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user",
        wp_app_password="pass",
        existing_wp_post_id=654,
        post_status="draft",
        author_id=7,
        category_ids=[9],
        category_candidates=[],
        timeout_seconds=5,
        creator_timeout_seconds=5,
        poll_timeout_seconds=5,
        poll_interval_seconds=1,
        image_width=1024,
        image_height=576,
        leonardo_api_key="configured-but-should-not-be-used",
        leonardo_base_url="https://leonardo.example.com",
        leonardo_model_id="model-id",
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="",
        category_llm_model="",
        category_llm_max_categories=1,
        category_llm_confidence_threshold=0.5,
    )

    assert result["image_url"] == ""
    assert result["media_payload"] == {}
    assert result["media_url"] is None
    assert calls["update_post"]["featured_media_id"] == 0
