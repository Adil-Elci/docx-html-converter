from __future__ import annotations

import json

import pytest

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


def _creator_output_without_prompt_trace() -> dict[str, object]:
    return {
        "phase3": {
            "final_article_topic": "Kinder Sonnenbrillen",
            "primary_keyword": "kinder sonnenbrillen",
            "secondary_keywords": ["uv schutz kinderaugen"],
            "search_intent_type": "informational",
            "article_angle": "practical_guidance",
            "topic_class": "parenting_health",
            "style_profile": {"tone": "factual"},
            "specificity_profile": {"min_specifics": 2},
            "title_package": {"title": "Sonnenbrillen fuer Kinder"},
            "content_brief": {"must_cover": ["uv schutz", "passform"]},
            "faq_candidates": ["Worauf sollten Eltern achten?"],
        },
        "phase4": {
            "h1": "Sonnenbrillen fuer Kinder",
            "sections": [
                {
                    "section_id": "sec_1",
                    "kind": "body",
                    "h2": "Worauf sollten Eltern beim Kauf achten?",
                    "subquestion": "Welche Kriterien sind wichtig?",
                    "required_keywords": ["kinder sonnenbrillen"],
                    "required_terms": ["uv schutz", "passform"],
                    "required_elements": [],
                }
            ],
            "faq_questions": ["Worauf sollten Eltern achten?"],
        },
        "phase5": {
            "meta_title": "Sonnenbrillen fuer Kinder",
            "excerpt": "Kurzbeschreibung",
            "slug": "sonnenbrillen-fuer-kinder",
            "article_html": "<p>Artikelinhalt</p>",
        },
        "phase6": {
            "featured_image": {
                "prompt": "Editorial photo illustrating: Sonnenbrillen fuer Kinder",
                "alt_text": "Sonnenbrillen fuer Kinder",
            }
        },
        "debug": {
            "planning_quality": {"score": 82},
            "internal_linking": {"candidates": ["https://publisher.example.com/uv-tipps"]},
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


def test_run_create_article_pipeline_backfills_prompt_trace_when_creator_payload_is_older(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        automation_service,
        "call_creator_service",
        lambda **_kwargs: _creator_output_without_prompt_trace(),
    )

    def fake_create_post(**kwargs):
        calls["create_post"] = kwargs
        return {"id": 321, "link": "https://publisher.example.com/draft"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create_post)
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

    prompt_trace = result["creator_output"]["debug"]["prompt_trace"]
    assert prompt_trace["planner"]["mode"] == "deterministic"
    assert prompt_trace["planner"]["attempts"][0]["input_packet"]["topic"] == "Kinder Sonnenbrillen"
    assert prompt_trace["writer_attempts"][0]["request_label"] == "phase5_writer_attempt_1"
    assert "Do not write advertorial copy" in prompt_trace["writer_attempts"][0]["user_prompt"]


def test_run_create_article_pipeline_emits_structured_trace_events(monkeypatch) -> None:
    trace_events: list[dict[str, object]] = []

    monkeypatch.setattr(
        automation_service,
        "call_creator_service",
        lambda **_kwargs: _creator_output_without_images(),
    )
    monkeypatch.setattr(
        automation_service,
        "wp_create_post",
        lambda **_kwargs: {"id": 321, "status": "draft", "link": "https://publisher.example.com/draft"},
    )
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    automation_service.run_create_article_pipeline(
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
        leonardo_api_key="",
        leonardo_base_url="https://leonardo.example.com",
        leonardo_model_id="model-id",
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="",
        category_llm_model="",
        category_llm_max_categories=1,
        category_llm_confidence_threshold=0.5,
        trace_event=lambda level, phase, event, message, details=None: trace_events.append(
            {
                "level": level,
                "phase": phase,
                "event": event,
                "message": message,
                "details": details or {},
            }
        ),
    )

    assert trace_events[0]["event"] == "request_started"
    assert trace_events[1]["event"] == "response_received"
    assert trace_events[-1]["event"] == "wp_post_created"


def test_run_create_article_pipeline_passes_recent_article_titles_to_creator(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_call_creator_service(**kwargs):
        captured.update(kwargs)
        return _creator_output_without_images()

    monkeypatch.setattr(automation_service, "call_creator_service", fake_call_creator_service)
    monkeypatch.setattr(
        automation_service,
        "wp_create_post",
        lambda **_kwargs: {"id": 321, "link": "https://publisher.example.com/draft"},
    )
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher.example.com",
        publishing_site_id="site-id",
        client_target_site_id="target-id",
        anchor="Kinderbrille",
        topic=None,
        exclude_topics=["Kinder Sonnenbrillen"],
        recent_article_titles=["Sonnenbrillen fuer Kinder: Welche Kriterien wirklich zaehlen"],
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

    assert captured["exclude_topics"] == ["Kinder Sonnenbrillen"]
    assert captured["recent_article_titles"] == ["Sonnenbrillen fuer Kinder: Welche Kriterien wirklich zaehlen"]


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


def test_call_creator_stream_preserves_error_details(monkeypatch) -> None:
    class _FakeResponse:
        status_code = 200

        def iter_lines(self, decode_unicode=True):
            yield 'event: error'
            yield f'data: {json.dumps({"error": "Phase 4 plan invalid: [\'outline_mixed_intent_or_angle\']", "details": {"creator_output": {"phase3": {"final_article_topic": "Immobilie verkaufen"}, "phase4": {"h1": "Immobilie verkaufen"}, "debug": {"prompt_trace": {"planner": {"mode": "deterministic", "attempts": []}}}}}})}'
            yield ""

        def close(self):
            return None

    monkeypatch.setattr(automation_service.requests, "post", lambda *args, **kwargs: _FakeResponse())

    with pytest.raises(automation_service.AutomationError) as exc_info:
        automation_service._call_creator_stream(
            "https://creator.example.com",
            {"target_site_url": "https://target.example.com", "publishing_site_url": "https://publisher.example.com"},
            5,
            lambda *_args: None,
        )

    assert "Creator pipeline failed: Phase 4 plan invalid" in str(exc_info.value)
    assert exc_info.value.details["creator_output"]["phase3"]["final_article_topic"] == "Immobilie verkaufen"


def test_run_create_article_pipeline_strips_leading_h1_before_publish(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        automation_service,
        "call_creator_service",
        lambda **_kwargs: {
            "phase5": {
                "meta_title": "Immobilie verkaufen",
                "excerpt": "Kurzbeschreibung",
                "slug": "immobilie-verkaufen",
                "article_html": "<h1>Immobilie verkaufen</h1><p>Einleitung.</p><h2>Abschnitt</h2><p>Text.</p>",
            },
            "phase6": {"featured_image": {"prompt": "x", "alt_text": "x"}},
            "images": [],
        },
    )

    def fake_create_post(**kwargs):
        calls["create_post"] = kwargs
        return {"id": 321, "link": "https://publisher.example.com/draft"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create_post)
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher.example.com",
        publishing_site_id="site-id",
        client_target_site_id="target-id",
        anchor="Immobilie verkaufen",
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
        leonardo_api_key="",
        leonardo_base_url="https://leonardo.example.com",
        leonardo_model_id="model-id",
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="",
        category_llm_model="",
        category_llm_max_categories=1,
        category_llm_confidence_threshold=0.5,
    )

    assert calls["create_post"]["clean_html"] == "<p>Einleitung.</p><h2>Abschnitt</h2><p>Text.</p>"
