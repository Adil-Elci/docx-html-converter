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


def test_wp_check_site_access_creates_and_cleans_up_probe_assets(monkeypatch) -> None:
    request_calls: list[tuple[str, str, dict[str, object] | None]] = []
    media_calls: list[dict[str, object]] = []

    def fake_request_json(method: str, url: str, **kwargs):
        request_calls.append((method, url, kwargs.get("json_body")))
        if method == "POST" and url.endswith("/wp-json/wp/v2/posts"):
            return {"id": 321}
        if method == "DELETE" and url.endswith("/wp-json/wp/v2/media/654?force=true"):
            return {"deleted": True}
        if method == "DELETE" and url.endswith("/wp-json/wp/v2/posts/321?force=true"):
            return {"deleted": True}
        raise AssertionError(f"Unexpected request {method} {url}")

    def fake_create_media_item(**kwargs):
        media_calls.append(kwargs)
        return {"id": 654}

    monkeypatch.setattr(automation_service, "_request_json", fake_request_json)
    monkeypatch.setattr(automation_service, "wp_create_media_item", fake_create_media_item)

    result = automation_service.wp_check_site_access(
        site_url="https://publisher.example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="publisher-user",
        wp_app_password="app-password",
        timeout_seconds=9,
    )

    assert result == {"ok": True, "post_id": 321, "media_id": 654}
    assert request_calls[0][0] == "POST"
    assert request_calls[0][1].endswith("/wp-json/wp/v2/posts")
    assert request_calls[0][2]["status"] == "draft"
    assert request_calls[-2][0] == "DELETE"
    assert request_calls[-2][1].endswith("/wp-json/wp/v2/media/654?force=true")
    assert request_calls[-1][0] == "DELETE"
    assert request_calls[-1][1].endswith("/wp-json/wp/v2/posts/321?force=true")
    assert media_calls[0]["data"] == automation_service.ACCESS_CHECK_IMAGE_BYTES
    assert media_calls[0]["content_type"] == "image/png"


def test_wp_check_site_access_cleans_up_post_when_media_upload_fails(monkeypatch) -> None:
    request_calls: list[tuple[str, str]] = []

    def fake_request_json(method: str, url: str, **_kwargs):
        request_calls.append((method, url))
        if method == "POST" and url.endswith("/wp-json/wp/v2/posts"):
            return {"id": 321}
        if method == "DELETE" and url.endswith("/wp-json/wp/v2/posts/321?force=true"):
            return {"deleted": True}
        raise AssertionError(f"Unexpected request {method} {url}")

    monkeypatch.setattr(automation_service, "_request_json", fake_request_json)
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(
            automation_service.AutomationError("WordPress media upload failed, HTTP 403: forbidden")
        ),
    )

    with pytest.raises(automation_service.AutomationError, match="HTTP 403"):
        automation_service.wp_check_site_access(
            site_url="https://publisher.example.com",
            wp_rest_base="/wp-json/wp/v2",
            wp_username="publisher-user",
            wp_app_password="app-password",
            timeout_seconds=9,
        )

    assert ("DELETE", "https://publisher.example.com/wp-json/wp/v2/posts/321?force=true") in request_calls


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
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


@pytest.mark.skip(reason="Legacy 4llm path; tests + production code deleted in Phase 7d")
def test_run_create_article_pipeline_publishes_to_creator_selected_candidate(monkeypatch) -> None:
    captured: dict[str, object] = {}
    creator_calls: dict[str, object] = {}

    def fake_call_creator_service(**kwargs):
        creator_calls.update(kwargs)
        return {
            "host_site_url": "https://publisher-two.example.com",
            "phase5": {
                "meta_title": "Wohnraumplanung fuer kleine Räume",
                "excerpt": "Kurzbeschreibung",
                "slug": "wohnraumplanung-kleine-raeume",
                "article_html": "<p>Artikelinhalt</p>",
            },
            "phase6": {"featured_image": {"prompt": "x", "alt_text": "x"}},
            "images": [],
        }

    def fake_create_post(**kwargs):
        captured.update(kwargs)
        return {"id": 999, "link": "https://publisher-two.example.com/draft"}

    monkeypatch.setattr(automation_service, "call_creator_service", fake_call_creator_service)
    monkeypatch.setattr(automation_service, "wp_create_post", fake_create_post)
    monkeypatch.setattr(
        automation_service,
        "wp_create_media_item",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected media upload")),
    )

    result = automation_service.run_create_article_pipeline(
        creator_endpoint="http://creator.test",
        target_site_url="https://target.example.com",
        publishing_site_url="https://publisher-one.example.com",
        publishing_site_id="site-1",
        client_target_site_id="target-id",
        anchor="Wohnraumplanung",
        topic=None,
        exclude_topics=[],
        recent_article_titles=[],
        internal_link_inventory=[],
        publishing_candidates=[
            {
                "site_url": "https://publisher-one.example.com",
                "site_id": "site-1",
                "fit_score": 62,
                "notes": ["home"],
                "internal_link_inventory": [],
                "publishing_profile_payload": {"normalized_url": "https://publisher-one.example.com"},
                "publishing_profile_content_hash": "hash-1",
                "wp_rest_base": "/wp-json/wp/v2",
                "wp_username": "user-1",
                "wp_app_password": "pass-1",
                "category_ids": [11],
                "category_candidates": [{"id": 11, "name": "Wohnen", "slug": "wohnen"}],
            },
            {
                "site_url": "https://publisher-two.example.com",
                "site_id": "site-2",
                "fit_score": 81,
                "notes": ["home"],
                "internal_link_inventory": [],
                "publishing_profile_payload": {"normalized_url": "https://publisher-two.example.com"},
                "publishing_profile_content_hash": "hash-2",
                "wp_rest_base": "/wp-json/wp/v2",
                "wp_username": "user-2",
                "wp_app_password": "pass-2",
                "category_ids": [22],
                "category_candidates": [{"id": 22, "name": "Haus", "slug": "haus"}],
            },
        ],
        phase1_cache_payload=None,
        phase1_cache_content_hash="",
        phase2_cache_payload=None,
        phase2_cache_content_hash="",
        target_profile_payload=None,
        target_profile_content_hash="",
        publishing_profile_payload={"normalized_url": "https://publisher-one.example.com"},
        publishing_profile_content_hash="hash-1",
        site_url="https://publisher-one.example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user-1",
        wp_app_password="pass-1",
        existing_wp_post_id=None,
        post_status="draft",
        author_id=7,
        category_ids=[11],
        category_candidates=[{"id": 11, "name": "Wohnen", "slug": "wohnen"}],
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

    assert len(creator_calls["publishing_candidates"]) == 2
    assert captured["site_url"] == "https://publisher-two.example.com"
    assert captured["wp_username"] == "user-2"
    assert captured["category_ids"] == [22]
    assert result["selected_site_id"] == "site-2"
    assert result["selected_site_url"] == "https://publisher-two.example.com"


# ---- call_creator_v2_pipeline -------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload, text_override: str | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text_override if text_override is not None else json.dumps(payload, ensure_ascii=False)

    def json(self):
        if isinstance(self._payload, ValueError):
            raise self._payload
        return self._payload


def _v2_happy_payload() -> dict:
    return {
        "ok": True,
        "target_keyword": "steuerberater hamburg",
        "publishing_site_host": "example.de",
        "contract": {"target_keyword": "steuerberater hamburg", "h1": "x"},
        "sections": [],
        "article_html": {"final": "<h1>x</h1>"},
        "judge_scores": None,
        "quality_report": {"passed": True},
    }


def test_call_creator_v2_pipeline_returns_payload(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout, allow_redirects):
        captured["url"] = url
        captured["body"] = json
        captured["timeout"] = timeout
        return _FakeResponse(200, _v2_happy_payload())

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    result = automation_service.call_creator_v2_pipeline(
        creator_endpoint="https://creator.example",
        target_keyword="steuerberater hamburg",
        target_backlink_url="https://client.de/x",
        publishing_site_url="https://example.de",
        anchor_hint="partial_match",
        timeout_seconds=120,
    )
    assert result["ok"] is True
    assert captured["url"] == "https://creator.example/v2/run-pipeline"
    assert captured["body"]["target_keyword"] == "steuerberater hamburg"
    assert captured["body"]["anchor_hint"] == "partial_match"
    assert captured["body"]["skip_voice_pass"] is False
    assert captured["timeout"] == 120


def test_call_creator_v2_pipeline_forwards_skip_flags(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout, allow_redirects):
        captured["body"] = json
        return _FakeResponse(200, _v2_happy_payload())

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    automation_service.call_creator_v2_pipeline(
        creator_endpoint="https://creator.example",
        target_keyword="x x",
        target_backlink_url="https://client.de/y",
        publishing_site_url="https://example.de",
        skip_voice_pass=True,
        skip_judge=True,
        skip_related_keywords=True,
        skip_entity_extraction=True,
    )
    body = captured["body"]
    assert body["skip_voice_pass"] is True
    assert body["skip_judge"] is True
    assert body["skip_related_keywords"] is True
    assert body["skip_entity_extraction"] is True


def test_call_creator_v2_pipeline_raises_on_pipeline_failed(monkeypatch):
    failure = {
        "ok": False,
        "error": "pipeline_failed",
        "phase": "contract",
        "message": "Schema validation failed",
    }

    def fake_post(url, json, timeout, allow_redirects):
        return _FakeResponse(422, failure)

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    with pytest.raises(automation_service.AutomationError, match=r"\[contract\]"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="https://creator.example",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


def test_call_creator_v2_pipeline_raises_on_5xx(monkeypatch):
    def fake_post(url, json, timeout, allow_redirects):
        return _FakeResponse(503, {"error": "service unavailable"})

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    with pytest.raises(automation_service.AutomationError, match="HTTP 503"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="https://creator.example",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


def test_call_creator_v2_pipeline_raises_on_network_error(monkeypatch):
    import requests as requests_module

    def fake_post(url, json, timeout, allow_redirects):
        raise requests_module.ConnectionError("dns")

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    with pytest.raises(automation_service.AutomationError, match="request failed"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="https://creator.example",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


def test_call_creator_v2_pipeline_raises_on_non_json(monkeypatch):
    def fake_post(url, json, timeout, allow_redirects):
        return _FakeResponse(200, ValueError("bad json"), text_override="not json")

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    with pytest.raises(automation_service.AutomationError, match="non-JSON"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="https://creator.example",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


def test_call_creator_v2_pipeline_raises_when_endpoint_missing():
    with pytest.raises(automation_service.AutomationError, match="not configured"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


def test_call_creator_v2_pipeline_raises_when_payload_missing_ok(monkeypatch):
    def fake_post(url, json, timeout, allow_redirects):
        return _FakeResponse(200, {"contract": {}})

    monkeypatch.setattr(automation_service.requests, "post", fake_post)
    with pytest.raises(automation_service.AutomationError, match="malformed payload"):
        automation_service.call_creator_v2_pipeline(
            creator_endpoint="https://creator.example",
            target_keyword="x x",
            target_backlink_url="https://client.de/y",
            publishing_site_url="https://example.de",
        )


# ---- _build_creator_output_for_v2 ---------------------------------------


def _v2_response_with_sections() -> dict:
    return {
        "ok": True,
        "target_keyword": "steuerberater hamburg",
        "target_backlink_url": "https://mandant.de/leistungen",
        "publishing_site_host": "host.de",
        "research": {"top_serp_urls": ["https://a.de", "https://b.de"]},
        "contract": {
            "target_keyword": "steuerberater hamburg",
            "h1": "Steuerberater Hamburg: Ihr Leitfaden",
            "meta_title": "Steuerberater Hamburg",
            "meta_description": "Alles ueber Steuerberatung in Hamburg",
            "slug": "steuerberater-hamburg",
            "competitor_top_urls": ["https://a.de", "https://b.de"],
        },
        "sections": [
            {
                "section_index": 0,
                "h2": "Worauf achten",
                "body_html": "<p>...</p>",
                "links_inserted": [
                    {"anchor_text": "Steuerberater in Hamburg", "target_url": "https://mandant.de/leistungen", "link_type": "backlink"},
                ],
                "word_count": 320,
            },
        ],
        "article_html": {
            "assembled": "<h1>x</h1>",
            "refined_body": "<p>refined</p>",
            "final": "<h1>Steuerberater Hamburg: Ihr Leitfaden</h1><p>...</p>",
        },
        "judge_scores": {"intent_match": 8},
        "quality_report": {"passed": True, "checks": []},
        "skipped_voice_pass": False,
        "skipped_judge": False,
        "notes": ["voice pass succeeded"],
    }


def test_build_creator_output_for_v2_maps_phase5_and_pipeline_state():
    output = automation_service._build_creator_output_for_v2(
        v2_response=_v2_response_with_sections(),
        target_site_url="https://mandant.de",
        selected_site_url="https://host.de",
        selected_site_id="11111111-1111-1111-1111-111111111111",
        target_keyword="steuerberater hamburg",
        article_html="<h1>Steuerberater Hamburg: Ihr Leitfaden</h1><p>...</p>",
    )
    assert output["ok"] is True
    assert output["target_site_url"] == "https://mandant.de"
    assert output["host_site_url"] == "https://host.de"
    assert output["host_site_id"] == "11111111-1111-1111-1111-111111111111"

    phase5 = output["phase5"]
    assert phase5["title"] == "Steuerberater Hamburg: Ihr Leitfaden"
    assert phase5["meta_title"] == "Steuerberater Hamburg"
    assert phase5["meta_description"].startswith("Alles ueber Steuerberatung")
    assert phase5["slug"] == "steuerberater-hamburg"
    assert phase5["article_markdown"] == ""
    assert phase5["linked_markdown"] == ""
    assert phase5["article_html"].startswith("<h1>Steuerberater Hamburg")
    assert phase5["sections"][0]["links_inserted"][0]["target_url"] == "https://mandant.de/leistungen"

    assert output["phase3"]["target_keyword"]["keyword"] == "steuerberater hamburg"
    assert {ref["url"] for ref in output["phase3"]["competitor_references"]} == {"https://a.de", "https://b.de"}

    pipeline_state = output["pipeline_state"]
    assert pipeline_state["v2"] is True
    assert pipeline_state["contract"]["target_keyword"] == "steuerberater hamburg"
    assert pipeline_state["judge_scores"] == {"intent_match": 8}
    assert pipeline_state["selected_publishing_site"] == {
        "site_url": "https://host.de",
        "site_id": "11111111-1111-1111-1111-111111111111",
    }


def test_build_creator_output_for_v2_handles_missing_meta_description():
    response = _v2_response_with_sections()
    response["contract"]["meta_description"] = ""
    output = automation_service._build_creator_output_for_v2(
        v2_response=response,
        target_site_url="https://mandant.de",
        selected_site_url="https://host.de",
        selected_site_id=None,
        target_keyword="steuerberater hamburg",
        article_html="<p>x</p>",
    )
    assert output["phase5"]["excerpt"] == ""
    assert output["host_site_id"] is None


# ---- _derive_keyword_from_target_profile --------------------------------


def test_derive_keyword_prefers_domain_topic():
    profile = {"domain_level_topic": "kinder sonnenbrillen", "primary_context": "shopping"}
    assert automation_service._derive_keyword_from_target_profile(profile) == "kinder sonnenbrillen"


def test_derive_keyword_falls_back_to_topics_list():
    profile = {"topics": [{"label": "uv schutz"}, {"label": "passform"}]}
    assert automation_service._derive_keyword_from_target_profile(profile) == "uv schutz"


def test_derive_keyword_returns_empty_when_nothing_usable():
    assert automation_service._derive_keyword_from_target_profile(None) == ""
    assert automation_service._derive_keyword_from_target_profile({}) == ""


# ---- _run_create_article_pipeline_v2 -----------------------------------


def _common_v2_pipeline_kwargs(**overrides):
    base = dict(
        creator_endpoint="https://creator.example",
        target_site_url="https://mandant.de/leistungen",
        publishing_site_url="https://host.de",
        publishing_site_id="22222222-2222-2222-2222-222222222222",
        publishing_candidates=[],
        internal_link_inventory=[],
        target_profile_payload={"domain_level_topic": "steuerberater"},
        anchor="partial_match",
        topic="steuerberater hamburg",
        site_url="https://host.de",
        wp_rest_base="/wp-json",
        wp_username="admin",
        wp_app_password="pw",
        existing_wp_post_id=None,
        post_status="draft",
        author_id=4,
        category_ids=[1, 2],
        category_candidates=[{"id": 1, "name": "SEO"}, {"id": 2, "name": "Recht"}],
        timeout_seconds=60,
        creator_timeout_seconds=300,
        category_llm_enabled=False,
        category_llm_api_key="",
        category_llm_base_url="https://api.openai.com/v1",
        category_llm_model="gpt-4.1-mini",
        category_llm_max_categories=2,
        category_llm_confidence_threshold=0.55,
    )
    base.update(overrides)
    return base


def test_run_create_article_pipeline_v2_publishes_and_adapts(monkeypatch):
    captured_v2 = {}
    captured_post = {}

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    def fake_create(**kwargs):
        captured_post.update(kwargs)
        return {"id": 9911, "link": "https://host.de/?p=9911"}

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)

    result = automation_service._run_create_article_pipeline_v2(**_common_v2_pipeline_kwargs())

    assert captured_v2["target_keyword"] == "steuerberater hamburg"
    assert captured_v2["target_backlink_url"] == "https://mandant.de/leistungen"
    assert captured_v2["publishing_site_url"] == "https://host.de"
    assert captured_v2["anchor_hint"] == "partial_match"

    assert result["post_event_type"] == "wp_post_created"
    assert result["selected_site_url"] == "https://host.de"
    assert result["image_url"] == ""
    assert result["media_url"] is None

    creator_output = result["creator_output"]
    assert creator_output["pipeline_state"]["v2"] is True
    assert creator_output["phase5"]["sections"][0]["links_inserted"][0]["target_url"] == "https://mandant.de/leistungen"

    # Verify H1 stripped before publish.
    assert "<h1>" not in captured_post["clean_html"]
    assert captured_post["title"] == "Steuerberater Hamburg: Ihr Leitfaden"
    assert captured_post["slug"] == "steuerberater-hamburg"


def test_run_create_article_pipeline_v2_falls_back_to_target_profile_topic(monkeypatch):
    captured_v2 = {}

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(
        automation_service,
        "wp_create_post",
        lambda **kwargs: {"id": 1, "link": "https://host.de/?p=1"},
    )

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(topic=None, target_profile_payload={"domain_level_topic": "kanzlei berlin"}),
    )
    assert captured_v2["target_keyword"] == "kanzlei berlin"


def test_run_create_article_pipeline_v2_raises_when_no_keyword(monkeypatch):
    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_pipeline",
        lambda **kwargs: pytest.fail("v2 should not be called without a keyword"),
    )
    with pytest.raises(automation_service.AutomationError, match="target keyword"):
        automation_service._run_create_article_pipeline_v2(
            **_common_v2_pipeline_kwargs(topic=None, target_profile_payload={}),
        )


def test_run_create_article_pipeline_v2_uses_update_when_post_exists(monkeypatch):
    captured = {}

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())

    def fake_update(**kwargs):
        captured.update(kwargs)
        return {"id": 7, "link": "https://host.de/?p=7"}

    monkeypatch.setattr(automation_service, "wp_update_post", fake_update)

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(existing_wp_post_id=7),
    )
    assert result["post_event_type"] == "wp_post_updated"
    assert captured["post_id"] == 7
    assert captured["featured_media_id"] == 0  # legacy parity: clear featured image on update


def test_run_create_article_pipeline_v2_raises_when_html_empty(monkeypatch):
    response = _v2_response_with_sections()
    response["article_html"] = {"assembled": "", "refined_body": "", "final": ""}
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: response)

    with pytest.raises(automation_service.AutomationError, match="no article HTML"):
        automation_service._run_create_article_pipeline_v2(**_common_v2_pipeline_kwargs())


def test_run_create_article_pipeline_v2_emits_trace_events(monkeypatch):
    events: list[tuple[str, str, str]] = []

    def trace(level, phase, event, message="", details=None):
        events.append((level, phase, event))

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())
    monkeypatch.setattr(
        automation_service,
        "wp_create_post",
        lambda **kwargs: {"id": 1, "link": "https://host.de/?p=1"},
    )

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(trace_event=trace),
    )
    phases = [event for _, phase, event in events if phase == "creator_v2"]
    assert phases == ["start", "complete"]
