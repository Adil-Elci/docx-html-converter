from __future__ import annotations

import json

import pytest

from portal_backend.api import automation_service


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
        "language": "de",
        "research": {"top_serp_urls": ["https://a.de", "https://b.de"]},
        "contract": {
            "target_keyword": "steuerberater hamburg",
            "language": "de",
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
            "refined_body": "<h1>Steuerberater Hamburg: Ihr Leitfaden</h1><p>...</p>",
            "final": (
                '<h1>Steuerberater Hamburg: Ihr Leitfaden</h1><p>...</p>'
                '<script type="application/ld+json">{"@type":"Article"}</script>'
                '<script type="application/ld+json">{"@type":"FAQPage"}</script>'
            ),
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
        publishing_candidates=[
            {
                "site_url": "https://host.de",
                "site_id": "22222222-2222-2222-2222-222222222222",
                "fit_score": 80,
                "publishing_profile_payload": {
                    "language": "de",
                    "primary_context": "steuerberatung",
                },
                "wp_rest_base": "/wp-json",
                "wp_username": "admin",
                "wp_app_password": "pw",
                "category_ids": [1, 2],
                "category_candidates": [{"id": 1, "name": "SEO"}, {"id": 2, "name": "Recht"}],
                "internal_link_inventory": [],
                "is_general": False,
            },
        ],
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
    # Late-bind: site is selected AFTER the creator pipeline returns.
    assert captured_v2["publishing_site_url"] is None
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
    # Verify JSON-LD <script> blocks stripped before publish (firewall block).
    assert "<script" not in captured_post["clean_html"]
    assert "FAQPage" not in captured_post["clean_html"]
    # The full schema-included HTML is still preserved upstream for review.
    assert '<script type="application/ld+json">' in creator_output["phase5"]["article_html"]
    assert captured_post["title"] == "Steuerberater Hamburg: Ihr Leitfaden"
    assert captured_post["slug"] == "steuerberater-hamburg"


def test_strip_jsonld_script_blocks_removes_only_script_tags():
    html = (
        '<h1>Title</h1><p>Body</p>'
        '<script type="application/ld+json">{"@type":"Article"}</script>'
        '<p>More body</p>'
        '<script type="application/ld+json">{"@type":"FAQPage"}</script>'
    )
    cleaned = automation_service._strip_jsonld_script_blocks(html)
    assert "<script" not in cleaned
    assert "@type" not in cleaned
    assert "<h1>Title</h1>" in cleaned
    assert "<p>Body</p>" in cleaned
    assert "<p>More body</p>" in cleaned


def test_strip_jsonld_script_blocks_handles_multiline_payloads():
    html = (
        '<p>x</p><script type="application/ld+json">\n'
        '{\n  "@type": "Article",\n  "headline": "h"\n}\n'
        '</script><p>y</p>'
    )
    cleaned = automation_service._strip_jsonld_script_blocks(html)
    assert "<script" not in cleaned
    assert "@type" not in cleaned
    assert "<p>x</p>" in cleaned
    assert "<p>y</p>" in cleaned


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


def test_run_create_article_pipeline_v2_passes_none_keyword_for_creator_derivation(monkeypatch):
    """Phase C: when no topic and no profile fallback, the creator service is
    called with target_keyword=None and is expected to derive it from the
    target_site_url itself."""

    captured_v2 = {}

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(topic=None, target_profile_payload={}),
    )
    assert captured_v2["target_keyword"] is None
    assert captured_v2["target_backlink_url"] == "https://mandant.de/leistungen"


def test_run_create_article_pipeline_v2_raises_when_no_publishing_candidates(monkeypatch):
    """Phase C pre-flight: zero publishing candidates fails before the
    contract spend (~$0.25)."""

    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_pipeline",
        lambda **kwargs: pytest.fail("v2 should not be called without candidates"),
    )
    with pytest.raises(automation_service.AutomationError, match="publishing candidate"):
        automation_service._run_create_article_pipeline_v2(
            **_common_v2_pipeline_kwargs(publishing_candidates=[]),
        )


def test_run_create_article_pipeline_v2_raises_when_no_language_match(monkeypatch):
    """Phase C: candidates exist but none match the article's language."""

    french_response = _v2_response_with_sections()
    french_response["language"] = "fr"
    french_response["contract"]["language"] = "fr"

    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_pipeline",
        lambda **kwargs: french_response,
    )
    with pytest.raises(automation_service.AutomationError, match="language"):
        automation_service._run_create_article_pipeline_v2(
            # All candidates are German -- but the article came back as French.
            **_common_v2_pipeline_kwargs(),
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


# ---- featured image generation in v2 -----------------------------------


def test_build_image_prompt_uses_h1_and_meta_description():
    contract = {
        "h1": "Steuerberater Hamburg: Ihr Leitfaden",
        "meta_description": "Alles ueber Steuerberatung in Hamburg",
        "target_keyword": "steuerberater hamburg",
    }
    prompt = automation_service._build_image_prompt_from_contract(contract)
    assert "Editorial photo illustrating: Steuerberater Hamburg: Ihr Leitfaden" in prompt
    assert "Alles ueber Steuerberatung in Hamburg" in prompt
    # Style + negative directives must be present so Flux Schnell knows to
    # produce hyperrealistic photography without text artifacts.
    assert "Hyperrealistic" in prompt
    assert "No text" in prompt


def test_build_image_prompt_falls_back_to_h1_only():
    contract = {"h1": "X Y", "meta_description": "", "target_keyword": "kw"}
    prompt = automation_service._build_image_prompt_from_contract(contract)
    assert prompt.startswith("Editorial photo illustrating: X Y")
    assert "Hyperrealistic" in prompt
    assert "No text" in prompt


def test_build_image_prompt_falls_back_to_target_keyword():
    contract = {"h1": "", "meta_description": "", "target_keyword": "kanzlei berlin"}
    prompt = automation_service._build_image_prompt_from_contract(contract)
    assert prompt.startswith("Editorial photo illustrating: kanzlei berlin")
    assert "Hyperrealistic" in prompt
    assert "No text" in prompt


def test_run_create_article_pipeline_v2_skips_image_when_no_api_key(monkeypatch):
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())
    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return {"id": 1, "link": "https://host.de/?p=1"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)
    monkeypatch.setattr(
        automation_service,
        "_generate_featured_image_for_v2",
        lambda **kwargs: pytest.fail("image step must not run when leonardo_api_key is empty"),
    )

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(),  # leonardo_api_key default is ""
    )
    assert result["image_url"] == ""
    assert result["media_payload"] == {}
    assert result["media_url"] is None
    assert captured["featured_media_id"] is None  # new post, no image


def test_run_create_article_pipeline_v2_attaches_image_when_api_key_set(monkeypatch):
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())
    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return {"id": 1, "link": "https://host.de/?p=1"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)
    monkeypatch.setattr(
        automation_service,
        "_generate_featured_image_for_v2",
        lambda **kwargs: ("https://leonardo.test/img.jpg", {"id": 4242, "source_url": "https://host.de/wp-content/uploads/img.jpg"}),
    )

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(leonardo_api_key="leo-key"),
    )
    assert result["image_url"] == "https://leonardo.test/img.jpg"
    assert result["media_payload"]["id"] == 4242
    assert result["media_url"] == "https://host.de/wp-content/uploads/img.jpg"
    assert captured["featured_media_id"] == 4242
    # phase6 must carry the prompt for the worker's image_prompt_ok JobEvent.
    phase6 = result["creator_output"]["phase6"]
    assert phase6["featured_image"]["media_id"] == 4242
    assert phase6["featured_image"]["image_url"] == "https://leonardo.test/img.jpg"
    assert phase6["featured_image"]["prompt"].startswith("Editorial photo illustrating: Steuerberater Hamburg")


def test_run_create_article_pipeline_v2_publishes_without_image_on_failure(monkeypatch):
    """Image errors must NOT block publish (parity with legacy 4llm: no image at all)."""

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())
    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return {"id": 1, "link": "https://host.de/?p=1"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)

    def boom(**kwargs):
        raise automation_service.AutomationError("Leonardo HTTP 503: rate limited")

    monkeypatch.setattr(automation_service, "_generate_featured_image_for_v2", boom)

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(leonardo_api_key="leo-key"),
    )
    assert result["image_url"] == ""
    assert result["media_payload"] == {}
    assert captured["featured_media_id"] is None
    assert captured["title"]  # publish still happened


def test_run_create_article_pipeline_v2_skip_image_flag(monkeypatch):
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", lambda **kwargs: _v2_response_with_sections())
    monkeypatch.setattr(
        automation_service,
        "wp_create_post",
        lambda **kwargs: {"id": 1, "link": "https://host.de/?p=1"},
    )
    monkeypatch.setattr(
        automation_service,
        "_generate_featured_image_for_v2",
        lambda **kwargs: pytest.fail("image step must not run when skip_image=True"),
    )
    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(leonardo_api_key="leo-key", skip_image=True),
    )
