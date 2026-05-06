from __future__ import annotations

import json

import pytest

from portal_backend.api import automation_service


@pytest.fixture(autouse=True)
def _stub_creator_pre_pipeline_calls(monkeypatch):
    """The pre-contract publisher selection step issues real HTTP calls to
    the creator service (derive-topic + select-publisher). In unit tests we
    don't want to hit the network -- stub them with sensible no-op defaults.
    Tests that need to assert selector behaviour can override the stub
    explicitly.
    """

    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_derive_topic",
        lambda **kwargs: {
            "ok": True,
            "target_keyword": "fallback derived keyword",
            "language_code": "de",
            "cache_hit": False,
        },
    )

    def _fake_select(*, candidates, target_keyword, **_kwargs):
        # Default stub: pick the first candidate, no refinement, no_fit=False.
        first = (candidates or [{}])[0]
        site_id = str(first.get("site_id") or "")
        site_url = str(first.get("site_url") or "")
        return {
            "ok": True,
            "best_pick": {
                "site_id": site_id,
                "site_url": site_url,
                "refined_topic": target_keyword,
                "confidence": 0.9,
                "rationale": "stubbed",
            },
            "no_fit": False,
            "soft_passed": False,
            "ranking": [
                {"site_id": site_id, "site_url": site_url, "fit_score": 0.9, "rationale": "stubbed"}
            ],
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_select_publisher",
        _fake_select,
    )

    # Brainstorm runs only when no explicit topic is provided. Stub it as
    # empty by default so existing tests (which all set topic explicitly)
    # don't make HTTP calls. Tests that need to assert brainstorm wiring
    # can override this stub.
    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_brainstorm_topics",
        lambda **kwargs: {"ok": True, "angles": [], "cost_usd": 0.0},
    )


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


def test_derive_keyword_skips_page_title_so_derive_topic_runs():
    """A raw page_title like the brillenhaus24 case is too noisy to use as a
    keyword; force the pipeline to fall through to /v2/derive-topic."""

    profile = {"page_title": "Brillenhaus24.de - Ihr Onlineshop fuer guenstige Brillen"}
    assert automation_service._derive_keyword_from_target_profile(profile) == ""


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
    # Publisher is locked in BEFORE the creator pipeline now (selector ran
    # against the deterministic shortlist and returned the chosen site).
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


def test_run_create_article_pipeline_v2_derives_topic_upfront_when_missing(monkeypatch):
    """Phase D: when no topic and no profile fallback, portal_backend calls
    /v2/derive-topic BEFORE /v2/run-pipeline so the publisher-fit refiner can
    run against the derived keyword without wasting the contract budget."""

    captured_v2 = {}
    captured_derive: dict[str, object] = {}

    def fake_derive(**kwargs):
        captured_derive.update(kwargs)
        return {
            "ok": True,
            "target_keyword": "günstige brillen online kaufen",
            "language_code": "de",
            "cache_hit": False,
        }

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_derive_topic", fake_derive)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(topic=None, target_profile_payload={}),
    )
    # Portal_backend resolves the keyword upfront via /v2/derive-topic, then
    # passes it explicitly to /v2/run-pipeline.
    assert captured_derive["target_url"] == "https://mandant.de/leistungen"
    assert captured_v2["target_keyword"] == "günstige brillen online kaufen"
    assert captured_v2["language"] == "de"


def test_run_create_article_pipeline_v2_raises_when_no_publishing_candidates_and_no_explicit_site(monkeypatch):
    """Phase C pre-flight: only fails when neither auto-discovered candidates
    NOR an explicit publishing_site_url are available."""

    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_pipeline",
        lambda **kwargs: pytest.fail("v2 should not be called without candidates"),
    )
    with pytest.raises(automation_service.AutomationError, match="publishing candidate"):
        automation_service._run_create_article_pipeline_v2(
            **_common_v2_pipeline_kwargs(
                publishing_candidates=[],
                publishing_site_url="",
                publishing_site_id=None,
                site_url="",
            ),
        )


def test_run_create_article_pipeline_v2_brainstorms_when_no_explicit_topic(monkeypatch):
    """Phase E: when no explicit topic, the brainstorm runs after fit-refine
    and the auto-picked editorial angle (top of the ranked list) flows into
    /v2/run-pipeline. The angle's keyword overrides the derived/refined one."""

    captured_v2 = {}
    captured_brainstorm: dict = {}

    def fake_derive(**kwargs):
        return {
            "ok": True,
            "target_keyword": "günstige brillen online kaufen",
            "language_code": "de",
            "cache_hit": False,
        }

    def fake_brainstorm(**kwargs):
        captured_brainstorm.update(kwargs)
        return {
            "ok": True,
            "angles": [
                {
                    "title": "Kurzsichtigkeit bei Kindern: Warum immer mehr Grundschüler eine Brille brauchen",
                    "target_keyword": "kurzsichtigkeit kinder",
                    "hook": "Studien zeigen einen alarmierenden Trend.",
                    "rationale": "Trend-Story passt zur Eltern-Audience.",
                },
                {
                    "title": "Bildschirmzeit und Augengesundheit",
                    "target_keyword": "bildschirmzeit kinder augen",
                    "hook": "Was tun, wenn die Augen müde werden?",
                    "rationale": "Aktuelles Thema.",
                },
            ],
            "cost_usd": 0.02,
        }

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_derive_topic", fake_derive)
    monkeypatch.setattr(automation_service, "call_creator_v2_brainstorm_topics", fake_brainstorm)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(
            topic=None,
            target_profile_payload={},
            publishing_candidates=[
                {
                    "site_url": "https://kidsblatt.de",
                    "site_id": "k-id",
                    "fit_score": 30,
                    "publishing_profile_payload": {
                        "language": "de",
                        "primary_context": "kids and family",
                    },
                    "wp_rest_base": "/wp-json",
                    "wp_username": "admin",
                    "wp_app_password": "pw",
                    "category_ids": [],
                    "category_candidates": [],
                    "internal_link_inventory": [],
                    "is_general": False,
                },
            ],
        ),
    )

    # Brainstorm got the publisher profile so the LLM could pick a
    # publisher-relevant angle.
    assert captured_brainstorm["target_url"] == "https://mandant.de/leistungen"
    assert captured_brainstorm["publishing_profile_payload"]["primary_context"] == "kids and family"
    # The auto-picked top angle's keyword overrides the derived one and
    # the angle metadata flows into the pipeline call.
    assert captured_v2["target_keyword"] == "kurzsichtigkeit kinder"
    angle = captured_v2["editorial_angle"]
    assert angle["title"].startswith("Kurzsichtigkeit")
    assert "Studien" in angle["hook"]


def test_run_create_article_pipeline_v2_rejects_title_shaped_brainstorm_keyword(monkeypatch):
    """Regression for the brillenhaus DataForSEO 40501 failure: when
    brainstorm hands back a title-shaped string in target_keyword (LLM drift),
    the portal must keep the cleaner upfront keyword instead of forwarding
    the title to /v2/run-pipeline (which would hit DataForSEO and fail)."""

    captured_v2: dict[str, object] = {}

    def fake_derive(**_kwargs):
        return {
            "ok": True,
            "target_keyword": "kinderbrillen",
            "language_code": "de",
            "cache_hit": False,
        }

    def fake_brainstorm(**_kwargs):
        return {
            "ok": True,
            "angles": [
                {
                    "title": "Augengesundheit und Sehhilfen: Wie die richtige Brille passt",
                    # LLM put a title in the keyword field by mistake.
                    "target_keyword": "Augengesundheit und Sehhilfen: Wie die richtige Brille passt",
                    "hook": "h",
                    "rationale": "r",
                },
            ],
            "cost_usd": 0.02,
        }

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_derive_topic", fake_derive)
    monkeypatch.setattr(automation_service, "call_creator_v2_brainstorm_topics", fake_brainstorm)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(
            topic=None,
            target_profile_payload={},
            publishing_candidates=[
                {
                    "site_url": "https://kidsblatt.de",
                    "site_id": "k-id",
                    "fit_score": 30,
                    "publishing_profile_payload": {"language": "de", "primary_context": "kids"},
                    "wp_rest_base": "/wp-json",
                    "wp_username": "admin",
                    "wp_app_password": "pw",
                    "category_ids": [],
                    "category_candidates": [],
                    "internal_link_inventory": [],
                    "is_general": False,
                },
            ],
        ),
    )
    # Title-shaped keyword was rejected; pipeline saw the clean upfront one.
    assert captured_v2["target_keyword"] == "kinderbrillen"
    # The angle metadata still flows through (title is still useful for the contract).
    assert captured_v2["editorial_angle"]["title"].startswith("Augengesundheit")


def test_looks_like_seo_keyword_helper():
    assert automation_service._looks_like_seo_keyword("kinderbrillen") is True
    assert automation_service._looks_like_seo_keyword("kinderbrillen kaufen") is True
    assert automation_service._looks_like_seo_keyword("steuerberater hamburg altona") is True
    # Title-shaped strings get rejected.
    assert automation_service._looks_like_seo_keyword("Was Eltern wissen muessen: Der grosse Ratgeber") is False
    assert automation_service._looks_like_seo_keyword("Brille fuer Kinder - alle Fakten") is False
    assert automation_service._looks_like_seo_keyword("Brille | Shop | Online") is False
    assert automation_service._looks_like_seo_keyword("Was kostet eine Brille?") is False
    # Empty or whitespace-only inputs are also rejected.
    assert automation_service._looks_like_seo_keyword("") is False
    assert automation_service._looks_like_seo_keyword("   ") is False


def test_run_create_article_pipeline_v2_skips_brainstorm_when_explicit_topic(monkeypatch):
    """When the admin pinned an explicit topic in the webhook, the brainstorm
    is skipped — we respect the explicit choice and don't override it."""

    captured_v2 = {}
    brainstorm_calls = {"count": 0}

    def fake_brainstorm(**kwargs):
        brainstorm_calls["count"] += 1
        return {"ok": True, "angles": [{"title": "X", "target_keyword": "x", "hook": "h", "rationale": "r"}], "cost_usd": 0.02}

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_brainstorm_topics", fake_brainstorm)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(topic="explicit admin topic"),
    )
    assert brainstorm_calls["count"] == 0
    assert captured_v2["target_keyword"] == "explicit admin topic"
    assert captured_v2.get("editorial_angle") is None


def test_run_create_article_pipeline_v2_selector_picks_winner_and_refines_topic(monkeypatch):
    """Reproduces brillenhaus24 -> kidsblatt: shortlist contains an off-topic
    Klimaschutz site and a family magazine. The new selector picks the family
    magazine, refines the topic to 'kinderbrillen', and the contract step
    runs against the chosen publisher (no late-binding)."""

    captured_v2: dict[str, object] = {}
    captured_select: dict[str, object] = {}

    def fake_select(**kwargs):
        captured_select.update(kwargs)
        return {
            "ok": True,
            "best_pick": {
                "site_id": "kids-id",
                "site_url": "https://kidsblatt.de",
                "refined_topic": "kinderbrillen online kaufen",
                "confidence": 0.86,
                "rationale": "family-magazine audience fits kids' eyewear angle",
            },
            "no_fit": False,
            "soft_passed": False,
            "ranking": [
                {"site_id": "kids-id", "site_url": "https://kidsblatt.de", "fit_score": 0.86, "rationale": "family fit"},
                {"site_id": "solar-id", "site_url": "https://solar.de", "fit_score": 0.1, "rationale": "no overlap"},
            ],
            "cost_usd": 0.005,
        }

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_select_publisher", fake_select)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    captured_post: dict[str, object] = {}

    def fake_create(**kwargs):
        captured_post.update(kwargs)
        return {"id": 1, "link": "x"}

    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(
            topic="günstige brillen online kaufen",
            # Caller's author_id is 4 (matches the originally-associated site,
            # not the chosen publisher). The publish call must use the chosen
            # candidate's author_id (12) instead -- otherwise WP 400's with
            # rest_invalid_author.
            author_id=4,
            publishing_candidates=[
                {
                    "site_url": "https://solar.de",
                    "site_id": "solar-id",
                    "fit_score": 60,
                    "publishing_profile_payload": {
                        "language": "de",
                        "primary_context": "klimaschutz und solar",
                    },
                    "wp_rest_base": "/wp-json",
                    "wp_username": "admin",
                    "wp_app_password": "pw",
                    "author_id": 7,  # solar.de's WP user
                    "category_ids": [],
                    "category_candidates": [],
                    "internal_link_inventory": [],
                    "is_general": False,
                },
                {
                    "site_url": "https://kidsblatt.de",
                    "site_id": "kids-id",
                    "fit_score": 40,
                    "publishing_profile_payload": {
                        "language": "de",
                        "primary_context": "kids and family",
                        "topics": ["parenting", "schule"],
                    },
                    "wp_rest_base": "/wp-json",
                    "wp_username": "kidsblatt-admin",
                    "wp_app_password": "kids-pw",
                    "author_id": 12,  # kidsblatt's WP user
                    "category_ids": [11],
                    "category_candidates": [{"id": 11, "name": "Familie"}],
                    "internal_link_inventory": [],
                    "is_general": False,
                },
            ],
        ),
    )
    # Selector saw the full shortlist with both candidates.
    selector_ids = [c["site_id"] for c in captured_select["candidates"]]
    assert "solar-id" in selector_ids
    assert "kids-id" in selector_ids
    # Pipeline ran with the SELECTOR's chosen publisher locked in -- not the
    # deterministic top (solar) but the LLM-picked one (kids).
    assert captured_v2["publishing_site_url"] == "https://kidsblatt.de"
    # Pipeline used the refined topic, not the original buying-guide keyword.
    assert captured_v2["target_keyword"] == "kinderbrillen online kaufen"
    # Publish step routes to the chosen publisher's WP credentials AND author_id.
    assert result["selected_site_url"] == "https://kidsblatt.de"
    assert captured_post["wp_username"] == "kidsblatt-admin"
    assert captured_post["author_id"] == 12  # not 4 (caller default), not 7 (solar)


def test_run_create_article_pipeline_v2_falls_back_to_allgemein_when_no_fit(monkeypatch):
    """When the selector returns ``no_fit=true``, the pipeline falls back to a
    candidate flagged ``is_general`` instead of hard-failing."""

    captured_v2: dict[str, object] = {}

    def fake_select(**_kwargs):
        return {
            "ok": True,
            "best_pick": {"site_id": "", "site_url": "", "refined_topic": "", "confidence": 0.1, "rationale": "no overlap"},
            "no_fit": True,
            "soft_passed": False,
            "ranking": [],
            "cost_usd": 0.005,
        }

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_select_publisher", fake_select)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(
            topic="adult product xyz",
            publishing_candidates=[
                {
                    "site_url": "https://kidsblatt.de",
                    "site_id": "k-id",
                    "fit_score": 30,
                    "publishing_profile_payload": {"primary_context": "kids only"},
                    "wp_rest_base": "/wp-json",
                    "wp_username": "admin",
                    "wp_app_password": "pw",
                    "category_ids": [],
                    "category_candidates": [],
                    "internal_link_inventory": [],
                    "is_general": False,
                },
                {
                    "site_url": "https://allgemein.de",
                    "site_id": "g-id",
                    "fit_score": 10,
                    "publishing_profile_payload": {"primary_context": "lifestyle"},
                    "wp_rest_base": "/wp-json",
                    "wp_username": "g-admin",
                    "wp_app_password": "g-pw",
                    "category_ids": [],
                    "category_candidates": [],
                    "internal_link_inventory": [],
                    "is_general": True,
                },
            ],
        ),
    )
    assert captured_v2["publishing_site_url"] == "https://allgemein.de"
    assert result["selected_site_url"] == "https://allgemein.de"


def test_run_create_article_pipeline_v2_hard_fails_when_no_fit_and_no_general(monkeypatch):
    """No editorial fit and no Allgemein candidate -> hard-fail before the
    contract spend with a clear admin-facing message."""

    def fake_select(**_kwargs):
        return {
            "ok": True,
            "best_pick": {"site_id": "", "refined_topic": "", "confidence": 0.0, "rationale": "adult product on kids site"},
            "no_fit": True,
            "soft_passed": False,
            "ranking": [],
            "cost_usd": 0.005,
        }

    monkeypatch.setattr(automation_service, "call_creator_v2_select_publisher", fake_select)
    monkeypatch.setattr(
        automation_service,
        "call_creator_v2_pipeline",
        lambda **kwargs: pytest.fail("v2 pipeline must not run when no fit + no general fallback"),
    )

    with pytest.raises(automation_service.AutomationError, match="no editorial fit"):
        automation_service._run_create_article_pipeline_v2(
            **_common_v2_pipeline_kwargs(
                topic="adult product xyz",
                publishing_candidates=[
                    {
                        "site_url": "https://kidsblatt.de",
                        "site_id": "k-id",
                        "fit_score": 30,
                        "publishing_profile_payload": {"primary_context": "kids only"},
                        "wp_rest_base": "/wp-json",
                        "wp_username": "admin",
                        "wp_app_password": "pw",
                        "category_ids": [],
                        "category_candidates": [],
                        "internal_link_inventory": [],
                        "is_general": False,
                    },
                ],
            ),
        )


def test_run_create_article_pipeline_v2_falls_back_to_top_candidate_when_selector_unavailable(monkeypatch):
    """When the selector call itself raises (infra failure), the pipeline
    keeps running with the deterministic top candidate -- network blips
    must not block real work."""

    captured_v2: dict[str, object] = {}

    def fake_select(**_kwargs):
        raise automation_service.AutomationError("Creator publisher-selector request failed: timeout")

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    monkeypatch.setattr(automation_service, "call_creator_v2_select_publisher", fake_select)
    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", lambda **kw: {"id": 1, "link": "x"})

    automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(),
    )
    # The pipeline still ran; deterministic top of the shortlist (host.de) wins.
    assert captured_v2["publishing_site_url"] == "https://host.de"


def test_run_create_article_pipeline_v2_synthesises_explicit_publishing_site(monkeypatch):
    """When auto-discovery is empty but the user explicitly picked a publishing
    site, we synthesise a single candidate from the explicit selection so the
    pipeline can run."""

    captured_v2 = {}

    def fake_v2(**kwargs):
        captured_v2.update(kwargs)
        return _v2_response_with_sections()

    captured_post = {}

    def fake_create(**kwargs):
        captured_post.update(kwargs)
        return {"id": 9911, "link": "https://kidsblatt.de/?p=9911"}

    monkeypatch.setattr(automation_service, "call_creator_v2_pipeline", fake_v2)
    monkeypatch.setattr(automation_service, "wp_create_post", fake_create)

    result = automation_service._run_create_article_pipeline_v2(
        **_common_v2_pipeline_kwargs(
            publishing_candidates=[],  # no auto-discovery
            publishing_site_url="https://kidsblatt.de",
            publishing_site_id="33333333-3333-3333-3333-333333333333",
            site_url="https://kidsblatt.de",
        ),
    )

    # Pipeline must run end-to-end and publish to the explicitly-chosen site.
    assert captured_v2["target_keyword"] == "steuerberater hamburg"
    assert result["selected_site_url"] == "https://kidsblatt.de"
    assert result["post_event_type"] == "wp_post_created"


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


# ---- apply_image_style_directives (used by the regenerate-image flow) -----


def test_apply_image_style_directives_appends_to_legacy_prompt():
    """Old jobs that stored a minimal prompt get upgraded to Flux directives."""

    legacy = "Featured image for article titled: Steuerberater Hamburg"
    out = automation_service.apply_image_style_directives(legacy)
    assert out.startswith(legacy)
    assert "Hyperrealistic editorial photograph" in out
    assert "No text" in out


def test_apply_image_style_directives_is_idempotent_on_v2_prompts():
    """v2 prompts already include the directives -- don't double-append."""

    contract = {"h1": "Steuerberater Hamburg", "meta_description": "Beratung"}
    v2_prompt = automation_service._build_image_prompt_from_contract(contract)
    out = automation_service.apply_image_style_directives(v2_prompt)
    assert out == v2_prompt
    # Sanity: only one style directive block in the final string.
    assert out.count("Hyperrealistic editorial photograph") == 1


def test_apply_image_style_directives_handles_empty_input():
    out = automation_service.apply_image_style_directives("")
    assert "Editorial photo" in out
    assert "Hyperrealistic" in out
    assert "No text" in out


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


# ---- _request_json retry behaviour ---------------------------------------


class _FakeHttpResponse:
    """Stand-in for requests.Response with the fields _request_json reads."""

    def __init__(self, status_code: int, payload=None, headers=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text or json.dumps(self._payload)

    def json(self):
        if isinstance(self._payload, ValueError):
            raise self._payload
        return self._payload


def test_request_json_retries_503_and_eventually_succeeds(monkeypatch):
    """Brillenhaus regression: a transient 503 from a shared WP host must
    NOT lose the article. _request_json now retries 5xx-transient codes
    with exponential backoff before raising."""

    calls = {"count": 0}
    sleeps: list[float] = []

    def fake_request(**kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            return _FakeHttpResponse(503, text="<html>Error 503</html>")
        return _FakeHttpResponse(200, payload={"id": 9911})

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: sleeps.append(s))

    result = automation_service._request_json(
        "POST",
        "https://wp.example/wp-json/wp/v2/posts",
        json_body={"title": "x"},
    )
    assert result == {"id": 9911}
    assert calls["count"] == 3  # two 503 retries + final 200
    # Exponential backoff: 2s then 4s.
    assert sleeps == [2.0, 4.0]


def test_request_json_does_not_retry_4xx(monkeypatch):
    """4xx errors are permanent client problems -- bad credentials, bad
    payload -- and must fail loud on the first try."""

    calls = {"count": 0}

    def fake_request(**_kwargs):
        calls["count"] += 1
        return _FakeHttpResponse(401, text="Unauthorized")

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: None)

    with pytest.raises(automation_service.AutomationError, match="HTTP 401"):
        automation_service._request_json("GET", "https://wp.example/")
    assert calls["count"] == 1


def test_request_json_retries_429_rate_limit(monkeypatch):
    calls = {"count": 0}

    def fake_request(**_kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            return _FakeHttpResponse(429, text="Too Many Requests")
        return _FakeHttpResponse(200, payload={"ok": True})

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: None)

    result = automation_service._request_json("GET", "https://wp.example/")
    assert result == {"ok": True}
    assert calls["count"] == 2


def test_request_json_raises_after_retry_exhaustion(monkeypatch):
    """If all attempts hit transient errors, surface a clear error that
    names the attempt count so logs make the failure mode obvious."""

    calls = {"count": 0}

    def fake_request(**_kwargs):
        calls["count"] += 1
        return _FakeHttpResponse(503, text="<html>Error 503</html>")

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: None)

    with pytest.raises(automation_service.AutomationError, match="after 3 attempts"):
        automation_service._request_json("POST", "https://wp.example/")
    assert calls["count"] == 3


def test_request_json_retries_connection_errors(monkeypatch):
    """Network blips (timeouts, DNS hiccups) get the same retry treatment
    as 5xx -- they're symmetrically transient."""

    calls = {"count": 0}

    def fake_request(**_kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            raise automation_service.requests.ConnectionError("dns failure")
        return _FakeHttpResponse(200, payload={"id": 1})

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: None)

    result = automation_service._request_json("GET", "https://wp.example/")
    assert result == {"id": 1}
    assert calls["count"] == 2


def test_request_json_max_attempts_one_disables_retry(monkeypatch):
    """Callers that want hard-fail-on-first-error can opt out via
    max_attempts=1."""

    calls = {"count": 0}

    def fake_request(**_kwargs):
        calls["count"] += 1
        return _FakeHttpResponse(503, text="<html>Error 503</html>")

    monkeypatch.setattr(automation_service.requests, "request", fake_request)
    monkeypatch.setattr(automation_service.time, "sleep", lambda s: None)

    with pytest.raises(automation_service.AutomationError, match="HTTP 503"):
        automation_service._request_json("GET", "https://wp.example/", max_attempts=1)
    assert calls["count"] == 1
