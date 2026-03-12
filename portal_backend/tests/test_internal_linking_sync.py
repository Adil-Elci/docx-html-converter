from __future__ import annotations

import requests

from portal_backend.api.internal_linking_sync import _fetch_posts_for_site


class _FakeResponse:
    def __init__(self, *, status_code: int, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {"X-WP-TotalPages": "1"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self):
        return self._payload


def test_fetch_posts_for_site_falls_back_to_public_on_auth_failure(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if params.get("context") == "edit":
            return _FakeResponse(status_code=401, payload={"code": "rest_forbidden"})
        return _FakeResponse(
            status_code=200,
            payload=[
                {
                    "id": 12,
                    "link": "https://example.com/beitrag",
                    "slug": "beitrag",
                    "title": {"rendered": "Beitrag"},
                    "excerpt": {"rendered": "Kurz"},
                    "categories": [7],
                }
            ],
        )

    monkeypatch.setattr("portal_backend.api.internal_linking_sync.requests.get", fake_get)

    posts = _fetch_posts_for_site(
        site_url="https://example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user",
        wp_app_password="pass",
        per_page=100,
        timeout_seconds=10,
    )

    assert len(calls) == 2
    assert calls[0]["params"]["context"] == "edit"
    assert "Authorization" in calls[0]["headers"]
    assert "context" not in calls[1]["params"]
    assert "Authorization" not in calls[1]["headers"]
    assert posts[0]["status"] == "publish"


def test_fetch_posts_for_site_keeps_non_auth_http_errors(monkeypatch):
    def fake_get(url, *, headers, params, timeout):
        return _FakeResponse(status_code=500, payload={"code": "server_error"})

    monkeypatch.setattr("portal_backend.api.internal_linking_sync.requests.get", fake_get)

    try:
        _fetch_posts_for_site(
            site_url="https://example.com",
            wp_rest_base="/wp-json/wp/v2",
            wp_username="user",
            wp_app_password="pass",
            per_page=100,
            timeout_seconds=10,
        )
    except requests.HTTPError as exc:
        assert exc.response.status_code == 500
    else:
        raise AssertionError("Expected HTTPError for non-auth failure.")


def test_fetch_posts_for_site_falls_back_when_authenticated_response_is_not_json(monkeypatch):
    calls = []

    class _NonJsonResponse(_FakeResponse):
        def json(self):
            raise requests.exceptions.JSONDecodeError("Expecting value", "", 0)

    def fake_get(url, *, headers, params, timeout):
        calls.append({"headers": headers, "params": params})
        if params.get("context") == "edit":
            return _NonJsonResponse(status_code=200, payload=None)
        return _FakeResponse(
            status_code=200,
            payload=[
                {
                    "id": 22,
                    "link": "https://example.com/beitrag",
                    "slug": "beitrag",
                    "title": {"rendered": "Beitrag"},
                    "excerpt": {"rendered": "Kurz"},
                    "categories": [],
                }
            ],
        )

    monkeypatch.setattr("portal_backend.api.internal_linking_sync.requests.get", fake_get)

    posts = _fetch_posts_for_site(
        site_url="https://example.com",
        wp_rest_base="/wp-json/wp/v2",
        wp_username="user",
        wp_app_password="pass",
        per_page=100,
        timeout_seconds=10,
    )

    assert len(calls) == 2
    assert calls[0]["params"]["context"] == "edit"
    assert "context" not in calls[1]["params"]
    assert posts[0]["status"] == "publish"
