from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.automation_service import AutomationError
from portal_backend.api.routers import sites_routes


def _site_row(name: str, site_url: str):
    site = SimpleNamespace(
        id=uuid4(),
        name=name,
        site_url=site_url,
        wp_rest_base="/wp-json/wp/v2",
    )
    credential = SimpleNamespace(
        wp_username=f"{name.lower()}-user",
        wp_app_password="app-password",
    )
    return site, credential


def test_run_site_access_checks_returns_success_summary() -> None:
    rows = [
        _site_row("Alpha", "https://alpha.example.com"),
        _site_row("Bravo", "https://bravo.example.com"),
    ]
    calls: list[dict[str, object]] = []

    def fake_checker(**kwargs):
        calls.append(kwargs)
        return {"ok": True}

    result = sites_routes._run_site_access_checks(rows, checker=fake_checker, timeout_seconds=11)

    assert result.tested_count == 2
    assert result.accessible_count == 2
    assert result.failed_count == 0
    assert result.failures == []
    assert calls[0]["timeout_seconds"] == 11
    assert calls[1]["site_url"] == "https://bravo.example.com"


def test_run_site_access_checks_collects_failures() -> None:
    rows = [
        _site_row("Alpha", "https://alpha.example.com"),
        _site_row("Blocked", "https://blocked.example.com"),
    ]

    def fake_checker(**kwargs):
        if kwargs["site_url"] == "https://blocked.example.com":
            raise AutomationError("WordPress media upload failed, HTTP 403: forbidden")
        return {"ok": True}

    result = sites_routes._run_site_access_checks(rows, checker=fake_checker, timeout_seconds=11)

    assert result.tested_count == 2
    assert result.accessible_count == 1
    assert result.failed_count == 1
    assert len(result.failures) == 1
    assert result.failures[0].site_name == "Blocked"
    assert result.failures[0].site_url == "https://blocked.example.com"
    assert "HTTP 403" in result.failures[0].error
