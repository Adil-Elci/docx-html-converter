from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.routers.clients_routes import _normalize_target_site_payloads
from portal_backend.api.routers.clients_routes import _client_to_out
from portal_backend.api.portal_schemas import ClientTargetSiteIn


def test_normalize_target_site_payloads_derives_root_url_from_page_url() -> None:
    rows = _normalize_target_site_payloads(
        [
            ClientTargetSiteIn(
                target_site_url="https://www.example.com/path/to/page",
                is_primary=True,
            )
        ],
        legacy_primary_domain=None,
        legacy_backlink_url=None,
    )

    assert len(rows) == 1
    assert rows[0]["target_site_domain"] == "example.com"
    assert rows[0]["target_site_url"] == "https://www.example.com/path/to/page"
    assert rows[0]["target_site_root_url"] == "https://www.example.com"


def test_normalize_target_site_payloads_uses_root_url_when_only_domain_is_provided() -> None:
    rows = _normalize_target_site_payloads(
        [
            ClientTargetSiteIn(
                target_site_domain="example.com",
                is_primary=True,
            )
        ],
        legacy_primary_domain=None,
        legacy_backlink_url=None,
    )

    assert len(rows) == 1
    assert rows[0]["target_site_domain"] == "example.com"
    assert rows[0]["target_site_url"] == "https://example.com"
    assert rows[0]["target_site_root_url"] == "https://example.com"


def test_client_to_out_includes_publish_notifications_flag() -> None:
    now = datetime.now(timezone.utc)
    client = SimpleNamespace(
        id=uuid4(),
        name="Acme",
        primary_domain="example.com",
        backlink_url="https://example.com",
        email="client@example.com",
        phone_number=None,
        publish_notifications_enabled=False,
        status="active",
        created_at=now,
        updated_at=now,
    )

    result = _client_to_out(client, [])

    assert result.publish_notifications_enabled is False
