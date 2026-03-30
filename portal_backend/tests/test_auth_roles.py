from __future__ import annotations

from types import SimpleNamespace

from portal_backend.api import auth


def test_is_admin_role_accepts_super_admin() -> None:
    assert auth.is_admin_role("admin") is True
    assert auth.is_admin_role("super_admin") is True
    assert auth.is_admin_role("client") is False


def test_is_super_admin_requires_email_and_role() -> None:
    user = SimpleNamespace(role="super_admin", email="aat@elci.cloud")
    assert auth.is_super_admin(user) is True

    wrong_email = SimpleNamespace(role="super_admin", email="ops@example.com")
    wrong_role = SimpleNamespace(role="admin", email="aat@elci.cloud")
    assert auth.is_super_admin(wrong_email) is False
    assert auth.is_super_admin(wrong_role) is False
