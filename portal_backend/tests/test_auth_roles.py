from __future__ import annotations

from types import SimpleNamespace

from portal_backend.api import auth


def test_is_admin_role_accepts_super_admin() -> None:
    assert auth.is_admin_role("admin") is True
    assert auth.is_admin_role("super_admin") is True
    assert auth.is_admin_role("client") is False


def test_is_super_admin_requires_designated_email() -> None:
    user = SimpleNamespace(role="super_admin", email="aat@elci.cloud")
    assert auth.is_super_admin(user) is True

    wrong_email = SimpleNamespace(role="super_admin", email="ops@example.com")
    restored_role = SimpleNamespace(role="admin", email="aat@elci.cloud")
    assert auth.is_super_admin(wrong_email) is False
    assert auth.is_super_admin(restored_role) is True


def test_effective_role_for_user_promotes_designated_super_admin_email() -> None:
    restored_user = SimpleNamespace(role="admin", email="aat@elci.cloud")
    assert auth.effective_role_for_user(restored_user) == "super_admin"

    normal_admin = SimpleNamespace(role="admin", email="ops@example.com")
    assert auth.effective_role_for_user(normal_admin) == "admin"
