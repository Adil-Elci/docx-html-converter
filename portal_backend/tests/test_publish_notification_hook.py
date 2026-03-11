from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api import publish_notification_hook


def test_send_client_publish_notification_returns_false_when_module_is_missing(monkeypatch) -> None:
    def fake_import_module(name: str):
        raise ModuleNotFoundError(name=name)

    publish_notification_hook._load_publish_notification_sender.cache_clear()
    monkeypatch.setattr(publish_notification_hook.importlib, "import_module", fake_import_module)

    result = publish_notification_hook.send_client_publish_notification(
        None,
        job_id=uuid4(),
        post_payload={"status": "publish"},
    )

    assert result is False


def test_send_client_publish_notification_forwards_to_real_sender(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_sender(db, *, job_id, post_payload):
        calls["db"] = db
        calls["job_id"] = job_id
        calls["post_payload"] = post_payload
        return True

    def fake_import_module(name: str):
        return SimpleNamespace(send_client_publish_notification=fake_sender)

    publish_notification_hook._load_publish_notification_sender.cache_clear()
    monkeypatch.setattr(publish_notification_hook.importlib, "import_module", fake_import_module)
    job_id = uuid4()
    payload = {"status": "publish"}

    result = publish_notification_hook.send_client_publish_notification(
        "db-session",
        job_id=job_id,
        post_payload=payload,
    )

    assert result is True
    assert calls == {
        "db": "db-session",
        "job_id": job_id,
        "post_payload": payload,
    }
