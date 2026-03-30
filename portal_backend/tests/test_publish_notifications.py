from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api import publish_notifications


def test_resolve_recipient_emails_prefers_primary_client_email() -> None:
    recipients = publish_notifications._resolve_recipient_emails(
        " Client@Example.com ",
        ["user1@example.com", "user2@example.com"],
    )

    assert recipients == ["client@example.com"]


def test_resolve_recipient_emails_falls_back_to_client_users_and_dedupes() -> None:
    recipients = publish_notifications._resolve_recipient_emails(
        None,
        [" User1@example.com ", "user2@example.com", "user1@example.com", ""],
    )

    assert recipients == ["user1@example.com", "user2@example.com"]


def test_build_publish_notification_message_uses_guest_post_copy() -> None:
    context = publish_notifications.PublishNotificationContext(
        recipients=["client@example.com"],
        client_name="Acme",
        request_kind="submit_article",
        site_name="Publisher One",
        site_url="https://publisher.example.com",
        post_url="https://publisher.example.com/live-post",
        post_title="Live Guest Post",
    )

    subject, body = publish_notifications._build_publish_notification_message(context)

    assert subject == "Your guest post is live: Live Guest Post"
    assert "Your guest post has been published." in body
    assert "Publishing site: Publisher One" in body
    assert "URL: https://publisher.example.com/live-post" in body


def test_post_is_published_rejects_draft_payload_even_if_submission_wants_publish() -> None:
    submission = SimpleNamespace(post_status="publish")

    result = publish_notifications._post_is_published(
        {"status": "draft"},
        submission,
    )

    assert result is False


def test_load_publish_notification_context_returns_none_when_client_disabled(monkeypatch) -> None:
    fake_job = SimpleNamespace(
        id=uuid4(),
        job_status="succeeded",
        requires_admin_approval=False,
        approved_at=None,
        submission_id="submission-id",
        client_id="client-id",
        site_id="site-id",
        wp_post_url="https://publisher.example.com/live-post",
    )
    fake_submission = SimpleNamespace(
        post_status="publish",
        request_kind="submit_article",
        title="Live Guest Post",
    )
    fake_client = SimpleNamespace(
        id="client-id",
        name="Acme",
        email="client@example.com",
        publish_notifications_enabled=False,
    )

    class _FakeEmailQuery:
        def join(self, *_args, **_kwargs):
            return self

        def filter(self, *_args, **_kwargs):
            return self

        def order_by(self, *_args, **_kwargs):
            return self

        def all(self):
            return []

    class _FakeEntityQuery:
        def __init__(self, result):
            self._result = result

        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return self._result

    class _FakeSession:
        def query(self, entity):
            if entity is publish_notifications.Job:
                return _FakeEntityQuery(fake_job)
            if entity is publish_notifications.Submission:
                return _FakeEntityQuery(fake_submission)
            if entity is publish_notifications.Client:
                return _FakeEntityQuery(fake_client)
            if entity is publish_notifications.User.email:
                return _FakeEmailQuery()
            if entity is publish_notifications.Site:
                return _FakeEntityQuery(None)
            raise AssertionError(f"Unexpected query entity: {entity}")

    result = publish_notifications._load_publish_notification_context(
        _FakeSession(),
        job_id=fake_job.id,
        post_payload={"status": "publish", "link": fake_job.wp_post_url},
    )

    assert result is None


def test_send_client_publish_notification_sends_email_when_context_is_available(monkeypatch) -> None:
    context = publish_notifications.PublishNotificationContext(
        recipients=["client@example.com"],
        client_name="Acme",
        request_kind="create_article",
        site_name="Publisher One",
        site_url="https://publisher.example.com",
        post_url="https://publisher.example.com/live-post",
        post_title="Live Post",
    )
    sent: dict[str, object] = {}

    monkeypatch.setattr(
        publish_notifications,
        "_load_publish_notification_context",
        lambda db, *, job_id, post_payload: context,
    )
    monkeypatch.setattr(
        publish_notifications,
        "send_plain_text_email",
        lambda **kwargs: sent.update(kwargs),
    )

    result = publish_notifications.send_client_publish_notification(
        db=object(),
        job_id=uuid4(),
        post_payload={"status": "publish", "link": context.post_url},
    )

    assert result is True
    assert sent["to_emails"] == ["client@example.com"]
    assert sent["subject"] == "Your post is live: Live Post"
    assert "URL: https://publisher.example.com/live-post" in str(sent["body"])


def test_send_client_publish_notification_returns_false_when_context_is_missing(monkeypatch) -> None:
    sent = {"count": 0}

    monkeypatch.setattr(
        publish_notifications,
        "_load_publish_notification_context",
        lambda db, *, job_id, post_payload: None,
    )
    monkeypatch.setattr(
        publish_notifications,
        "send_plain_text_email",
        lambda **kwargs: sent.__setitem__("count", sent["count"] + 1),
    )

    result = publish_notifications.send_client_publish_notification(
        db=object(),
        job_id=uuid4(),
        post_payload={"status": "draft"},
    )

    assert result is False
    assert sent["count"] == 0
