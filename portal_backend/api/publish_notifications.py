from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from .mailer import send_plain_text_email
from .portal_models import Client, ClientUser, Job, Site, Submission, User

logger = logging.getLogger("portal_backend.publish_notifications")


@dataclass(frozen=True)
class PublishNotificationContext:
    recipients: list[str]
    client_name: str
    request_kind: str
    site_name: str
    site_url: str
    post_url: str
    post_title: Optional[str]


def _normalize_email(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def _dedupe_emails(values: Iterable[str]) -> list[str]:
    recipients: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _normalize_email(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        recipients.append(cleaned)
    return recipients


def _resolve_recipient_emails(primary_email: Optional[str], fallback_user_emails: Iterable[str]) -> list[str]:
    primary = _normalize_email(primary_email)
    if primary:
        return [primary]
    return _dedupe_emails(fallback_user_emails)


def _request_kind_label(request_kind: str) -> str:
    return "guest post" if (request_kind or "").strip().lower() == "submit_article" else "post"


def _extract_post_title(post_payload: Optional[Mapping[str, Any]], fallback: Optional[str] = None) -> Optional[str]:
    if isinstance(post_payload, Mapping):
        title_value = post_payload.get("title")
        if isinstance(title_value, Mapping):
            rendered = str(title_value.get("rendered") or "").strip()
            if rendered:
                return rendered
        if isinstance(title_value, str) and title_value.strip():
            return title_value.strip()
    cleaned_fallback = str(fallback or "").strip()
    return cleaned_fallback or None


def _post_is_published(post_payload: Optional[Mapping[str, Any]], submission: Submission) -> bool:
    if isinstance(post_payload, Mapping):
        status_value = str(post_payload.get("status") or "").strip().lower()
        if status_value:
            return status_value in {"publish", "published"}
    return str(submission.post_status or "").strip().lower() == "publish"


def _build_publish_notification_message(context: PublishNotificationContext) -> tuple[str, str]:
    item_label = _request_kind_label(context.request_kind)
    greeting_name = context.client_name.strip()
    greeting = f"Hello {greeting_name}," if greeting_name else "Hello,"
    subject_suffix = f": {context.post_title}" if context.post_title else ""
    subject = f"Your {item_label} is live{subject_suffix}"

    lines = [
        greeting,
        "",
        f"Your {item_label} has been published.",
    ]
    if context.post_title:
        lines.append(f"Title: {context.post_title}")
    if context.site_name.strip():
        lines.append(f"Publishing site: {context.site_name.strip()}")
    elif context.site_url.strip():
        lines.append(f"Publishing site: {context.site_url.strip()}")
    lines.extend(
        [
            f"URL: {context.post_url}",
            "",
            "If you have any questions, reply to this email.",
        ]
    )
    return subject, "\n".join(lines)


def _load_publish_notification_context(
    db: Session,
    *,
    job_id: UUID,
    post_payload: Optional[Mapping[str, Any]],
) -> Optional[PublishNotificationContext]:
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job or job.job_status != "succeeded":
        return None
    if bool(job.requires_admin_approval) and job.approved_at is None:
        return None

    submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
    if not submission:
        return None
    if not _post_is_published(post_payload, submission):
        return None

    post_url = str(job.wp_post_url or "").strip()
    if not post_url and isinstance(post_payload, Mapping):
        post_url = str(post_payload.get("link") or "").strip()
    if not post_url:
        return None

    client = db.query(Client).filter(Client.id == job.client_id).first()
    if not client:
        return None
    if not bool(client.publish_notifications_enabled):
        logger.info("publish_notification.skipped_disabled job_id=%s client_id=%s", job_id, client.id)
        return None
    fallback_rows = (
        db.query(User.email)
        .join(ClientUser, ClientUser.user_id == User.id)
        .filter(
            ClientUser.client_id == client.id,
            User.is_active.is_(True),
            User.role == "client",
        )
        .order_by(User.created_at.asc())
        .all()
    )
    recipients = _resolve_recipient_emails(client.email, (row[0] for row in fallback_rows))
    if not recipients:
        logger.warning("publish_notification.skipped_no_recipient job_id=%s client_id=%s", job_id, client.id)
        return None

    site = db.query(Site).filter(Site.id == job.site_id).first()
    site_name = ""
    site_url = ""
    if site:
        site_name = str(site.name or "").strip()
        site_url = str(site.site_url or "").strip()

    return PublishNotificationContext(
        recipients=recipients,
        client_name=str(client.name or "").strip(),
        request_kind=str(submission.request_kind or "").strip().lower(),
        site_name=site_name,
        site_url=site_url,
        post_url=post_url,
        post_title=_extract_post_title(post_payload, submission.title),
    )


def send_client_publish_notification(
    db: Session,
    *,
    job_id: UUID,
    post_payload: Optional[Mapping[str, Any]],
) -> bool:
    context = _load_publish_notification_context(db, job_id=job_id, post_payload=post_payload)
    if context is None:
        return False

    subject, body = _build_publish_notification_message(context)
    send_plain_text_email(
        to_emails=context.recipients,
        subject=subject,
        body=body,
    )
    logger.info(
        "publish_notification.sent job_id=%s recipients=%s post_url=%s",
        job_id,
        ",".join(context.recipients),
        context.post_url,
    )
    return True
