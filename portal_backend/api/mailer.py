from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Iterable


def _read_int_env(name: str, default: int, minimum: int) -> int:
    raw_value = (os.getenv(name) or str(default)).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = default
    return max(parsed, minimum)


def _smtp_port() -> int:
    return _read_int_env("SMTP_PORT", 587, 1)


def _smtp_use_tls() -> bool:
    return (os.getenv("SMTP_USE_TLS") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _smtp_use_ssl() -> bool:
    return (os.getenv("SMTP_USE_SSL") or "false").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_recipient_list(to_emails: Iterable[str]) -> list[str]:
    recipients: list[str] = []
    seen: set[str] = set()
    for raw_email in to_emails:
        cleaned = str(raw_email or "").strip().lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        recipients.append(cleaned)
    return recipients


def send_plain_text_email(*, to_emails: Iterable[str], subject: str, body: str) -> None:
    recipients = _normalize_recipient_list(to_emails)
    if not recipients:
        raise ValueError("to_emails must contain at least one recipient.")

    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_username = (os.getenv("SMTP_USERNAME") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()
    from_email = (os.getenv("SMTP_FROM_EMAIL") or smtp_username).strip()
    from_name = (os.getenv("SMTP_FROM_NAME") or "Elci Solutions").strip()

    if not smtp_host or not from_email:
        raise RuntimeError("SMTP_HOST and SMTP_FROM_EMAIL (or SMTP_USERNAME) are required for email delivery.")

    message = EmailMessage()
    message["Subject"] = subject.strip()
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    port = _smtp_port()
    context = ssl.create_default_context()
    if _smtp_use_ssl():
        with smtplib.SMTP_SSL(smtp_host, port, timeout=20, context=context) as smtp:
            if smtp_username:
                smtp.login(smtp_username, smtp_password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(smtp_host, port, timeout=20) as smtp:
        if _smtp_use_tls():
            smtp.starttls(context=context)
        if smtp_username:
            smtp.login(smtp_username, smtp_password)
        smtp.send_message(message)
