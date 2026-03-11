from __future__ import annotations

import importlib
import logging
from functools import lru_cache
from typing import Any, Callable, Mapping, Optional
from uuid import UUID

from sqlalchemy.orm import Session

logger = logging.getLogger("portal_backend.publish_notification_hook")


@lru_cache(maxsize=1)
def _load_publish_notification_sender() -> Optional[Callable[..., bool]]:
    module_name = f"{__package__}.publish_notifications" if __package__ else "publish_notifications"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing_name = str(exc.name or "").strip()
        if missing_name in {
            module_name,
            "publish_notifications",
            "mailer",
            f"{__package__}.mailer" if __package__ else "mailer",
        } or missing_name.endswith(".publish_notifications") or missing_name.endswith(".mailer"):
            logger.warning("publish_notification.disabled missing_module=%s", missing_name or module_name)
            return None
        raise

    sender = getattr(module, "send_client_publish_notification", None)
    if not callable(sender):
        logger.warning("publish_notification.disabled invalid_module=%s", module_name)
        return None
    return sender


def send_client_publish_notification(
    db: Session,
    *,
    job_id: UUID,
    post_payload: Optional[Mapping[str, Any]],
) -> bool:
    sender = _load_publish_notification_sender()
    if sender is None:
        return False
    return bool(sender(db, job_id=job_id, post_payload=post_payload))
