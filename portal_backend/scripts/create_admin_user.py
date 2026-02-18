#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy.orm import Session

SCRIPT_DIR = Path(__file__).resolve().parent
PORTAL_BACKEND_DIR = SCRIPT_DIR.parent
if str(PORTAL_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(PORTAL_BACKEND_DIR))

from api.auth import hash_password  # noqa: E402
from api.db import get_sessionmaker  # noqa: E402
from api.portal_models import User  # noqa: E402


def _upsert_admin_user(session: Session, *, email: str, password: str, force_reset_password: bool) -> str:
    normalized_email = email.strip().lower()
    user = session.query(User).filter(User.email == normalized_email).first()

    if user is None:
        user = User(
            email=normalized_email,
            password_hash=hash_password(password),
            role="admin",
            is_active=True,
        )
        session.add(user)
        session.commit()
        return f"created admin user email={normalized_email}"

    changed = False
    if user.role != "admin":
        user.role = "admin"
        changed = True
    if not user.is_active:
        user.is_active = True
        changed = True
    if force_reset_password:
        user.password_hash = hash_password(password)
        changed = True

    if changed:
        session.add(user)
        session.commit()
        return f"updated admin user email={normalized_email}"
    return f"no change email={normalized_email}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or update an admin user for portal auth.")
    parser.add_argument("--email", required=True, help="Admin email.")
    parser.add_argument("--password", required=True, help="Admin password.")
    parser.add_argument(
        "--force-reset-password",
        action="store_true",
        help="Reset password hash if user already exists.",
    )
    args = parser.parse_args()

    email = args.email.strip().lower()
    if not email:
        raise RuntimeError("--email must not be empty.")
    password = args.password.strip()
    if len(password) < 8:
        raise RuntimeError("--password must be at least 8 characters.")

    session = get_sessionmaker()()
    try:
        result = _upsert_admin_user(
            session,
            email=email,
            password=password,
            force_reset_password=args.force_reset_password,
        )
        print(result)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())

