#!/usr/bin/env python3
"""One-time migration: encrypt plaintext WP passwords already stored in the database.

Run once after deploying the EncryptedText column change. Safe to re-run —
values that are already encrypted are decrypted first (via the TypeDecorator),
then re-encrypted, producing a fresh Fernet token. Net effect is idempotent.

Usage:
    cd portal_backend
    WP_CREDENTIAL_ENCRYPTION_KEY=<key> python scripts/encrypt_wp_credentials.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent))

try:
    from ssh_tunnel_helper import setup_ssh_tunnel
    setup_ssh_tunnel()
except ImportError:
    pass

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from api.db import get_engine
from api.portal_models import SiteAdminCredential, SiteCredential


def _run() -> None:
    if not os.environ.get("WP_CREDENTIAL_ENCRYPTION_KEY", "").strip():
        print("ERROR: WP_CREDENTIAL_ENCRYPTION_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    engine = get_engine()
    with Session(engine) as session:
        creds = session.query(SiteCredential).all()
        for cred in creds:
            flag_modified(cred, "wp_app_password")
        session.commit()
        print(f"publishing_site_credentials: encrypted {len(creds)} rows.")

        admin_creds = session.query(SiteAdminCredential).all()
        for cred in admin_creds:
            flag_modified(cred, "wp_admin_username")
            flag_modified(cred, "wp_admin_password")
        session.commit()
        print(f"publishing_site_admin_credentials: encrypted {len(admin_creds)} rows.")

    print("Done.")


if __name__ == "__main__":
    _run()
