#!/usr/bin/env python3
"""Print WP credentials in plaintext for auditing or debugging.

The ORM decrypts values automatically via the EncryptedText TypeDecorator,
so output is always plaintext regardless of what is stored in the database.

Usage:
    cd portal_backend
    WP_CREDENTIAL_ENCRYPTION_KEY=<key> python scripts/show_credentials.py

Optional flags:
    --admin    Show admin credentials instead of application credentials
"""
from __future__ import annotations

import argparse
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

from api.db import get_engine
from api.portal_models import Site, SiteAdminCredential, SiteCredential


def _show_app_credentials(session: Session) -> None:
    rows = (
        session.query(SiteCredential, Site)
        .join(Site, SiteCredential.site_id == Site.id)
        .order_by(Site.name)
        .all()
    )
    if not rows:
        print("No application credentials found.")
        return
    print(f"\n{'Site':<40} {'Username':<30} {'App Password'}")
    print("-" * 110)
    for cred, site in rows:
        print(f"{site.name:<40} {cred.wp_username:<30} {cred.wp_app_password}")


def _show_admin_credentials(session: Session) -> None:
    rows = (
        session.query(SiteAdminCredential, Site)
        .join(Site, SiteAdminCredential.site_id == Site.id)
        .order_by(Site.name)
        .all()
    )
    if not rows:
        print("No admin credentials found.")
        return
    print(f"\n{'Site':<40} {'Admin Username':<30} {'Admin Password'}")
    print("-" * 110)
    for cred, site in rows:
        print(f"{site.name:<40} {cred.wp_admin_username:<30} {cred.wp_admin_password}")


def _run() -> None:
    import os
    if not os.environ.get("WP_CREDENTIAL_ENCRYPTION_KEY", "").strip():
        print("ERROR: WP_CREDENTIAL_ENCRYPTION_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Print WP credentials in plaintext.")
    parser.add_argument("--admin", action="store_true", help="Show admin credentials.")
    args = parser.parse_args()

    engine = get_engine()
    with Session(engine) as session:
        if args.admin:
            _show_admin_credentials(session)
        else:
            _show_app_credentials(session)


if __name__ == "__main__":
    _run()
