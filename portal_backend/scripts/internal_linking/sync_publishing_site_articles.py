from __future__ import annotations

import argparse
import logging

try:
    from portal_backend.api.db import get_sessionmaker
    from portal_backend.api.internal_linking_sync import run_internal_link_inventory_sync
except ModuleNotFoundError:
    from api.db import get_sessionmaker
    from api.internal_linking_sync import run_internal_link_inventory_sync


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync published WordPress posts into the publishing-site article index.")
    parser.add_argument("--site-url", dest="site_url", default="", help="Only sync one publishing site URL.")
    parser.add_argument("--per-page", dest="per_page", type=int, default=100)
    parser.add_argument("--timeout-seconds", dest="timeout_seconds", type=int, default=20)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    summary = run_internal_link_inventory_sync(
        get_sessionmaker(),
        site_url_filter=args.site_url.strip() or None,
        per_page=args.per_page,
        timeout_seconds=args.timeout_seconds,
    )
    logging.getLogger("portal_backend.scripts.internal_linking").info("sync_complete summary=%s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
