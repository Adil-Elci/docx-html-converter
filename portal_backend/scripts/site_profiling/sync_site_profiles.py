from __future__ import annotations

import argparse
import logging

try:
    from api.db import get_sessionmaker
    from api.site_profile_sync import run_site_profile_sync
except ImportError:  # pragma: no cover
    from portal_backend.api.db import get_sessionmaker
    from portal_backend.api.site_profile_sync import run_site_profile_sync


logger = logging.getLogger("portal_backend.site_profile_sync")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Populate site_profile_cache for publishing and target sites.")
    parser.add_argument("--publishing-only", action="store_true", help="Sync publishing sites only.")
    parser.add_argument("--target-only", action="store_true", help="Sync target sites only.")
    parser.add_argument("--force-refresh", action="store_true", help="Refresh even if a cached profile already exists.")
    parser.add_argument("--timeout-seconds", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=3)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.publishing_only and args.target_only:
        parser.error("--publishing-only and --target-only cannot be used together.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sessionmaker = get_sessionmaker()
    summary = run_site_profile_sync(
        sessionmaker,
        publishing_only=args.publishing_only,
        target_only=args.target_only,
        force_refresh=args.force_refresh,
        timeout_seconds=args.timeout_seconds,
        max_pages=args.max_pages,
    )
    logger.info("site_profile_sync.complete summary=%s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
