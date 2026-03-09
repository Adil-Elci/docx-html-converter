from __future__ import annotations

import argparse
import logging

try:
    from api.db import get_sessionmaker
    from api.portal_models import ClientTargetSite, Site
    from api.site_profiles import ensure_publishing_site_profile, ensure_target_site_profile
except ImportError:  # pragma: no cover
    from portal_backend.api.db import get_sessionmaker
    from portal_backend.api.portal_models import ClientTargetSite, Site
    from portal_backend.api.site_profiles import ensure_publishing_site_profile, ensure_target_site_profile


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
    with sessionmaker() as db:
        if not args.target_only:
            sites = db.query(Site).filter(Site.status == "active").order_by(Site.name.asc()).all()
            for site in sites:
                try:
                    ensure_publishing_site_profile(
                        db,
                        site=site,
                        timeout_seconds=args.timeout_seconds,
                        max_pages=args.max_pages,
                        force_refresh=args.force_refresh,
                    )
                    db.commit()
                    logger.info("site_profile_sync.publishing.ok site=%s", site.site_url)
                except Exception as exc:  # pragma: no cover
                    db.rollback()
                    logger.warning("site_profile_sync.publishing.failed site=%s error=%s", site.site_url, exc)

        if not args.publishing_only:
            target_sites = (
                db.query(ClientTargetSite)
                .filter(ClientTargetSite.target_site_url.isnot(None))
                .order_by(ClientTargetSite.created_at.asc())
                .all()
            )
            for row in target_sites:
                url = (row.target_site_url or "").strip()
                if not url:
                    continue
                try:
                    ensure_target_site_profile(
                        db,
                        target_site_url=url,
                        client_target_site_id=row.id,
                        timeout_seconds=args.timeout_seconds,
                        max_pages=args.max_pages,
                        force_refresh=args.force_refresh,
                    )
                    db.commit()
                    logger.info("site_profile_sync.target.ok url=%s", url)
                except Exception as exc:  # pragma: no cover
                    db.rollback()
                    logger.warning("site_profile_sync.target.failed url=%s error=%s", url, exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
