Run from the backend app root to populate deterministic site profiles used by Creator redesign.

Examples:

```bash
python -m scripts.site_profiling.sync_site_profiles
python -m scripts.site_profiling.sync_site_profiles --publishing-only
python -m scripts.site_profiling.sync_site_profiles --target-only --force-refresh
```

The script profiles:
- active publishing sites from `publishing_sites`
- target page URLs and derived root URLs from `client_target_sites`

It writes to:
- `site_profile_cache`

The backend also schedules this sync automatically by default every 24 hours.

Relevant env vars:
- `SITE_PROFILE_SYNC_ENABLED=true`
- `SITE_PROFILE_SYNC_INTERVAL_SECONDS=86400`
- `SITE_PROFILE_SYNC_TIMEOUT_SECONDS=10`
- `SITE_PROFILE_SYNC_MAX_PAGES=3`
