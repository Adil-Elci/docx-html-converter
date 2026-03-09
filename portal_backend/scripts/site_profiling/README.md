Run from the backend app root to populate deterministic site profiles used by Creator redesign.

Examples:

```bash
python -m scripts.site_profiling.sync_site_profiles
python -m scripts.site_profiling.sync_site_profiles --publishing-only
python -m scripts.site_profiling.sync_site_profiles --target-only --force-refresh
```

The script profiles:
- active publishing sites from `publishing_sites`
- target URLs from `client_target_sites` when `target_site_url` is set

It writes to:
- `site_profile_cache`
