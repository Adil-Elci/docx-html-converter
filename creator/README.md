# Creator Service

## Run locally
```bash
pip install -r requirements.txt
uvicorn api.server:app --reload --port 8100
```

## Environment
- `CREATOR_LLM_API_KEY` (preferred for LLM calls)
- `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` (used if `CREATOR_LLM_API_KEY` is not set)
- `CREATOR_DATABASE_URL` or `DATABASE_URL` (optional but required for persistent keyword trend caching in Postgres)
- `CREATOR_LLM_BASE_URL` (default: https://api.openai.com/v1, or https://api.anthropic.com/v1 when using Anthropic)
- `CREATOR_LLM_MODEL` (default: gpt-4.1-mini or claude-haiku-4-5-20251001 for Anthropic)
- `CREATOR_LLM_MODEL_PLANNING` / `CREATOR_LLM_MODEL_WRITING` (override per-purpose models)
- `LEONARDO_API_KEY` (required for image generation)
- `LEONARDO_BASE_URL` (default: https://cloud.leonardo.ai/api/rest/v1)
- `CREATOR_IMAGE_GENERATION_ENABLED` (default: false; skips Phase 6 entirely while testing)
- `CREATOR_HTTP_TIMEOUT_SECONDS` (default: 20)
- `CREATOR_HTTP_RETRIES` (default: 0; one fetch attempt, no retry loop)
- `CREATOR_SITE_ANALYSIS_MAX_PAGES` (default: 4; homepage + additional internal pages for richer site-analysis caching)
- `CREATOR_PHASE2_PROMPT_CHARS` (default: 2500)
- `CREATOR_PHASE2_MAX_TOKENS` (default: 400)
- `CREATOR_PHASE4_MAX_ATTEMPTS` (default: 1; one outline attempt, then deterministic fallback)
- `CREATOR_PHASE5_MAX_ATTEMPTS` (default: 1, capped at 2)
- `CREATOR_PHASE5_MAX_TOKENS_ATTEMPT1` (default: 1800)
- `CREATOR_PHASE5_MAX_TOKENS_RETRY` (default: 1200)
- `CREATOR_PHASE5_FALLBACK_EXPAND_PASSES` (default: 0)
- `CREATOR_PHASE7_REPAIR_ATTEMPTS` (default: 0)
- `CREATOR_INTERNAL_LINK_MIN` (default: 2)
- `CREATOR_INTERNAL_LINK_MAX` (default: 4)
- `CREATOR_INTERNAL_LINK_CANDIDATES_MAX` (default: 10)
- `CREATOR_KEYWORD_TRENDS_ENABLED` (default: true; fetches DE query suggestions for keyword enrichment)
- `CREATOR_KEYWORD_TRENDS_TIMEOUT_SECONDS` (default: 4)
- `CREATOR_KEYWORD_TRENDS_MAX_TERMS` (default: 10)
- `CREATOR_KEYWORD_TREND_CACHE_TTL_SECONDS` (default: 604800 = 7 days)

## Tests
```bash
pytest
```

## Notes
- The creator service is standalone and does not run database migrations.
- It fetches and analyzes live target/publishing sites; ensure outbound HTTP is allowed.
- Keyword enrichment uses German Google Suggest (`hl=de`, `gl=de`) and keeps a lightweight in-process cache for repeated queries.
- Every article now includes a mandatory `FAQ` section as the final `H2`, with `Fazit` immediately before it.
- When `portal_backend` provides an indexed internal-link inventory, Creator prefers those same-site article candidates over homepage link extraction.
- Site analysis caching now uses a multi-page site snapshot and reuses older cached summaries/categories/titles as warm context when the live snapshot changes or is temporarily unavailable.
- Keyword trend discovery now checks Postgres first, refreshes stale entries older than 7 days with a live Google Suggest lookup, and falls back to stale cached data only if refresh fails.
- Titles, slugs, and meta descriptions are now built deterministically from the selected keyword/topic package so H1/meta SEO stays consistent across retries.
- Creator enforces stronger on-page SEO validation for exact H1 usage, title/meta-description length, slug quality, internal-link anchor diversity, and required structured content patterns when the topic supports a list or table.
- Creator returns `seo_evaluation` in the final payload/debug output so published-article quality can be compared downstream.
