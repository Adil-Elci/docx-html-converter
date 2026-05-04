# Creator Rebuild — Plan & State

**Branch:** `creator-rebuild` · **Last updated:** 2026-05-04 · **Last commit:** `Phase 7d (legacy 4llm code removed; Phase 7 complete)`

> Living document. Update as part of every commit on this branch. When fresh sessions start, read this first.

## Goal

Replace the legacy/supervisor brain-loop creator pipeline with a research-grounded, contract-driven pipeline that produces objectively-high-quality German SEO guest posts at $0.50–$1.00 per article. Quality is measured by an objective rubric, not subjective taste.

## Why this rebuild

The previous "4 LLM" plan (built with ChatGPT/Codex) optimized the wrong axis. Counting LLM calls is not what determines quality — research depth, prompt focus, and validation tightness are. Specific prior issues: placeholder-then-link-replace produces unnatural anchors; deterministic-only site selection is brittle; no SERP-driven outline; no entity coverage; no German AI-tell control; markdown intermediate format (we publish to WP); three-pipeline coexistence (legacy/supervisor/4llm) created maintenance rot.

## Architecture (target)

```
RESEARCH (deterministic + Haiku) → ContentContract (Opus 4.7 + extended thinking, immutable)
        → Section writers (Sonnet 4.6, parallel, prompt-cached)
        → Voice & coherence pass (Sonnet 4.6)
        → Deterministic enforcer (no LLM)
        → Review surface for human approve
        → WP publish
```

**Single pipeline. No mode flags. Contract is immutable once generated.**

## Decisions log (don't relitigate)

- **German tone**: `Sie` everywhere. Hardcoded in Contract (`GermanTone.SIE`).
- **Schema markup**: `Article` + `FAQPage` only. Skip `BreadcrumbList` and `Person`.
- **AI-tell handling**: option (c) — auto-repair on enforcer trigger + telemetry, log every trigger so prompt regression is visible.
- **Models**: Sonnet 4.6 with extended thinking (Contract; was Opus 4.7 — switched 2026-05-03 for cost; override via `CREATOR_CONTRACT_MODEL=claude-opus-4-7` if quality regresses) · Sonnet 4.6 (sections + voice) · Haiku 4.5 (research helpers + LLM judge).
- **Locale**: Germany (`location_code=2276`, `language_code="de"`) pinned in DataForSEO.
- **Eval = objective rubric**, not user-curated gold articles. Rubric grounded in published SEO guidelines + measurable axes.
- **Cost target**: ~$0.25/article with Sonnet contract default (~$0.50 if reverting to Opus) — Sonnet contract + 5×Sonnet sections w/ caching + Haiku helpers + DataForSEO + image.
- **Branch strategy**: rebuild lives on `creator-rebuild`. Always push after commit. Open PR when ready to merge.

## State of the rebuild

### Phase 0 — Demolition · ✅ DONE (`43041ce`)
Deleted: `pipeline.py` (14,930 lines), `supervisor.py`, `critic.py`, `repair.py`, `writer.py`, `validators.py`, `decision_schemas.py`, their tests, `test_automation_service_4llm.py`. Stripped `CREATOR_PIPELINE_MODE` env var and all branching from portal_backend. Dropped `jobs.pipeline_mode` column (alembic 0048).

### Phase 1 — Scaffolds · ✅ DONE (`43041ce`)
- `creator/api/contract.py` — `ContentContract` Pydantic schema (immutable per-article spec).
- `creator/api/prompt_registry.py` — file-based versioned prompt loader.
- `creator/prompts/README.md` — prompt versioning conventions.
- `creator/api/eval_harness.py` — objective rubric (12 deterministic + LLM-judged stubs).

### Phase 2 — Research layer · ✅ DONE (Phase 2c done at `3324091`)
- **2a** `aab42a8`: `dataforseo.py` typed client (SERP, keyword volume, related keywords). Live integration verified.
- **2b** `6d68816`: `serp_scrape.py` (competitor body fetch + heading/schema/link extraction) + `entity_extract.py` (Haiku entity extraction with regex verification).
- **2c** `3324091`: `research.py` orchestrator (`ResearchPayload`) + `topical_gap` helper. Wired 2 of 5 LLM-judged eval stubs (`topical_entity_coverage`, `paa_coverage`) deterministically against research.

### Phase 3 — ContentContract generator · ✅ COMPLETE
- **3a** ✅: `creator/api/contract_generator.py` (Opus 4.7 + extended thinking, 4000-token thinking budget, temperature=1.0 as required). Prompt `creator/prompts/contract_generator/v1.md`. Inline `call_opus_with_thinking` because shared `_call_anthropic` doesn't pass the `thinking` parameter (promote to `llm.py` if reused). Schema embedded in system prompt for caching.
- **3b** ✅: `creator/api/eval_judge.py` — single Haiku 4.5 call producing all three judge scores (intent_match, backlink_anchor_naturalness, eeat_signal_density) with one-sentence German rationales. Thresholds: 7/7/6 of 10. Wired into `eval_harness.evaluate()` via optional `judge_scores=` param; falls back to stubs when omitted. Prompt `creator/prompts/eval_judge/v1.md`.
- **3c** ✅: integration test (`creator/tests/test_pipeline_integration.py`) exercises research → contract → evaluate end-to-end against mocks. Smoke script `creator/scripts/smoke_pipeline.py` runs the chain LIVE on real APIs (~$0.35/run) with explicit confirmation before the Opus call. **94 tests passing.**

**Eval harness scorecard at end of Phase 3:**

| Check axis | Source | Status |
|---|---|---|
| 12 deterministic SEO checks (keyword density, word count, links, anchors, schema, AI-tells, German readability, etc.) | code | ✅ |
| `topical_entity_coverage` | research-driven | ✅ |
| `paa_coverage` | research-driven | ✅ |
| `intent_match` | Haiku judge | ✅ |
| `backlink_anchor_naturalness` | Haiku judge | ✅ |
| `eeat_signal_density` | Haiku judge | ✅ |

### Phase 4 — Section writer · ✅ COMPLETE
- **4a** ✅: `creator/api/section_writer.py` — one Sonnet 4.6 call per H2 section, parallel execution via `ThreadPoolExecutor` (default 4 workers; serial fallback for single-section contracts). Output validated against `SectionDraft` Pydantic model with `body_html`, `links_inserted`, `word_count`. Backlink injection routed to the section whose `section_index` matches the contract's `link_plan` entry. Required entities filtered by `placement_hint` text matching ("section N"). Prompt v1 hardcodes German Sie tone, AI-tell blocklist, structural-tag whitelist, and explicit anchor-strategy rules.
- **4b** ✅: `creator/api/article_assembler.py` — pure deterministic stitcher. Sorts `SectionDraft`s by index (out-of-range indices silently dropped), prepends `<h1>`, appends an FAQ block (heading customizable), emits Article + FAQPage JSON-LD `<script>` blocks gated on `contract.schema_spec`. FAQ answer text in JSON-LD is HTML-stripped to plain text (Google requires this); article HTML uses raw `answer_outline`. HTML-escapes user-controlled headings (defense-in-depth).
- **4c** ✅: prompt caching on the section system prompt. `call_llm_json` and `call_llm_text` now accept `cache_system: bool = False` which routes through `_call_anthropic` to convert the `system` field from a plain string into a list-form content block with `cache_control: {type: "ephemeral"}`. `section_writer.write_section` always passes `cache_system=True`, so within the 5-min cache TTL the second through Nth sections of an article (and any retries) get a 90% input-token discount on the system prompt. **138 tests passing.**

### Phase 5 — Voice & coherence pass · ✅ COMPLETE
`creator/api/voice_pass.py` — single Sonnet 4.6 call that takes the assembled article HTML + contract and returns a refined version. Strict preservation rules in the prompt: H1/H2/H3 text, all `<a href>` URLs and anchor texts, all numbers/statistics/dates, all named entities, all `<table>`/`<ul>`/`<ol>` structures, all `<script>` blocks must remain unchanged. Only `<p>` prose is editable. Cached system prompt (cache_system=True). Output post-processed to strip stray `\`\`\`html` codeblock wrapping. Hard validation: every `href=` URL from input must appear in output, otherwise raises `VoicePassValidationError` — voice-pass losing the backlink is the kind of silent regression we want loud. Override model via `CREATOR_VOICE_MODEL`. Prompt `creator/prompts/voice_pass/v1.md` includes a German AI-tell substitution table (Darüber hinaus → Außerdem, etc.). 20 new tests; full creator suite at 156 passing.

### Phase 6 — Enforcer + pipeline orchestrator + review surface · 🔄 IN PROGRESS
- **6a** ✅: `creator/api/pipeline_runner.py` — `run_pipeline()` chains all seven phases (research → contract → sections → assemble → voice → judge → eval) and returns a `PipelineRun` dataclass with every intermediate artifact. Each step wrapped in try/except that re-raises as `PipelineError` with phase label, so a single failure surfaces with context. Optional `skip_voice_pass` and `skip_judge` flags for faster cheaper runs during dev. Schema blocks come from the deterministic assembler and are re-attached AFTER voice pass (voice pass operates on body HTML only, so schema is never at risk of LLM mangling). 17 new tests; full suite at 173 passing.
- **6b** ✅: `creator/scripts/smoke_full_pipeline.py` — runs the pipeline live for one keyword + target URL and saves every artifact to a timestamped directory under `creator/smoke_outputs/` (research.json, contract.json, sections.json, three article HTML variants — assembled / refined / final, judge_scores.json, quality_report.json, summary.json). Asks for confirmation before any spend; supports `--skip-voice-pass`, `--skip-judge`, `--skip-related-keywords`, `--skip-entity-extraction` for cheaper dev runs. Output dir gitignored.
- **6c** 🔜 review-card HTML renderer that takes a `PipelineRun` and produces a one-page review surface.

### Phase 7 — Wire into portal_backend · ✅ COMPLETE
- **7a** ✅: `POST /v2/run-pipeline` endpoint on creator service. Single HTTP call wraps `pipeline_runner.run_pipeline()`; request schema is `V2RunPipelineRequest` (target_keyword, target_backlink_url, publishing_site_url, optional anchor_hint / canonical_url / four skip flags). Response is the full PipelineRun serialized as JSON (research, contract, sections, three article HTML variants — assembled / refined_body / final, judge_scores, quality_report). On failure returns HTTP 422 with `{ok: false, error: "pipeline_failed", phase, message}`. Long-running (~30s end-to-end), so callers must use timeout ≥ 120s. Legacy `/site-understanding` / `/draft-article` / `/integrate-links` / `/generate-meta` endpoints kept functional for now — portal_backend still calls those until 7b lands. 6 new tests; full suite at 179 passing. Added `httpx>=0.27.0` to creator/requirements.txt for FastAPI TestClient.
- **7b** 🔄 IN PROGRESS — replace 4llm orchestration with /v2/run-pipeline calls, in three sub-steps:
  - **7b-1** ✅: `automation_service.call_creator_v2_pipeline()` HTTP client. POSTs to `/v2/run-pipeline`, returns parsed dict, raises `AutomationError` with phase label on failure (`Creator v2 pipeline failed at phase [contract]: ...`). 8 new tests + 7 pre-existing legacy tests skipped (they were broken since Phase 0; will be deleted in 7d).
  - **7b-2** ✅: audited `_run_create_article_pipeline_4llm` (`automation_service.py:1976-2324`) and the worker's `_mark_creator_success` / `_persist_failed_creator_output` (`automation_worker.py:1258-1527`). Two clarifications vs the original plan:
    - **No image generation in 4llm.** Legacy returns `image_url=""`, `media_payload={}`, posts with `featured_media_id=0` (or `None`). Image generation is only in `run_submit_article_pipeline` (the converter flow). 7b-3 doesn't need to "split out" image gen — just match the no-image behavior. Image integration is deferred (could become Phase 8).
    - **Markdown link extraction breaks on v2.** Worker at line 1499 calls `_extract_markdown_links(linked_markdown)` to populate `PlacedLink` rows. v2 produces HTML, no markdown. 7b-3 must either populate `linked_markdown=""` and route link records from `SectionDraft.links_inserted`, or parse `<a href>` out of `final_html`. Cleaner: use `links_inserted` directly.

    **Legacy creator_output → adapter mapping** (top-level keys consumed downstream):
    | key | downstream consumer | v2 source |
    | --- | --- | --- |
    | `ok` | `_persist_failed_creator_output` setdefault | hardcode `True` |
    | `target_site_url` | CreatorOutput row, TargetSitePage filter, link classification | request param |
    | `host_site_url` | site sync, JobEvents, CreatorOutput row, link classification | selected publishing site URL |
    | `host_site_id` | persisted | selected publishing site UUID |
    | `phase1` (= site_understanding) | event "site_understood", `scraped_pages` → TargetSitePage rows, `language` | empty `{}` (v2 has no target-site analysis) |
    | `phase1_cache_meta` | conditional cache write | omit (skips cache write) |
    | `phase2` | event payload | `{selected_publishing_site_url, selected_publishing_site_id}` |
    | `phase2_cache_meta` | conditional cache write | omit |
    | `phase3.target_keyword.keyword` | event "keyword_research_complete" | `contract.target_keyword` |
    | `phase3.competitor_references` | persisted in payload | `contract.competitor_top_urls` |
    | `phase4.content_brief` | persisted in payload | minimal stub from contract |
    | `phase5.title` / `meta_title` / `meta_description` / `slug` | event "converter_ok", WP post | from `contract` |
    | `phase5.excerpt` | WP post | first 220 chars of meta_description (or refined HTML intro) |
    | `phase5.article_markdown` / `linked_markdown` | `_extract_markdown_links` for PlacedLinks; CreatorOutput payload | `""` (v2 has no markdown) |
    | `phase5.article_html` | `extract_draft_article_html` → CreatorOutput.draft_article_html | `article_html.final` |
    | `phase5.quality_report` | persisted | `quality_report` (PipelineRun) |
    | `phase6.featured_image.prompt` | event "image_prompt_ok" | omit (no image) |
    | `debug.quality_report` | event "quality_checked" | `quality_report` |
    | `debug.prompt_trace` / `creator_trace` / `backend_trace` | trace normalization | omit; backend_trace still appended by worker |
    | `pipeline_state` | persisted to `job.pipeline_state` | `{v2: True, research, contract, quality_report, judge_scores}` |

    **Side effects 7b-3 must drive directly (not via creator_output):**
    - `wp_create_post` / `wp_update_post` with `clean_html=_strip_leading_h1_from_article_html(final_html)` — the contract's H1 is already inside the assembled article, so we still strip it before posting (legacy parity).
    - Optional category LLM selection (unchanged from legacy).
    - PlacedLink rows: build from `sections[*].links_inserted` instead of regex over markdown.
    - `selected_site_id` / `selected_site_url` / `post_payload` / `post_event_type` / `selected_category_ids` returned in pipeline_result for `_mark_creator_success`.

  - **7b-3** ✅: `_run_create_article_pipeline_v2()` and `_build_creator_output_for_v2()` in `automation_service.py`. `run_create_article_pipeline()` now dispatches to v2 (legacy `_run_create_article_pipeline_4llm` preserved until 7d). Implementation choices:
    - **Keyword source**: prefer `payload.get("topic")`; fall back to `_derive_keyword_from_target_profile()` which reads `target_profile_payload["domain_level_topic"]` → `primary_context` → `page_title` → `topics[0]`. Raises `AutomationError` if nothing usable.
    - **Backlink URL**: `target_site_url` from the webhook (the URL the link points to).
    - **Anchor hint**: `payload.get("anchor")` passed straight through to `call_creator_v2_pipeline(anchor_hint=…)`.
    - **Site selection**: still uses `_select_publish_target_for_4llm()` against a synthetic site_understanding (`{main_topic: target_keyword, primary_niche: target_profile.primary_context, language: "de"}`). Keeps the deterministic candidate match working without needing the legacy /site-understanding call.
    - **Image generation**: skipped, matching legacy 4llm behavior. Posts with `featured_media_id=0` on update / `None` on create.
    - **Worker PlacedLinks** (`automation_worker.py:1461`): branches on `pipeline_state.v2`. v2 path reads `creator_output.phase5.sections[*].links_inserted` (each entry: `{anchor_text, target_url, link_type}`) and writes `PlacedLink` rows directly — no markdown parsing. Legacy 4llm branch unchanged. JobEvents (`site_matched`, `keyword_research_complete`, `link_mapping_complete`, `content_brief_ready`, `quality_checked` with judge_scores, `review_ready`) emit with `source="v2"`.
    - **Tests**: 11 new tests in `test_automation_service.py` covering the adapter, keyword fallback chain, full v2 pipeline flow (create + update post paths), the `no-keyword` and `empty-html` failure modes, and trace event emission. **Suite green: creator 179 passing, portal 104 passing + 7 pre-existing skipped.**
- **7c** ✅: end-to-end test through the live admin portal worked after a string of real-world fixes:
  - **Section JSON parsing — multi-line HTML** (`d280d74`): standard `json.loads` rejects raw control chars (`\n`, `\t`, `\r`) inside string values; `body_html` legitimately contains literal newlines between `<li>` items. Switched all three `json.loads` sites in `_extract_json` to `strict=False`.
  - **Section JSON parsing — German typographic quotes** (`2b9edf9`): unconditional smart-quote → ASCII translation in `_normalize_json_text` was destroying valid quotes inside string values (`„Steuerberater"` → `"Steuerberater"`, breaking parsing). Split the helper: `_strip_json_envelope` removes BOM/fences only; smart-quote translation is now a fallback path, not always-applied.
  - **Section LLM resilience** (`c3fee5f`): `call_llm_json` retries default 1, with `"LLM returned invalid JSON"` and `"LLM response missing content"` added to the retryable-error set. `_extract_json` logs the head/tail of raw text on parse failure for next-time triage. `section_writer.DEFAULT_MAX_TOKENS` 2400 → 4000 to remove truncation risk.
  - **NinjaFirewall blocking JSON-LD** (`c5def9f`): the v2 article assembler emits Article + FAQPage `<script type="application/ld+json">` blocks, and WP firewalls (NinjaFirewall, Wordfence) block any POST containing `<script>` as XSS. v2 publish now uses `article_html.refined_body` (no schema) and runs `_strip_jsonld_script_blocks()` defensively. Schema-included `final` is still preserved in `creator_output.phase5.article_html` for the review surface.
  - **Fernet credential decryption was missing from main** (`bc1b7e9`): the live DB stored `wp_app_password` / `wp_admin_username` / `wp_admin_password` as Fernet ciphertext (encrypted by the `b88f018` one-time migration on branch `codex/task-board-sort-updated-at`), but **that branch never merged into `main`**. The deployed application was reading ciphertext and sending it as the WP basic-auth credential — HTTP 401 every time. Restored `credential_crypto.py` (the `EncryptedText` TypeDecorator), wired it into `portal_models.py` for the three columns, added `cryptography` to requirements. `WP_CREDENTIAL_ENCRYPTION_KEY` is read from env. 5 new unit tests cover round-trip + plaintext fallback + missing-key behavior.
- **7d** ✅: deleted the legacy 4llm code surface in one pass.
  - **Creator service**: removed `creator/api/four_llm.py`, `creator/api/four_llm_schemas.py`, `creator/tests/test_four_llm.py`, and the four endpoint handlers (`/site-understanding`, `/draft-article`, `/integrate-links`, `/generate-meta`) from `creator/api/server.py`. Kept `/v2/run-pipeline` and `/health`.
  - **Portal_backend**: deleted ~1,160 lines from `automation_service.py` — `_run_create_article_pipeline_4llm`, `_build_creator_output_for_4llm`, `_build_content_brief_4llm`, `_select_target_keyword`, `_call_dataforseo_keyword`, `_scrape_competitor_reference`, `_select_internal_link_candidates` / `_select_target_page_candidates` / `_select_cross_network_candidates`, `_build_outline`, `_recommended_word_count`, the markdown family (`markdown_to_html`, `_extract_markdown_*`, `_render_inline_markdown`), `_build_quality_report`, `_validate_links`, `_classify_link`, `_keyword_density`, `_run_copyscape_check`, `_call_creator_4llm_endpoint` + the four `call_creator_*` shims, plus the deferred `call_creator_service` and `_call_creator_stream` orphans. Dropped the `from .four_llm_schemas import …` and `from bs4 import BeautifulSoup` imports. Deleted `portal_backend/api/four_llm_schemas.py`.
  - **Worker**: removed the legacy 4llm post-processing branch in `_mark_creator_success` (`scraped_pages → TargetSitePage` rows + markdown link extraction + duplicated JobEvents). Dropped the now-unused `_extract_markdown_links` helper and `linked_markdown` extraction. Cleaned up `import re`, `import hashlib`, and `TargetSitePage` from `portal_models` import (no longer referenced).
  - **Tests**: deleted the 7 skipped legacy tests + `test_call_creator_stream_preserves_error_details` (tested a deleted helper) + the two legacy fixtures (`_creator_output_without_images`, `_creator_output_without_prompt_trace`) from `test_automation_service.py`. **Suite is now 293 passing, 0 skipped** (was 296 passing + 7 skipped at end of 7c). Net: deleted ~1,800 lines of code + ~2,000 lines of dead-test scaffolding while **gaining** 8 v2-path tests vs 7 skipped legacy tests.
  - **Kept (intentionally)**: `_select_publish_target_for_4llm` (used by v2 with a synthetic site_understanding — name is now misleading, will rename in cleanup); `call_creator_pair_fit` and the `_select_best_accepted_pair` route logic (unreachable in production today since the creator `/pair-fit` endpoint is gone, but the pair-fit selection logic is still useful and the deferred-items list said keep). `_select_publish_target` (line 164 in automation_service.py — a separate, still-used helper).

## Deferred items / follow-ups

- **Cache `ResearchPayload` to `seo_research_cache`** — needs CHECK-constraint migration for `cache_kind='research_payload'`. Costs $0.05/uncached run. Will land as its own commit.
- **`_select_best_accepted_pair` + `call_creator_pair_fit`** — currently unreachable from production (creator `/pair-fit` endpoint was deleted with 7d). The selection logic and its 9 tests are kept as a building block for v2-aware pair-fit ranking. Revisit when redesigning auto-site-selection.
- **Rename `_select_publish_target_for_4llm` → `_select_publish_target_for_v2`** — name is now misleading after 7d; it's the only deterministic site matcher in production. One-line rename + import update.
- **Backport remaining security hardening from `b88f018`** — that commit (on `codex/task-board-sort-updated-at`) brought in more than the Fernet TypeDecorator we restored. Still missing on `main`:
  - Login rate limiter only trusts `X-Forwarded-For` from `TRUSTED_PROXY_IPS`. Without this, an attacker can spoof the source IP to defeat per-IP rate limiting.
  - Password-reset endpoint returns a neutral response regardless of whether the email exists. Without this, the endpoint leaks account-existence info via timing/response.
  - Other items in that commit message: master-site-sync hardening, ssh_tunnel_helper.py, db_updater report gitignore.
  Audit `b88f018`'s diff against current `main`, cherry-pick only the security pieces that aren't already present.
- **Live env file cleanup** — Dokploy env vars on the live `portal_backend` and `creator` services likely still hold legacy variables that no longer have any code reading them (e.g. `CREATOR_PIPELINE_MODE` was removed in Phase 0; legacy 4llm-only flags from before the rebuild). Walk the live env, cross-check against the codebase, and prune anything that's no longer referenced. Confirm `WP_CREDENTIAL_ENCRYPTION_KEY` (portal) and `ANTHROPIC_API_KEY` / `DATAFORSEO_LOGIN` / `DATAFORSEO_PASSWORD` (creator) are present, since those are now load-bearing.
- **Phase 6c — review-card HTML renderer** — was the last open item from Phase 6. Skipped during 7c because the live admin portal already exposes `quality_report` + `judge_scores` via the existing job-detail page. Revisit only if a richer review surface is needed.
- **Phase 8 — featured image for v2** — v2 publishes with no featured image (legacy 4llm parity). If we want images on creator-mode articles, wire a Leonardo image generation step around the v2 pipeline.

## External services & env vars

```
ANTHROPIC_API_KEY         # console.anthropic.com (NOT Max plan; production usage burns Tier credits)
DATAFORSEO_LOGIN          # account email
DATAFORSEO_PASSWORD       # API key from API Access page (not account password)
LEONARDO_API_KEY          # later, for Phase 6 image generation wiring
```

`creator/.env` is gitignored; `creator/.env.example` documents the shape.

## Smoke validation

```bash
# DataForSEO live integration (~$0.002/run)
python -m creator.scripts.smoke_dataforseo "steuerberater hamburg"

# Research + contract only LIVE (~$0.10/run, asks for confirmation)
python -m creator.scripts.smoke_pipeline "steuerberater hamburg" https://client.de/leistungen

# FULL pipeline LIVE end-to-end (~$0.30/run, asks for confirmation, saves all artifacts)
python -m creator.scripts.smoke_full_pipeline "steuerberater hamburg" https://client.de/leistungen

# Full creator unit test suite
python -m pytest creator/tests/ -v
# Expected: 94 passing as of Phase 3c
```

## Cross-session context pointers

- Memory: `/Users/Adil/.claude/projects/-Users-adil-Projects-Elci-doc-converter-service/memory/MEMORY.md` carries the company context, creator priority, and always-push preference.
- Repo conventions: see `README.md` for portal_backend infra (Hostinger + Dokploy + SSH tunnel, Fernet credential encryption).
- This file: rebuild plan and state. Always reflects last commit on `creator-rebuild`.
