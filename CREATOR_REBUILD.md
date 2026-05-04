# Creator Rebuild — Plan & State

**Branch:** `creator-rebuild` · **Last updated:** 2026-05-04 · **Last commit:** `Phase 7b-3 (v2 orchestration wired)`

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

### Phase 7 — Wire into portal_backend · 🔄 IN PROGRESS
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
- **7c** 🔜 End-to-end test through `/automation/submit-article-webhook` with a real order; verify article publishes to WordPress.
- **7d** 🔜 Delete the legacy 4llm endpoints from creator (`/site-understanding`, `/draft-article`, `/integrate-links`, `/generate-meta`) and the legacy orchestration in portal_backend (`call_creator_4llm_endpoint`, `_run_create_article_pipeline_4llm`, the 7 skipped legacy tests).

## Deferred items (intentionally)

- **Cache `ResearchPayload` to `seo_research_cache`** — needs CHECK-constraint migration for `cache_kind='research_payload'`. Costs $0.05/uncached run. Will land as its own commit when production code path is wired.
- **`call_creator_service` and `_call_creator_stream`** in `automation_service.py` — orphan helpers from deleted legacy pipeline, still imported by `test_automation_service.py`. Removal blocked on test cleanup; tag for Phase 7.
- **`_select_best_accepted_pair`** in `automation_routes.py` — unreachable from production but useful as building block when we redesign pair-fit selection. Keep with its 9 tests; revisit during portal integration.

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
