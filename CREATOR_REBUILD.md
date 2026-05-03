# Creator Rebuild — Plan & State

**Branch:** `creator-rebuild` · **Last updated:** 2026-05-03 · **Last commit:** `3324091`

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
- **Models**: Opus 4.7 (Contract w/ extended thinking) · Sonnet 4.6 (sections + voice) · Haiku 4.5 (research helpers + LLM judge).
- **Locale**: Germany (`location_code=2276`, `language_code="de"`) pinned in DataForSEO.
- **Eval = objective rubric**, not user-curated gold articles. Rubric grounded in published SEO guidelines + measurable axes.
- **Cost target**: ~$0.50/article (Opus + 5×Sonnet w/ caching + Haiku helpers + DataForSEO + image).
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

### Phase 3 — ContentContract generator · 🔜 NEXT
- **3a** Contract generator module (`creator/api/contract_generator.py`) + prompt v1 + tests. Opus 4.7 with extended thinking, prompt caching primed for downstream section writers.
- **3b** LLM judge for the remaining 3 eval stubs (`intent_match`, `backlink_anchor_naturalness`, `eeat_signal_density`). Haiku, shares context with contract.
- **3c** Integration: research → contract → enforcer end-to-end against eval harness.

### Phase 4 — Section writer · 🔜
Parallel Sonnet 4.6 calls, one per H2 section, prompt-cached on the Contract. Output structured JSON; assembled into HTML.

### Phase 5 — Voice & coherence pass · 🔜
Single Sonnet pass: assemble sections, smooth transitions, enforce voice consistency, strip German AI-tells from blocklist.

### Phase 6 — Deterministic enforcer + review surface · 🔜
Strict code-based enforcement. Failures → human queue or auto-repair (Sonnet 1-shot). One-page review card for the approver.

### Phase 7 — Wire into portal_backend · 🔜
Update `automation_service.py` and `automation_worker.py` to call new pipeline. Run 5 real orders end-to-end. Measure against eval harness.

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
# DataForSEO live integration (~$0.002 per run)
python -m creator.scripts.smoke_dataforseo "steuerberater hamburg"

# Full creator unit test suite
python -m pytest creator/tests/ -v
# Expected: 62 passing as of 3324091
```

## Cross-session context pointers

- Memory: `/Users/Adil/.claude/projects/-Users-adil-Projects-Elci-doc-converter-service/memory/MEMORY.md` carries the company context, creator priority, and always-push preference.
- Repo conventions: see `README.md` for portal_backend infra (Hostinger + Dokploy + SSH tunnel, Fernet credential encryption).
- This file: rebuild plan and state. Always reflects last commit on `creator-rebuild`.
