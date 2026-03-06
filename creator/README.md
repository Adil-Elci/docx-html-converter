# Creator Service

## Run locally
```bash
pip install -r requirements.txt
uvicorn api.server:app --reload --port 8100
```

## Environment
- `CREATOR_LLM_API_KEY` (preferred for LLM calls)
- `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` (used if `CREATOR_LLM_API_KEY` is not set)
- `CREATOR_LLM_BASE_URL` (default: https://api.openai.com/v1, or https://api.anthropic.com/v1 when using Anthropic)
- `CREATOR_LLM_MODEL` (default: gpt-4.1-mini or claude-haiku-4-5-20251001 for Anthropic)
- `CREATOR_LLM_MODEL_PLANNING` / `CREATOR_LLM_MODEL_WRITING` (override per-purpose models)
- `LEONARDO_API_KEY` (required for image generation)
- `LEONARDO_BASE_URL` (default: https://cloud.leonardo.ai/api/rest/v1)
- `CREATOR_HTTP_TIMEOUT_SECONDS` (default: 20)
- `CREATOR_HTTP_RETRIES` (default: 2)
- `CREATOR_PHASE2_PROMPT_CHARS` (default: 2500)
- `CREATOR_PHASE2_MAX_TOKENS` (default: 400)
- `CREATOR_PHASE5_MAX_ATTEMPTS` (default: 2, capped at 2)
- `CREATOR_PHASE5_MAX_TOKENS_ATTEMPT1` (default: 1800)
- `CREATOR_PHASE5_MAX_TOKENS_RETRY` (default: 1200)
- `CREATOR_KEYWORD_TRENDS_ENABLED` (default: true; fetches DE query suggestions for keyword enrichment)
- `CREATOR_KEYWORD_TRENDS_TIMEOUT_SECONDS` (default: 4)
- `CREATOR_KEYWORD_TRENDS_MAX_TERMS` (default: 10)

## Tests
```bash
pytest
```

## Notes
- The creator service is standalone and does not run database migrations.
- It fetches and analyzes live target/publishing sites; ensure outbound HTTP is allowed.
