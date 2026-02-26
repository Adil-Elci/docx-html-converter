# Creator Service

## Run locally
```bash
pip install -r requirements.txt
uvicorn api.server:app --reload --port 8100
```

## Environment
- `CREATOR_LLM_API_KEY` (required for LLM calls)
- `CREATOR_LLM_BASE_URL` (default: https://api.openai.com/v1)
- `CREATOR_LLM_MODEL` (default: gpt-4.1-mini)
- `LEONARDO_API_KEY` (required for image generation)
- `LEONARDO_BASE_URL` (default: https://cloud.leonardo.ai/api/rest/v1)
- `CREATOR_HTTP_TIMEOUT_SECONDS` (default: 20)
- `CREATOR_HTTP_RETRIES` (default: 2)

## Tests
```bash
pytest
```

## Notes
- The creator service is standalone and does not run database migrations.
- It fetches and analyzes live target/publishing sites; ensure outbound HTTP is allowed.
