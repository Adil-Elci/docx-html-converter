from creator.api.decision_schemas import CriticReview
from creator.api.llm import LLMError
from creator.api.llm_provider import (
    CreatorLLMProvider,
    LLMProviderConfig,
    LLMRole,
    load_provider_config,
    schema_prompt_block,
)


def test_load_provider_config_uses_role_specific_env(monkeypatch) -> None:
    monkeypatch.setenv("CREATOR_SUPERVISOR_LLM_MODEL", "claude-test-model")
    monkeypatch.setenv("CREATOR_SUPERVISOR_LLM_API_KEY", "test-key")
    monkeypatch.setenv("CREATOR_SUPERVISOR_LLM_TIMEOUT_SECONDS", "33")
    monkeypatch.setenv("CREATOR_SUPERVISOR_LLM_MAX_TOKENS", "1234")
    monkeypatch.setenv("CREATOR_SUPERVISOR_LLM_TEMPERATURE", "0.15")

    config = load_provider_config(LLMRole.SUPERVISOR)

    assert config.model == "claude-test-model"
    assert config.api_key == "test-key"
    assert config.base_url == "https://api.anthropic.com/v1"
    assert config.timeout_seconds == 33
    assert config.max_tokens == 1234
    assert config.temperature == 0.15


def test_load_provider_config_requires_api_key(monkeypatch) -> None:
    monkeypatch.delenv("CREATOR_CRITIC_LLM_API_KEY", raising=False)
    monkeypatch.delenv("CREATOR_LLM_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    try:
        load_provider_config(LLMRole.CRITIC)
    except LLMError as exc:
        assert "critic" in str(exc)
    else:
        raise AssertionError("Expected missing-key error.")


def test_schema_prompt_block_exports_json_schema() -> None:
    schema_text = schema_prompt_block(CriticReview)

    assert "overall_score" in schema_text
    assert "repair_needed" in schema_text


def test_provider_call_schema_validates_payload(monkeypatch) -> None:
    provider = CreatorLLMProvider(
        config=LLMProviderConfig(
            role=LLMRole.CRITIC,
            model="claude-test-model",
            api_key="test-key",
            base_url="https://api.anthropic.com/v1",
        )
    )

    def _fake_call_json(**_: object) -> dict[str, object]:
        return {
            "verdict": "pass",
            "overall_score": 92,
            "plan_alignment_score": 93,
            "editorial_quality_score": 91,
            "seo_quality_score": 90,
            "strengths": ["Natural headings."],
            "issues": [],
            "repair_instructions": [],
            "final_recommendation": "Approve the article.",
        }

    monkeypatch.setattr("creator.api.llm_provider.call_llm_json", _fake_call_json)

    review = provider.call_schema(
        schema_model=CriticReview,
        system_prompt="system",
        user_prompt="user",
        request_label="critic_test",
    )

    assert review.verdict == "pass"
    assert review.overall_score == 92
