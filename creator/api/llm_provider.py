from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional, Type, TypeVar

from pydantic import BaseModel

from .decision_schemas import SchemaBackedModel
from .llm import LLMError, call_llm_json, call_llm_text

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_SUPERVISOR_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_WRITER_MODEL = "claude-sonnet-4-6"
DEFAULT_CRITIC_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_MAX_TOKENS = 3200
DEFAULT_TEMPERATURE = 0.2

SchemaModelT = TypeVar("SchemaModelT", bound=SchemaBackedModel)


class LLMRole(str, Enum):
    SUPERVISOR = "supervisor"
    WRITER = "writer"
    CRITIC = "critic"
    REPAIR = "repair"


@dataclass(frozen=True)
class LLMProviderConfig:
    role: LLMRole
    model: str
    api_key: str
    base_url: str
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_tokens: int = DEFAULT_MAX_TOKENS
    temperature: float = DEFAULT_TEMPERATURE


class CreatorLLMProvider:
    def __init__(
        self,
        *,
        config: LLMProviderConfig,
        usage_collector: Optional[Callable[[Dict[str, int | str]], None]] = None,
    ) -> None:
        self.config = config
        self.usage_collector = usage_collector

    def call_text(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        request_label: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> str:
        return call_llm_text(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=self.config.api_key,
            base_url=self.config.base_url,
            model=self.config.model,
            timeout_seconds=self.config.timeout_seconds,
            max_tokens=max_tokens or self.config.max_tokens,
            temperature=self.config.temperature if temperature is None else temperature,
            request_label=request_label,
            usage_collector=self.usage_collector,
        )

    def call_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        request_label: str,
        allow_html_fallback: bool = False,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> Dict[str, object]:
        return call_llm_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=self.config.api_key,
            base_url=self.config.base_url,
            model=self.config.model,
            timeout_seconds=self.config.timeout_seconds,
            max_tokens=max_tokens or self.config.max_tokens,
            temperature=self.config.temperature if temperature is None else temperature,
            allow_html_fallback=allow_html_fallback,
            request_label=request_label,
            usage_collector=self.usage_collector,
        )

    def call_schema(
        self,
        *,
        schema_model: Type[SchemaModelT],
        system_prompt: str,
        user_prompt: str,
        request_label: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> SchemaModelT:
        payload = self.call_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            request_label=request_label,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return schema_model.model_validate(payload)


def _read_env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _read_env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _env_first(*names: str) -> str:
    for name in names:
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    return ""


def _default_model_for_role(role: LLMRole) -> str:
    if role == LLMRole.WRITER:
        return DEFAULT_WRITER_MODEL
    if role == LLMRole.CRITIC:
        return DEFAULT_CRITIC_MODEL
    return DEFAULT_SUPERVISOR_MODEL


def _default_base_url_for_model(model: str) -> str:
    if str(model or "").strip().lower().startswith("claude"):
        return DEFAULT_ANTHROPIC_BASE_URL
    return DEFAULT_OPENAI_BASE_URL


def load_provider_config(role: LLMRole) -> LLMProviderConfig:
    role_prefix = f"CREATOR_{role.value.upper()}_LLM"
    model = _env_first(f"{role_prefix}_MODEL", "CREATOR_LLM_MODEL") or _default_model_for_role(role)
    base_url = _env_first(f"{role_prefix}_BASE_URL", "CREATOR_LLM_BASE_URL") or _default_base_url_for_model(model)
    api_key = _env_first(
        f"{role_prefix}_API_KEY",
        "CREATOR_LLM_API_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    )
    if not api_key:
        raise LLMError(f"Missing API key for creator {role.value} role.")
    timeout_seconds = _read_env_int(f"{role_prefix}_TIMEOUT_SECONDS", _read_env_int("CREATOR_LLM_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    max_tokens = _read_env_int(f"{role_prefix}_MAX_TOKENS", _read_env_int("CREATOR_LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS))
    temperature = _read_env_float(f"{role_prefix}_TEMPERATURE", _read_env_float("CREATOR_LLM_TEMPERATURE", DEFAULT_TEMPERATURE))
    return LLMProviderConfig(
        role=role,
        model=model,
        api_key=api_key,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def build_provider(
    role: LLMRole,
    *,
    usage_collector: Optional[Callable[[Dict[str, int | str]], None]] = None,
    config: Optional[LLMProviderConfig] = None,
) -> CreatorLLMProvider:
    return CreatorLLMProvider(
        config=config or load_provider_config(role),
        usage_collector=usage_collector,
    )


def schema_prompt_block(schema_model: Type[BaseModel]) -> str:
    if not issubclass(schema_model, BaseModel):
        raise TypeError("schema_model must be a Pydantic BaseModel subclass.")
    return json.dumps(schema_model.model_json_schema(), ensure_ascii=False, indent=2, sort_keys=True)
