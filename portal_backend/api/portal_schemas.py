from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, root_validator, validator

CLIENT_STATUSES = {"active", "inactive"}
SITE_STATUSES = {"active", "inactive"}
AUTH_TYPES = {"application_password"}
SOURCE_TYPES = {"google-doc", "docx-upload"}
BACKLINK_PLACEMENTS = {"intro", "conclusion"}
POST_STATUSES = {"draft", "publish"}
SUBMISSION_STATUSES = {"received", "validated", "rejected", "queued"}
JOB_STATUSES = {"queued", "processing", "succeeded", "failed", "retrying"}
EVENT_TYPES = {
    "converter_called",
    "converter_ok",
    "image_prompt_ok",
    "image_generated",
    "wp_post_created",
    "wp_post_updated",
    "failed",
}
ASSET_TYPES = {"featured_image"}
ASSET_PROVIDERS = {"leonardo", "openai", "other"}


class ClientCreate(BaseModel):
    name: str
    primary_domain: Optional[str] = None
    backlink_url: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    status: str = "active"

    @validator("name")
    def non_empty_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned

    @validator("primary_domain", "backlink_url", "email", "phone_number")
    def optional_non_empty_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @validator("status")
    def validate_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in CLIENT_STATUSES:
            raise ValueError("status must be 'active' or 'inactive'.")
        return cleaned


class ClientUpdate(BaseModel):
    name: Optional[str] = None
    primary_domain: Optional[str] = None
    backlink_url: Optional[str] = None
    email: Optional[str] = None
    phone_number: Optional[str] = None
    status: Optional[str] = None

    @validator("name")
    def optional_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned

    @validator("primary_domain", "backlink_url", "email", "phone_number")
    def optional_nullable_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @validator("status")
    def validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in CLIENT_STATUSES:
            raise ValueError("status must be 'active' or 'inactive'.")
        return cleaned


class ClientOut(BaseModel):
    id: UUID
    name: str
    primary_domain: Optional[str]
    backlink_url: Optional[str]
    email: Optional[str]
    phone_number: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime


class SiteCreate(BaseModel):
    name: str
    site_url: str
    wp_rest_base: str = "/wp-json/wp/v2"
    hosting_provider: Optional[str] = None
    hosting_panel: Optional[str] = None
    status: str = "active"

    @validator("name", "site_url", "wp_rest_base")
    def non_empty_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned

    @validator("hosting_provider", "hosting_panel")
    def optional_non_empty_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned

    @validator("status")
    def validate_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in SITE_STATUSES:
            raise ValueError("status must be 'active' or 'inactive'.")
        return cleaned


class SiteUpdate(BaseModel):
    name: Optional[str] = None
    site_url: Optional[str] = None
    wp_rest_base: Optional[str] = None
    hosting_provider: Optional[str] = None
    hosting_panel: Optional[str] = None
    status: Optional[str] = None

    @validator("name", "site_url", "wp_rest_base", "hosting_provider", "hosting_panel")
    def optional_non_empty_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned

    @validator("status")
    def validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in SITE_STATUSES:
            raise ValueError("status must be 'active' or 'inactive'.")
        return cleaned


class SiteOut(BaseModel):
    id: UUID
    name: str
    site_url: str
    wp_rest_base: str
    hosting_provider: Optional[str]
    hosting_panel: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime


class SiteCredentialCreate(BaseModel):
    site_id: UUID
    auth_type: str = "application_password"
    wp_username: str
    wp_app_password: str
    enabled: bool = True

    @validator("auth_type")
    def validate_auth_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in AUTH_TYPES:
            raise ValueError("auth_type must be 'application_password'.")
        return cleaned

    @validator("wp_username", "wp_app_password")
    def non_empty_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned


class SiteCredentialUpdate(BaseModel):
    auth_type: Optional[str] = None
    wp_username: Optional[str] = None
    wp_app_password: Optional[str] = None
    enabled: Optional[bool] = None

    @validator("auth_type")
    def validate_auth_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in AUTH_TYPES:
            raise ValueError("auth_type must be 'application_password'.")
        return cleaned

    @validator("wp_username", "wp_app_password")
    def optional_non_empty_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned


class SiteCredentialOut(BaseModel):
    id: UUID
    site_id: UUID
    auth_type: str
    wp_username: str
    wp_app_password: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


class ClientSiteAccessCreate(BaseModel):
    client_id: UUID
    site_id: UUID
    enabled: bool = True


class ClientSiteAccessUpdate(BaseModel):
    enabled: Optional[bool] = None


class ClientSiteAccessOut(BaseModel):
    id: UUID
    client_id: UUID
    site_id: UUID
    enabled: bool
    created_at: datetime
    updated_at: datetime


class SubmissionCreate(BaseModel):
    client_id: UUID
    site_id: UUID
    source_type: str
    doc_url: Optional[str] = None
    file_url: Optional[str] = None
    backlink_placement: str
    post_status: str
    title: Optional[str] = None
    raw_text: Optional[str] = None
    notes: Optional[str] = None
    status: str = "received"
    rejection_reason: Optional[str] = None

    @validator("source_type")
    def validate_source_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in SOURCE_TYPES:
            raise ValueError("source_type must be 'google-doc' or 'docx-upload'.")
        return cleaned

    @validator("backlink_placement")
    def validate_backlink_placement(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in BACKLINK_PLACEMENTS:
            raise ValueError("backlink_placement must be 'intro' or 'conclusion'.")
        return cleaned

    @validator("post_status")
    def validate_post_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in POST_STATUSES:
            raise ValueError("post_status must be 'draft' or 'publish'.")
        return cleaned

    @validator("status")
    def validate_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in SUBMISSION_STATUSES:
            raise ValueError("status must be one of received/validated/rejected/queued.")
        return cleaned

    @root_validator(skip_on_failure=True)
    def validate_source_payload(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        source_type = values.get("source_type")
        doc_url = (values.get("doc_url") or "").strip() or None
        file_url = (values.get("file_url") or "").strip() or None

        if source_type == "google-doc":
            if doc_url is None or file_url is not None:
                raise ValueError("google-doc requires doc_url and forbids file_url.")
        elif source_type == "docx-upload":
            if file_url is None or doc_url is not None:
                raise ValueError("docx-upload requires file_url and forbids doc_url.")

        values["doc_url"] = doc_url
        values["file_url"] = file_url
        return values


class SubmissionUpdate(BaseModel):
    client_id: Optional[UUID] = None
    site_id: Optional[UUID] = None
    source_type: Optional[str] = None
    doc_url: Optional[str] = None
    file_url: Optional[str] = None
    backlink_placement: Optional[str] = None
    post_status: Optional[str] = None
    title: Optional[str] = None
    raw_text: Optional[str] = None
    notes: Optional[str] = None
    status: Optional[str] = None
    rejection_reason: Optional[str] = None

    @validator("source_type")
    def validate_source_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in SOURCE_TYPES:
            raise ValueError("source_type must be 'google-doc' or 'docx-upload'.")
        return cleaned

    @validator("backlink_placement")
    def validate_backlink_placement(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in BACKLINK_PLACEMENTS:
            raise ValueError("backlink_placement must be 'intro' or 'conclusion'.")
        return cleaned

    @validator("post_status")
    def validate_post_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in POST_STATUSES:
            raise ValueError("post_status must be 'draft' or 'publish'.")
        return cleaned

    @validator("status")
    def validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in SUBMISSION_STATUSES:
            raise ValueError("status must be one of received/validated/rejected/queued.")
        return cleaned


class SubmissionOut(BaseModel):
    id: UUID
    client_id: UUID
    site_id: UUID
    source_type: str
    doc_url: Optional[str]
    file_url: Optional[str]
    backlink_placement: str
    post_status: str
    title: Optional[str]
    raw_text: Optional[str]
    notes: Optional[str]
    status: str
    rejection_reason: Optional[str]
    created_at: datetime
    updated_at: datetime


class JobCreate(BaseModel):
    submission_id: UUID
    client_id: Optional[UUID] = None
    site_id: Optional[UUID] = None
    job_status: str = "queued"
    attempt_count: int = 0
    last_error: Optional[str] = None
    wp_post_id: Optional[int] = None
    wp_post_url: Optional[str] = None

    @validator("job_status")
    def validate_job_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in JOB_STATUSES:
            raise ValueError("job_status must be one of queued/processing/succeeded/failed/retrying.")
        return cleaned


class JobUpdate(BaseModel):
    job_status: Optional[str] = None
    attempt_count: Optional[int] = None
    last_error: Optional[str] = None
    wp_post_id: Optional[int] = None
    wp_post_url: Optional[str] = None

    @validator("job_status")
    def validate_job_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in JOB_STATUSES:
            raise ValueError("job_status must be one of queued/processing/succeeded/failed/retrying.")
        return cleaned


class JobOut(BaseModel):
    id: UUID
    submission_id: UUID
    client_id: UUID
    site_id: UUID
    job_status: str
    attempt_count: int
    last_error: Optional[str]
    wp_post_id: Optional[int]
    wp_post_url: Optional[str]
    created_at: datetime
    updated_at: datetime


class JobEventCreate(BaseModel):
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    @validator("event_type")
    def validate_event_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in EVENT_TYPES:
            raise ValueError("Unsupported event_type.")
        return cleaned


class JobEventOut(BaseModel):
    id: UUID
    job_id: UUID
    event_type: str
    payload: Dict[str, Any]
    created_at: datetime


class AssetCreate(BaseModel):
    asset_type: str = "featured_image"
    provider: str
    source_url: Optional[str] = None
    storage_url: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @validator("asset_type")
    def validate_asset_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in ASSET_TYPES:
            raise ValueError("asset_type must be 'featured_image'.")
        return cleaned

    @validator("provider")
    def validate_provider(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in ASSET_PROVIDERS:
            raise ValueError("provider must be leonardo, openai, or other.")
        return cleaned


class AssetOut(BaseModel):
    id: UUID
    job_id: UUID
    asset_type: str
    provider: str
    source_url: Optional[str]
    storage_url: Optional[str]
    meta: Dict[str, Any]
    created_at: datetime


class AutomationGuestPostIn(BaseModel):
    source_type: str
    target_site: str
    doc_url: Optional[str] = None
    docx_file: Optional[str] = None
    client_id: Optional[UUID] = None
    client_name: Optional[str] = None
    idempotency_key: Optional[str] = None
    execution_mode: str = "async"
    backlink_placement: str = "intro"
    post_status: Optional[str] = None
    author: Optional[int] = None

    @validator("source_type")
    def validate_source_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"google-doc", "word-doc", "docx-upload"}:
            raise ValueError("source_type must be one of google-doc, word-doc, docx-upload.")
        return cleaned

    @validator("target_site")
    def validate_target_site(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("target_site must not be empty.")
        return cleaned

    @validator("doc_url", "docx_file", "client_name")
    def optional_trimmed_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @validator("idempotency_key")
    def optional_idempotency_key(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned[:200]

    @validator("execution_mode")
    def validate_execution_mode(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"sync", "async", "shadow"}:
            raise ValueError("execution_mode must be sync, async, or shadow.")
        return cleaned

    @validator("backlink_placement")
    def validate_backlink_placement(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in BACKLINK_PLACEMENTS:
            raise ValueError("backlink_placement must be intro or conclusion.")
        return cleaned

    @validator("post_status")
    def validate_post_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in POST_STATUSES:
            raise ValueError("post_status must be draft or publish.")
        return cleaned

    @root_validator(skip_on_failure=True)
    def validate_source_fields(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        source_type = values.get("source_type")
        doc_url = values.get("doc_url")
        docx_file = values.get("docx_file")
        if source_type == "google-doc" and not doc_url:
            raise ValueError("doc_url is required for source_type=google-doc.")
        if source_type in {"word-doc", "docx-upload"} and not docx_file:
            raise ValueError("docx_file is required for source_type=word-doc/docx-upload.")
        return values


class AutomationGuestPostResultOut(BaseModel):
    source_type: str
    target_site: str
    source_url: str
    converter: Dict[str, Any]
    generated_image_url: str
    wp_media_id: int
    wp_media_url: Optional[str]
    wp_post_id: int
    wp_post_url: Optional[str]
    site_id: UUID
    site_credential_id: UUID


class AutomationGuestPostOut(BaseModel):
    ok: bool = True
    execution_mode: str
    deduplicated: bool = False
    submission_id: Optional[UUID] = None
    job_id: Optional[UUID] = None
    job_status: Optional[str] = None
    shadow_dispatched: bool = False
    result: Optional[AutomationGuestPostResultOut] = None


class AutomationStatusEventOut(BaseModel):
    event_type: str
    payload: Dict[str, Any]
    created_at: datetime


class AutomationStatusOut(BaseModel):
    found: bool
    idempotency_key: Optional[str] = None
    submission_id: Optional[UUID] = None
    submission_status: Optional[str] = None
    job_id: Optional[UUID] = None
    job_status: Optional[str] = None
    attempt_count: Optional[int] = None
    last_error: Optional[str] = None
    wp_post_id: Optional[int] = None
    wp_post_url: Optional[str] = None
    events: List[AutomationStatusEventOut] = Field(default_factory=list)
