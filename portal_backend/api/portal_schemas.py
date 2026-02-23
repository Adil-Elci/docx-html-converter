from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, root_validator, validator

CLIENT_STATUSES = {"active", "inactive"}
SITE_STATUSES = {"active", "inactive"}
AUTH_TYPES = {"application_password"}
SOURCE_TYPES = {"google-doc", "docx-upload"}
BACKLINK_PLACEMENTS = {"intro", "conclusion"}
POST_STATUSES = {"draft", "publish"}
REQUEST_KINDS = {"guest_post", "order"}
SUBMISSION_STATUSES = {"received", "validated", "rejected", "queued"}
JOB_STATUSES = {"queued", "processing", "pending_approval", "rejected", "succeeded", "failed", "retrying"}
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
USER_ROLES = {"admin", "client"}


class UserOut(BaseModel):
    id: UUID
    email: EmailStr
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class AuthLoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=256)

    @validator("email")
    def normalize_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()

    @validator("password")
    def non_empty_password(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("password must not be empty.")
        return cleaned


class AuthLoginOut(BaseModel):
    ok: bool = True
    access_token: str
    token_type: str = "bearer"
    user: UserOut


class AuthLogoutOut(BaseModel):
    ok: bool = True


class AuthPasswordResetRequestIn(BaseModel):
    email: EmailStr

    @validator("email")
    def normalize_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()


class AuthPasswordResetRequestOut(BaseModel):
    ok: bool = True
    message: str


class AuthPasswordResetConfirmIn(BaseModel):
    token: str = Field(min_length=20, max_length=512)
    new_password: str = Field(min_length=8, max_length=256)

    @validator("token")
    def non_empty_token(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("token must not be empty.")
        return cleaned

    @validator("new_password")
    def non_empty_password(cls, value: str) -> str:
        cleaned = value.strip()
        if len(cleaned) < 8:
            raise ValueError("new_password must be at least 8 characters.")
        return cleaned


class AuthPasswordResetConfirmOut(BaseModel):
    ok: bool = True
    message: str


class AdminUserCreate(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    password: str = Field(min_length=8, max_length=256)
    role: str = "client"
    is_active: bool = True
    client_ids: List[UUID] = Field(default_factory=list)

    @validator("email")
    def normalize_email(cls, value: EmailStr) -> str:
        return str(value).strip().lower()

    @validator("password")
    def validate_password(cls, value: str) -> str:
        cleaned = value.strip()
        if len(cleaned) < 8:
            raise ValueError("password must be at least 8 characters.")
        return cleaned

    @validator("role")
    def validate_role(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in USER_ROLES:
            raise ValueError("role must be admin or client.")
        return cleaned

    @validator("full_name")
    def optional_full_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class AdminUserUpdate(BaseModel):
    full_name: Optional[str] = None
    password: Optional[str] = Field(default=None, min_length=8, max_length=256)
    role: Optional[str] = None
    is_active: Optional[bool] = None
    client_ids: Optional[List[UUID]] = None

    @validator("password")
    def optional_password(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if len(cleaned) < 8:
            raise ValueError("password must be at least 8 characters.")
        return cleaned

    @validator("role")
    def optional_role(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in USER_ROLES:
            raise ValueError("role must be admin or client.")
        return cleaned

    @validator("full_name")
    def optional_nullable_full_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class AdminUserOut(UserOut):
    client_ids: List[UUID] = Field(default_factory=list)


class ClientTargetSiteIn(BaseModel):
    target_site_domain: Optional[str] = None
    target_site_url: Optional[str] = None
    is_primary: bool = False

    @validator("target_site_domain", "target_site_url")
    def optional_trimmed_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @root_validator(skip_on_failure=True)
    def require_domain_or_url(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not values.get("target_site_domain") and not values.get("target_site_url"):
            raise ValueError("At least one of target_site_domain or target_site_url is required.")
        return values


class ClientTargetSiteOut(BaseModel):
    id: UUID
    client_id: UUID
    target_site_domain: Optional[str]
    target_site_url: Optional[str]
    is_primary: bool
    created_at: datetime
    updated_at: datetime


class ClientCreate(BaseModel):
    name: str
    primary_domain: Optional[str] = None
    backlink_url: Optional[str] = None
    target_sites: List[ClientTargetSiteIn] = Field(default_factory=list)
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
    target_sites: Optional[List[ClientTargetSiteIn]] = None
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
    target_sites: List[ClientTargetSiteOut] = Field(default_factory=list)
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
    author_name: Optional[str] = None
    author_id: Optional[int] = None
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

    @validator("author_name")
    def optional_author_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @validator("author_id")
    def optional_positive_author_id(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("author_id must be a positive integer.")
        return value


class SiteCredentialUpdate(BaseModel):
    auth_type: Optional[str] = None
    wp_username: Optional[str] = None
    wp_app_password: Optional[str] = None
    author_name: Optional[str] = None
    author_id: Optional[int] = None
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

    @validator("author_name")
    def optional_nullable_author_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @validator("author_id")
    def optional_positive_author_id(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("author_id must be a positive integer.")
        return value


class SiteCredentialOut(BaseModel):
    id: UUID
    site_id: UUID
    auth_type: str
    wp_username: str
    wp_app_password: str
    author_name: Optional[str]
    author_id: Optional[int]
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
    request_kind: str = "guest_post"
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

    @validator("request_kind")
    def validate_request_kind(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in REQUEST_KINDS:
            raise ValueError("request_kind must be guest_post or order.")
        return cleaned

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
    request_kind: Optional[str] = None
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

    @validator("request_kind")
    def validate_request_kind(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if cleaned not in REQUEST_KINDS:
            raise ValueError("request_kind must be guest_post or order.")
        return cleaned

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
    request_kind: str
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
    requires_admin_approval: bool = False
    approved_by: Optional[UUID] = None
    approved_by_name_snapshot: Optional[str] = None
    approved_at: Optional[datetime] = None
    attempt_count: int = 0
    last_error: Optional[str] = None
    wp_post_id: Optional[int] = None
    wp_post_url: Optional[str] = None

    @validator("job_status")
    def validate_job_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in JOB_STATUSES:
            raise ValueError("job_status must be one of queued/processing/pending_approval/rejected/succeeded/failed/retrying.")
        return cleaned


class JobUpdate(BaseModel):
    job_status: Optional[str] = None
    requires_admin_approval: Optional[bool] = None
    approved_by: Optional[UUID] = None
    approved_by_name_snapshot: Optional[str] = None
    approved_at: Optional[datetime] = None
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
            raise ValueError("job_status must be one of queued/processing/pending_approval/rejected/succeeded/failed/retrying.")
        return cleaned


class JobOut(BaseModel):
    id: UUID
    submission_id: UUID
    client_id: UUID
    site_id: UUID
    job_status: str
    requires_admin_approval: bool
    approved_by: Optional[UUID]
    approved_by_name_snapshot: Optional[str]
    approved_at: Optional[datetime]
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
    publishing_site: str
    request_kind: str = "guest_post"
    doc_url: Optional[str] = None
    docx_file: Optional[str] = None
    client_id: Optional[UUID] = None
    client_name: Optional[str] = None
    target_site_id: Optional[UUID] = None
    target_site_url: Optional[str] = None
    idempotency_key: Optional[str] = None
    execution_mode: str = "async"
    backlink_placement: str = "intro"
    post_status: Optional[str] = None
    author: Optional[int] = None

    @validator("request_kind")
    def validate_request_kind(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in REQUEST_KINDS:
            raise ValueError("request_kind must be guest_post or order.")
        return cleaned

    @validator("source_type")
    def validate_source_type(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"google-doc", "word-doc", "docx-upload"}:
            raise ValueError("source_type must be one of google-doc, word-doc, docx-upload.")
        return cleaned

    @validator("publishing_site")
    def validate_publishing_site(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("publishing_site must not be empty.")
        return cleaned

    @validator("doc_url", "docx_file", "client_name", "target_site_url")
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

    @validator("author")
    def validate_author(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("author must be a positive integer.")
        return value

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
    publishing_site: str
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


class PendingJobOut(BaseModel):
    job_id: UUID
    submission_id: UUID
    request_kind: str
    client_id: UUID
    client_name: str
    site_id: UUID
    site_name: str
    site_url: str
    content_title: Optional[str] = None
    job_status: str
    wp_post_id: Optional[int]
    wp_post_url: Optional[str]
    created_at: datetime
    updated_at: datetime


class PendingJobPublishOut(BaseModel):
    ok: bool = True
    job: JobOut


class PendingJobRegenerateImageOut(BaseModel):
    ok: bool = True
    job: JobOut
    wp_media_id: int
    wp_media_url: Optional[str] = None


class PendingJobRejectIn(BaseModel):
    reason_code: str
    other_reason: Optional[str] = None

    @validator("reason_code")
    def validate_reason_code(cls, value: str) -> str:
        cleaned = value.strip().lower()
        allowed = {
            "quality_below_standard",
            "policy_or_compliance_issue",
            "seo_or_link_issue",
            "format_or_structure_issue",
            "other",
        }
        if cleaned not in allowed:
            raise ValueError("Unsupported rejection reason.")
        return cleaned

    @validator("other_reason")
    def validate_other_reason(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @root_validator(skip_on_failure=True)
    def require_other_reason_when_other(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("reason_code") == "other" and not values.get("other_reason"):
            raise ValueError("other_reason is required when reason_code is other.")
        return values


class PendingJobRejectOut(BaseModel):
    ok: bool = True
    job: JobOut
