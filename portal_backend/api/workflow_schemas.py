from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator

WORKFLOW_REQUEST_KINDS = {"manual", "submit_article", "create_article"}
WORKFLOW_COMMENT_LANGUAGES = {"en", "de"}
WORKFLOW_JOB_TYPES = {"articles", "develop", "fix", "research"}
WORKFLOW_PRIORITY_LEVELS = {"urgent", "high", "medium", "low"}
WORKFLOW_FLAG_TYPES = {"bug", "needs_levent_attention"}


class WorkflowCommentOut(BaseModel):
    id: UUID
    author_user_id: Optional[UUID] = None
    author_name: str
    body: str
    created_at: datetime
    updated_at: datetime
    can_edit: bool = False


class WorkflowCardOut(BaseModel):
    id: UUID
    job_id: Optional[UUID] = None
    submission_id: Optional[UUID] = None
    column_id: UUID
    column_key: str
    title: str
    description: Optional[str] = None
    card_kind: str = "job"
    created_by_name: Optional[str] = None
    assignee_user_id: Optional[UUID] = None
    assignee_name: Optional[str] = None
    job_type: Optional[str] = None
    priority: str = "medium"
    flag_type: Optional[str] = None
    request_kind: Optional[str] = None
    job_status: str
    wp_post_url: Optional[str] = None
    last_error: Optional[str] = None
    position: int
    created_at: datetime
    updated_at: datetime
    comments: List[WorkflowCommentOut] = Field(default_factory=list)


class WorkflowColumnOut(BaseModel):
    id: UUID
    key: str
    name: str
    color: Optional[str] = None
    is_system: bool = False
    position: int
    cards: List[WorkflowCardOut] = Field(default_factory=list)


class WorkflowBoardOut(BaseModel):
    columns: List[WorkflowColumnOut] = Field(default_factory=list)
    open_card_count: int = 0
    completed_card_count: int = 0
    updated_at: datetime


class WorkflowCardMoveIn(BaseModel):
    column_id: UUID

    @validator("column_id")
    def validate_column_id(cls, value: UUID) -> UUID:
        if not value:
            raise ValueError("column_id is required.")
        return value


class WorkflowColumnCreateIn(BaseModel):
    name: str

    @validator("name")
    def validate_name(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        if len(normalized) > 80:
            raise ValueError("name must be 80 characters or fewer.")
        return normalized


class WorkflowColumnUpdateIn(BaseModel):
    name: str

    @validator("name")
    def validate_name(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        if len(normalized) > 80:
            raise ValueError("name must be 80 characters or fewer.")
        return normalized


class WorkflowCardCreateIn(BaseModel):
    title: str
    job_type: str
    priority: str = "medium"
    assignee_user_id: UUID
    description: Optional[str] = None

    @validator("title")
    def validate_title(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("title is required.")
        if len(normalized) > 160:
            raise ValueError("title must be 160 characters or fewer.")
        return normalized

    @validator("job_type")
    def validate_job_type(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in WORKFLOW_JOB_TYPES:
            raise ValueError("job_type must be one of articles, develop, fix, research.")
        return normalized

    @validator("priority")
    def validate_priority(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in WORKFLOW_PRIORITY_LEVELS:
            raise ValueError("priority must be one of urgent, high, medium, low.")
        return normalized

    @validator("assignee_user_id")
    def validate_assignee_user_id(cls, value: UUID) -> UUID:
        if not value:
            raise ValueError("assignee_user_id is required.")
        return value

    @validator("description")
    def validate_description(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if len(normalized) > 4000:
            raise ValueError("description must be 4000 characters or fewer.")
        return normalized


class WorkflowCommentCreateIn(BaseModel):
    body: str

    @validator("body")
    def validate_body(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("body is required.")
        if len(normalized) > 4000:
            raise ValueError("body must be 4000 characters or fewer.")
        return normalized


class WorkflowCommentUpdateIn(BaseModel):
    body: str

    @validator("body")
    def validate_body(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("body is required.")
        if len(normalized) > 4000:
            raise ValueError("body must be 4000 characters or fewer.")
        return normalized


class WorkflowCommentRewriteIn(BaseModel):
    body: str
    language: str = "en"

    @validator("body")
    def validate_body(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("body is required.")
        if len(normalized) > 4000:
            raise ValueError("body must be 4000 characters or fewer.")
        return normalized

    @validator("language")
    def validate_language(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in WORKFLOW_COMMENT_LANGUAGES:
            raise ValueError("language must be en or de.")
        return normalized


class WorkflowCommentRewriteOut(BaseModel):
    body: str


class WorkflowCardUpdateIn(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    flag_type: Optional[str] = None

    @validator("title")
    def validate_title(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("title is required.")
        if len(normalized) > 160:
            raise ValueError("title must be 160 characters or fewer.")
        return normalized

    @validator("description")
    def validate_description(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if len(normalized) > 4000:
            raise ValueError("description must be 4000 characters or fewer.")
        return normalized

    @validator("flag_type")
    def validate_flag_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized not in WORKFLOW_FLAG_TYPES:
            raise ValueError("flag_type must be bug or needs_levent_attention.")
        return normalized
