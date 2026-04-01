from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator

TASK_BOARD_REQUEST_KINDS = {"manual", "submit_article", "create_article"}
TASK_BOARD_COMMENT_LANGUAGES = {"en", "de"}
TASK_BOARD_JOB_TYPES = {"articles", "develop", "fix", "research"}
TASK_BOARD_PRIORITY_LEVELS = {"urgent", "high", "medium", "low"}
TASK_BOARD_FLAG_ORDER = ("bug", "needs_levent_attention", "needs_adil_attention")
TASK_BOARD_FLAG_TYPES = set(TASK_BOARD_FLAG_ORDER)


class TaskBoardCommentOut(BaseModel):
    id: UUID
    author_user_id: Optional[UUID] = None
    author_name: str
    body: str
    created_at: datetime
    updated_at: datetime
    can_edit: bool = False


class TaskBoardCardOut(BaseModel):
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
    flag_types: List[str] = Field(default_factory=list)
    request_kind: Optional[str] = None
    job_status: str
    wp_post_url: Optional[str] = None
    last_error: Optional[str] = None
    position: int
    created_at: datetime
    updated_at: datetime
    comments: List[TaskBoardCommentOut] = Field(default_factory=list)


class TaskBoardColumnOut(BaseModel):
    id: UUID
    key: str
    name: str
    color: Optional[str] = None
    is_system: bool = False
    position: int
    cards: List[TaskBoardCardOut] = Field(default_factory=list)


class TaskBoardOut(BaseModel):
    columns: List[TaskBoardColumnOut] = Field(default_factory=list)
    open_card_count: int = 0
    completed_card_count: int = 0
    updated_at: datetime


class TaskBoardCardMoveIn(BaseModel):
    column_id: UUID

    @validator("column_id")
    def validate_column_id(cls, value: UUID) -> UUID:
        if not value:
            raise ValueError("column_id is required.")
        return value


class TaskBoardColumnCreateIn(BaseModel):
    name: str

    @validator("name")
    def validate_name(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        if len(normalized) > 80:
            raise ValueError("name must be 80 characters or fewer.")
        return normalized


class TaskBoardColumnUpdateIn(BaseModel):
    name: str

    @validator("name")
    def validate_name(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("name is required.")
        if len(normalized) > 80:
            raise ValueError("name must be 80 characters or fewer.")
        return normalized


class TaskBoardCardCreateIn(BaseModel):
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
        if normalized not in TASK_BOARD_JOB_TYPES:
            raise ValueError("job_type must be one of articles, develop, fix, research.")
        return normalized

    @validator("priority")
    def validate_priority(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in TASK_BOARD_PRIORITY_LEVELS:
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


class TaskBoardCommentCreateIn(BaseModel):
    body: str

    @validator("body")
    def validate_body(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("body is required.")
        if len(normalized) > 4000:
            raise ValueError("body must be 4000 characters or fewer.")
        return normalized


class TaskBoardCommentUpdateIn(BaseModel):
    body: str

    @validator("body")
    def validate_body(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("body is required.")
        if len(normalized) > 4000:
            raise ValueError("body must be 4000 characters or fewer.")
        return normalized


class TaskBoardCommentRewriteIn(BaseModel):
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
        if normalized not in TASK_BOARD_COMMENT_LANGUAGES:
            raise ValueError("language must be en or de.")
        return normalized


class TaskBoardCommentRewriteOut(BaseModel):
    body: str


class TaskBoardCardUpdateIn(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    job_type: Optional[str] = None
    priority: Optional[str] = None
    assignee_user_id: Optional[UUID] = None
    flag_types: Optional[List[str]] = None

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

    @validator("job_type")
    def validate_job_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in TASK_BOARD_JOB_TYPES:
            raise ValueError("job_type must be one of articles, develop, fix, research.")
        return normalized

    @validator("priority")
    def validate_priority(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in TASK_BOARD_PRIORITY_LEVELS:
            raise ValueError("priority must be one of urgent, high, medium, low.")
        return normalized

    @validator("assignee_user_id")
    def validate_assignee_user_id(cls, value: Optional[UUID]) -> Optional[UUID]:
        if value is None:
            return None
        if not value:
            raise ValueError("assignee_user_id is required.")
        return value

    @validator("flag_types")
    def validate_flag_types(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        seen = set()
        normalized_values: list[str] = []
        for flag_type in value:
            normalized = str(flag_type or "").strip().lower()
            if not normalized:
                continue
            if normalized not in TASK_BOARD_FLAG_TYPES:
                raise ValueError("flag_types may only include bug, needs_levent_attention, or needs_adil_attention.")
            if normalized in seen:
                continue
            seen.add(normalized)
        for flag_type in TASK_BOARD_FLAG_ORDER:
            if flag_type in seen:
                normalized_values.append(flag_type)
        return normalized_values
