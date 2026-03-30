from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator


class WorkflowCardOut(BaseModel):
    id: UUID
    job_id: UUID
    submission_id: UUID
    client_id: UUID
    client_name: str
    site_id: UUID
    site_name: str
    site_url: str
    column_id: UUID
    column_key: str
    title: str
    request_kind: Optional[str] = None
    job_status: str
    wp_post_url: Optional[str] = None
    last_error: Optional[str] = None
    position: int
    created_at: datetime
    updated_at: datetime


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
