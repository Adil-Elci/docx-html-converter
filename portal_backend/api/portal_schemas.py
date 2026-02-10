from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, validator


class Section(BaseModel):
    section_title: str
    section_body: str

    @validator("section_title", "section_body")
    def validate_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Section fields must not be empty.")
        return cleaned


class ContentJson(BaseModel):
    introduction: str = ""
    sections: List[Section] = Field(default_factory=list, min_items=1, max_items=6)

    @validator("introduction", pre=True, always=True)
    def normalize_intro(cls, value: Optional[str]) -> str:
        if value is None:
            return ""
        return str(value).strip()


class GuestPostBase(BaseModel):
    target_site_id: UUID
    title_h1: str
    backlink_url: str
    auto_backlink: bool = True
    backlink_placement: Optional[str] = None
    content_json: ContentJson

    @validator("title_h1")
    def validate_title(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("title_h1 is required.")
        return cleaned

    @validator("backlink_placement")
    def validate_backlink_placement(cls, value: Optional[str], values: dict) -> Optional[str]:
        if values.get("auto_backlink", True):
            return None
        if value not in {"intro", "conclusion"}:
            raise ValueError("backlink_placement must be 'intro' or 'conclusion'.")
        return value


class GuestPostCreate(GuestPostBase):
    pass


class GuestPostUpdate(BaseModel):
    target_site_id: Optional[UUID] = None
    title_h1: Optional[str] = None
    backlink_url: Optional[str] = None
    auto_backlink: Optional[bool] = None
    backlink_placement: Optional[str] = None
    content_json: Optional[ContentJson] = None

    @validator("title_h1")
    def validate_title(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("title_h1 is required.")
        return cleaned

    @validator("backlink_placement")
    def validate_backlink_placement(cls, value: Optional[str], values: dict) -> Optional[str]:
        auto = values.get("auto_backlink")
        if auto is None:
            return value
        if auto:
            return None
        if value not in {"intro", "conclusion"}:
            raise ValueError("backlink_placement must be 'intro' or 'conclusion'.")
        return value


class GuestPostOut(BaseModel):
    id: UUID
    client_id: UUID
    target_site_id: UUID
    status: str
    title_h1: str
    backlink_url: str
    backlink_placement: Optional[str]
    auto_backlink: bool
    content_json: ContentJson
    content_markdown: str
    created_at: datetime
    updated_at: datetime
    submitted_at: Optional[datetime]


class ClientOut(BaseModel):
    id: UUID
    name: str
    website_domain: str
    active: bool
    created_at: datetime
    updated_at: datetime


class ClientUpdate(BaseModel):
    name: Optional[str] = None
    website_domain: Optional[str] = None
    active: Optional[bool] = None

    @validator("name", "website_domain")
    def normalize_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned


class TargetSiteCreate(BaseModel):
    site_name: str
    site_url: str

    @validator("site_name", "site_url")
    def normalize_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned


class TargetSiteUpdate(BaseModel):
    site_name: Optional[str] = None
    site_url: Optional[str] = None
    active: Optional[bool] = None

    @validator("site_name", "site_url")
    def normalize_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Value must not be empty.")
        return cleaned


class TargetSiteOut(BaseModel):
    id: UUID
    site_name: str
    site_url: str
    active: bool
    created_at: datetime
    updated_at: datetime


class InviteCreate(BaseModel):
    email: EmailStr
    client_id: UUID


class RegisterRequest(BaseModel):
    token: str
    email: EmailStr
    password: str = Field(min_length=8)
    ui_language: str = "en"

    @validator("ui_language")
    def validate_language(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"en", "de"}:
            raise ValueError("ui_language must be 'en' or 'de'.")
        return cleaned


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UiLanguageUpdate(BaseModel):
    ui_language: str

    @validator("ui_language")
    def validate_language(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"en", "de"}:
            raise ValueError("ui_language must be 'en' or 'de'.")
        return cleaned


class UserOut(BaseModel):
    id: UUID
    email: EmailStr
    role: str
    client_id: Optional[UUID]
    ui_language: str
    active: bool
    created_at: datetime
    updated_at: datetime


class AuthResponse(BaseModel):
    ok: bool = True
    user: UserOut


class GuestPostStatusUpdate(BaseModel):
    status: str

    @validator("status")
    def validate_status(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned not in {"draft", "submitted"}:
            raise ValueError("status must be 'draft' or 'submitted'.")
        return cleaned
