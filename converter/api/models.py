from __future__ import annotations

import ipaddress
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, HttpUrl, validator


class ConvertOptions(BaseModel):
    remove_images: bool = True
    fix_headings: bool = True
    max_slug_length: int = Field(default=80, ge=20, le=120)
    max_meta_length: int = Field(default=155, ge=80, le=200)
    max_excerpt_length: int = Field(default=180, ge=80, le=300)


class ConvertRequest(BaseModel):
    publishing_site: str
    source_url: HttpUrl
    post_status: str = "draft"
    language: str = "de"
    client_id: Optional[str] = None
    post_id: Optional[str] = None
    client_url: Optional[str] = None
    options: ConvertOptions = Field(default_factory=ConvertOptions)

    @validator("publishing_site")
    def validate_publishing_site(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("publishing_site is required.")
        return cleaned

    @validator("post_status")
    def validate_post_status(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned not in {"draft", "publish"}:
            raise ValueError("post_status must be 'draft' or 'publish'.")
        return cleaned

    @validator("language")
    def validate_language(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned != "de":
            raise ValueError("language must be 'de'.")
        return cleaned

    @validator("source_url")
    def validate_source_url(cls, value: HttpUrl) -> HttpUrl:
        url_str = str(value)
        parsed = urlparse(url_str)

        if parsed.scheme not in {"http", "https"}:
            raise ValueError("source_url must start with http:// or https://.")

        hostname = parsed.hostname
        if not hostname:
            raise ValueError("source_url must be a valid URL.")

        lowered = hostname.lower()
        if lowered in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("source_url hostname is not allowed.")

        try:
            ip = ipaddress.ip_address(lowered)
        except ValueError:
            return value

        blocked_ranges = (
            ipaddress.ip_network("10.0.0.0/8"),
            ipaddress.ip_network("172.16.0.0/12"),
            ipaddress.ip_network("192.168.0.0/16"),
            ipaddress.ip_network("169.254.0.0/16"),
            ipaddress.ip_network("127.0.0.0/8"),
            ipaddress.ip_network("::1/128"),
        )
        if any(ip in net for net in blocked_ranges) or ip.is_private or ip.is_link_local or ip.is_loopback:
            raise ValueError("source_url hostname resolves to a private or local IP.")

        return value


class ConvertDebug(BaseModel):
    download_ms: int
    convert_ms: int
    sanitize_ms: int
    total_ms: int


class ConvertResponse(BaseModel):
    ok: bool
    publishing_site: str
    source_url: str
    source_type: str
    source_filename: str
    title: str = Field(min_length=1, max_length=200)
    slug: str = Field(min_length=1, max_length=120)
    excerpt: str = Field(max_length=300)
    meta_description: str = Field(max_length=200)
    clean_html: str = Field(min_length=1)
    image_prompt: str
    warnings: List[str] = Field(default_factory=list)
    debug: Optional[ConvertDebug] = None


class ErrorResponse(BaseModel):
    ok: bool = False
    error: str
    details: Optional[Any] = None
