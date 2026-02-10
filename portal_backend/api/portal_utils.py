from __future__ import annotations

import re
from typing import List, Optional, Union
from urllib.parse import urlparse

from .portal_schemas import ContentJson, Section


def normalize_domain(domain: str) -> str:
    cleaned = domain.strip().lower()
    return cleaned[4:] if cleaned.startswith("www.") else cleaned


def validate_backlink_url(backlink_url: str, client_domain: str) -> str:
    parsed = urlparse(backlink_url.strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("backlink_url must start with http:// or https://.")
    if not parsed.hostname:
        raise ValueError("backlink_url must be a valid URL.")
    host = normalize_domain(parsed.hostname)
    domain = normalize_domain(client_domain)
    if host != domain and not host.endswith(f".{domain}"):
        raise ValueError("backlink_url domain must match the client website domain.")
    return backlink_url.strip()


def split_paragraphs(text: str) -> List[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    return paragraphs


def render_paragraphs(paragraphs: List[str]) -> str:
    if not paragraphs:
        return ""
    return "\n\n".join(paragraphs)


def backlink_markdown(backlink_url: str) -> str:
    return f"[{backlink_url}]({backlink_url})"


def apply_backlink(
    introduction: str,
    sections: List[Section],
    backlink_url: str,
    auto_backlink: bool,
    backlink_placement: Optional[str],
) -> str:
    lines: List[str] = []

    intro_paragraphs = split_paragraphs(introduction)
    if auto_backlink:
        if intro_paragraphs:
            intro_paragraphs.append(backlink_markdown(backlink_url))
    elif backlink_placement == "intro":
        if intro_paragraphs:
            intro_paragraphs.append(backlink_markdown(backlink_url))
        else:
            intro_paragraphs = [backlink_markdown(backlink_url)]

    if intro_paragraphs:
        lines.append(render_paragraphs(intro_paragraphs))

    for section in sections:
        lines.append(f"## {section.section_title}")
        body_paragraphs = split_paragraphs(section.section_body)
        if body_paragraphs:
            lines.append(render_paragraphs(body_paragraphs))

    if (auto_backlink and not intro_paragraphs) or backlink_placement == "conclusion":
        lines.append(backlink_markdown(backlink_url))

    return "\n\n".join([line for line in lines if line])


def generate_markdown(
    title_h1: str,
    content_json: Union[ContentJson, dict],
    backlink_url: str,
    auto_backlink: bool,
    backlink_placement: Optional[str],
) -> str:
    if not isinstance(content_json, ContentJson):
        content_json = ContentJson.parse_obj(content_json)
    sections = content_json.sections
    markdown_body = apply_backlink(
        introduction=content_json.introduction or "",
        sections=sections,
        backlink_url=backlink_url,
        auto_backlink=auto_backlink,
        backlink_placement=backlink_placement,
    )
    return "\n\n".join([f"# {title_h1.strip()}", markdown_body]).strip()
