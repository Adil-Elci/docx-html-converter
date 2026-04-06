from __future__ import annotations

import os
import re
from dataclasses import dataclass
from html import unescape
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .four_llm_schemas import (
    ArticleDraftWithPlaceholders,
    BriefLinkCandidate,
    ContentBriefInput,
    DraftArticleRequest,
    IntegrateLinksRequest,
    LinkedArticleDraft,
    MetaTagsPayload,
    MetaTagsRequest,
    ScrapedPage,
    SiteUnderstandingRequest,
    TargetSiteUnderstanding,
)
from .llm import LLMError, call_llm_json, call_llm_text


DEFAULT_4LLM_MODEL = "claude-sonnet-4-20250514"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


@dataclass
class LLMConfig:
    api_key: str
    base_url: str
    model: str
    timeout_seconds: int
    max_tokens: int
    temperature: float


def _read_llm_config(role: str, *, default_max_tokens: int, default_temperature: float) -> LLMConfig:
    role_key = role.upper()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    api_key = os.getenv(f"CREATOR_{role_key}_LLM_API_KEY", "").strip() or os.getenv("CREATOR_LLM_API_KEY", "").strip()
    if not api_key:
        api_key = anthropic_key or openai_key
    explicit_base = os.getenv(f"CREATOR_{role_key}_LLM_BASE_URL", "").strip() or os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    explicit_model = os.getenv(f"CREATOR_{role_key}_LLM_MODEL", "").strip() or os.getenv("CREATOR_4LLM_MODEL", "").strip()
    explicit_timeout = os.getenv(f"CREATOR_{role_key}_LLM_TIMEOUT_SECONDS", "").strip() or os.getenv("CREATOR_LLM_TIMEOUT_SECONDS", "").strip()
    explicit_max_tokens = os.getenv(f"CREATOR_{role_key}_LLM_MAX_TOKENS", "").strip()
    explicit_temperature = os.getenv(f"CREATOR_{role_key}_LLM_TEMPERATURE", "").strip()

    if explicit_base:
        base_url = explicit_base
    elif anthropic_key and not openai_key:
        base_url = DEFAULT_ANTHROPIC_BASE_URL
    else:
        base_url = DEFAULT_OPENAI_BASE_URL

    model = explicit_model or DEFAULT_4LLM_MODEL
    try:
        timeout_seconds = int(explicit_timeout or "90")
    except ValueError:
        timeout_seconds = 90
    try:
        max_tokens = int(explicit_max_tokens or str(default_max_tokens))
    except ValueError:
        max_tokens = default_max_tokens
    try:
        temperature = float(explicit_temperature or str(default_temperature))
    except ValueError:
        temperature = default_temperature
    return LLMConfig(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _strip_html(value: str) -> str:
    return _normalize_whitespace(re.sub(r"<[^>]+>", " ", unescape(value or "")))


def _same_host(candidate_url: str, site_url: str) -> bool:
    candidate_host = (urlparse(candidate_url).netloc or "").strip().lower()
    site_host = (urlparse(site_url).netloc or "").strip().lower()
    if not candidate_host or not site_host:
        return False
    return candidate_host == site_host


def _extract_candidate_links(site_url: str, soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        absolute = urljoin(site_url, href).split("#", 1)[0].rstrip("/")
        if not _same_host(absolute, site_url):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
        if len(links) >= 18:
            break
    return links


def _extract_page_payload(page_url: str, html: str) -> ScrapedPage:
    soup = BeautifulSoup(html, "lxml")
    title = _normalize_whitespace(soup.title.get_text(" ", strip=True)) if soup.title else ""
    meta_title = title
    meta_description = ""
    meta_desc_tag = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if meta_desc_tag and meta_desc_tag.get("content"):
        meta_description = _normalize_whitespace(str(meta_desc_tag.get("content")))
    h1 = _normalize_whitespace(soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""
    h2s = [
        _normalize_whitespace(node.get_text(" ", strip=True))
        for node in soup.find_all("h2")
        if _normalize_whitespace(node.get_text(" ", strip=True))
    ][:8]
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = _normalize_whitespace(soup.get_text(" ", strip=True))
    return ScrapedPage(
        url=page_url,
        title=title,
        meta_title=meta_title,
        meta_description=meta_description,
        h1=h1,
        h2s=h2s,
        text_excerpt=text[:1500],
    )


def scrape_target_site(request: SiteUnderstandingRequest) -> List[ScrapedPage]:
    headers = {"User-Agent": "creator-4llm/1.0"}
    root_url = str(request.target_site_url).rstrip("/")
    visited: set[str] = set()
    queue: List[str] = [root_url]
    pages: List[ScrapedPage] = []
    timeout_seconds = max(5, int(os.getenv("CREATOR_HTTP_TIMEOUT_SECONDS", "30")))

    while queue and len(pages) < request.max_pages:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            response = requests.get(current, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()
        except requests.RequestException:
            continue
        page = _extract_page_payload(current, response.text)
        pages.append(page)
        if len(pages) == 1:
            soup = BeautifulSoup(response.text, "lxml")
            queue.extend(_extract_candidate_links(root_url, soup))
    return pages


def understand_target_site(request: SiteUnderstandingRequest) -> TargetSiteUnderstanding:
    pages = scrape_target_site(request)
    if not pages:
        raise LLMError("Failed to scrape target site pages for understanding.")
    scraped_text = "\n\n".join(
        [
            f"URL: {page.url}\nTITLE: {page.title}\nMETA: {page.meta_description}\nH1: {page.h1}\nH2S: {' | '.join(page.h2s)}\nTEXT: {page.text_excerpt}"
            for page in pages
        ]
    )
    config = _read_llm_config("SITE_UNDERSTANDING", default_max_tokens=2000, default_temperature=0.0)
    system_prompt = (
        "Du bist ein erfahrener SEO-Analyst. Antworte immer mit gültigem JSON und ohne Zusatztext. "
        "Analysiere die Website inhaltlich und extrahiere nur belastbare Kerndaten."
    )
    user_prompt = (
        "Analysiere den folgenden Website-Inhalt und gib ein JSON-Objekt mit genau diesen Feldern zurück:\n"
        "{\n"
        '  "primary_niche": "string",\n'
        '  "main_topic": "string",\n'
        '  "target_audience": "string",\n'
        '  "seed_keywords": ["string"],\n'
        '  "content_tone": "string",\n'
        '  "site_type": "string",\n'
        '  "language": "string"\n'
        "}\n\n"
        f"SCRAPED CONTENT:\n{scraped_text}"
    )
    payload = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=config.api_key,
        base_url=config.base_url,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
        max_tokens=config.max_tokens,
        temperature=config.temperature,
        request_label="4llm_site_understanding",
    )
    payload["scraped_pages"] = [page.model_dump() for page in pages]
    return TargetSiteUnderstanding.model_validate(payload)


def draft_article(request: DraftArticleRequest) -> ArticleDraftWithPlaceholders:
    brief = request.content_brief
    config = _read_llm_config("DRAFT_ARTICLE", default_max_tokens=7000, default_temperature=0.2)
    system_prompt = (
        "Du bist ein erfahrener SEO-Content-Writer. Schreibe hilfreiche, natürliche und fachlich klare Artikel. "
        "Antworte ausschließlich mit Markdown. Verwende immer deutsche Umlaute korrekt. "
        "Füge keine echten URLs ein. Verwende stattdessen nur Platzhalter wie [[INTERNAL_LINK_1]] oder [[EXTERNAL_LINK_1]]."
    )
    user_prompt = (
        "Schreibe einen vollständigen SEO-Artikel auf Basis dieses Content-Briefs.\n\n"
        "Wichtige Link-Regeln:\n"
        "- Verwende nur Platzhalter [[INTERNAL_LINK_1]], [[INTERNAL_LINK_2]], ... für interne Links\n"
        "- Verwende nur Platzhalter [[EXTERNAL_LINK_1]], [[EXTERNAL_LINK_2]], ... für externe Links\n"
        "- Platziere Platzhalter nur dort, wo sie natürlich wirken\n"
        "- Verwende 2 bis 4 interne Platzhalter und 1 bis 3 externe Platzhalter\n"
        "- Schreibe den Artikel vollständig in Markdown mit H1, H2 und bei Bedarf H3\n\n"
        f"CONTENT BRIEF JSON:\n{brief.model_dump_json(indent=2)}"
    )
    markdown = call_llm_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=config.api_key,
        base_url=config.base_url,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
        max_tokens=config.max_tokens,
        temperature=config.temperature,
        request_label="4llm_draft_article",
    ).strip()
    return ArticleDraftWithPlaceholders(markdown=markdown)


def integrate_links(request: IntegrateLinksRequest) -> LinkedArticleDraft:
    config = _read_llm_config("INTEGRATE_LINKS", default_max_tokens=7000, default_temperature=0.0)
    system_prompt = (
        "Du bist ein erfahrener SEO-Editor. Ersetze Link-Platzhalter präzise und natürlich. "
        "Antworte ausschließlich mit Markdown. Erfinde niemals zusätzliche Links. Verwende immer deutsche Umlaute korrekt."
    )
    user_prompt = (
        "Ersetze in diesem Artikel die vorhandenen Platzhalter mit passenden echten Links.\n\n"
        "Regeln:\n"
        "- Ersetze [[INTERNAL_LINK_N]] nur mit URLs aus internal_links\n"
        "- Ersetze [[EXTERNAL_LINK_N]] nur mit URLs aus external_links\n"
        "- Wenn kein natürlicher Match existiert, entferne den Platzhalter vollständig\n"
        "- Füge keine neuen Links hinzu\n"
        "- Bewahre die bestehende Struktur und Überschriften exakt\n\n"
        f"ARTICLE MARKDOWN:\n{request.article_markdown}\n\n"
        f"INTERNAL LINKS JSON:\n{[item.model_dump() for item in request.internal_links]}\n\n"
        f"EXTERNAL LINKS JSON:\n{[item.model_dump() for item in request.external_links]}"
    )
    markdown = call_llm_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=config.api_key,
        base_url=config.base_url,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
        max_tokens=config.max_tokens,
        temperature=config.temperature,
        request_label="4llm_integrate_links",
    ).strip()
    placed_links = _extract_markdown_links(markdown)
    return LinkedArticleDraft(markdown=markdown, placed_links=placed_links)


def generate_meta(request: MetaTagsRequest) -> MetaTagsPayload:
    config = _read_llm_config("GENERATE_META", default_max_tokens=800, default_temperature=0.0)
    system_prompt = (
        "Du bist ein SEO-Spezialist. Antworte immer mit gültigem JSON und ohne Zusatztext. "
        "Verwende deutsche Umlaute korrekt."
    )
    user_prompt = (
        "Erzeuge SEO-Metadaten für diesen Artikel.\n\n"
        f"Target keyword: {request.target_keyword}\n"
        f"Article title: {request.article_title}\n"
        f"Article intro: {request.article_intro}\n\n"
        "Gib exakt dieses JSON zurück:\n"
        '{\n  "meta_title": "string",\n  "meta_description": "string",\n  "tags": ["string"]\n}'
    )
    payload = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=config.api_key,
        base_url=config.base_url,
        model=config.model,
        timeout_seconds=config.timeout_seconds,
        max_tokens=config.max_tokens,
        temperature=config.temperature,
        request_label="4llm_generate_meta",
    )
    return MetaTagsPayload.model_validate(payload)


def _extract_markdown_links(markdown: str) -> List[Dict[str, str]]:
    placed: List[Dict[str, str]] = []
    for anchor_text, url in re.findall(r"\[([^\]]+)\]\((https?://[^)]+)\)", markdown or ""):
        placed.append({"anchor_text": anchor_text.strip(), "url": url.strip()})
    return placed
