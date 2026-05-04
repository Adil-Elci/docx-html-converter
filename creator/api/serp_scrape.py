"""Fetch and parse competitor article pages from DataForSEO SERP results.

For each top-N organic URL, we extract: title, headings (H1/H2/H3), main body
text, internal/external link counts, and schema.org @type values. Designed
to be resilient: failures on individual URLs are captured as a ``fetch_status``
field rather than raising, so a single dead competitor does not break research.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .dataforseo import OrganicResult

logger = logging.getLogger("creator.serp_scrape")

DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_MAX_BODY_CHARS = 6000
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; ElciContentResearchBot/1.0; +https://elci.cloud/research-bot)"
)
_DROP_TAGS = ("script", "style", "nav", "header", "footer", "aside", "form", "noscript", "svg")


@dataclass
class ScrapedCompetitor:
    url: str
    final_url: str = ""
    fetch_status: str = "pending"  # ok | forbidden | timeout | network_error | parse_failed | empty
    http_status: Optional[int] = None
    title: str = ""
    h1: str = ""
    h2s: List[str] = field(default_factory=list)
    h3s: List[str] = field(default_factory=list)
    body_text: str = ""
    word_count: int = 0
    internal_link_count: int = 0
    external_link_count: int = 0
    schema_types: List[str] = field(default_factory=list)
    has_faq_schema: bool = False
    has_article_schema: bool = False


# ---- HTTP fetch -------------------------------------------------------------


def _fetch(url: str, *, timeout_seconds: int, user_agent: str) -> tuple[Optional[str], str, Optional[int], str]:
    """Returns (html_text, final_url, http_status, fetch_status)."""

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
    }
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout_seconds,
            allow_redirects=True,
        )
    except requests.Timeout:
        return None, url, None, "timeout"
    except requests.RequestException as exc:
        logger.info("serp_scrape.network_error url=%s err=%s", url, exc)
        return None, url, None, "network_error"

    final_url = response.url or url
    if response.status_code == 403:
        return None, final_url, 403, "forbidden"
    if response.status_code >= 400:
        return None, final_url, response.status_code, "network_error"
    text = response.text or ""
    if not text.strip():
        return None, final_url, response.status_code, "empty"
    return text, final_url, response.status_code, "ok"


# ---- HTML extraction --------------------------------------------------------


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _heading_text(node) -> str:
    return _normalize_whitespace(node.get_text(" ", strip=True))


def _select_main_content(soup: BeautifulSoup):
    for selector in ("article", "main", "[role=main]", "#content", ".content", ".post", ".entry"):
        node = soup.select_one(selector)
        if node:
            return node
    return soup.body or soup


def _extract_body_text(soup: BeautifulSoup, *, max_chars: int) -> str:
    main = _select_main_content(soup)
    for tag in main(_DROP_TAGS):
        tag.decompose()
    text = _normalize_whitespace(main.get_text(" ", strip=True))
    return text[:max_chars]


def _word_count(text: str) -> int:
    return len([word for word in re.split(r"\s+", text) if word.strip()])


def _link_breakdown(soup: BeautifulSoup, base_url: str) -> tuple[int, int]:
    base_host = (urlparse(base_url).netloc or "").lower()
    if base_host.startswith("www."):
        base_host = base_host[4:]
    internal = external = 0
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue
        absolute = urljoin(base_url, href)
        host = (urlparse(absolute).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        if host == base_host:
            internal += 1
        else:
            external += 1
    return internal, external


def _walk_schema_types(node, types: List[str]) -> None:
    if isinstance(node, dict):
        raw_type = node.get("@type")
        if isinstance(raw_type, str):
            types.append(raw_type)
        elif isinstance(raw_type, list):
            for inner in raw_type:
                if isinstance(inner, str):
                    types.append(inner)
        for value in node.values():
            _walk_schema_types(value, types)
    elif isinstance(node, list):
        for inner in node:
            _walk_schema_types(inner, types)


def _extract_schema_types(soup: BeautifulSoup) -> List[str]:
    types: List[str] = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.text or ""
        if not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        _walk_schema_types(parsed, types)
    seen: set[str] = set()
    deduped: List[str] = []
    for value in types:
        cleaned = value.strip()
        if not cleaned or cleaned.lower() in seen:
            continue
        seen.add(cleaned.lower())
        deduped.append(cleaned)
    return deduped


def _parse(html: str, *, source_url: str, max_body_chars: int) -> ScrapedCompetitor:
    soup = BeautifulSoup(html, "lxml")
    title = _normalize_whitespace(soup.title.get_text(" ", strip=True)) if soup.title else ""
    h1_node = soup.find("h1")
    h1 = _heading_text(h1_node) if h1_node else ""
    h2s = [_heading_text(node) for node in soup.find_all("h2")]
    h2s = [text for text in h2s if text][:20]
    h3s = [_heading_text(node) for node in soup.find_all("h3")]
    h3s = [text for text in h3s if text][:30]
    # Extract schema and links BEFORE body-text extraction, which decomposes
    # <script> and other auxiliary tags from the soup in place.
    schema_types = _extract_schema_types(soup)
    internal_count, external_count = _link_breakdown(soup, source_url)
    body_text = _extract_body_text(soup, max_chars=max_body_chars)
    types_lower = {t.lower() for t in schema_types}

    return ScrapedCompetitor(
        url=source_url,
        title=title,
        h1=h1,
        h2s=h2s,
        h3s=h3s,
        body_text=body_text,
        word_count=_word_count(body_text),
        internal_link_count=internal_count,
        external_link_count=external_count,
        schema_types=schema_types,
        has_faq_schema="faqpage" in types_lower,
        has_article_schema=any(t in types_lower for t in ("article", "newsarticle", "blogposting")),
    )


# ---- public API -------------------------------------------------------------


def scrape_competitor(
    url: str,
    *,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_body_chars: int = DEFAULT_MAX_BODY_CHARS,
    user_agent: str = DEFAULT_USER_AGENT,
) -> ScrapedCompetitor:
    html, final_url, http_status, status = _fetch(url, timeout_seconds=timeout_seconds, user_agent=user_agent)
    if status != "ok" or html is None:
        return ScrapedCompetitor(url=url, final_url=final_url, fetch_status=status, http_status=http_status)
    try:
        scraped = _parse(html, source_url=final_url, max_body_chars=max_body_chars)
    except Exception as exc:  # parsing should be best-effort
        logger.info("serp_scrape.parse_failed url=%s err=%s", url, exc)
        return ScrapedCompetitor(url=url, final_url=final_url, http_status=http_status, fetch_status="parse_failed")
    scraped.final_url = final_url
    scraped.http_status = http_status
    scraped.fetch_status = "ok"
    return scraped


def scrape_top_results(
    organic: Sequence[OrganicResult],
    *,
    top_n: int = 5,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_body_chars: int = DEFAULT_MAX_BODY_CHARS,
    user_agent: str = DEFAULT_USER_AGENT,
) -> List[ScrapedCompetitor]:
    if top_n <= 0:
        return []
    targets: Iterable[OrganicResult] = organic[:top_n]
    return [
        scrape_competitor(
            result.url,
            timeout_seconds=timeout_seconds,
            max_body_chars=max_body_chars,
            user_agent=user_agent,
        )
        for result in targets
        if result.url
    ]
