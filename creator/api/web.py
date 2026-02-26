from __future__ import annotations

import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("creator.web")


def fetch_url(
    url: str,
    *,
    purpose: str,
    warnings: List[str],
    debug: Dict[str, object],
    timeout_seconds: int,
    retries: int,
) -> str:
    cleaned = url.strip()
    if not cleaned:
        warnings.append(f"missing_url:{purpose}")
        return ""

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "creator-service/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    last_error: Optional[str] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = session.get(cleaned, timeout=timeout_seconds, allow_redirects=True)
            if response.status_code >= 400:
                last_error = f"http_{response.status_code}"
                time.sleep(0.4 * attempt)
                continue
            debug.setdefault("fetched_pages", []).append({"url": cleaned, "purpose": purpose})
            return response.text
        except requests.RequestException as exc:
            last_error = str(exc)
            time.sleep(0.4 * attempt)

    warnings.append(f"fetch_failed:{purpose}")
    if last_error:
        logger.warning("creator.fetch_failed url=%s purpose=%s error=%s", cleaned, purpose, last_error)
    return ""


def sanitize_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_page_title(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string if soup.title else ""
    return str(title or "").strip()


def extract_meta_content(html: str, keys: List[Tuple[str, str]]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for attr, value in keys:
        tag = soup.find("meta", attrs={attr: value})
        if tag and tag.get("content"):
            return str(tag.get("content")).strip()
    return ""


def extract_canonical_link(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("link", rel="canonical")
    if tag and tag.get("href"):
        return str(tag.get("href")).strip()
    return ""


def extract_internal_links(html: str, base_url: str, limit: int = 12) -> List[str]:
    if not html:
        return []
    parsed = urlparse(base_url)
    host = parsed.netloc
    if not host:
        return []
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("/"):
            href = f"{parsed.scheme}://{host}{href}"
        elif not href.startswith("http"):
            continue
        if urlparse(href).netloc != host:
            continue
        if href not in links:
            links.append(href)
        if len(links) >= limit:
            break
    return links
