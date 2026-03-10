from __future__ import annotations

import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("creator.web")

PROFILE_MIN_PRIMARY_TEXT_CHARS = 220
PROFILE_NOISE_TAGS = (
    "script",
    "style",
    "noscript",
    "template",
    "svg",
    "header",
    "footer",
    "nav",
    "aside",
    "form",
    "dialog",
    "button",
    "input",
    "select",
    "textarea",
    "option",
    "label",
)
PROFILE_BOILERPLATE_LINK_TOKENS = {
    "account",
    "agb",
    "cart",
    "checkout",
    "contact",
    "datenschutz",
    "impressum",
    "konto",
    "kontakt",
    "kontaktformular",
    "login",
    "privacy",
    "register",
    "registrieren",
    "suche",
    "support",
    "terms",
    "warenkorb",
}


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
    soup = _extract_profile_content_fragment(html)
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
    soup = _extract_profile_content_fragment(html)
    candidates: List[Tuple[float, int, str]] = []
    seen_links: set[str] = set()
    for index, a in enumerate(soup.find_all("a", href=True)):
        href = str(a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("/"):
            href = f"{parsed.scheme}://{host}{href}"
        elif not href.startswith("http"):
            continue
        if urlparse(href).netloc != host:
            continue
        if href in seen_links:
            continue
        score = _score_profile_internal_link_candidate(
            anchor_text=re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip(),
            absolute_url=href,
        )
        if score <= 0:
            continue
        seen_links.add(href)
        candidates.append((score, index, href))
    ranked = sorted(candidates, key=lambda item: (-item[0], item[1], item[2]))
    return [href for _score, _index, href in ranked[:limit]]


def _strip_profile_noise(container):
    for tag in container(PROFILE_NOISE_TAGS):
        tag.decompose()
    return container


def _extract_profile_content_fragment(html: str):
    base = BeautifulSoup(html or "", "lxml")
    _strip_profile_noise(base)
    body = base.body or base
    candidates: List[Tuple[int, str]] = []
    for source in (
        body.find("main"),
        body.find("article"),
        body.find(attrs={"role": "main"}),
        body,
    ):
        if source is None:
            continue
        fragment = BeautifulSoup(str(source), "lxml")
        root = _strip_profile_noise(fragment.body or fragment)
        text = re.sub(r"\s+", " ", root.get_text(" ", strip=True)).strip()
        if text:
            candidates.append((len(text), str(root)))
            if source is not body and len(text) >= PROFILE_MIN_PRIMARY_TEXT_CHARS:
                return root
    if candidates:
        best_html = max(candidates, key=lambda item: item[0])[1]
        best = BeautifulSoup(best_html, "lxml")
        return best.body or best
    fallback = BeautifulSoup("", "lxml")
    return fallback


def _profile_link_tokens(value: str) -> List[str]:
    tokens: List[str] = []
    for token in re.findall(r"\b[a-zA-ZäöüÄÖÜß-]{3,}\b", (value or "").lower()):
        cleaned = re.sub(r"[^a-zA-ZäöüÄÖÜß]", "", token).strip().lower()
        if cleaned:
            tokens.append(cleaned)
    return tokens


def _score_profile_internal_link_candidate(*, anchor_text: str, absolute_url: str) -> float:
    parsed = urlparse(absolute_url)
    path_tokens = _profile_link_tokens((parsed.path or "").replace("/", " "))
    anchor_tokens = _profile_link_tokens(anchor_text)
    combined_tokens = set(path_tokens + anchor_tokens)
    signal_tokens = {
        token
        for token in combined_tokens
        if len(token) >= 4 and token not in PROFILE_BOILERPLATE_LINK_TOKENS
    }
    boilerplate_hits = combined_tokens & PROFILE_BOILERPLATE_LINK_TOKENS
    if boilerplate_hits and len(signal_tokens) <= 1:
        return -1.0
    if not signal_tokens:
        return -1.0
    depth = len([segment for segment in (parsed.path or "").split("/") if segment.strip()])
    score = 3.0 * len(signal_tokens) + min(1.5, depth * 0.5)
    if anchor_text:
        score += min(1.5, len(anchor_tokens) * 0.3)
    return score
