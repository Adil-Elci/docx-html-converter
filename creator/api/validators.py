from __future__ import annotations

import re
from typing import Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def count_hyperlinks(html: str) -> int:
    soup = BeautifulSoup(html or "", "lxml")
    return len(soup.find_all("a", href=True))


def count_h2(html: str) -> int:
    soup = BeautifulSoup(html or "", "lxml")
    return len(soup.find_all("h2"))


def word_count_from_html(html: str) -> int:
    soup = BeautifulSoup(html or "", "lxml")
    text = soup.get_text(" ")
    words = re.findall(r"\b\w+\b", text)
    return len(words)


def locate_backlink(html: str, backlink_url: str) -> Tuple[Optional[str], int]:
    """Return (location, total_links). location is intro or section_N when found."""
    soup = BeautifulSoup(html or "", "lxml")
    target = _normalize_url(backlink_url)
    total_links = 0
    location: Optional[str] = None
    section_idx = 0
    for node in soup.body.descendants if soup.body else soup.descendants:
        if getattr(node, "name", None) == "h2":
            section_idx += 1
        if getattr(node, "name", None) == "a" and node.has_attr("href"):
            total_links += 1
            href = _normalize_url(str(node.get("href") or ""))
            if location is None and href and href == target:
                location = "intro" if section_idx == 0 else f"section_{section_idx}"
    return location, total_links


def validate_word_count(html: str, min_words: int, max_words: int) -> Optional[str]:
    count = word_count_from_html(html)
    if count < min_words:
        return f"word_count_too_short:{count}"
    if count > max_words:
        return f"word_count_too_long:{count}"
    return None


def validate_hyperlink_count(html: str, expected: int = 1) -> Optional[str]:
    count = count_hyperlinks(html)
    if count != expected:
        return f"hyperlink_count_invalid:{count}"
    return None


def validate_backlink_placement(html: str, backlink_url: str, placement: str) -> Optional[str]:
    location, _ = locate_backlink(html, backlink_url)
    if not location:
        return "backlink_missing"
    if location != placement:
        return f"backlink_wrong_placement:{location}"
    return None
