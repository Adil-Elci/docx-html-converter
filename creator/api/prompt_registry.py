from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


_PROMPTS_ROOT = Path(__file__).resolve().parents[1] / "prompts"
_PLAIN_VERSION = re.compile(r"^v(\d+)$")
_LANG_VERSION = re.compile(r"^v(\d+)\.([a-z]{2})$")


@dataclass(frozen=True)
class Prompt:
    name: str
    version: str  # e.g. "v1" -- the language suffix is not part of the version
    language: Optional[str]  # ISO 639-1 lowercase, or None for language-neutral prompts
    body: str
    metadata: Dict[str, str]


def _parse_front_matter(text: str) -> tuple[Dict[str, str], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    raw = text[4:end]
    body = text[end + len("\n---\n") :]
    metadata: Dict[str, str] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        metadata[key.strip()] = value.strip()
    return metadata, body


def _scan_versions(name: str) -> Dict[str, Dict[Optional[str], Path]]:
    """Returns {version_key: {language_or_None: path}} for all .md files under name/.

    Language-neutral files are keyed under ``None``; language-specific files
    under the 2-letter ISO code.
    """

    folder = _PROMPTS_ROOT / name
    if not folder.is_dir():
        return {}
    out: Dict[str, Dict[Optional[str], Path]] = {}
    for entry in folder.iterdir():
        if not entry.is_file() or entry.suffix != ".md":
            continue
        stem = entry.stem
        plain_match = _PLAIN_VERSION.match(stem)
        lang_match = _LANG_VERSION.match(stem)
        if plain_match:
            version_key = stem
            out.setdefault(version_key, {})[None] = entry
        elif lang_match:
            version_key = f"v{lang_match.group(1)}"
            language = lang_match.group(2)
            out.setdefault(version_key, {})[language] = entry
    return out


def _list_versions(name: str) -> List[str]:
    versions = _scan_versions(name)
    if not versions:
        return []

    def _sort_key(version: str) -> int:
        match = _PLAIN_VERSION.match(version)
        return int(match.group(1)) if match else 0

    return sorted(versions.keys(), key=_sort_key)


def list_prompts() -> Dict[str, List[str]]:
    if not _PROMPTS_ROOT.is_dir():
        return {}
    return {entry.name: _list_versions(entry.name) for entry in _PROMPTS_ROOT.iterdir() if entry.is_dir()}


def load(name: str, version: Optional[str] = None, *, language: Optional[str] = None) -> Prompt:
    """Load a prompt body by ``name`` (folder), optional ``version`` (e.g. ``v1``),
    and optional ``language`` (ISO 639-1 lowercase).

    Resolution order when ``language`` is provided:
    1. ``<version>.<language>.md`` (e.g. ``v1.fr.md``)
    2. ``<version>.md`` (language-neutral fallback for prompts that haven't
       been translated yet)

    With no ``language``, picks ``<version>.md`` if present, otherwise the
    German variant ``<version>.de.md`` (the project's historical default).
    """

    versions = _scan_versions(name)
    if not versions:
        raise FileNotFoundError(f"No prompt versions found for '{name}' under {_PROMPTS_ROOT}.")

    def _sort_key(v: str) -> int:
        match = _PLAIN_VERSION.match(v)
        return int(match.group(1)) if match else 0

    resolved = version or sorted(versions.keys(), key=_sort_key)[-1]
    if resolved not in versions:
        raise FileNotFoundError(f"Prompt '{name}' has no version '{resolved}'. Available: {sorted(versions.keys())}.")

    language_map = versions[resolved]
    chosen_path: Optional[Path] = None
    chosen_language: Optional[str] = None
    if language:
        normalized = language.lower()
        if normalized in language_map:
            chosen_path = language_map[normalized]
            chosen_language = normalized
        elif None in language_map:
            chosen_path = language_map[None]
            chosen_language = None
    if chosen_path is None:
        # No explicit language requested, or no match: prefer language-neutral,
        # then fall back to German (historical default).
        if None in language_map:
            chosen_path = language_map[None]
            chosen_language = None
        elif "de" in language_map:
            chosen_path = language_map["de"]
            chosen_language = "de"
        else:
            # Last-resort: any file we have. Sorted for determinism.
            for lang_key in sorted(language_map.keys(), key=lambda k: ("" if k is None else k)):
                chosen_path = language_map[lang_key]
                chosen_language = lang_key
                break

    if chosen_path is None:
        raise FileNotFoundError(
            f"Prompt '{name}/{resolved}' has no usable file (language={language!r})."
        )

    text = chosen_path.read_text(encoding="utf-8")
    metadata, body = _parse_front_matter(text)
    return Prompt(
        name=name,
        version=resolved,
        language=chosen_language,
        body=body.strip(),
        metadata=metadata,
    )
