from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


_PROMPTS_ROOT = Path(__file__).resolve().parents[1] / "prompts"
_VERSION_PATTERN = re.compile(r"^v(\d+)$")


@dataclass(frozen=True)
class Prompt:
    name: str
    version: str
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


def _list_versions(name: str) -> List[str]:
    folder = _PROMPTS_ROOT / name
    if not folder.is_dir():
        return []
    versions: List[tuple[int, str]] = []
    for entry in folder.iterdir():
        if not entry.is_file() or entry.suffix != ".md":
            continue
        match = _VERSION_PATTERN.match(entry.stem)
        if match is None:
            continue
        versions.append((int(match.group(1)), entry.stem))
    return [name for _, name in sorted(versions)]


def list_prompts() -> Dict[str, List[str]]:
    if not _PROMPTS_ROOT.is_dir():
        return {}
    return {entry.name: _list_versions(entry.name) for entry in _PROMPTS_ROOT.iterdir() if entry.is_dir()}


def load(name: str, version: Optional[str] = None) -> Prompt:
    versions = _list_versions(name)
    if not versions:
        raise FileNotFoundError(f"No prompt versions found for '{name}' under {_PROMPTS_ROOT}.")
    resolved = version or versions[-1]
    if resolved not in versions:
        raise FileNotFoundError(f"Prompt '{name}' has no version '{resolved}'. Available: {versions}.")
    path = _PROMPTS_ROOT / name / f"{resolved}.md"
    text = path.read_text(encoding="utf-8")
    metadata, body = _parse_front_matter(text)
    return Prompt(name=name, version=resolved, body=body.strip(), metadata=metadata)
