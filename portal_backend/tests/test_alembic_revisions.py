from __future__ import annotations

import re
from pathlib import Path


def _read_revision_metadata(path: Path) -> tuple[str | None, str | tuple[str, ...] | None]:
    text = path.read_text()
    revision_match = re.search(r'^revision\s*=\s*"([^"]+)"', text, re.MULTILINE)
    down_revision_match = re.search(
        r"^down_revision\s*=\s*(?:\"([^\"]+)\"|\(([^)]*)\)|None)",
        text,
        re.MULTILINE,
    )
    revision = revision_match.group(1) if revision_match else None
    if not down_revision_match:
        return revision, None
    single = down_revision_match.group(1)
    if single is not None:
        return revision, single
    tuple_body = down_revision_match.group(2)
    if tuple_body is None:
        return revision, None
    parts = tuple(
        item.strip().strip('"').strip("'")
        for item in tuple_body.split(",")
        if item.strip().strip('"').strip("'")
    )
    return revision, parts


def test_alembic_down_revisions_point_to_existing_revision_ids() -> None:
    versions_dir = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    metadata = {
        path.name: _read_revision_metadata(path)
        for path in versions_dir.glob("*.py")
        if path.name != "__init__.py"
    }
    revisions = {revision for revision, _ in metadata.values() if revision}
    missing: list[str] = []
    for file_name, (_, down_revision) in sorted(metadata.items()):
        if down_revision is None:
            continue
        if isinstance(down_revision, tuple):
            for item in down_revision:
                if item not in revisions:
                    missing.append(f"{file_name}: missing down_revision {item}")
            continue
        if down_revision not in revisions:
            missing.append(f"{file_name}: missing down_revision {down_revision}")
    assert not missing, "Invalid Alembic revision chain:\n" + "\n".join(missing)
