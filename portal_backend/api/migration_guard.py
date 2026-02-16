from __future__ import annotations

import os
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from .db import get_engine


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _expected_heads() -> set[str]:
    project_root = Path(__file__).resolve().parents[1]
    alembic_cfg = Config(str(project_root / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(project_root / "alembic"))
    return set(ScriptDirectory.from_config(alembic_cfg).get_heads())


def _current_db_heads() -> set[str]:
    with get_engine().connect() as connection:
        rows = connection.execute(text("SELECT version_num FROM alembic_version")).fetchall()
    return {row[0] for row in rows if row[0]}


def verify_db_is_at_head() -> None:
    expected = _expected_heads()
    current = _current_db_heads()
    if current != expected:
        raise RuntimeError(
            "Database schema is not at Alembic head. "
            f"current={sorted(current)} expected={sorted(expected)}. "
            "Ensure Dokploy starts backend via entrypoint.sh so `alembic upgrade head` runs."
        )


def should_verify_db_head_on_startup() -> bool:
    return _env_flag("REQUIRE_DB_AT_HEAD", True)
