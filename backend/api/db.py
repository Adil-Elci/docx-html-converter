from __future__ import annotations

import os
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

_ENGINE: Optional[Engine] = None
_SESSIONMAKER: Optional[sessionmaker] = None


def _get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")
    return database_url


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_get_database_url(), pool_pre_ping=True)
    return _ENGINE


def get_sessionmaker() -> sessionmaker:
    global _SESSIONMAKER
    if _SESSIONMAKER is None:
        _SESSIONMAKER = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SESSIONMAKER


def get_db() -> Generator:
    session = get_sessionmaker()()
    try:
        yield session
    finally:
        session.close()
