from __future__ import annotations

import os

from sqlalchemy.types import UserDefinedType


DEFAULT_VECTOR_DIM = 1536


def get_configured_vector_dim() -> int:
    raw = os.getenv("EMBEDDING_VECTOR_DIM", str(DEFAULT_VECTOR_DIM)).strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_VECTOR_DIM
    return value if value > 0 else DEFAULT_VECTOR_DIM


class Vector(UserDefinedType):
    def __init__(self, dim: int | None = None):
        self.dim = dim or get_configured_vector_dim()

    def get_col_spec(self, **_kwargs) -> str:
        return f"VECTOR({self.dim})"

