from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator


def _get_fernet() -> Fernet:
    key = os.environ.get("WP_CREDENTIAL_ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError("WP_CREDENTIAL_ENCRYPTION_KEY is not set.")
    return Fernet(key.encode())


def encrypt_credential(value: str) -> str:
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_credential(value: str) -> str:
    """Decrypt a Fernet token. Returns the value as-is if it is not a valid token (plaintext fallback for migration)."""
    try:
        return _get_fernet().decrypt(value.encode()).decode()
    except (InvalidToken, Exception):
        return value


class EncryptedText(TypeDecorator):
    """SQLAlchemy column type that transparently encrypts on write and decrypts on read."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return encrypt_credential(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return decrypt_credential(value)
