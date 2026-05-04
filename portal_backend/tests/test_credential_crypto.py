from __future__ import annotations

import pytest
from cryptography.fernet import Fernet


@pytest.fixture
def fernet_key(monkeypatch):
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("WP_CREDENTIAL_ENCRYPTION_KEY", key)
    yield key


def test_encrypt_decrypt_roundtrip(fernet_key):
    from portal_backend.api import credential_crypto

    token = credential_crypto.encrypt_credential("J9FL 6ebn kjfn NHH& BJtu")
    assert token != "J9FL 6ebn kjfn NHH& BJtu"
    assert credential_crypto.decrypt_credential(token) == "J9FL 6ebn kjfn NHH& BJtu"


def test_decrypt_returns_value_as_is_for_legacy_plaintext(fernet_key):
    # The migration was rolled out without downtime, so the decryption layer
    # must transparently pass plaintext (non-Fernet) values through.
    from portal_backend.api import credential_crypto

    plaintext = "legacy_unencrypted_value"
    assert credential_crypto.decrypt_credential(plaintext) == plaintext


def test_encrypt_raises_when_key_missing(monkeypatch):
    monkeypatch.delenv("WP_CREDENTIAL_ENCRYPTION_KEY", raising=False)
    from portal_backend.api import credential_crypto

    with pytest.raises(RuntimeError, match="WP_CREDENTIAL_ENCRYPTION_KEY"):
        credential_crypto.encrypt_credential("x")


def test_decrypt_with_missing_key_returns_value_as_is(monkeypatch):
    # decrypt_credential's broad except clause is intentional: callers must
    # never crash on read. Without a key, we hand back the stored value.
    monkeypatch.delenv("WP_CREDENTIAL_ENCRYPTION_KEY", raising=False)
    from portal_backend.api import credential_crypto

    assert credential_crypto.decrypt_credential("anything") == "anything"


def test_encrypted_text_processes_bind_and_result(fernet_key):
    from portal_backend.api.credential_crypto import EncryptedText

    column_type = EncryptedText()
    encrypted = column_type.process_bind_param("secret123", dialect=None)
    assert encrypted is not None
    assert encrypted != "secret123"
    assert column_type.process_result_value(encrypted, dialect=None) == "secret123"

    assert column_type.process_bind_param(None, dialect=None) is None
    assert column_type.process_result_value(None, dialect=None) is None
