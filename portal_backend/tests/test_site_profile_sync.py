from __future__ import annotations

from portal_backend.api.site_profile_sync import run_site_profile_sync


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeSessionmaker:
    def __init__(self, session):
        self._session = session
        self.kw = {"bind": object()}

    def __call__(self):
        return self._session


def test_run_site_profile_sync_processes_publishing_and_target_sites(monkeypatch):
    publishing_sites = [
        type("Site", (), {"site_url": "https://publisher-a.example.com", "name": "A"})(),
        type("Site", (), {"site_url": "https://publisher-b.example.com", "name": "B"})(),
    ]
    target_rows = [
        (type("Target", (), {"id": "t1"})(), "https://target-a.example.com"),
        (type("Target", (), {"id": "t2"})(), "https://target-b.example.com"),
    ]
    session = _FakeSession()
    sessionmaker = _FakeSessionmaker(session)
    calls = {"publishing": [], "target": []}

    monkeypatch.setattr("portal_backend.api.site_profile_sync._select_publishing_sites", lambda db: publishing_sites)
    monkeypatch.setattr("portal_backend.api.site_profile_sync._select_target_site_rows", lambda db: target_rows)

    monkeypatch.setattr(
        "portal_backend.api.site_profile_sync.ensure_publishing_site_profile",
        lambda db, **kwargs: calls["publishing"].append(kwargs["site"].site_url),
    )
    monkeypatch.setattr(
        "portal_backend.api.site_profile_sync.ensure_target_site_profile",
        lambda db, **kwargs: calls["target"].append(kwargs["target_site_url"]),
    )

    summary = run_site_profile_sync(sessionmaker, timeout_seconds=5, max_pages=2)

    assert summary == {
        "publishing_processed": 2,
        "publishing_failed": 0,
        "target_processed": 2,
        "target_failed": 0,
    }
    assert calls["publishing"] == ["https://publisher-a.example.com", "https://publisher-b.example.com"]
    assert calls["target"] == ["https://target-a.example.com", "https://target-b.example.com"]
    assert session.commits == 4


def test_run_site_profile_sync_skips_duplicate_target_urls_per_client(monkeypatch):
    session = _FakeSession()
    sessionmaker = _FakeSessionmaker(session)
    calls = {"target": []}

    monkeypatch.setattr("portal_backend.api.site_profile_sync._select_publishing_sites", lambda db: [])
    monkeypatch.setattr(
        "portal_backend.api.site_profile_sync._select_target_site_rows",
        lambda db: [(type("Target", (), {"id": "t1"})(), "https://target.example.com")],
    )

    monkeypatch.setattr(
        "portal_backend.api.site_profile_sync.ensure_publishing_site_profile",
        lambda db, **kwargs: None,
    )
    monkeypatch.setattr(
        "portal_backend.api.site_profile_sync.ensure_target_site_profile",
        lambda db, **kwargs: calls["target"].append(kwargs["target_site_url"]),
    )

    summary = run_site_profile_sync(sessionmaker, target_only=True)

    assert summary["target_processed"] == 1
    assert calls["target"] == ["https://target.example.com"]
