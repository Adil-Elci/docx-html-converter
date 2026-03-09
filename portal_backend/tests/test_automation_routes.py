from portal_backend.api.routers import automation_routes


def _candidate(site_url: str, *, score: int = 40) -> dict[str, object]:
    return {
        "site_id": f"{site_url}-id",
        "site_url": site_url,
        "score": score,
        "profile": {
            "normalized_url": site_url,
            "topics": ["Familie", "Gesundheit"],
            "contexts": ["family_life", "health"],
        },
        "content_hash": f"{site_url}-hash",
    }


def _pair_fit_result(decision: str, *, fit_score: int = 40, backlink_fit_ok: bool | None = None) -> dict[str, object]:
    return {
        "pair_fit": {
            "final_match_decision": decision,
            "backlink_fit_ok": decision == "accepted" if backlink_fit_ok is None else backlink_fit_ok,
            "fit_score": fit_score,
        },
        "cached": False,
    }


def test_select_best_accepted_pair_rejects_weak_fit_without_override(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)
    monkeypatch.setattr(
        automation_routes,
        "call_creator_pair_fit",
        lambda **_kwargs: _pair_fit_result("weak_fit", backlink_fit_ok=False),
    )

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Kinder Sonnenbrillen"]},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[_candidate("https://publisher.example.com")],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is None
    assert len(evaluated) == 1
    assert evaluated[0]["accepted"] is False
    assert evaluated[0]["override_selected"] is False
    assert evaluated[0]["final_match_decision"] == "weak_fit"


def test_select_best_accepted_pair_allows_override_in_testing(monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", "true")
    monkeypatch.setattr(
        automation_routes,
        "call_creator_pair_fit",
        lambda **_kwargs: _pair_fit_result("weak_fit", fit_score=55, backlink_fit_ok=False),
    )

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Kinder Sonnenbrillen"]},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[_candidate("https://publisher.example.com")],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert len(evaluated) == 1
    assert selected["accepted"] is True
    assert selected["override_selected"] is True
    assert selected["final_match_decision"] == "weak_fit"


def test_select_best_accepted_pair_prefers_real_acceptance_over_override(monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", "true")

    def _fake_pair_fit(**kwargs):
        publishing_site_url = kwargs.get("publishing_site_url") or ""
        if "accepted" in publishing_site_url:
            return _pair_fit_result("accepted", fit_score=30, backlink_fit_ok=True)
        return _pair_fit_result("weak_fit", fit_score=60, backlink_fit_ok=False)

    monkeypatch.setattr(automation_routes, "call_creator_pair_fit", _fake_pair_fit)

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Kinder Sonnenbrillen"]},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[
            _candidate("https://weak.example.com", score=45),
            _candidate("https://accepted.example.com", score=30),
        ],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert len(evaluated) == 2
    assert selected["site_url"] == "https://accepted.example.com"
    assert selected["override_selected"] is False
    assert selected["final_match_decision"] == "accepted"
