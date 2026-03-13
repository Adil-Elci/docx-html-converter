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
        "inventory_context": {
            "article_titles": [],
            "prominent_titles": [],
            "site_categories": [],
            "topic_clusters": [],
        },
    }


def _pair_fit_result(
    decision: str,
    *,
    fit_score: int = 40,
    backlink_fit_ok: bool | None = None,
    final_article_topic: str = "",
) -> dict[str, object]:
    return {
        "pair_fit": {
            "final_match_decision": decision,
            "backlink_fit_ok": decision == "accepted" if backlink_fit_ok is None else backlink_fit_ok,
            "fit_score": fit_score,
            "final_article_topic": final_article_topic,
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


def test_select_best_accepted_pair_prefers_higher_pair_fit_over_higher_site_score(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)

    def _fake_pair_fit(**kwargs):
        publishing_site_url = kwargs.get("publishing_site_url") or ""
        if "specialist" in publishing_site_url:
            return _pair_fit_result("accepted", fit_score=78, backlink_fit_ok=True)
        return _pair_fit_result("accepted", fit_score=42, backlink_fit_ok=True)

    monkeypatch.setattr(automation_routes, "call_creator_pair_fit", _fake_pair_fit)

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Immobilie verkaufen"]},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[
            _candidate("https://broad.example.com", score=86),
            _candidate("https://specialist.example.com", score=52),
        ],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert len(evaluated) == 2
    assert selected["site_url"] == "https://specialist.example.com"
    assert selected["pair_fit_score"] == 78


def test_select_best_accepted_pair_allows_broad_site_with_strong_target_cluster(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)

    def _fake_pair_fit(**kwargs):
        publishing_site_url = kwargs.get("publishing_site_url") or ""
        if "broad" in publishing_site_url:
            return _pair_fit_result("accepted", fit_score=88, backlink_fit_ok=True)
        return _pair_fit_result("accepted", fit_score=74, backlink_fit_ok=True)

    monkeypatch.setattr(automation_routes, "call_creator_pair_fit", _fake_pair_fit)

    broad = _candidate("https://broad.example.com", score=91)
    broad["profile"] = {
        "normalized_url": "https://broad.example.com",
        "primary_context": "lifestyle",
        "contexts": ["lifestyle", "home", "real_estate"],
        "snapshot_contexts": ["real_estate"],
        "inventory_contexts": ["real_estate"],
    }
    broad["details"] = {"publishing_primary_context": "lifestyle", "semantic_score": 46, "internal_link_support": 15}

    specialist = _candidate("https://specialist.example.com", score=62)
    specialist["profile"] = {
        "normalized_url": "https://specialist.example.com",
        "primary_context": "real_estate",
        "contexts": ["real_estate", "finance"],
        "snapshot_contexts": ["real_estate"],
    }
    specialist["details"] = {"publishing_primary_context": "real_estate", "semantic_score": 58, "internal_link_support": 10}

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Immobilie verkaufen"], "primary_context": "real_estate"},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[broad, specialist],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert len(evaluated) == 2
    assert selected["site_url"] == "https://broad.example.com"
    assert selected["specialized_context_match"] is True


def test_select_best_accepted_pair_prefers_specialist_when_broad_has_no_target_context(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)
    calls: list[str] = []

    def _fake_pair_fit(**kwargs):
        publishing_site_url = kwargs.get("publishing_site_url") or ""
        calls.append(str(publishing_site_url))
        if "specialist" in publishing_site_url:
            return _pair_fit_result("accepted", fit_score=61, backlink_fit_ok=True)
        return _pair_fit_result("accepted", fit_score=99, backlink_fit_ok=True)

    monkeypatch.setattr(automation_routes, "call_creator_pair_fit", _fake_pair_fit)

    broad = _candidate("https://broad.example.com", score=90)
    broad["profile"] = {"normalized_url": "https://broad.example.com", "primary_context": "lifestyle", "contexts": ["lifestyle", "home"]}
    broad["details"] = {"publishing_primary_context": "lifestyle", "semantic_score": 45, "internal_link_support": 15}

    specialist = _candidate("https://specialist.example.com", score=55)
    specialist["profile"] = {
        "normalized_url": "https://specialist.example.com",
        "primary_context": "real_estate",
        "contexts": ["real_estate", "finance"],
        "snapshot_contexts": ["real_estate"],
    }
    specialist["details"] = {"publishing_primary_context": "real_estate", "semantic_score": 58, "internal_link_support": 9}

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={"topics": ["Immobilie verkaufen"], "primary_context": "real_estate"},
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[specialist, broad],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert selected["site_url"] == "https://specialist.example.com"
    assert calls == ["https://specialist.example.com", "https://broad.example.com"]
    assert len(evaluated) == 2


def test_select_best_accepted_pair_prefers_topic_supporting_inventory(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)

    def _fake_pair_fit(**kwargs):
        publishing_site_url = kwargs.get("publishing_site_url") or ""
        if "support" in publishing_site_url:
            return _pair_fit_result(
                "accepted",
                fit_score=74,
                backlink_fit_ok=True,
                final_article_topic="Nahrungsergänzungsmittel Kosten Vergleich",
            )
        return _pair_fit_result(
            "accepted",
            fit_score=82,
            backlink_fit_ok=True,
            final_article_topic="Nahrungsergänzungsmittel Kosten Vergleich",
        )

    monkeypatch.setattr(automation_routes, "call_creator_pair_fit", _fake_pair_fit)

    zero_support = _candidate("https://zero-support.example.com", score=82)
    zero_support["profile"] = {
        "normalized_url": "https://zero-support.example.com",
        "primary_context": "health",
        "contexts": ["health", "lifestyle"],
        "snapshot_contexts": ["health"],
    }
    zero_support["details"] = {"publishing_primary_context": "health", "semantic_score": 54, "internal_link_support": 6}

    support = _candidate("https://support.example.com", score=76)
    support["profile"] = {
        "normalized_url": "https://support.example.com",
        "primary_context": "health",
        "contexts": ["health", "lifestyle"],
        "snapshot_contexts": ["health"],
    }
    support["details"] = {"publishing_primary_context": "health", "semantic_score": 53, "internal_link_support": 10}
    support["inventory_context"] = {
        "article_titles": [
            "Nahrungsergänzungsmittel Kosten im Vergleich",
            "Omega-3 kaufen: Preis pro Tagesdosis verstehen",
            "Vitamin D Preisvergleich für Einsteiger",
        ],
        "prominent_titles": [
            "Nahrungsergänzungsmittel Kosten im Vergleich",
            "Omega-3 kaufen: Preis pro Tagesdosis verstehen",
        ],
        "site_categories": ["Gesundheit", "Ernährung"],
        "topic_clusters": ["nahrungsergänzungsmittel", "kosten", "omega", "vitamin"],
    }

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={
            "topics": ["Nahrungsergänzungsmittel", "Preisvergleich"],
            "services_or_products": ["Omega 3", "Vitamine"],
            "primary_context": "health",
        },
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[zero_support, support],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert selected["site_url"] == "https://support.example.com"
    assert selected["topic_internal_support_count"] >= 1
    assert len(evaluated) == 2


def test_select_best_accepted_pair_prefers_deeper_relevant_inventory_when_topic_support_ties(monkeypatch) -> None:
    monkeypatch.delenv("ALLOW_REJECTED_PAIRS_FOR_TESTING", raising=False)
    monkeypatch.setattr(
        automation_routes,
        "call_creator_pair_fit",
        lambda **_kwargs: _pair_fit_result(
            "accepted",
            fit_score=79,
            backlink_fit_ok=True,
            final_article_topic="Greens und Kollagen Kosten im Vergleich",
        ),
    )

    shallow = _candidate("https://shallow.example.com", score=88)
    shallow["profile"] = {
        "normalized_url": "https://shallow.example.com",
        "primary_context": "finance",
        "contexts": ["finance", "health"],
        "snapshot_contexts": ["finance"],
    }
    shallow["details"] = {
        "publishing_primary_context": "finance",
        "semantic_score": 52,
        "internal_link_support": 6,
        "relevant_inventory_count": 0,
    }

    deep = _candidate("https://deep.example.com", score=80)
    deep["profile"] = {
        "normalized_url": "https://deep.example.com",
        "primary_context": "nutrition",
        "contexts": ["nutrition", "health", "shopping"],
        "snapshot_contexts": ["nutrition"],
    }
    deep["details"] = {
        "publishing_primary_context": "nutrition",
        "semantic_score": 50,
        "internal_link_support": 9,
        "relevant_inventory_count": 3,
    }
    deep["inventory_context"] = {
        "article_titles": [
            "Greens Kosten im Vergleich",
            "Kollagenpräparate: Preis pro Portion",
            "Nahrungsergänzungsmittel Preise richtig einordnen",
        ],
        "prominent_titles": [
            "Greens Kosten im Vergleich",
            "Kollagenpräparate: Preis pro Portion",
        ],
        "site_categories": ["Gesundheit", "Supplements"],
        "topic_clusters": ["greens", "kollagen", "preise", "supplements"],
    }

    selected, evaluated = automation_routes._select_best_accepted_pair(
        creator_endpoint="https://creator.example.com",
        target_site_url="https://target.example.com",
        target_profile_payload={
            "topics": ["Greens Kosten", "Kollagenpräparate Preisvergleich"],
            "services_or_products": ["Greens", "Kollagenpräparate"],
            "primary_context": "nutrition",
        },
        target_profile_content_hash="target-hash",
        client_target_site_id=None,
        candidate_rankings=[shallow, deep],
        requested_topic=None,
        exclude_topics=[],
        timeout_seconds=5,
    )

    assert selected is not None
    assert selected["site_url"] == "https://deep.example.com"
    assert len(evaluated) == 2


def test_compose_submission_notes_includes_origin_metadata() -> None:
    notes = automation_routes._compose_submission_notes(
        "idem-123",
        "draft",
        0,
        custom_target_site_url="https://www.orangefit.de/",
        manual_create_article=True,
        creator_mode=True,
        auto_selected_site=True,
        submission_origin_metadata={
            "submission_origin": "portal_frontend:admin:create_article",
            "submission_actor_user_id": "user-123",
            "submission_actor_role": "admin",
            "submission_actor_email": "admin@example.com",
            "submission_request_ip": "10.0.1.13",
            "submission_user_agent": "Mozilla/5.0 Test Agent",
        },
    )

    note_map = automation_routes._extract_note_map(notes)
    assert note_map["idempotency_key"] == "idem-123"
    assert note_map["submission_origin"] == "portal_frontend:admin:create_article"
    assert note_map["submission_actor_user_id"] == "user-123"
    assert note_map["submission_actor_role"] == "admin"
    assert note_map["submission_actor_email"] == "admin@example.com"
    assert note_map["submission_request_ip"] == "10.0.1.13"
    assert note_map["submission_user_agent"] == "Mozilla/5.0 Test Agent"
