"""Phase C — late-binding publishing-site selection + allgemein fallback +
pre-flight feasibility tests.
"""

from __future__ import annotations

import pytest

from portal_backend.api.automation_service import (
    pre_flight_publishing_feasibility,
    select_publish_target_after_contract,
)


def _candidate(
    *,
    site_url: str,
    language: str = "de",
    fit_score: int = 50,
    primary_context: str = "",
    is_general: bool = False,
) -> dict:
    return {
        "site_url": site_url,
        "site_id": f"id-{site_url}",
        "fit_score": fit_score,
        "publishing_profile_payload": {
            "language": language,
            "primary_context": primary_context,
        },
        "wp_rest_base": "/wp-json",
        "wp_username": "admin",
        "wp_app_password": "pw",
        "category_ids": [],
        "category_candidates": [],
        "internal_link_inventory": [],
        "is_general": is_general,
    }


_FALLBACK = {
    "site_url": "fallback.de",
    "site_id": "fallback-id",
    "wp_rest_base": "/wp-json",
    "wp_username": "admin",
    "wp_app_password": "pw",
    "category_ids": [],
    "category_candidates": [],
    "internal_link_inventory": [],
}


# ---- pre_flight_publishing_feasibility ------------------------------------


class TestPreFlight:
    def test_no_candidates_is_infeasible(self):
        ok, reason = pre_flight_publishing_feasibility(target_language="de", publishing_candidates=[])
        assert ok is False
        assert "no_publishing_candidates_available" in reason

    def test_matching_language_is_feasible(self):
        ok, reason = pre_flight_publishing_feasibility(
            target_language="de",
            publishing_candidates=[_candidate(site_url="a.de", language="de")],
        )
        assert ok is True
        assert "matching_language_candidate" in reason

    def test_unknown_language_candidate_is_feasible(self):
        # Defensive: don't block real work over a missing language field.
        ok, reason = pre_flight_publishing_feasibility(
            target_language="de",
            publishing_candidates=[_candidate(site_url="a.de", language="")],
        )
        assert ok is True
        assert "unknown_language" in reason

    def test_only_french_candidates_for_german_target_is_infeasible(self):
        ok, reason = pre_flight_publishing_feasibility(
            target_language="de",
            publishing_candidates=[_candidate(site_url="a.fr", language="fr")],
        )
        assert ok is False
        assert "no_candidates_match_language:de" in reason

    def test_no_target_language_passes(self):
        ok, _ = pre_flight_publishing_feasibility(
            target_language="",
            publishing_candidates=[_candidate(site_url="a.fr", language="fr")],
        )
        assert ok is True


# ---- select_publish_target_after_contract --------------------------------


class TestSelectPublishTargetAfterContract:
    def test_topical_match_above_floor_is_chosen(self):
        contract = {
            "target_keyword": "steuerberater hamburg",
            "required_entities": [{"name": "DATEV"}, {"name": "Hamburg"}],
            "secondary_keywords": ["steuerberatung"],
        }
        candidates = [
            _candidate(site_url="tax.de", language="de", fit_score=90, primary_context="steuerberatung hamburg datev"),
            _candidate(site_url="cooking.de", language="de", fit_score=10, primary_context="rezepte"),
        ]
        chosen, reason = select_publish_target_after_contract(
            contract=contract,
            target_language="de",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert chosen["site_url"] == "tax.de"
        assert reason["mode"] == "topical_fit"

    def test_french_article_rejects_german_candidates(self):
        contract = {"target_keyword": "expert-comptable paris"}
        candidates = [
            _candidate(site_url="german1.de", language="de", fit_score=80),
            _candidate(site_url="german2.de", language="de", fit_score=90),
        ]
        chosen, reason = select_publish_target_after_contract(
            contract=contract,
            target_language="fr",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert reason["mode"] == "no_candidates"
        assert chosen["site_url"] == _FALLBACK["site_url"]
        assert "german1.de" in reason["rejected_for_language"]
        assert "german2.de" in reason["rejected_for_language"]

    def test_falls_back_to_allgemein_when_no_topical_match(self):
        contract = {"target_keyword": "ein sehr nischen-thema xyz"}
        candidates = [
            _candidate(site_url="specialist.de", language="de", fit_score=10, primary_context="rezepte"),
            _candidate(
                site_url="allgemein.de",
                language="de",
                fit_score=20,
                primary_context="allgemein magazin",
                is_general=True,
            ),
        ]
        chosen, reason = select_publish_target_after_contract(
            contract=contract,
            target_language="de",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert chosen["site_url"] == "allgemein.de"
        assert reason["mode"] == "allgemein_fallback"

    def test_allgemein_detected_via_primary_context_heuristic(self):
        # No explicit is_general flag, but primary_context contains "allgemein".
        contract = {"target_keyword": "etwas anderes"}
        candidates = [
            _candidate(site_url="specialist.de", language="de", fit_score=5, primary_context="auto"),
            _candidate(
                site_url="magazin.de",
                language="de",
                fit_score=5,
                primary_context="allgemein lifestyle",
                is_general=False,  # heuristic should still catch it
            ),
        ]
        chosen, reason = select_publish_target_after_contract(
            contract=contract,
            target_language="de",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert chosen["site_url"] == "magazin.de"
        assert reason["mode"] == "allgemein_fallback"

    def test_unknown_language_candidate_passes_filter(self):
        contract = {"target_keyword": "kw"}
        candidates = [
            _candidate(site_url="unknown-lang.de", language="", fit_score=80, primary_context="kw"),
        ]
        chosen, _ = select_publish_target_after_contract(
            contract=contract,
            target_language="de",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert chosen["site_url"] == "unknown-lang.de"

    def test_returns_best_below_floor_when_no_general_available(self):
        contract = {"target_keyword": "obscure niche"}
        candidates = [
            _candidate(site_url="best-but-bad.de", language="de", fit_score=5, primary_context="cars"),
        ]
        chosen, reason = select_publish_target_after_contract(
            contract=contract,
            target_language="de",
            fallback_target=_FALLBACK,
            publishing_candidates=candidates,
        )
        assert chosen["site_url"] == "best-but-bad.de"
        assert reason["mode"] == "best_below_floor"
