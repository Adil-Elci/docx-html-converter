from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.routers import jobs_routes


def test_build_creator_debug_payload_uses_recorded_prompt_trace() -> None:
    job = SimpleNamespace(id=uuid4(), job_status="succeeded")
    creator_output = {
        "phase3": {"final_article_topic": "Kinder Sonnenbrillen"},
        "phase4": {"h1": "Sonnenbrillen fuer Kinder"},
        "debug": {
            "prompt_trace": {
                "planner": {
                    "mode": "deterministic",
                    "attempts": [{"attempt": 1, "input_packet": {"topic": "Kinder Sonnenbrillen"}}],
                },
                "writer_attempts": [
                    {
                        "attempt": 1,
                        "request_label": "phase5_writer_attempt_1",
                        "system_prompt": "system",
                        "user_prompt": "user",
                    }
                ],
            },
            "quality_scores": {"coherence_score": 88},
            "planning_quality": {"score": 84},
            "internal_linking": {"candidate_count": 2},
        },
    }

    payload = jobs_routes._build_creator_debug_payload(job, creator_output)

    assert payload["job_id"] == job.id
    assert payload["planner"]["attempts"][0]["input_packet"]["topic"] == "Kinder Sonnenbrillen"
    assert payload["writer_attempts"][0]["request_label"] == "phase5_writer_attempt_1"
    assert payload["writer_prompt_recorded"] is True


def test_build_creator_debug_payload_falls_back_to_deterministic_planner_packet() -> None:
    job = SimpleNamespace(id=uuid4(), job_status="succeeded")
    creator_output = {
        "phase3": {
            "final_article_topic": "Immobilie verkaufen",
            "primary_keyword": "immobilie verkaufen",
            "secondary_keywords": ["verkaufspreis haus"],
            "search_intent_type": "informational",
            "article_angle": "process_guidance",
            "topic_class": "real_estate",
            "style_profile": {"tone": "factual"},
            "specificity_profile": {"min_specifics": 3},
            "title_package": {"title": "Immobilie verkaufen"},
            "content_brief": {"must_cover": ["unterlagen"]},
            "faq_candidates": ["Welche Unterlagen braucht man?"],
        },
        "phase4": {"h1": "Immobilie verkaufen", "outline": [{"h2": "Welche Unterlagen braucht man?"}]},
        "debug": {
            "planning_quality": {"score": 81},
            "keyword_selection": {"intent_type": "informational"},
            "internal_linking": {"candidates": ["https://site.example.com/verkauf"]},
        },
    }

    payload = jobs_routes._build_creator_debug_payload(job, creator_output)

    assert payload["planner"]["mode"] == "deterministic"
    assert payload["planner"]["attempts"][0]["input_packet"]["topic_class"] == "real_estate"
    assert payload["planner"]["attempts"][0]["input_packet"]["internal_link_candidates"] == ["https://site.example.com/verkauf"]
    assert payload["writer_attempts"] == []
    assert payload["writer_prompt_recorded"] is False


def test_get_latest_creator_output_payload_merges_normalized_prompt_trace_columns() -> None:
    job_id = uuid4()

    class FakeQuery:
        def query(self, *_args, **_kwargs):
            return self

        def filter(self, *_args, **_kwargs):
            return self

        def order_by(self, *_args, **_kwargs):
            return self

        def first(self):
            return (
                {"debug": {}},
                {"mode": "deterministic", "attempts": [{"attempt": 1}]},
                [{"attempt": 1, "request_label": "phase5_writer_attempt_1"}],
            )

    payload = jobs_routes._get_latest_creator_output_payload(FakeQuery(), job_id)

    assert payload["debug"]["prompt_trace"]["planner"]["mode"] == "deterministic"
    assert payload["debug"]["prompt_trace"]["writer_attempts"][0]["request_label"] == "phase5_writer_attempt_1"


def test_pending_job_to_out_includes_target_site_url() -> None:
    job_id = uuid4()
    submission_id = uuid4()
    client_id = uuid4()
    site_id = uuid4()
    out = jobs_routes._pending_job_to_out(
        SimpleNamespace(
            id=job_id,
            job_status="pending_approval",
            wp_post_id=123,
            wp_post_url="https://publisher.example.com/draft",
            created_at="2026-03-12T00:00:00Z",
            updated_at="2026-03-12T00:00:00Z",
        ),
        SimpleNamespace(id=submission_id, request_kind="create_article"),
        SimpleNamespace(id=client_id, name="Client"),
        SimpleNamespace(id=site_id, name="Publisher", site_url="https://publisher.example.com"),
        content_title="Example title",
        target_site_url="https://target.example.com",
    )

    assert out.target_site_url == "https://target.example.com"


def test_extract_rejection_event_metadata_reads_admin_reject_payload() -> None:
    fallback_created_at = datetime(2026, 3, 13, 9, 0, tzinfo=timezone.utc)

    metadata = jobs_routes._extract_rejection_event_metadata(
        {
            "action": "admin_reject",
            "reason_summary": "Content quality below publishing standard",
            "rejected_by_email": "admin@example.com",
            "rejected_at": "2026-03-13T10:15:00+00:00",
        },
        fallback_created_at=fallback_created_at,
    )

    assert metadata == {
        "rejected_at": datetime(2026, 3, 13, 10, 15, tzinfo=timezone.utc),
        "rejected_by": "admin@example.com",
        "rejection_reason": "Content quality below publishing standard",
    }


def test_rejected_article_to_out_includes_rejection_fields() -> None:
    rejected_at = datetime(2026, 3, 13, 11, 30, tzinfo=timezone.utc)
    out = jobs_routes._rejected_article_to_out(
        SimpleNamespace(
            id=uuid4(),
            job_status="rejected",
            wp_post_url="https://publisher.example.com/draft",
            created_at=rejected_at,
            updated_at=rejected_at,
        ),
        SimpleNamespace(id=uuid4(), request_kind="submit_article"),
        SimpleNamespace(id=uuid4(), name="Client"),
        SimpleNamespace(id=uuid4(), name="Publisher", site_url="https://publisher.example.com"),
        content_title="Rejected example",
        target_site_url="https://target.example.com",
        rejection_reason="Formatting or structure issue",
        rejected_by="admin@example.com",
        rejected_at=rejected_at,
    )

    assert out.content_title == "Rejected example"
    assert out.target_site_url == "https://target.example.com"
    assert out.rejection_reason == "Formatting or structure issue"
    assert out.rejected_by == "admin@example.com"
    assert out.rejected_at == rejected_at
