from __future__ import annotations

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
