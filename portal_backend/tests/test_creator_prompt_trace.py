from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.creator_prompt_trace import (
    append_execution_trace_event,
    extract_draft_article_html,
    normalize_execution_trace_payload,
    normalize_prompt_trace_payload,
)
from portal_backend.scripts.backfill_creator_prompt_traces import backfill_creator_prompt_trace_columns


def _legacy_creator_output() -> dict[str, object]:
    return {
        "phase3": {
            "final_article_topic": "Kinder Sonnenbrillen",
            "primary_keyword": "kinder sonnenbrillen",
            "secondary_keywords": ["uv schutz kinderaugen"],
            "search_intent_type": "informational",
            "article_angle": "practical_guidance",
            "topic_class": "parenting_health",
            "style_profile": {"tone": "factual"},
            "specificity_profile": {"min_specifics": 2},
            "title_package": {"title": "Sonnenbrillen fuer Kinder"},
            "content_brief": {"must_cover": ["uv schutz", "passform"]},
            "faq_candidates": ["Worauf sollten Eltern achten?"],
        },
        "phase4": {
            "h1": "Sonnenbrillen fuer Kinder",
            "sections": [
                {
                    "section_id": "sec_1",
                    "kind": "body",
                    "h2": "Worauf sollten Eltern beim Kauf achten?",
                    "subquestion": "Welche Kriterien sind wichtig?",
                    "required_keywords": ["kinder sonnenbrillen"],
                    "required_terms": ["uv schutz", "passform"],
                    "required_elements": [],
                }
            ],
            "faq_questions": ["Worauf sollten Eltern achten?"],
        },
        "phase5": {
            "meta_title": "Sonnenbrillen fuer Kinder",
            "excerpt": "Kurzbeschreibung",
            "slug": "sonnenbrillen-fuer-kinder",
            "article_html": "<p>Artikelinhalt</p>",
        },
        "debug": {
            "planning_quality": {"score": 82},
            "internal_linking": {"candidates": ["https://publisher.example.com/uv-tipps"]},
        },
    }


def test_normalize_prompt_trace_payload_backfills_columns_and_payload() -> None:
    normalized_payload, planner_trace, writer_prompt_trace = normalize_prompt_trace_payload(_legacy_creator_output())

    assert planner_trace["mode"] == "deterministic"
    assert planner_trace["attempts"][0]["input_packet"]["topic"] == "Kinder Sonnenbrillen"
    assert writer_prompt_trace[0]["request_label"] == "phase5_writer_attempt_1"
    assert "Do not write advertorial copy" in writer_prompt_trace[0]["user_prompt"]
    assert normalized_payload["debug"]["prompt_trace"]["planner"] == planner_trace
    assert normalized_payload["debug"]["prompt_trace"]["writer_attempts"] == writer_prompt_trace


def test_extract_draft_article_html_reads_phase5_article_html() -> None:
    assert extract_draft_article_html(_legacy_creator_output()) == "<p>Artikelinhalt</p>"


def test_normalize_execution_trace_payload_reads_debug_traces() -> None:
    payload = _legacy_creator_output()
    payload["debug"]["creator_trace"] = [{"phase": "phase4", "event": "complete"}]
    payload["debug"]["backend_trace"] = [{"phase": "worker", "event": "payload_loaded"}]

    normalized_payload, creator_trace, backend_trace = normalize_execution_trace_payload(payload)

    assert creator_trace == [{"phase": "phase4", "event": "complete"}]
    assert backend_trace == [{"phase": "worker", "event": "payload_loaded"}]
    assert normalized_payload["debug"]["creator_trace"] == creator_trace
    assert normalized_payload["debug"]["backend_trace"] == backend_trace


def test_append_execution_trace_event_builds_structured_event() -> None:
    trace: list[dict[str, object]] = []

    append_execution_trace_event(
        trace,
        level="warning",
        phase="internal_links",
        event="inventory_live_fetch_failed",
        message="Live fetch failed.",
        details={"count": 0},
    )

    assert len(trace) == 1
    assert trace[0]["level"] == "warning"
    assert trace[0]["phase"] == "internal_links"
    assert trace[0]["event"] == "inventory_live_fetch_failed"
    assert trace[0]["message"] == "Live fetch failed."
    assert trace[0]["details"] == {"count": 0}
    assert "ts" in trace[0]


class _FakeQuery:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self._rows = rows

    def filter(self, *_args, **_kwargs) -> "_FakeQuery":
        return self

    def order_by(self, *_args, **_kwargs) -> "_FakeQuery":
        return self

    def yield_per(self, _batch_size: int):
        return iter(self._rows)


class _FakeSession:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self.rows = rows
        self.commits = 0

    def query(self, _model) -> _FakeQuery:
        return _FakeQuery(self.rows)

    def commit(self) -> None:
        self.commits += 1


def test_backfill_creator_prompt_trace_columns_updates_empty_rows() -> None:
    row = SimpleNamespace(
        id=uuid4(),
        job_id=uuid4(),
        created_at=datetime.now(timezone.utc),
        payload=_legacy_creator_output(),
        draft_article_html="",
        planner_trace={},
        writer_prompt_trace=[],
        creator_trace=[],
        backend_trace=[],
    )
    session = _FakeSession([row])

    summary = backfill_creator_prompt_trace_columns(session, batch_size=50)

    assert summary["scanned"] == 1
    assert summary["updated"] == 1
    assert summary["payload_synced"] == 1
    assert summary["draft_backfilled"] == 1
    assert summary["creator_trace_backfilled"] == 0
    assert summary["backend_trace_backfilled"] == 0
    assert row.planner_trace["mode"] == "deterministic"
    assert row.writer_prompt_trace[0]["request_label"] == "phase5_writer_attempt_1"
    assert row.draft_article_html == "<p>Artikelinhalt</p>"
    assert row.payload["debug"]["prompt_trace"]["planner"] == row.planner_trace
    assert session.commits == 1
