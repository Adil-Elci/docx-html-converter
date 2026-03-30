from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.routers import workflow_routes


def test_workflow_column_key_for_status_maps_job_states() -> None:
    assert workflow_routes._workflow_column_key_for_status("queued") == "todo"
    assert workflow_routes._workflow_column_key_for_status("processing") == "in_progress"
    assert workflow_routes._workflow_column_key_for_status("pending_approval") == "in_progress"
    assert workflow_routes._workflow_column_key_for_status("failed") == "todo"
    assert workflow_routes._workflow_column_key_for_status("succeeded") == "done"


def test_is_system_workflow_column_key_detects_core_columns() -> None:
    assert workflow_routes._is_system_workflow_column_key("todo") is True
    assert workflow_routes._is_system_workflow_column_key("done") is True
    assert workflow_routes._is_system_workflow_column_key("custom_review") is False


def test_build_custom_workflow_column_key_deduplicates_names() -> None:
    key = workflow_routes._build_custom_workflow_column_key(
        "In Progress",
        ["backlog", "custom_in_progress"],
    )
    assert key == "custom_in_progress_2"


def test_apply_card_job_sync_preserves_manual_open_column() -> None:
    todo_id = uuid4()
    manual_column_id = uuid4()
    card = SimpleNamespace(
        column_id=manual_column_id,
        column_source="manual",
        card_kind="job",
        job_status_snapshot="queued",
        title_snapshot="Old title",
        request_kind_snapshot="submit_article",
        updated_at=None,
        position=100,
    )

    event = workflow_routes._apply_card_job_sync(
        card,
        desired_column_id=todo_id,
        job_status="processing",
        title_snapshot="Fresh title",
        request_kind="create_article",
        next_position=lambda _column_id: 999,
    )

    assert event == {
        "moved": False,
        "dirty": True,
    }
    assert card.column_id == manual_column_id
    assert card.column_source == "manual"
    assert card.card_kind == "job"
    assert card.job_status_snapshot == "processing"
    assert card.title_snapshot == "Fresh title"
    assert card.request_kind_snapshot == "create_article"


def test_apply_card_job_sync_forces_terminal_column() -> None:
    blocked_id = uuid4()
    current_column_id = uuid4()
    card = SimpleNamespace(
        column_id=current_column_id,
        column_source="manual",
        card_kind="job",
        job_status_snapshot="processing",
        title_snapshot="Old title",
        request_kind_snapshot="submit_article",
        updated_at=None,
        position=100,
    )

    event = workflow_routes._apply_card_job_sync(
        card,
        desired_column_id=blocked_id,
        job_status="failed",
        title_snapshot="Fresh title",
        request_kind="submit_article",
        next_position=lambda _column_id: 700,
    )

    assert event == {
        "moved": True,
        "dirty": True,
        "from_column_id": current_column_id,
        "to_column_id": blocked_id,
        "previous_status": "processing",
        "job_status": "failed",
    }
    assert card.column_id == blocked_id
    assert card.column_source == "auto"
    assert card.position == 700


def test_build_workflow_card_title_prefers_content_title() -> None:
    submission = SimpleNamespace(title="Submission title", request_kind="submit_article")
    assert workflow_routes._build_workflow_card_title(submission, "Rendered title") == "Rendered title"
    assert workflow_routes._build_workflow_card_title(submission, "") == "Submission title"


def test_build_workflow_card_title_uses_manual_title_when_submission_is_missing() -> None:
    assert workflow_routes._build_workflow_card_title(None, "", "Manual ops task") == "Manual ops task"


def test_build_actor_name_prefers_full_name_then_email() -> None:
    user = SimpleNamespace(full_name="Ada Admin", email="ada@example.com")
    assert workflow_routes._build_actor_name(user) == "Ada Admin"

    fallback = SimpleNamespace(full_name=" ", email="ada@example.com")
    assert workflow_routes._build_actor_name(fallback) == "ada@example.com"


def test_extract_anthropic_text_returns_combined_text_blocks() -> None:
    payload = {
        "content": [
            {"type": "text", "text": "First line"},
            {"type": "tool_use", "name": "ignored"},
            {"type": "text", "text": "Second line"},
        ]
    }
    assert workflow_routes._extract_anthropic_text(payload) == "First line\nSecond line"


def test_parse_submission_notes_map_extracts_submission_actor_fields() -> None:
    submission = SimpleNamespace(notes="submission_actor_email=ops@example.com;submission_actor_user_id=123;foo=bar")
    parsed = workflow_routes._parse_submission_notes_map(submission)
    assert parsed["submission_actor_email"] == "ops@example.com"
    assert parsed["submission_actor_user_id"] == "123"


def test_infer_job_type_defaults_job_cards_to_articles() -> None:
    card = SimpleNamespace(job_type=None, request_kind_snapshot="submit_article", card_kind="job")
    assert workflow_routes._infer_job_type(card, None) == "articles"
