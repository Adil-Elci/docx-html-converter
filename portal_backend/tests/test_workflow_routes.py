from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from portal_backend.api.routers import workflow_routes


def test_workflow_column_key_for_status_maps_job_states() -> None:
    assert workflow_routes._workflow_column_key_for_status("queued") == "backlog"
    assert workflow_routes._workflow_column_key_for_status("processing") == "in_progress"
    assert workflow_routes._workflow_column_key_for_status("pending_approval") == "pending_review"
    assert workflow_routes._workflow_column_key_for_status("failed") == "blocked"
    assert workflow_routes._workflow_column_key_for_status("succeeded") == "done"


def test_is_system_workflow_column_key_detects_core_columns() -> None:
    assert workflow_routes._is_system_workflow_column_key("backlog") is True
    assert workflow_routes._is_system_workflow_column_key("done") is True
    assert workflow_routes._is_system_workflow_column_key("custom_review") is False


def test_build_custom_workflow_column_key_deduplicates_names() -> None:
    key = workflow_routes._build_custom_workflow_column_key(
        "In Progress",
        ["backlog", "custom_in_progress"],
    )
    assert key == "custom_in_progress_2"


def test_apply_card_job_sync_preserves_manual_open_column() -> None:
    backlog_id = uuid4()
    manual_column_id = uuid4()
    card = SimpleNamespace(
        column_id=manual_column_id,
        column_source="manual",
        job_status_snapshot="queued",
        title_snapshot="Old title",
        request_kind_snapshot="submit_article",
        updated_at=None,
        position=100,
    )

    event = workflow_routes._apply_card_job_sync(
        card,
        desired_column_id=backlog_id,
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
    assert card.job_status_snapshot == "processing"
    assert card.title_snapshot == "Fresh title"
    assert card.request_kind_snapshot == "create_article"


def test_apply_card_job_sync_forces_terminal_column() -> None:
    blocked_id = uuid4()
    current_column_id = uuid4()
    card = SimpleNamespace(
        column_id=current_column_id,
        column_source="manual",
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
