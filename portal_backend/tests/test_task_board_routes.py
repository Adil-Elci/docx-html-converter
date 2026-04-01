from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from portal_backend.api.routers import task_board_routes
from portal_backend.api.task_board_schemas import TaskBoardCardCreateIn, TaskBoardCardUpdateIn


def test_is_system_task_board_column_key_detects_core_columns() -> None:
    assert task_board_routes._is_system_task_board_column_key("todo") is True
    assert task_board_routes._is_system_task_board_column_key("done") is True
    assert task_board_routes._is_system_task_board_column_key("custom_review") is False


def test_build_custom_task_board_column_key_deduplicates_names() -> None:
    key = task_board_routes._build_custom_task_board_column_key(
        "In Progress",
        ["backlog", "custom_in_progress"],
    )
    assert key == "custom_in_progress_2"


def test_build_task_board_card_title_uses_snapshot_and_fallback() -> None:
    assert task_board_routes._build_task_board_card_title("Manual ops task") == "Manual ops task"
    assert task_board_routes._build_task_board_card_title("   ") == "Task Board task"


def test_build_actor_name_prefers_full_name_then_email() -> None:
    user = SimpleNamespace(full_name="Ada Admin", email="ada@example.com")
    assert task_board_routes._build_actor_name(user) == "Ada Admin"

    fallback = SimpleNamespace(full_name=" ", email="ada@example.com")
    assert task_board_routes._build_actor_name(fallback) == "ada@example.com"


def test_extract_anthropic_text_returns_combined_text_blocks() -> None:
    payload = {
        "content": [
            {"type": "text", "text": "First line"},
            {"type": "tool_use", "name": "ignored"},
            {"type": "text", "text": "Second line"},
        ]
    }
    assert task_board_routes._extract_anthropic_text(payload) == "First line\nSecond line"


def test_task_board_card_create_in_accepts_research_and_priority() -> None:
    payload = TaskBoardCardCreateIn(
        title="Research site access failures",
        job_type="research",
        priority="high",
        assignee_user_id="00000000-0000-0000-0000-000000000001",
        description="Investigate site access checks and summarize blockers.",
    )
    assert payload.job_type == "research"
    assert payload.priority == "high"


def test_task_board_card_create_in_rejects_invalid_priority() -> None:
    with pytest.raises(ValidationError):
        TaskBoardCardCreateIn(
            title="Broken priority",
            job_type="develop",
            priority="critical",
            assignee_user_id="00000000-0000-0000-0000-000000000001",
        )


def test_task_board_card_update_in_allows_title_and_description_edit() -> None:
    payload = TaskBoardCardUpdateIn(title="Updated title", description="Updated notes")
    assert payload.title == "Updated title"
    assert payload.description == "Updated notes"


def test_task_board_card_update_in_accepts_multiple_flags() -> None:
    payload = TaskBoardCardUpdateIn(flag_types=["needs_adil_attention", "bug", "needs_levent_attention"])
    assert payload.flag_types == ["bug", "needs_levent_attention", "needs_adil_attention"]


def test_task_board_card_update_in_rejects_invalid_flag() -> None:
    with pytest.raises(ValidationError):
        TaskBoardCardUpdateIn(flag_types=["needs_ops_attention"])


def test_task_board_card_update_in_allows_full_super_admin_edit_fields() -> None:
    payload = TaskBoardCardUpdateIn(
        title="Updated title",
        description="Updated notes",
        job_type="research",
        priority="urgent",
        assignee_user_id="00000000-0000-0000-0000-000000000001",
    )
    assert payload.job_type == "research"
    assert payload.priority == "urgent"
