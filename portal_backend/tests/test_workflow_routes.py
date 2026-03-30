from __future__ import annotations

from types import SimpleNamespace

from portal_backend.api.routers import workflow_routes


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


def test_build_workflow_card_title_uses_snapshot_and_fallback() -> None:
    assert workflow_routes._build_workflow_card_title("Manual ops task") == "Manual ops task"
    assert workflow_routes._build_workflow_card_title("   ") == "Workflow task"


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
