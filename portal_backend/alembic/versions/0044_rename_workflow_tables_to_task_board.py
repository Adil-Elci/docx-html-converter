"""rename workflow tables to task board

Revision ID: 0044_rename_workflow_tables_to_task_board
Revises: 0043_convert_workflow_flags_to_array
Create Date: 2026-04-01 11:45:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0044_rename_workflow_tables_to_task_board"
down_revision = "0043_convert_workflow_flags_to_array"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("workflow_columns", "task_board_columns")
    op.rename_table("workflow_cards", "task_board_cards")
    op.rename_table("workflow_card_events", "task_board_card_events")
    op.rename_table("workflow_card_comments", "task_board_card_comments")

    op.execute(
        """
        ALTER TABLE task_board_columns
        RENAME CONSTRAINT workflow_columns_pkey TO task_board_columns_pkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_columns
        RENAME CONSTRAINT workflow_columns_key_unique TO task_board_columns_key_unique
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_pkey TO task_board_cards_pkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_column_source_check TO task_board_cards_column_source_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_kind_check TO task_board_cards_kind_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_job_type_check TO task_board_cards_job_type_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_priority_check TO task_board_cards_priority_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_flag_types_check TO task_board_cards_flag_types_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_job_unique TO task_board_cards_job_unique
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_job_id_fkey TO task_board_cards_job_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_submission_id_fkey TO task_board_cards_submission_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_client_id_fkey TO task_board_cards_client_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_publishing_site_id_fkey TO task_board_cards_publishing_site_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_column_id_fkey TO task_board_cards_column_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_created_by_user_id_fkey TO task_board_cards_created_by_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT workflow_cards_assignee_user_id_fkey TO task_board_cards_assignee_user_id_fkey
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_pkey TO task_board_card_events_pkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_type_check TO task_board_card_events_type_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_card_id_fkey TO task_board_card_events_card_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_job_id_fkey TO task_board_card_events_job_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_actor_user_id_fkey TO task_board_card_events_actor_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_from_column_id_fkey TO task_board_card_events_from_column_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT workflow_card_events_to_column_id_fkey TO task_board_card_events_to_column_id_fkey
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT workflow_card_comments_pkey TO task_board_card_comments_pkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT workflow_card_comments_card_id_fkey TO task_board_card_comments_card_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT workflow_card_comments_author_user_id_fkey TO task_board_card_comments_author_user_id_fkey
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT task_board_card_comments_author_user_id_fkey TO workflow_card_comments_author_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT task_board_card_comments_card_id_fkey TO workflow_card_comments_card_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_comments
        RENAME CONSTRAINT task_board_card_comments_pkey TO workflow_card_comments_pkey
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_to_column_id_fkey TO workflow_card_events_to_column_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_from_column_id_fkey TO workflow_card_events_from_column_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_actor_user_id_fkey TO workflow_card_events_actor_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_job_id_fkey TO workflow_card_events_job_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_card_id_fkey TO workflow_card_events_card_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_type_check TO workflow_card_events_type_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_card_events
        RENAME CONSTRAINT task_board_card_events_pkey TO workflow_card_events_pkey
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_assignee_user_id_fkey TO workflow_cards_assignee_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_created_by_user_id_fkey TO workflow_cards_created_by_user_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_column_id_fkey TO workflow_cards_column_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_publishing_site_id_fkey TO workflow_cards_publishing_site_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_client_id_fkey TO workflow_cards_client_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_submission_id_fkey TO workflow_cards_submission_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_job_id_fkey TO workflow_cards_job_id_fkey
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_job_unique TO workflow_cards_job_unique
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_flag_types_check TO workflow_cards_flag_types_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_priority_check TO workflow_cards_priority_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_job_type_check TO workflow_cards_job_type_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_kind_check TO workflow_cards_kind_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_column_source_check TO workflow_cards_column_source_check
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_cards
        RENAME CONSTRAINT task_board_cards_pkey TO workflow_cards_pkey
        """
    )

    op.execute(
        """
        ALTER TABLE task_board_columns
        RENAME CONSTRAINT task_board_columns_key_unique TO workflow_columns_key_unique
        """
    )
    op.execute(
        """
        ALTER TABLE task_board_columns
        RENAME CONSTRAINT task_board_columns_pkey TO workflow_columns_pkey
        """
    )

    op.rename_table("task_board_card_comments", "workflow_card_comments")
    op.rename_table("task_board_card_events", "workflow_card_events")
    op.rename_table("task_board_cards", "workflow_cards")
    op.rename_table("task_board_columns", "workflow_columns")
