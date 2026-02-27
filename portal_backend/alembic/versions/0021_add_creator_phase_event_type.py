"""add creator_phase to job_events event_type check

Revision ID: 0021_creator_phase_evt
Revises: 0020_creator_outputs
Create Date: 2026-02-27 00:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "0021_creator_phase_evt"
down_revision = "0020_creator_outputs"
branch_labels = None
depends_on = None

OLD_CHECK = (
    "event_type IN ("
    "'converter_called','converter_ok','image_prompt_ok','image_generated',"
    "'wp_post_created','wp_post_updated','failed'"
    ")"
)

NEW_CHECK = (
    "event_type IN ("
    "'converter_called','converter_ok','image_prompt_ok','image_generated',"
    "'wp_post_created','wp_post_updated','failed','creator_phase'"
    ")"
)


def upgrade() -> None:
    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint("job_events_event_type_check", "job_events", NEW_CHECK)


def downgrade() -> None:
    op.execute("DELETE FROM job_events WHERE event_type = 'creator_phase'")
    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint("job_events_event_type_check", "job_events", OLD_CHECK)
