"""add canceled job status and event type

Revision ID: 0022_job_canceled
Revises: 0021_creator_phase_evt
Create Date: 2026-02-27 00:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "0022_job_canceled"
down_revision = "0021_creator_phase_evt"
branch_labels = None
depends_on = None

OLD_JOB_CHECK = (
    "job_status IN ('queued','processing','pending_approval','rejected','succeeded','failed','retrying')"
)

NEW_JOB_CHECK = (
    "job_status IN ('queued','processing','pending_approval','rejected','succeeded','failed','retrying','canceled')"
)

OLD_EVENT_CHECK = (
    "event_type IN ("
    "'converter_called','converter_ok','image_prompt_ok','image_generated',"
    "'wp_post_created','wp_post_updated','failed','creator_phase'"
    ")"
)

NEW_EVENT_CHECK = (
    "event_type IN ("
    "'converter_called','converter_ok','image_prompt_ok','image_generated',"
    "'wp_post_created','wp_post_updated','failed','creator_phase','canceled'"
    ")"
)


def upgrade() -> None:
    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint("jobs_job_status_check", "jobs", NEW_JOB_CHECK)

    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint("job_events_event_type_check", "job_events", NEW_EVENT_CHECK)


def downgrade() -> None:
    op.execute("UPDATE jobs SET job_status = 'failed' WHERE job_status = 'canceled'")
    op.execute("DELETE FROM job_events WHERE event_type = 'canceled'")
    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint("job_events_event_type_check", "job_events", OLD_EVENT_CHECK)

    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint("jobs_job_status_check", "jobs", OLD_JOB_CHECK)
