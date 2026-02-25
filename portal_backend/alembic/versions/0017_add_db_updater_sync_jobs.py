"""add db updater sync jobs table

Revision ID: 0017_db_updater_sync_jobs
Revises: 0016_master_site_info
Create Date: 2026-02-25 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0017_db_updater_sync_jobs"
down_revision = "0016_master_site_info"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "db_updater_sync_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("job_type", sa.Text(), nullable=False, server_default=sa.text("'master_site_sync'")),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("progress_percent", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("stage", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("report", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("status IN ('queued','running','completed','failed')", name="db_updater_sync_jobs_status_check"),
    )
    op.create_index("idx_db_updater_sync_jobs_job_type", "db_updater_sync_jobs", ["job_type"])
    op.create_index("idx_db_updater_sync_jobs_status", "db_updater_sync_jobs", ["status"])
    op.create_index("idx_db_updater_sync_jobs_created_at", "db_updater_sync_jobs", ["created_at"])
    op.execute(
        """
        CREATE TRIGGER trg_db_updater_sync_jobs_set_updated_at
        BEFORE UPDATE ON db_updater_sync_jobs
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_db_updater_sync_jobs_set_updated_at ON db_updater_sync_jobs;")
    op.drop_index("idx_db_updater_sync_jobs_created_at", table_name="db_updater_sync_jobs")
    op.drop_index("idx_db_updater_sync_jobs_status", table_name="db_updater_sync_jobs")
    op.drop_index("idx_db_updater_sync_jobs_job_type", table_name="db_updater_sync_jobs")
    op.drop_table("db_updater_sync_jobs")

