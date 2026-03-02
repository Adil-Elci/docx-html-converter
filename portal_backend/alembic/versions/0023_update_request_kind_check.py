"""update request_kind check constraint

Revision ID: 0023_request_kind_check
Revises: 0022_job_canceled
Create Date: 2026-03-02 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0023_request_kind_check"
down_revision = "0022_job_canceled"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("submissions_source_payload_check", "submissions", type_="check")
    op.drop_constraint("submissions_request_kind_check", "submissions", type_="check")

    op.execute(
        "UPDATE submissions SET request_kind = 'submit_article' "
        "WHERE request_kind IN ('guest_post','guest-post','guestpost')"
    )
    op.execute(
        "UPDATE submissions SET request_kind = 'create_article' "
        "WHERE request_kind IN ('order','orders')"
    )
    op.execute(
        "UPDATE submissions SET request_kind = 'create_article' "
        "WHERE request_kind = 'submit_article' AND doc_url IS NULL AND file_url IS NULL"
    )

    op.alter_column("submissions", "request_kind", server_default=sa.text("'submit_article'"))
    op.create_check_constraint(
        "submissions_request_kind_check",
        "submissions",
        "request_kind IN ('submit_article','create_article')",
    )
    op.create_check_constraint(
        "submissions_source_payload_check",
        "submissions",
        "((request_kind = 'create_article' AND doc_url IS NULL AND file_url IS NULL) "
        "OR (source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
        "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL))",
    )


def downgrade() -> None:
    op.drop_constraint("submissions_source_payload_check", "submissions", type_="check")
    op.drop_constraint("submissions_request_kind_check", "submissions", type_="check")
    op.execute(
        "UPDATE submissions SET request_kind = 'guest_post' "
        "WHERE request_kind = 'submit_article'"
    )
    op.execute(
        "UPDATE submissions SET request_kind = 'order' "
        "WHERE request_kind = 'create_article'"
    )
    op.alter_column("submissions", "request_kind", server_default=sa.text("'guest_post'"))
    op.create_check_constraint(
        "submissions_request_kind_check",
        "submissions",
        "request_kind IN ('guest_post','order')",
    )
    op.create_check_constraint(
        "submissions_source_payload_check",
        "submissions",
        "(source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
        "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL)",
    )
