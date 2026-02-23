"""allow order submissions without document source

Revision ID: 0015_manual_orders_no_doc
Revises: 0014_client_target_sites
Create Date: 2026-02-23 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0015_manual_orders_no_doc"
down_revision = "0014_client_target_sites"
branch_labels = None
depends_on = None


_OLD_CHECK = (
    "(source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
    "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL)"
)

_NEW_CHECK = (
    "((request_kind = 'order' AND doc_url IS NULL AND file_url IS NULL) "
    "OR (source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
    "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL))"
)


def upgrade() -> None:
    op.drop_constraint("submissions_source_payload_check", "submissions", type_="check")
    op.create_check_constraint("submissions_source_payload_check", "submissions", _NEW_CHECK)


def downgrade() -> None:
    op.drop_constraint("submissions_source_payload_check", "submissions", type_="check")
    op.create_check_constraint("submissions_source_payload_check", "submissions", _OLD_CHECK)
