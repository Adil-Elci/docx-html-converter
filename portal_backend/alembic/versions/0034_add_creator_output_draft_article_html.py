"""add creator output draft article html

Revision ID: 0034_add_creator_output_draft_article_html
Revises: 0033_drop_client_publishing_site_access
Create Date: 2026-03-12 16:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0034_add_creator_output_draft_article_html"
down_revision = "0033_drop_client_publishing_site_access"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "creator_outputs",
        sa.Column(
            "draft_article_html",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
    )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            """
            UPDATE creator_outputs
            SET draft_article_html = COALESCE(
                payload #>> '{phase5,article_html}',
                payload ->> 'article_html',
                ''
            )
            """
        )
    )


def downgrade() -> None:
    op.drop_column("creator_outputs", "draft_article_html")
