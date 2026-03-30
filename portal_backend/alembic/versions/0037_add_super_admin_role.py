"""add super admin role

Revision ID: 0037_add_super_admin_role
Revises: 0036_add_workflow_board_tables
Create Date: 2026-03-30 16:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0037_add_super_admin_role"
down_revision = "0036_add_workflow_board_tables"
branch_labels = None
depends_on = None


SUPER_ADMIN_EMAIL = "aat@elci.cloud"


def upgrade() -> None:
    op.drop_constraint("users_role_check", "users", type_="check")
    op.create_check_constraint(
        "users_role_check",
        "users",
        "role IN ('super_admin','admin','client')",
    )
    op.execute(
        sa.text(
            """
            UPDATE users
            SET role = CASE
                WHEN lower(email) = :super_admin_email THEN 'super_admin'
                WHEN role = 'super_admin' THEN 'admin'
                ELSE role
            END
            """
        ).bindparams(super_admin_email=SUPER_ADMIN_EMAIL)
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE users
            SET role = 'admin'
            WHERE role = 'super_admin'
            """
        )
    )
    op.drop_constraint("users_role_check", "users", type_="check")
    op.create_check_constraint(
        "users_role_check",
        "users",
        "role IN ('admin','client')",
    )
