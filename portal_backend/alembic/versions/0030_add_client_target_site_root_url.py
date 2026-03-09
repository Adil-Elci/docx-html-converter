"""add target site root url

Revision ID: 0030_add_client_target_site_root_url
Revises: 0029_add_site_profiles_and_fit_cache
Create Date: 2026-03-09 13:15:00.000000
"""

from __future__ import annotations

from urllib.parse import urlparse

from alembic import op
import sqlalchemy as sa


revision = "0030_add_client_target_site_root_url"
down_revision = "0029_add_site_profiles_and_fit_cache"
branch_labels = None
depends_on = None


def _normalize_url(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or "").strip().lower().rstrip("/")
    if not host:
        return None
    path = parsed.path or ""
    return f"{(parsed.scheme or 'https').lower()}://{host}{path}".rstrip("/") or None


def _normalize_domain(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or parsed.path or "").strip().lower().rstrip("/")
    host = host.split("/")[0].strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _derive_root_url(target_site_url: str | None, target_site_domain: str | None) -> str | None:
    normalized_url = _normalize_url(target_site_url)
    if normalized_url:
        parsed = urlparse(normalized_url)
        host = (parsed.netloc or "").strip().lower().rstrip("/")
        if host:
            return f"{(parsed.scheme or 'https').lower()}://{host}"
    normalized_domain = _normalize_domain(target_site_domain)
    if normalized_domain:
        return f"https://{normalized_domain}"
    return None


def upgrade() -> None:
    op.add_column("client_target_sites", sa.Column("target_site_root_url", sa.Text(), nullable=True))

    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            """
            SELECT id, client_id, target_site_domain, target_site_url
            FROM client_target_sites
            ORDER BY created_at ASC, id ASC
            """
        )
    ).mappings().all()

    existing_pairs = {
        (
            str(row["client_id"]),
            _normalize_domain(row["target_site_domain"]),
            _normalize_url(row["target_site_url"]),
        )
        for row in rows
    }

    for row in rows:
        row_id = row["id"]
        client_id = str(row["client_id"])
        normalized_url = _normalize_url(row["target_site_url"])
        normalized_domain = _normalize_domain(row["target_site_domain"] or normalized_url)
        root_url = _derive_root_url(normalized_url, normalized_domain)
        update_payload = {
            "id": row_id,
            "target_site_domain": normalized_domain,
            "target_site_root_url": root_url,
        }
        new_url = normalized_url
        if not new_url and root_url:
            candidate_key = (client_id, normalized_domain, root_url)
            if candidate_key not in existing_pairs:
                new_url = root_url
                existing_pairs.add(candidate_key)
        update_payload["target_site_url"] = new_url
        connection.execute(
            sa.text(
                """
                UPDATE client_target_sites
                SET target_site_domain = :target_site_domain,
                    target_site_url = :target_site_url,
                    target_site_root_url = :target_site_root_url
                WHERE id = :id
                """
            ),
            update_payload,
        )


def downgrade() -> None:
    op.drop_column("client_target_sites", "target_site_root_url")
