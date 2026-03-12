from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

try:
    from portal_backend.api.creator_prompt_trace import extract_draft_article_html, normalize_prompt_trace_payload
    from portal_backend.api.db import get_sessionmaker
    from portal_backend.api.portal_models import CreatorOutput
except ModuleNotFoundError:
    from api.creator_prompt_trace import extract_draft_article_html, normalize_prompt_trace_payload
    from api.db import get_sessionmaker
    from api.portal_models import CreatorOutput


logger = logging.getLogger("portal_backend.scripts.creator_prompt_trace_backfill")


def _parse_job_id(raw_job_id: str) -> Optional[UUID]:
    cleaned = raw_job_id.strip()
    if not cleaned:
        return None
    return UUID(cleaned)


def backfill_creator_prompt_trace_columns(
    session: Session,
    *,
    job_id: Optional[UUID] = None,
    batch_size: int = 200,
    limit: int = 0,
    dry_run: bool = False,
    sync_payload: bool = True,
) -> Dict[str, Any]:
    query = session.query(CreatorOutput).filter(
        text(
            "("
            "planner_trace = '{}'::jsonb "
            "OR writer_prompt_trace = '[]'::jsonb "
            "OR COALESCE(draft_article_html, '') = ''"
            ")"
        )
    )
    if job_id is not None:
        query = query.filter(CreatorOutput.job_id == job_id)
    query = query.order_by(CreatorOutput.created_at.asc(), CreatorOutput.id.asc())

    scanned = 0
    updated = 0
    payload_synced = 0
    draft_backfilled = 0
    skipped = 0

    for row in query.yield_per(batch_size):
        if limit and scanned >= limit:
            break
        scanned += 1

        current_payload = row.payload if isinstance(row.payload, dict) else {}
        normalized_payload, planner_trace, writer_prompt_trace = normalize_prompt_trace_payload(current_payload)
        draft_article_html = extract_draft_article_html(normalized_payload)

        payload_changed = sync_payload and normalized_payload != current_payload
        planner_changed = planner_trace != (row.planner_trace if isinstance(row.planner_trace, dict) else {})
        writer_changed = writer_prompt_trace != (
            row.writer_prompt_trace if isinstance(row.writer_prompt_trace, list) else []
        )
        draft_changed = draft_article_html != str(getattr(row, "draft_article_html", "") or "")

        if not payload_changed and not planner_changed and not writer_changed and not draft_changed:
            skipped += 1
            continue

        updated += 1
        if payload_changed:
            payload_synced += 1
        if draft_changed:
            draft_backfilled += 1
        if dry_run:
            continue

        row.planner_trace = planner_trace
        row.writer_prompt_trace = writer_prompt_trace
        row.draft_article_html = draft_article_html
        if payload_changed:
            row.payload = normalized_payload

        if updated % batch_size == 0:
            session.commit()

    if not dry_run:
        session.commit()

    return {
        "scanned": scanned,
        "updated": updated,
        "payload_synced": payload_synced,
        "draft_backfilled": draft_backfilled,
        "skipped": skipped,
        "dry_run": dry_run,
        "job_id": str(job_id) if job_id else "",
        "limit": limit,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill creator_outputs planner_trace and writer_prompt_trace from stored payloads."
    )
    parser.add_argument("--job-id", dest="job_id", default="", help="Only backfill one job UUID.")
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=200)
    parser.add_argument("--limit", dest="limit", type=int, default=0)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument(
        "--skip-payload-sync",
        dest="skip_payload_sync",
        action="store_true",
        help="Only write normalized columns; do not write prompt_trace back into payload.debug.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    session = get_sessionmaker()()
    try:
        summary = backfill_creator_prompt_trace_columns(
            session,
            job_id=_parse_job_id(args.job_id),
            batch_size=max(1, args.batch_size),
            limit=max(0, args.limit),
            dry_run=bool(args.dry_run),
            sync_payload=not bool(args.skip_payload_sync),
        )
        logger.info("creator_prompt_trace_backfill_complete summary=%s", summary)
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
