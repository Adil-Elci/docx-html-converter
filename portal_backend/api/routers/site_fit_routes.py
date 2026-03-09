from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..portal_models import Client, CreatorOutput, Job, Site, SiteFitCache, SiteProfileCache, Submission

router = APIRouter(
    prefix="/admin/site-fit",
    tags=["site_fit"],
    dependencies=[Depends(require_admin)],
)


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return None


@router.get("/dashboard")
def site_fit_dashboard(db: Session = Depends(get_db)) -> Dict[str, Any]:
    total_profiles = db.query(func.count(SiteProfileCache.id)).scalar() or 0
    publishing_profiles = (
        db.query(func.count(SiteProfileCache.id))
        .filter(SiteProfileCache.profile_kind == "publishing_site")
        .scalar()
        or 0
    )
    target_profiles = max(0, int(total_profiles) - int(publishing_profiles))
    latest_profile_update = db.query(func.max(SiteProfileCache.updated_at)).scalar()

    recent_profile_rows = (
        db.query(SiteProfileCache)
        .order_by(SiteProfileCache.updated_at.desc(), SiteProfileCache.created_at.desc())
        .limit(10)
        .all()
    )
    recent_profiles: List[Dict[str, Any]] = []
    for row in recent_profile_rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        recent_profiles.append(
            {
                "profile_kind": (row.profile_kind or "").strip(),
                "normalized_url": (row.normalized_url or "").strip(),
                "primary_context": str(payload.get("primary_context") or "").strip(),
                "domain_level_topic": str(payload.get("domain_level_topic") or "").strip(),
                "updated_at": _iso_or_none(row.updated_at),
                "generator_mode": (row.generator_mode or "").strip(),
            }
        )

    total_pair_fits = db.query(func.count(SiteFitCache.id)).scalar() or 0
    accepted_pair_fits = (
        db.query(func.count(SiteFitCache.id))
        .filter(SiteFitCache.decision == "accepted")
        .scalar()
        or 0
    )
    rejected_pair_fits = max(0, int(total_pair_fits) - int(accepted_pair_fits))
    latest_pair_fit_update = db.query(func.max(SiteFitCache.updated_at)).scalar()

    recent_pair_fit_rows = (
        db.query(SiteFitCache, Site)
        .join(Site, Site.id == SiteFitCache.publishing_site_id)
        .order_by(SiteFitCache.updated_at.desc(), SiteFitCache.created_at.desc())
        .limit(10)
        .all()
    )
    recent_pair_fits: List[Dict[str, Any]] = []
    for row, site in recent_pair_fit_rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        recent_pair_fits.append(
            {
                "publishing_site_name": (site.name or "").strip(),
                "publishing_site_url": (site.site_url or "").strip(),
                "target_url": (row.target_normalized_url or "").strip(),
                "decision": (row.decision or "").strip(),
                "fit_score": int(row.fit_score or 0),
                "final_article_topic": str(payload.get("final_article_topic") or "").strip(),
                "best_overlap_reason": str(payload.get("best_overlap_reason") or "").strip(),
                "updated_at": _iso_or_none(row.updated_at),
            }
        )

    recent_job_rows = (
        db.query(Job, Submission, Site, Client)
        .join(Submission, Submission.id == Job.submission_id)
        .join(Site, Site.id == Job.site_id)
        .join(Client, Client.id == Job.client_id)
        .filter(
            Submission.request_kind == "create_article",
            Submission.notes.contains("creator_mode=true"),
        )
        .order_by(Job.created_at.desc())
        .limit(20)
        .all()
    )
    recent_host_decisions: List[Dict[str, Any]] = []
    job_ids = [job.id for job, _, _, _ in recent_job_rows]
    creator_output_rows = (
        db.query(CreatorOutput.job_id, CreatorOutput.payload)
        .filter(CreatorOutput.job_id.in_(job_ids))
        .order_by(CreatorOutput.created_at.desc())
        .all()
        if job_ids
        else []
    )
    creator_output_by_job: Dict[str, Dict[str, Any]] = {}
    for creator_job_id, payload in creator_output_rows:
        key = str(creator_job_id)
        if key in creator_output_by_job or not isinstance(payload, dict):
            continue
        creator_output_by_job[key] = payload

    for job, submission, site, client in recent_job_rows:
        notes = (submission.notes or "").strip().lower()
        payload = creator_output_by_job.get(str(job.id)) or {}
        phase3 = payload.get("phase3") if isinstance(payload, dict) else {}
        pair_fit = phase3.get("pair_fit") if isinstance(phase3, dict) else {}
        if not isinstance(pair_fit, dict):
            pair_fit = {}
        recent_host_decisions.append(
            {
                "job_id": str(job.id),
                "client_name": (client.name or "").strip(),
                "publishing_site_name": (site.name or "").strip(),
                "publishing_site_url": (site.site_url or "").strip(),
                "target_url": str((payload.get("target_site_url") if isinstance(payload, dict) else "") or "").strip(),
                "auto_selected": "auto_selected_site=true" in notes,
                "job_status": (job.job_status or "").strip(),
                "fit_score": int((pair_fit.get("fit_score") or 0) if isinstance(pair_fit, dict) else 0),
                "topic": str((pair_fit.get("final_article_topic") or phase3.get("final_article_topic") or "") if isinstance(phase3, dict) else "").strip(),
                "overlap_reason": str((pair_fit.get("best_overlap_reason") or "") if isinstance(pair_fit, dict) else "").strip(),
                "created_at": _iso_or_none(job.created_at),
            }
        )
        if len(recent_host_decisions) >= 12:
            break

    return {
        "ok": True,
        "profiles": {
            "summary": {
                "total_profiles": int(total_profiles),
                "publishing_profiles": int(publishing_profiles),
                "target_profiles": int(target_profiles),
                "latest_profile_update_at": _iso_or_none(latest_profile_update),
            },
            "recent_profiles": recent_profiles,
        },
        "pair_fits": {
            "summary": {
                "total_pair_fits": int(total_pair_fits),
                "accepted_pair_fits": int(accepted_pair_fits),
                "rejected_pair_fits": int(rejected_pair_fits),
                "latest_pair_fit_update_at": _iso_or_none(latest_pair_fit_update),
            },
            "recent_pair_fits": recent_pair_fits,
        },
        "recent_host_decisions": recent_host_decisions,
    }
