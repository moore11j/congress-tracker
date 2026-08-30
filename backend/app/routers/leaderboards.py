from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.leaderboard_snapshots import CONGRESS_LEADERBOARD_KEY, INSTITUTION_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY, read_leaderboard_snapshot
from app.services.top_stocks import build_top_stocks_response

router = APIRouter(tags=["leaderboards"])


def _preview_snapshot(snapshot: dict) -> dict:
    """Expose a deliberately small public teaser from an already-built snapshot."""
    preview = dict(snapshot)
    preview["items"] = list(snapshot.get("items") or [])[:3]
    # Filter variants are a Premium interaction. The public page shows the
    # filters as disabled affordances and receives only the all-stocks teaser.
    preview.pop("filter_items", None)
    return preview


@router.get("/leaderboards/preview")
def leaderboard_preview(response: Response, db: Session = Depends(get_db)):
    """Serve a cacheable three-row preview without evaluating any rankings."""
    response.headers["Cache-Control"] = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400"
    return {
        "top_stocks": _preview_snapshot(build_top_stocks_response(db)),
        "congress": _preview_snapshot(read_leaderboard_snapshot(db, CONGRESS_LEADERBOARD_KEY)),
        "insiders": _preview_snapshot(read_leaderboard_snapshot(db, INSIDER_LEADERBOARD_KEY)),
        "institutions": _preview_snapshot(read_leaderboard_snapshot(db, INSTITUTION_LEADERBOARD_KEY)),
        "can_view_performance": False,
        "can_view_institutions": False,
    }


@router.get("/leaderboards/dashboard")
def leaderboard_dashboard(request: Request, response: Response, db: Session = Depends(get_db)):
    """Serve the complete dashboard from prepared snapshots in one request.

    Ranking calculations are performed by the daily refresh job, never here.
    Keeping the entitlement check and all snapshot reads together avoids a
    page-load waterfall of individually authenticated API requests.
    """
    entitlements = current_entitlements(request, db)
    can_view_performance = entitlements.has_feature("leaderboards")
    can_view_institutions = entitlements.has_feature("institutional_feed")
    response.headers["Cache-Control"] = "private, max-age=300, stale-while-revalidate=3600"
    return {
        "top_stocks": build_top_stocks_response(db),
        "congress": read_leaderboard_snapshot(db, CONGRESS_LEADERBOARD_KEY) if can_view_performance else None,
        "insiders": read_leaderboard_snapshot(db, INSIDER_LEADERBOARD_KEY) if can_view_performance else None,
        "institutions": read_leaderboard_snapshot(db, INSTITUTION_LEADERBOARD_KEY) if can_view_institutions else None,
        "can_view_performance": can_view_performance,
        "can_view_institutions": can_view_institutions,
    }


@router.get("/leaderboards/{section}")
def leaderboard_section(section: str, request: Request, response: Response, db: Session = Depends(get_db)):
    normalized = (section or "").strip().lower()
    response.headers["Cache-Control"] = "private, max-age=300, stale-while-revalidate=3600"
    if normalized == "top-stocks":
        response.headers["Cache-Control"] = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400"
        return build_top_stocks_response(db)
    if normalized in {CONGRESS_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY}:
        require_feature(current_entitlements(request, db), "leaderboards", message="Leaderboards are included with Premium.")
    elif normalized == INSTITUTION_LEADERBOARD_KEY:
        require_feature(current_entitlements(request, db), "institutional_feed", message="Institutional performance is included with Pro.")
    else:
        raise HTTPException(status_code=404, detail="Unknown leaderboard section.")
    return read_leaderboard_snapshot(db, normalized)
