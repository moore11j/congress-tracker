from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.auth import current_user
from app.services.ranking_access import project_ranking
from app.entitlements import current_entitlements
from app.services.leaderboard_snapshots import CONGRESS_LEADERBOARD_KEY, INSTITUTION_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY, read_leaderboard_snapshot
from app.services.top_stocks import build_top_stocks_response

router = APIRouter(tags=["leaderboards"])


def _preview_snapshot(snapshot: dict, *, key: str) -> dict:
    return project_ranking(snapshot, authenticated=False, stocks=key == "top_stocks")


@router.get("/leaderboards/preview")
def leaderboard_preview(response: Response, db: Session = Depends(get_db)):
    """Serve only true ranks three through five; never serialize the winners."""
    response.headers["Cache-Control"] = "no-store"
    return {
        "top_stocks": _preview_snapshot(build_top_stocks_response(db), key="top_stocks"),
        "congress": _preview_snapshot(read_leaderboard_snapshot(db, CONGRESS_LEADERBOARD_KEY), key=CONGRESS_LEADERBOARD_KEY),
        "insiders": _preview_snapshot(read_leaderboard_snapshot(db, INSIDER_LEADERBOARD_KEY), key=INSIDER_LEADERBOARD_KEY),
        "institutions": _preview_snapshot(read_leaderboard_snapshot(db, INSTITUTION_LEADERBOARD_KEY), key=INSTITUTION_LEADERBOARD_KEY),
        "can_view_performance": False,
        "can_view_institutions": False,
    }


@router.get("/leaderboards/dashboard")
def leaderboard_dashboard(request: Request, response: Response, db: Session = Depends(get_db)):
    """Serve the complete dashboard from prepared snapshots in one request.

    Score calculations are performed by the daily refresh job or ticker cache
    builder. Top Stocks orders the prepared scores without recalculating them.
    Keeping the entitlement check and all snapshot reads together avoids a
    page-load waterfall of individually authenticated API requests.
    """
    entitlements = current_entitlements(request, db)
    authenticated = current_user(db, request) is not None
    can_view_performance = entitlements.has_feature("leaderboards")
    can_view_institutions = entitlements.has_feature("institutional_feed")
    response.headers["Cache-Control"] = "private, no-store"
    return {
        "top_stocks": project_ranking(build_top_stocks_response(db, entitlements=entitlements), authenticated=authenticated, entitlements=entitlements, stocks=True, full=can_view_performance),
        "congress": project_ranking(read_leaderboard_snapshot(db, CONGRESS_LEADERBOARD_KEY), authenticated=authenticated, full=can_view_performance),
        "insiders": project_ranking(read_leaderboard_snapshot(db, INSIDER_LEADERBOARD_KEY), authenticated=authenticated, full=can_view_performance),
        "institutions": project_ranking(read_leaderboard_snapshot(db, INSTITUTION_LEADERBOARD_KEY), authenticated=authenticated, full=can_view_institutions),
        "can_view_performance": can_view_performance,
        "can_view_institutions": can_view_institutions,
    }


@router.get("/leaderboards/{section}")
def leaderboard_section(section: str, request: Request, response: Response, db: Session = Depends(get_db)):
    normalized = (section or "").strip().lower()
    entitlements = current_entitlements(request, db)
    authenticated = current_user(db, request) is not None
    response.headers["Cache-Control"] = "private, no-store"
    if normalized == "top-stocks":
        response.headers["Cache-Control"] = "private, no-store"
        return project_ranking(build_top_stocks_response(db, entitlements=entitlements), authenticated=authenticated, entitlements=entitlements, stocks=True, full=entitlements.has_feature("leaderboards"))
    if normalized in {CONGRESS_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY}:
        full = entitlements.has_feature("leaderboards")
    elif normalized == INSTITUTION_LEADERBOARD_KEY:
        full = entitlements.has_feature("institutional_feed")
    else:
        raise HTTPException(status_code=404, detail="Unknown leaderboard section.")
    return project_ranking(read_leaderboard_snapshot(db, normalized), authenticated=authenticated, full=full)
