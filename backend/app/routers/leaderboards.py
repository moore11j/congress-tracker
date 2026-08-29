from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.leaderboard_snapshots import CONGRESS_LEADERBOARD_KEY, INSTITUTION_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY, read_leaderboard_snapshot
from app.services.top_stocks import build_top_stocks_response

router = APIRouter(tags=["leaderboards"])


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
