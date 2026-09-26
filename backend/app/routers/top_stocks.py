from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.top_stocks import build_top_stocks_response
from app.auth import current_user
from app.entitlements import current_entitlements
from app.services.ranking_access import project_ranking

router = APIRouter(tags=["top-stocks"])


@router.get("/top-stocks")
def top_stocks(request: Request, response: Response, db: Session = Depends(get_db)):
    # The scheduled leaderboard snapshot is the only source; this handler only reads it.
    response.headers["Cache-Control"] = "private, no-store"
    entitlements = current_entitlements(request, db)
    return project_ranking(build_top_stocks_response(db, entitlements=entitlements), authenticated=current_user(db, request) is not None, entitlements=entitlements, stocks=True, full=entitlements.has_feature("leaderboards"))
