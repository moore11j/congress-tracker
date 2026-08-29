from __future__ import annotations

from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.top_stocks import build_top_stocks_response

router = APIRouter(tags=["top-stocks"])


@router.get("/top-stocks")
def top_stocks(response: Response, db: Session = Depends(get_db)):
    # The scheduled leaderboard snapshot is the only source; this handler only reads it.
    response.headers["Cache-Control"] = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400"
    return build_top_stocks_response(db)
