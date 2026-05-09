from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.services.ticker_meta import debug_stable_search_row, get_ticker_meta
from app.utils.symbols import normalize_symbol

router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/ticker-meta")
def debug_ticker_meta(
    request: Request,
    symbol: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)

    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    meta = get_ticker_meta(db, [sym], allow_refresh=False).get(sym)
    return {
        "symbol_input": symbol,
        "symbol_normalized": sym,
        "meta": meta,
        "stable_preview": debug_stable_search_row(sym),
    }
