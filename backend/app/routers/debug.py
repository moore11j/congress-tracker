from __future__ import annotations

import os

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.ticker_meta import debug_stable_search_row, get_ticker_meta
from app.utils.symbols import normalize_symbol

router = APIRouter(prefix="/debug", tags=["debug"])


def _admin_token() -> str | None:
    t = os.getenv("ADMIN_TOKEN", "").strip()
    return t or None


@router.get("/ticker-meta")
def debug_ticker_meta(
    symbol: str = Query(..., min_length=1),
    token: str | None = None,
    db: Session = Depends(get_db),
):
    # If ADMIN_TOKEN is set, require it. If not set, leave open for dev.
    admin = _admin_token()
    if admin:
        if not token or token != admin:
            raise HTTPException(status_code=401, detail="Unauthorized")

    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    meta = get_ticker_meta(db, [sym]).get(sym)
    return {
        "symbol_input": symbol,
        "symbol_normalized": sym,
        "meta": meta,
        "stable_preview": debug_stable_search_row(sym),
    }
