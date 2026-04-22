from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.clients.fmp import FMPClientError
from app.db import get_db
from app.services.screener import ScreenerParams, build_screener_response

router = APIRouter(tags=["screener"])


@router.get("/screener")
def stock_screener(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1, le=10),
    page_size: int = Query(50, ge=10, le=100),
    sort: str = Query("relevance"),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    lookback_days: int = Query(30, ge=1, le=365),
    market_cap_min: float | None = Query(None, ge=0),
    market_cap_max: float | None = Query(None, ge=0),
    price_min: float | None = Query(None, ge=0),
    price_max: float | None = Query(None, ge=0),
    volume_min: float | None = Query(None, ge=0),
    beta_min: float | None = None,
    beta_max: float | None = None,
    sector: str | None = None,
    industry: str | None = None,
    country: str | None = None,
    exchange: str | None = None,
    dividend_yield_min: float | None = Query(None, ge=0),
    dividend_yield_max: float | None = Query(None, ge=0),
):
    if market_cap_min is not None and market_cap_max is not None and market_cap_min > market_cap_max:
        raise HTTPException(status_code=422, detail="market_cap_min cannot exceed market_cap_max.")
    if price_min is not None and price_max is not None and price_min > price_max:
        raise HTTPException(status_code=422, detail="price_min cannot exceed price_max.")
    if beta_min is not None and beta_max is not None and beta_min > beta_max:
        raise HTTPException(status_code=422, detail="beta_min cannot exceed beta_max.")

    params = ScreenerParams(
        page=page,
        page_size=page_size,
        sort=sort,
        sort_dir=sort_dir,
        lookback_days=lookback_days,
        market_cap_min=market_cap_min,
        market_cap_max=market_cap_max,
        price_min=price_min,
        price_max=price_max,
        volume_min=volume_min,
        beta_min=beta_min,
        beta_max=beta_max,
        sector=sector,
        industry=industry,
        country=country,
        exchange=exchange,
        dividend_yield_min=dividend_yield_min,
        dividend_yield_max=dividend_yield_max,
    )
    try:
        return build_screener_response(db, params)
    except FMPClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
