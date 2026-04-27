from __future__ import annotations

from datetime import datetime, timezone
from re import sub

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from app.clients.fmp import FMPClientError
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.screener import (
    MAX_EXPORT_ROWS,
    build_screener_csv_export,
    build_screener_response_for_entitlements,
    require_screener_intelligence_access,
    screener_params_from_mapping,
)

router = APIRouter(tags=["screener"])


@router.get("/screener")
def stock_screener(
    request: Request,
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
    congress_activity: str | None = Query(None, pattern="^(has_activity|no_activity|buy_leaning|sell_leaning)$"),
    insider_activity: str | None = Query(None, pattern="^(has_activity|no_activity|buy_leaning|sell_leaning)$"),
    confirmation_score_min: int | None = Query(None, ge=0, le=100),
    confirmation_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed)$"),
    confirmation_band: str | None = Query(None, pattern="^(moderate_plus|strong_plus|exceptional)$"),
    why_now_state: str | None = Query(None, pattern="^(early|strengthening|strong|limited|fading|inactive)$"),
    freshness: str | None = Query(None, pattern="^(fresh|early|active|maturing|stale|inactive)$"),
    government_contracts_active: bool | None = None,
    government_contracts_min_amount: float | None = Query(1_000_000, ge=0),
    government_contracts_lookback_days: int = Query(365, ge=1, le=1095),
    options_flow_active: bool | None = None,
    options_flow_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed|neutral)$"),
    options_flow_min_score: int | None = Query(None, ge=0, le=100),
    options_flow_min_premium: float | None = Query(None, ge=0),
    options_flow_lookback_days: int = Query(30, ge=1, le=365),
    institutional_activity_active: bool | None = None,
    institutional_activity_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed|neutral)$"),
    institutional_activity_min_value: float | None = Query(None, ge=0),
    institutional_activity_lookback_days: int = Query(90, ge=1, le=365),
):
    entitlements = current_entitlements(request, db)
    require_feature(entitlements, "screener", message="The stock screener is included with your plan.")
    params = _build_screener_params(
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
        congress_activity=congress_activity,
        insider_activity=insider_activity,
        confirmation_score_min=confirmation_score_min,
        confirmation_direction=confirmation_direction,
        confirmation_band=confirmation_band,
        why_now_state=why_now_state,
        freshness=freshness,
        government_contracts_active=government_contracts_active,
        government_contracts_min_amount=government_contracts_min_amount,
        government_contracts_lookback_days=government_contracts_lookback_days,
        options_flow_active=options_flow_active,
        options_flow_direction=options_flow_direction,
        options_flow_min_score=options_flow_min_score,
        options_flow_min_premium=options_flow_min_premium,
        options_flow_lookback_days=options_flow_lookback_days,
        institutional_activity_active=institutional_activity_active,
        institutional_activity_direction=institutional_activity_direction,
        institutional_activity_min_value=institutional_activity_min_value,
        institutional_activity_lookback_days=institutional_activity_lookback_days,
    )
    require_screener_intelligence_access(params, entitlements)
    try:
        return build_screener_response_for_entitlements(db, params, entitlements=entitlements)
    except FMPClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/screener/export.csv")
def stock_screener_export(
    request: Request,
    db: Session = Depends(get_db),
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
    congress_activity: str | None = Query(None, pattern="^(has_activity|no_activity|buy_leaning|sell_leaning)$"),
    insider_activity: str | None = Query(None, pattern="^(has_activity|no_activity|buy_leaning|sell_leaning)$"),
    confirmation_score_min: int | None = Query(None, ge=0, le=100),
    confirmation_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed)$"),
    confirmation_band: str | None = Query(None, pattern="^(moderate_plus|strong_plus|exceptional)$"),
    why_now_state: str | None = Query(None, pattern="^(early|strengthening|strong|limited|fading|inactive)$"),
    freshness: str | None = Query(None, pattern="^(fresh|early|active|maturing|stale|inactive)$"),
    government_contracts_active: bool | None = None,
    government_contracts_min_amount: float | None = Query(1_000_000, ge=0),
    government_contracts_lookback_days: int = Query(365, ge=1, le=1095),
    options_flow_active: bool | None = None,
    options_flow_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed|neutral)$"),
    options_flow_min_score: int | None = Query(None, ge=0, le=100),
    options_flow_min_premium: float | None = Query(None, ge=0),
    options_flow_lookback_days: int = Query(30, ge=1, le=365),
    institutional_activity_active: bool | None = None,
    institutional_activity_direction: str | None = Query(None, pattern="^(bullish|bearish|mixed|neutral)$"),
    institutional_activity_min_value: float | None = Query(None, ge=0),
    institutional_activity_lookback_days: int = Query(90, ge=1, le=365),
    filename_prefix: str | None = Query(None, max_length=160),
):
    entitlements = current_entitlements(request, db)
    require_feature(entitlements, "screener", message="The stock screener is included with your plan.")
    require_feature(entitlements, "screener_csv_export", message="CSV export is included with Premium.")
    params = _build_screener_params(
        page=1,
        page_size=MAX_EXPORT_ROWS,
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
        congress_activity=congress_activity,
        insider_activity=insider_activity,
        confirmation_score_min=confirmation_score_min,
        confirmation_direction=confirmation_direction,
        confirmation_band=confirmation_band,
        why_now_state=why_now_state,
        freshness=freshness,
        government_contracts_active=government_contracts_active,
        government_contracts_min_amount=government_contracts_min_amount,
        government_contracts_lookback_days=government_contracts_lookback_days,
        options_flow_active=options_flow_active,
        options_flow_direction=options_flow_direction,
        options_flow_min_score=options_flow_min_score,
        options_flow_min_premium=options_flow_min_premium,
        options_flow_lookback_days=options_flow_lookback_days,
        institutional_activity_active=institutional_activity_active,
        institutional_activity_direction=institutional_activity_direction,
        institutional_activity_min_value=institutional_activity_min_value,
        institutional_activity_lookback_days=institutional_activity_lookback_days,
    )
    require_screener_intelligence_access(params, entitlements)
    export_row_cap = min(MAX_EXPORT_ROWS, max(1, int(entitlements.limit("screener_results"))))
    try:
        csv_text, exported_rows = build_screener_csv_export(
            db,
            params,
            row_cap=export_row_cap,
        )
    except FMPClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    filename = _export_filename(filename_prefix)
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Screener-Export-Row-Cap": str(export_row_cap),
            "X-Screener-Exported-Rows": str(exported_rows),
        },
    )


def _build_screener_params(**raw_params):
    raw_params = {key: _query_default(value) for key, value in raw_params.items()}
    if raw_params.get("market_cap_min") is not None and raw_params.get("market_cap_max") is not None:
        if raw_params["market_cap_min"] > raw_params["market_cap_max"]:
            raise HTTPException(status_code=422, detail="market_cap_min cannot exceed market_cap_max.")
    if raw_params.get("price_min") is not None and raw_params.get("price_max") is not None:
        if raw_params["price_min"] > raw_params["price_max"]:
            raise HTTPException(status_code=422, detail="price_min cannot exceed price_max.")
    if raw_params.get("beta_min") is not None and raw_params.get("beta_max") is not None:
        if raw_params["beta_min"] > raw_params["beta_max"]:
            raise HTTPException(status_code=422, detail="beta_min cannot exceed beta_max.")
    return screener_params_from_mapping(raw_params, page=int(raw_params.get("page") or 1), page_size=int(raw_params.get("page_size") or 50))


def _export_filename(prefix: str | None) -> str:
    base = sub(r"[^A-Za-z0-9]+", "-", (prefix or "screener").strip()).strip("-").lower() or "screener"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{base}-{stamp}.csv"


def _query_default(value):
    default = getattr(value, "default", value)
    return None if default is ... else default
