from __future__ import annotations

import copy
import json
import os
import threading
import time
from datetime import datetime, timezone
from re import sub

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from app.clients.fmp import FMPClientError
from app.db import get_db
from app.entitlements import current_entitlements, require_feature, required_tier_for_feature
from app.rate_limit import rate_limit_export, rate_limit_provider_backed
from app.services.screener import (
    DEFAULT_PAGE_SIZE,
    MAX_EXPORT_ROWS,
    build_screener_csv_export,
    build_screener_response_for_entitlements,
    require_screener_intelligence_access,
    screener_params_from_mapping,
)

router = APIRouter(tags=["screener"])
_SCREENER_RESPONSE_CACHE: dict[str, tuple[float, dict]] = {}
_SCREENER_RESPONSE_INFLIGHT: dict[str, dict[str, object]] = {}
_SCREENER_RESPONSE_LOCK = threading.Lock()


def _screener_response_cache_ttl_seconds() -> int:
    try:
        return max(0, min(120, int(os.getenv("SCREENER_RESPONSE_CACHE_TTL_SECONDS", "30") or 30)))
    except ValueError:
        return 30


def _screener_response_dedupe_wait_seconds() -> float:
    try:
        return max(0.0, min(10.0, float(os.getenv("SCREENER_RESPONSE_DEDUPE_WAIT_SECONDS", "5") or 5)))
    except ValueError:
        return 5.0


def _screener_response_cache_key(request: Request, entitlements) -> str | None:
    if _screener_response_cache_ttl_seconds() <= 0:
        return None
    query_items = sorted((key, value) for key, value in request.query_params.multi_items())
    entitlement_key = {
        "tier": getattr(entitlements, "tier", "free"),
        "rank": getattr(entitlements, "rank", 0),
        "features": sorted(getattr(entitlements, "features", []) or []),
        "limits": getattr(entitlements, "limits", {}) or {},
    }
    return "screener:" + json.dumps(
        {"query": query_items, "entitlements": entitlement_key},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _screener_response_cache_get(cache_key: str | None) -> dict | None:
    if not cache_key:
        return None
    now = time.time()
    with _SCREENER_RESPONSE_LOCK:
        cached = _SCREENER_RESPONSE_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _SCREENER_RESPONSE_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _screener_response_inflight_start(cache_key: str | None) -> tuple[dict[str, object] | None, bool]:
    if not cache_key:
        return None, False
    with _SCREENER_RESPONSE_LOCK:
        state = _SCREENER_RESPONSE_INFLIGHT.get(cache_key)
        if state is not None:
            return state, False
        state = {"event": threading.Event(), "result": None, "error": None}
        _SCREENER_RESPONSE_INFLIGHT[cache_key] = state
        return state, True


def _screener_response_cache_finalize(
    cache_key: str | None,
    inflight_state: dict[str, object] | None,
    inflight_leader: bool,
    payload: dict,
) -> dict:
    if cache_key:
        stored = copy.deepcopy(payload)
        with _SCREENER_RESPONSE_LOCK:
            _SCREENER_RESPONSE_CACHE[cache_key] = (time.time() + _screener_response_cache_ttl_seconds(), stored)
    if inflight_leader and inflight_state is not None:
        inflight_state["result"] = copy.deepcopy(payload)
        event = inflight_state.get("event")
        if isinstance(event, threading.Event):
            event.set()
        with _SCREENER_RESPONSE_LOCK:
            _SCREENER_RESPONSE_INFLIGHT.pop(cache_key or "", None)
    return payload


@router.get("/screener")
def stock_screener(
    request: Request,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1, le=10),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=5, le=100),
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
    rel_volume_min: float | None = Query(None, ge=0),
    rel_volume_max: float | None = Query(None, ge=0),
    price_move_min: float | None = None,
    price_move_max: float | None = None,
    rsi_min: float | None = Query(None, ge=0, le=100),
    rsi_max: float | None = Query(None, ge=0, le=100),
    macd_state: str | None = Query(None, pattern="^([Aa]ny|bullish|bearish|crossover_bullish|crossover_bearish)$"),
    trend_state: str | None = Query(None, pattern="^([Aa]ny|sma_above_lma|sma_below_lma)$"),
    trailing_pe_min: float | None = None,
    trailing_pe_max: float | None = None,
    forward_pe_min: float | None = None,
    forward_pe_max: float | None = None,
    price_sales_min: float | None = None,
    price_sales_max: float | None = None,
    ev_ebitda_min: float | None = None,
    ev_ebitda_max: float | None = None,
    gross_margin_min: float | None = None,
    gross_margin_max: float | None = None,
    operating_margin_min: float | None = None,
    operating_margin_max: float | None = None,
    net_margin_min: float | None = None,
    net_margin_max: float | None = None,
    roe_min: float | None = None,
    roe_max: float | None = None,
    roic_min: float | None = None,
    roic_max: float | None = None,
    revenue_growth_min: float | None = None,
    revenue_growth_max: float | None = None,
    eps_growth_min: float | None = None,
    eps_growth_max: float | None = None,
    ebitda_growth_min: float | None = None,
    ebitda_growth_max: float | None = None,
    fcf_growth_min: float | None = None,
    fcf_growth_max: float | None = None,
    debt_equity_min: float | None = None,
    debt_equity_max: float | None = None,
    current_ratio_min: float | None = None,
    current_ratio_max: float | None = None,
    net_debt_ebitda_min: float | None = None,
    net_debt_ebitda_max: float | None = None,
    eps_ttm_min: float | None = None,
    eps_ttm_max: float | None = None,
    fcf_min: float | None = None,
    fcf_max: float | None = None,
    fcf_margin_min: float | None = None,
    fcf_margin_max: float | None = None,
    earnings_yield_min: float | None = None,
    earnings_yield_max: float | None = None,
):
    entitlements = current_entitlements(request, db)
    require_feature(entitlements, "screener", message="The stock screener is included with your plan.")
    params = _build_screener_params(
        _query_params=_request_query_params(request),
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
        rel_volume_min=rel_volume_min,
        rel_volume_max=rel_volume_max,
        price_move_min=price_move_min,
        price_move_max=price_move_max,
        rsi_min=rsi_min,
        rsi_max=rsi_max,
        macd_state=macd_state,
        trend_state=trend_state,
        trailing_pe_min=trailing_pe_min,
        trailing_pe_max=trailing_pe_max,
        forward_pe_min=forward_pe_min,
        forward_pe_max=forward_pe_max,
        price_sales_min=price_sales_min,
        price_sales_max=price_sales_max,
        ev_ebitda_min=ev_ebitda_min,
        ev_ebitda_max=ev_ebitda_max,
        gross_margin_min=gross_margin_min,
        gross_margin_max=gross_margin_max,
        operating_margin_min=operating_margin_min,
        operating_margin_max=operating_margin_max,
        net_margin_min=net_margin_min,
        net_margin_max=net_margin_max,
        roe_min=roe_min,
        roe_max=roe_max,
        roic_min=roic_min,
        roic_max=roic_max,
        revenue_growth_min=revenue_growth_min,
        revenue_growth_max=revenue_growth_max,
        eps_growth_min=eps_growth_min,
        eps_growth_max=eps_growth_max,
        ebitda_growth_min=ebitda_growth_min,
        ebitda_growth_max=ebitda_growth_max,
        fcf_growth_min=fcf_growth_min,
        fcf_growth_max=fcf_growth_max,
        debt_equity_min=debt_equity_min,
        debt_equity_max=debt_equity_max,
        current_ratio_min=current_ratio_min,
        current_ratio_max=current_ratio_max,
        net_debt_ebitda_min=net_debt_ebitda_min,
        net_debt_ebitda_max=net_debt_ebitda_max,
        eps_ttm_min=eps_ttm_min,
        eps_ttm_max=eps_ttm_max,
        fcf_min=fcf_min,
        fcf_max=fcf_max,
        fcf_margin_min=fcf_margin_min,
        fcf_margin_max=fcf_margin_max,
        earnings_yield_min=earnings_yield_min,
        earnings_yield_max=earnings_yield_max,
    )
    require_screener_intelligence_access(params, entitlements)
    cache_key = _screener_response_cache_key(request, entitlements)
    cached_response = _screener_response_cache_get(cache_key)
    if cached_response is not None:
        return cached_response
    inflight_state, inflight_leader = _screener_response_inflight_start(cache_key)
    if cache_key and not inflight_leader and inflight_state is not None:
        event = inflight_state.get("event")
        if isinstance(event, threading.Event) and event.wait(timeout=_screener_response_dedupe_wait_seconds()):
            if inflight_state.get("error") is not None:
                raise inflight_state["error"]
            result = inflight_state.get("result")
            if isinstance(result, dict):
                return copy.deepcopy(result)
    rate_limit_provider_backed(request)
    try:
        payload = build_screener_response_for_entitlements(db, params, entitlements=entitlements)
        return _screener_response_cache_finalize(cache_key, inflight_state, inflight_leader, payload)
    except FMPClientError as exc:
        if inflight_leader and inflight_state is not None:
            inflight_state["error"] = HTTPException(status_code=502, detail=str(exc))
            event = inflight_state.get("event")
            if isinstance(event, threading.Event):
                event.set()
            with _SCREENER_RESPONSE_LOCK:
                _SCREENER_RESPONSE_INFLIGHT.pop(cache_key or "", None)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        if inflight_leader and inflight_state is not None:
            inflight_state["error"] = exc
            event = inflight_state.get("event")
            if isinstance(event, threading.Event):
                event.set()
            with _SCREENER_RESPONSE_LOCK:
                _SCREENER_RESPONSE_INFLIGHT.pop(cache_key or "", None)
        raise


@router.get("/screener/export.csv", dependencies=[Depends(rate_limit_export)])
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
    rel_volume_min: float | None = Query(None, ge=0),
    rel_volume_max: float | None = Query(None, ge=0),
    price_move_min: float | None = None,
    price_move_max: float | None = None,
    rsi_min: float | None = Query(None, ge=0, le=100),
    rsi_max: float | None = Query(None, ge=0, le=100),
    macd_state: str | None = Query(None, pattern="^([Aa]ny|bullish|bearish|crossover_bullish|crossover_bearish)$"),
    trend_state: str | None = Query(None, pattern="^([Aa]ny|sma_above_lma|sma_below_lma)$"),
    trailing_pe_min: float | None = None,
    trailing_pe_max: float | None = None,
    forward_pe_min: float | None = None,
    forward_pe_max: float | None = None,
    price_sales_min: float | None = None,
    price_sales_max: float | None = None,
    ev_ebitda_min: float | None = None,
    ev_ebitda_max: float | None = None,
    gross_margin_min: float | None = None,
    gross_margin_max: float | None = None,
    operating_margin_min: float | None = None,
    operating_margin_max: float | None = None,
    net_margin_min: float | None = None,
    net_margin_max: float | None = None,
    roe_min: float | None = None,
    roe_max: float | None = None,
    roic_min: float | None = None,
    roic_max: float | None = None,
    revenue_growth_min: float | None = None,
    revenue_growth_max: float | None = None,
    eps_growth_min: float | None = None,
    eps_growth_max: float | None = None,
    ebitda_growth_min: float | None = None,
    ebitda_growth_max: float | None = None,
    fcf_growth_min: float | None = None,
    fcf_growth_max: float | None = None,
    debt_equity_min: float | None = None,
    debt_equity_max: float | None = None,
    current_ratio_min: float | None = None,
    current_ratio_max: float | None = None,
    net_debt_ebitda_min: float | None = None,
    net_debt_ebitda_max: float | None = None,
    eps_ttm_min: float | None = None,
    eps_ttm_max: float | None = None,
    fcf_min: float | None = None,
    fcf_max: float | None = None,
    fcf_margin_min: float | None = None,
    fcf_margin_max: float | None = None,
    earnings_yield_min: float | None = None,
    earnings_yield_max: float | None = None,
    filename_prefix: str | None = Query(None, max_length=160),
):
    entitlements = current_entitlements(request, db)
    require_feature(entitlements, "screener", message="The stock screener is included with your plan.")
    csv_export_required_tier = required_tier_for_feature(db, "screener_csv_export")
    require_feature(
        entitlements,
        "screener_csv_export",
        message=f"CSV export is a {csv_export_required_tier.title()} feature.",
    )
    params = _build_screener_params(
        _query_params=_request_query_params(request),
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
        rel_volume_min=rel_volume_min,
        rel_volume_max=rel_volume_max,
        price_move_min=price_move_min,
        price_move_max=price_move_max,
        rsi_min=rsi_min,
        rsi_max=rsi_max,
        macd_state=macd_state,
        trend_state=trend_state,
        trailing_pe_min=trailing_pe_min,
        trailing_pe_max=trailing_pe_max,
        forward_pe_min=forward_pe_min,
        forward_pe_max=forward_pe_max,
        price_sales_min=price_sales_min,
        price_sales_max=price_sales_max,
        ev_ebitda_min=ev_ebitda_min,
        ev_ebitda_max=ev_ebitda_max,
        gross_margin_min=gross_margin_min,
        gross_margin_max=gross_margin_max,
        operating_margin_min=operating_margin_min,
        operating_margin_max=operating_margin_max,
        net_margin_min=net_margin_min,
        net_margin_max=net_margin_max,
        roe_min=roe_min,
        roe_max=roe_max,
        roic_min=roic_min,
        roic_max=roic_max,
        revenue_growth_min=revenue_growth_min,
        revenue_growth_max=revenue_growth_max,
        eps_growth_min=eps_growth_min,
        eps_growth_max=eps_growth_max,
        ebitda_growth_min=ebitda_growth_min,
        ebitda_growth_max=ebitda_growth_max,
        fcf_growth_min=fcf_growth_min,
        fcf_growth_max=fcf_growth_max,
        debt_equity_min=debt_equity_min,
        debt_equity_max=debt_equity_max,
        current_ratio_min=current_ratio_min,
        current_ratio_max=current_ratio_max,
        net_debt_ebitda_min=net_debt_ebitda_min,
        net_debt_ebitda_max=net_debt_ebitda_max,
        eps_ttm_min=eps_ttm_min,
        eps_ttm_max=eps_ttm_max,
        fcf_min=fcf_min,
        fcf_max=fcf_max,
        fcf_margin_min=fcf_margin_min,
        fcf_margin_max=fcf_margin_max,
        earnings_yield_min=earnings_yield_min,
        earnings_yield_max=earnings_yield_max,
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
    query_params = raw_params.pop("_query_params", None)
    if query_params:
        raw_params = {
            **raw_params,
            **{
                key: value
                for key, value in query_params.items()
                if key not in raw_params or _query_default(raw_params.get(key)) is None
            },
        }
    raw_params = {key: _query_default(value) for key, value in raw_params.items()}
    params = screener_params_from_mapping(raw_params, page=int(raw_params.get("page") or 1), page_size=int(raw_params.get("page_size") or DEFAULT_PAGE_SIZE))
    if params.market_cap_min is not None and params.market_cap_max is not None and params.market_cap_min > params.market_cap_max:
        raise HTTPException(status_code=422, detail="market_cap_min cannot exceed market_cap_max.")
    if params.price_min is not None and params.price_max is not None and params.price_min > params.price_max:
        raise HTTPException(status_code=422, detail="price_min cannot exceed price_max.")
    if params.beta_min is not None and params.beta_max is not None and params.beta_min > params.beta_max:
        raise HTTPException(status_code=422, detail="beta_min cannot exceed beta_max.")
    for base in (
        "rel_volume",
        "price_move",
        "rsi",
        "trailing_pe",
        "forward_pe",
        "price_sales",
        "ev_ebitda",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "roe",
        "roic",
        "revenue_growth",
        "eps_growth",
        "ebitda_growth",
        "fcf_growth",
        "debt_equity",
        "current_ratio",
        "net_debt_ebitda",
        "eps_ttm",
        "fcf",
        "fcf_margin",
        "earnings_yield",
    ):
        minimum = getattr(params, f"{base}_min")
        maximum = getattr(params, f"{base}_max")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise HTTPException(status_code=422, detail=f"{base}_min cannot exceed {base}_max.")
    return params


def _request_query_params(request: Request) -> dict[str, str]:
    if "query_string" not in request.scope:
        return {}
    return dict(request.query_params)


def _export_filename(prefix: str | None) -> str:
    base = sub(r"[^A-Za-z0-9]+", "-", (prefix or "screener").strip()).strip("-").lower() or "screener"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{base}-{stamp}.csv"


def _query_default(value):
    default = getattr(value, "default", value)
    return None if default is ... else default
