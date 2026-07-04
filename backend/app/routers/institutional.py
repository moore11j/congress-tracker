from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.models import InstitutionalHolderIndustryBreakdown
from app.request_guards import api_prefetch_response, is_inactive_logged_out_api_request
from app.services.institutional_activity import (
    INSTITUTIONAL_ACTIVITY_TOOLTIP,
    activity_for_holder,
    holder_profile,
    industry_summary_payload,
    list_institutional_holders,
    filings_for_holder,
    get_ticker_institutional_activity,
    positions_for_holder,
    unavailable_institutional_summary,
    normalize_cik,
)
from app.utils.symbols import normalize_symbol

router = APIRouter(tags=["institutional"])


def _prefetch_response(request: Request, endpoint: str):
    return api_prefetch_response(request, endpoint=endpoint)


def _has_institutional_access(request: Request, db: Session) -> bool:
    return current_entitlements(request, db).has_feature("institutional_feed")


def _require_institutional_access(request: Request, db: Session) -> None:
    require_feature(
        current_entitlements(request, db),
        "institutional_feed",
        message="Institutional Activity requires Pro.",
    )


def _locked_ticker_payload(symbol: str | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol) if symbol else None
    summary = unavailable_institutional_summary(normalized, status="pro_locked")
    summary.update(
        {
            "locked": True,
            "required_plan": "pro",
            "available": False,
            "message": "Institutional Activity requires Pro. 13F filings disclose quarter-end holdings and may not reflect real-time trading.",
        }
    )
    return {
        "symbol": normalized,
        "source_label": "Institutional Activity",
        "availability": {"enabled": True, "status": "pro_locked", "filterable": False},
        "summary": summary,
        "items": [],
        "tooltip": INSTITUTIONAL_ACTIVITY_TOOLTIP,
        "locked": True,
        "required_plan": "pro",
    }


def _locked_institution_payload(cik: str) -> dict[str, Any]:
    return {
        "cik": normalize_cik(cik),
        "source_label": "Institutional Activity",
        "availability_status": "pro_locked",
        "locked": True,
        "required_plan": "pro",
        "message": "Institutional profiles are available on Pro.",
        "items": [],
    }


@router.get("/tickers/{symbol}/institutional-summary")
def ticker_institutional_summary(
    symbol: str,
    request: Request,
    lookback_days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "ticker_institutional_summary")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_ticker_payload(symbol)["summary"]
    if not _has_institutional_access(request, db):
        return _locked_ticker_payload(symbol)["summary"]
    payload = get_ticker_institutional_activity(db, symbol, lookback_days=lookback_days, limit=1)
    return payload["summary"]


@router.get("/tickers/{symbol}/institutional-activity")
def ticker_institutional_activity(
    symbol: str,
    request: Request,
    lookback_days: int = Query(30, ge=1, le=365),
    limit: int = Query(25, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "ticker_institutional_activity")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_ticker_payload(symbol)
    if not _has_institutional_access(request, db):
        return _locked_ticker_payload(symbol)
    return get_ticker_institutional_activity(db, symbol, lookback_days=lookback_days, limit=limit)


@router.get("/institutions")
def institutions(
    request: Request,
    q: str | None = Query(None),
    sort: str = Query("latest_filing_date"),
    direction: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institutions")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return {"status": "skipped", "items": [], "page": page, "limit": limit, "has_next": False}
    _require_institutional_access(request, db)
    return list_institutional_holders(db, q=q, sort=sort, direction=direction, page=page, limit=limit)


@router.get("/institutions/{cik}")
def institution_profile(cik: str, request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_profile")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        payload = _locked_institution_payload(cik)
        payload.update(
            {
                "holder_name": None,
                "latest_filing_date": None,
                "latest_report_year": None,
                "latest_report_quarter": None,
                "total_reported_value_usd": None,
                "holdings_count": None,
                "status": "skipped",
            }
        )
        return payload
    if not _has_institutional_access(request, db):
        payload = _locked_institution_payload(cik)
        payload.update(
            {
                "holder_name": None,
                "latest_filing_date": None,
                "latest_report_year": None,
                "latest_report_quarter": None,
                "total_reported_value_usd": None,
                "holdings_count": None,
            }
        )
        return payload
    profile = holder_profile(db, cik)
    if profile is None:
        return {
            "status": "no_data",
            "cik": normalize_cik(cik),
            "source_label": "Institutional Activity",
            "availability_status": "unavailable",
            "locked": False,
            "holder_name": None,
            "latest_filing_date": None,
            "latest_report_year": None,
            "latest_report_quarter": None,
            "total_reported_value_usd": None,
            "holdings_count": 0,
        }
    return {"status": "ok", **profile}


@router.get("/institutions/{cik}/positions")
def institution_positions(
    cik: str,
    request: Request,
    year: int | None = Query(None, ge=1999, le=2100),
    quarter: int | None = Query(None, ge=1, le=4),
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_positions")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_institution_payload(cik)
    if not _has_institutional_access(request, db):
        return _locked_institution_payload(cik)
    return positions_for_holder(db, cik, year=year, quarter=quarter, page=page, limit=limit)


@router.get("/institutions/{cik}/holdings")
def institution_holdings(
    cik: str,
    request: Request,
    year: int | None = Query(None, ge=1999, le=2100),
    quarter: int | None = Query(None, ge=1, le=4),
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_holdings")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_institution_payload(cik)
    if not _has_institutional_access(request, db):
        return _locked_institution_payload(cik)
    return positions_for_holder(db, cik, year=year, quarter=quarter, page=page, limit=limit)


@router.get("/institutions/{cik}/activity")
def institution_activity(
    cik: str,
    request: Request,
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_activity")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_institution_payload(cik)
    if not _has_institutional_access(request, db):
        return _locked_institution_payload(cik)
    return activity_for_holder(db, cik, page=page, limit=limit)


@router.get("/institutions/{cik}/filings")
def institution_filings(
    cik: str,
    request: Request,
    page: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_filings")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return _locked_institution_payload(cik)
    if not _has_institutional_access(request, db):
        return _locked_institution_payload(cik)
    return filings_for_holder(db, cik, page=page, limit=limit)


@router.get("/institutions/{cik}/performance")
def institution_performance(cik: str, request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_performance")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return {"status": "skipped", "cik": normalize_cik(cik), "items": []}
    _require_institutional_access(request, db)
    profile = holder_profile(db, cik)
    if profile is None:
        return {"status": "no_data", "cik": normalize_cik(cik), "items": []}
    return {
        "status": "ok",
        "cik": profile["cik"],
        "holder_name": profile["holder_name"],
        "quality_score": profile["quality_score"],
        "note": "Performance is derived from institutional holder analytics when available.",
    }


@router.get("/institutions/{cik}/industry-breakdown")
def institution_industry_breakdown(
    cik: str,
    request: Request,
    year: int | None = Query(None, ge=1999, le=2100),
    quarter: int | None = Query(None, ge=1, le=4),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institution_industry_breakdown")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return {"status": "skipped", "cik": normalize_cik(cik), "items": []}
    _require_institutional_access(request, db)
    normalized = normalize_cik(cik)
    if not normalized:
        return {"status": "invalid_cik", "items": []}
    query = select(InstitutionalHolderIndustryBreakdown).where(InstitutionalHolderIndustryBreakdown.cik == normalized)
    if year is not None:
        query = query.where(InstitutionalHolderIndustryBreakdown.report_year == int(year))
    if quarter is not None:
        query = query.where(InstitutionalHolderIndustryBreakdown.report_quarter == int(quarter))
    rows = db.execute(
        query.order_by(
            InstitutionalHolderIndustryBreakdown.report_year.desc(),
            InstitutionalHolderIndustryBreakdown.report_quarter.desc(),
            InstitutionalHolderIndustryBreakdown.value_usd.desc().nullslast(),
        ).limit(limit)
    ).scalars().all()
    return {
        "status": "ok",
        "cik": normalized,
        "items": [
            {
                "industry": row.industry,
                "sector": row.sector,
                "report_year": row.report_year,
                "report_quarter": row.report_quarter,
                "value_usd": row.value_usd,
                "weight_pct": row.weight_pct,
                "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            }
            for row in rows
        ],
    }


@router.get("/institutional/industry-summary")
def institutional_industry_summary(
    request: Request,
    year: int | None = Query(None, ge=1999, le=2100),
    quarter: int | None = Query(None, ge=1, le=4),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    prefetch_response = _prefetch_response(request, "institutional_industry_summary")
    if prefetch_response is not None:
        return prefetch_response
    if is_inactive_logged_out_api_request(request):
        return {"status": "skipped", "items": []}
    _require_institutional_access(request, db)
    return {"status": "ok", **industry_summary_payload(db, year=year, quarter=quarter, limit=limit)}
