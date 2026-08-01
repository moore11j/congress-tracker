from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.db import SessionLocal
from app.models import FundamentalsSnapshot, InsiderTransactionNormalized, TickerFinancialsCache


def _parse_day(value: Any) -> date | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _growth_pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return ((current / previous) - 1.0) * 100.0


def _has_number(mapping: dict[str, Any], *keys: str) -> bool:
    return any(_number(mapping.get(key)) is not None for key in keys)


def _load_payload(row: TickerFinancialsCache) -> dict[str, Any]:
    try:
        payload = json.loads(row.payload_json)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _statement_rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _eligible_quarterly_snapshot_count(payload: dict[str, Any]) -> int:
    rows = sorted(
        (row for row in _statement_rows(payload, "quarterly") if _parse_day(row.get("date")) is not None),
        key=lambda row: str(row.get("date")),
    )
    count = 0
    for index, row in enumerate(rows):
        revenue = _number(row.get("revenue"))
        eps = _number(row.get("eps"))
        free_cash_flow = _number(row.get("freeCashFlow"))
        prior_year = rows[index - 4] if index >= 4 else {}
        revenue_growth = _growth_pct(revenue, _number(prior_year.get("revenue")))
        eps_growth = _growth_pct(eps, _number(prior_year.get("eps")))
        fcf_growth = _growth_pct(free_cash_flow, _number(prior_year.get("freeCashFlow")))
        if any(
            value is not None
            for value in (
                revenue,
                eps,
                free_cash_flow,
                row.get("grossMargin"),
                row.get("operatingMargin"),
                revenue_growth,
                eps_growth,
                fcf_growth,
            )
        ):
            count += 1
    return count


def _field_availability(payload: dict[str, Any]) -> dict[str, Any]:
    annual = _statement_rows(payload, "annual")
    quarterly = sorted(
        (row for row in _statement_rows(payload, "quarterly") if _parse_day(row.get("date")) is not None),
        key=lambda row: str(row.get("date")),
    )
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    valuation = payload.get("valuation_metrics") if isinstance(payload.get("valuation_metrics"), dict) else {}
    health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    subsections = payload.get("subsections") if isinstance(payload.get("subsections"), dict) else {}

    derived_growth = {
        "revenue_growth": 0,
        "eps_growth": 0,
        "fcf_growth": 0,
    }
    for index, row in enumerate(quarterly):
        previous_year = quarterly[index - 4] if index >= 4 else {}
        if _growth_pct(_number(row.get("revenue")), _number(previous_year.get("revenue"))) is not None:
            derived_growth["revenue_growth"] += 1
        if _growth_pct(_number(row.get("eps")), _number(previous_year.get("eps"))) is not None:
            derived_growth["eps_growth"] += 1
        if _growth_pct(_number(row.get("freeCashFlow")), _number(previous_year.get("freeCashFlow"))) is not None:
            derived_growth["fcf_growth"] += 1

    statement_counts = {
        "quarterly_revenue": sum(1 for row in quarterly if _has_number(row, "revenue")),
        "quarterly_eps": sum(1 for row in quarterly if _has_number(row, "eps")),
        "quarterly_gross_margin": sum(1 for row in quarterly if _has_number(row, "grossMargin")),
        "quarterly_operating_margin": sum(1 for row in quarterly if _has_number(row, "operatingMargin")),
        "quarterly_free_cash_flow": sum(1 for row in quarterly if _has_number(row, "freeCashFlow")),
        "annual_revenue": sum(1 for row in annual if _has_number(row, "revenue")),
        "annual_eps": sum(1 for row in annual if _has_number(row, "eps")),
    }

    current_counts = {
        "summary_trailing_pe": 1 if _has_number(summary, "trailingPE") else 0,
        "summary_forward_pe": 1 if _has_number(summary, "forwardPE") else 0,
        "summary_debt_to_equity": 1 if _has_number(summary, "debtToEquity") else 0,
        "summary_current_ratio": 1 if _has_number(summary, "currentRatio") else 0,
        "valuation_forward_pe": 1 if _has_number(valuation, "forward_pe", "forwardPE") else 0,
        "valuation_forward_peg": 1 if _has_number(valuation, "forward_peg", "forwardPEG") else 0,
        "health_debt_to_equity": 1 if _has_number(health, "debtToEquity") else 0,
        "health_current_ratio": 1 if _has_number(health, "currentRatio") else 0,
        "health_asset_ratio": 1 if _has_number(health, "assetRatio") else 0,
    }

    valuation_as_of = valuation.get("as_of") or valuation.get("asOf")
    subsection_statuses = {
        str(key): value.get("status")
        for key, value in subsections.items()
        if isinstance(value, dict) and value.get("status")
    }
    return {
        "statement_counts": statement_counts,
        "derived_growth_counts": derived_growth,
        "current_or_derived_counts": current_counts,
        "has_valuation_as_of": bool(valuation_as_of),
        "section_statuses": {
            str(key): str(value)
            for key, value in sections.items()
            if value is not None
        },
        "subsection_statuses": subsection_statuses,
    }


def _payload_summary(row: TickerFinancialsCache) -> dict[str, Any]:
    payload = _load_payload(row)
    annual = _statement_rows(payload, "annual")
    quarterly = _statement_rows(payload, "quarterly")
    dates = [day for day in (_parse_day(item.get("date")) for item in (*annual, *quarterly)) if day is not None]
    availability = _field_availability(payload)
    return {
        "symbol": row.symbol,
        "status": row.status,
        "payload_bytes": len(row.payload_json or ""),
        "annual_rows": len(annual),
        "quarterly_rows": len(quarterly),
        "eligible_quarterly_snapshots": _eligible_quarterly_snapshot_count(payload),
        "earliest_statement_date": min(dates).isoformat() if dates else None,
        "latest_statement_date": max(dates).isoformat() if dates else None,
        "sections_present": payload.get("sections_present") if isinstance(payload.get("sections_present"), list) else [],
        "field_availability": availability,
    }


def _insider_purchase_symbols(db) -> set[str]:
    rows = db.execute(
        select(func.upper(InsiderTransactionNormalized.ticker_normalized))
        .where(InsiderTransactionNormalized.is_duplicate.is_(False))
        .where(InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase")
        .where(InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .group_by(func.upper(InsiderTransactionNormalized.ticker_normalized))
    ).scalars()
    return {symbol for symbol in rows if symbol}


def _snapshot_row_count(db) -> int | None:
    try:
        return int(db.execute(select(func.count()).select_from(FundamentalsSnapshot)).scalar() or 0)
    except SQLAlchemyError:
        return None


def audit_historical_fundamentals_backfill(*, sample_limit: int = 12) -> dict[str, Any]:
    with SessionLocal() as db:
        rows = db.execute(select(TickerFinancialsCache).order_by(TickerFinancialsCache.symbol.asc())).scalars().all()
        summaries = [_payload_summary(row) for row in rows]
        status_counts = Counter(item["status"] for item in summaries)
        section_counts: Counter[str] = Counter()
        section_status_counts: Counter[str] = Counter()
        subsection_status_counts: Counter[str] = Counter()
        statement_field_rows: Counter[str] = Counter()
        statement_field_symbols: Counter[str] = Counter()
        derived_growth_rows: Counter[str] = Counter()
        derived_growth_symbols: Counter[str] = Counter()
        current_field_symbols: Counter[str] = Counter()
        valuation_as_of_symbols = 0
        for item in summaries:
            section_counts.update(item["sections_present"])
            availability = item["field_availability"]
            for section, status in availability["section_statuses"].items():
                section_status_counts[f"{section}:{status}"] += 1
            for section, status in availability["subsection_statuses"].items():
                subsection_status_counts[f"{section}:{status}"] += 1
            for field, count in availability["statement_counts"].items():
                statement_field_rows[field] += int(count)
                if int(count) > 0:
                    statement_field_symbols[field] += 1
            for field, count in availability["derived_growth_counts"].items():
                derived_growth_rows[field] += int(count)
                if int(count) > 0:
                    derived_growth_symbols[field] += 1
            for field, count in availability["current_or_derived_counts"].items():
                if int(count) > 0:
                    current_field_symbols[field] += 1
            if availability["has_valuation_as_of"]:
                valuation_as_of_symbols += 1
        dates = [
            _parse_day(value)
            for item in summaries
            for value in (item["earliest_statement_date"], item["latest_statement_date"])
            if value
        ]
        eligible_symbols = [item for item in summaries if item["eligible_quarterly_snapshots"] > 0]
        insider_symbols = _insider_purchase_symbols(db)
        cached_symbols = {item["symbol"] for item in summaries}
        snapshot_rows = _snapshot_row_count(db)
        return {
            "status": "ok",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "read_only_audit",
            "ticker_financials_cache": {
                "symbols": len(summaries),
                "status_counts": dict(sorted(status_counts.items())),
                "symbols_with_eligible_quarterly_statement_rows": len(eligible_symbols),
                "estimated_quarterly_snapshot_rows_from_cache": sum(item["eligible_quarterly_snapshots"] for item in summaries),
                "earliest_statement_date": min(dates).isoformat() if dates else None,
                "latest_statement_date": max(dates).isoformat() if dates else None,
                "section_counts": dict(sorted(section_counts.items())),
                "section_status_counts": dict(sorted(section_status_counts.items())),
                "subsection_status_counts": dict(sorted(subsection_status_counts.items())),
                "statement_field_rows": dict(sorted(statement_field_rows.items())),
                "statement_field_symbols": dict(sorted(statement_field_symbols.items())),
                "derived_growth_rows": dict(sorted(derived_growth_rows.items())),
                "derived_growth_symbols": dict(sorted(derived_growth_symbols.items())),
                "current_or_derived_field_symbols": dict(sorted(current_field_symbols.items())),
                "valuation_as_of_symbols": valuation_as_of_symbols,
                "sample": summaries[: max(0, sample_limit)],
            },
            "fundamentals_snapshots": {
                "existing_rows": snapshot_rows,
                "available": snapshot_rows is not None,
            },
            "strategy_universe_overlap": {
                "normalized_insider_purchase_symbols": len(insider_symbols),
                "cached_financial_symbols": len(cached_symbols),
                "cached_financial_symbols_in_insider_purchase_universe": len(cached_symbols & insider_symbols),
                "insider_purchase_symbols_missing_financial_cache": len(insider_symbols - cached_symbols),
            },
            "point_in_time_assessment": {
                "statement_metrics": "partially reconstructable from cached annual/quarterly statement rows",
                "valuation_metrics": "current cached valuation fields may exist, but are not safe as historical point-in-time values without dated price/ratio/estimate rows",
                "leverage_metrics": "current cached health fields may exist, but are not safe as historical point-in-time values without dated balance/ratio rows",
                "recommendation": "use cached statements for a limited dry-run backfill, then provider backfill remaining symbols in batches",
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only audit for historical fundamentals snapshot backfill readiness.")
    parser.add_argument("--sample-limit", type=int, default=12)
    args = parser.parse_args()
    print(json.dumps(audit_historical_fundamentals_backfill(sample_limit=args.sample_limit), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
