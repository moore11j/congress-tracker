from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import func, select, tuple_

from app.db import SessionLocal
from app.models import FundamentalsSnapshot, TickerFinancialsCache
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "historical_fundamentals_statement_proxy_v1"
DEFAULT_PROVIDER = "fmp"
DEFAULT_QUARTERLY_AVAILABILITY_LAG_DAYS = 45


@dataclass(frozen=True)
class HistoricalFundamentalsCandidate:
    symbol: str
    provider: str
    snapshot_date: date
    period_date: date
    source_fetched_at: datetime
    revenue_growth: float | None
    eps_growth: float | None
    fcf_growth: float | None
    gross_margin: float | None
    operating_margin: float | None
    free_cash_flow: float | None
    eps_ttm: float | None
    source_payload_hash: str
    methodology_version: str = METHODOLOGY_VERSION


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


def _hash_payload(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _load_payload(row: TickerFinancialsCache) -> dict[str, Any]:
    try:
        payload = json.loads(row.payload_json)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _quarterly_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("quarterly")
    if not isinstance(rows, list):
        return []
    valid = [row for row in rows if isinstance(row, dict) and _parse_day(row.get("date")) is not None]
    return sorted(valid, key=lambda row: str(row.get("date")))


def _sum_latest_numeric(rows: list[dict[str, Any]], key: str, *, end_index: int, count: int = 4) -> float | None:
    window = rows[max(0, end_index - count + 1) : end_index + 1]
    values = [_number(row.get(key)) for row in window]
    if len(values) < count or any(value is None for value in values):
        return None
    return float(sum(value for value in values if value is not None))


def _candidate_from_quarter(
    *,
    symbol: str,
    provider: str,
    row: TickerFinancialsCache,
    quarter: dict[str, Any],
    quarterly_rows: list[dict[str, Any]],
    index: int,
    lag_days: int,
) -> HistoricalFundamentalsCandidate | None:
    period_date = _parse_day(quarter.get("date"))
    if period_date is None:
        return None
    previous_year = quarterly_rows[index - 4] if index >= 4 else {}
    revenue = _number(quarter.get("revenue"))
    eps = _number(quarter.get("eps"))
    free_cash_flow = _number(quarter.get("freeCashFlow"))
    revenue_growth = _growth_pct(revenue, _number(previous_year.get("revenue")))
    eps_growth = _growth_pct(eps, _number(previous_year.get("eps")))
    fcf_growth = _growth_pct(free_cash_flow, _number(previous_year.get("freeCashFlow")))
    gross_margin = _number(quarter.get("grossMargin"))
    operating_margin = _number(quarter.get("operatingMargin"))
    eps_ttm = _sum_latest_numeric(quarterly_rows, "eps", end_index=index, count=4)
    has_signal_fields = any(
        value is not None
        for value in (
            revenue_growth,
            eps_growth,
            fcf_growth,
            gross_margin,
            operating_margin,
            free_cash_flow,
            eps_ttm,
        )
    )
    if not has_signal_fields:
        return None
    source_payload = {
        "symbol": symbol,
        "provider": provider,
        "cache_fetched_at": row.fetched_at,
        "period_date": period_date,
        "quarter": quarter,
        "previous_year_quarter": previous_year,
        "lag_days": lag_days,
        "methodology_version": METHODOLOGY_VERSION,
    }
    return HistoricalFundamentalsCandidate(
        symbol=symbol,
        provider=provider,
        snapshot_date=period_date + timedelta(days=max(0, int(lag_days))),
        period_date=period_date,
        source_fetched_at=row.fetched_at,
        revenue_growth=revenue_growth,
        eps_growth=eps_growth,
        fcf_growth=fcf_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
        free_cash_flow=free_cash_flow,
        eps_ttm=eps_ttm,
        source_payload_hash=_hash_payload(source_payload),
    )


def candidates_from_financials_row(
    row: TickerFinancialsCache,
    *,
    lag_days: int = DEFAULT_QUARTERLY_AVAILABILITY_LAG_DAYS,
    provider: str = DEFAULT_PROVIDER,
) -> list[HistoricalFundamentalsCandidate]:
    symbol = normalize_symbol(row.symbol)
    if not symbol:
        return []
    payload = _load_payload(row)
    quarterly_rows = _quarterly_rows(payload)
    candidates = [
        candidate
        for index, quarter in enumerate(quarterly_rows)
        if (
            candidate := _candidate_from_quarter(
                symbol=symbol,
                provider=provider,
                row=row,
                quarter=quarter,
                quarterly_rows=quarterly_rows,
                index=index,
                lag_days=lag_days,
            )
        )
        is not None
    ]
    return candidates


def _normalize_symbols(symbols: Iterable[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return sorted({symbol for raw in symbols if (symbol := normalize_symbol(raw))})


def _existing_snapshot_keys(db, candidates: list[HistoricalFundamentalsCandidate]) -> set[tuple[str, str, date]]:
    keys = {(item.symbol, item.provider, item.snapshot_date) for item in candidates}
    if not keys:
        return set()
    rows = db.execute(
        select(FundamentalsSnapshot.symbol, FundamentalsSnapshot.provider, FundamentalsSnapshot.snapshot_date)
        .where(tuple_(FundamentalsSnapshot.symbol, FundamentalsSnapshot.provider, FundamentalsSnapshot.snapshot_date).in_(keys))
    ).all()
    return {(str(symbol), str(provider), snapshot_date) for symbol, provider, snapshot_date in rows}


def _candidate_json_size(candidate: HistoricalFundamentalsCandidate) -> int:
    return len(json.dumps(asdict(candidate), sort_keys=True, default=str, separators=(",", ":")).encode("utf-8"))


def dry_run_historical_fundamentals_snapshot_backfill(
    *,
    symbols: Iterable[str] | None = None,
    lag_days: int = DEFAULT_QUARTERLY_AVAILABILITY_LAG_DAYS,
    provider: str = DEFAULT_PROVIDER,
    as_of: date | None = None,
    sample_limit: int = 12,
) -> dict[str, Any]:
    normalized_symbols = _normalize_symbols(symbols)
    as_of_date = as_of or datetime.now(timezone.utc).date()
    with SessionLocal() as db:
        statement = select(TickerFinancialsCache).where(TickerFinancialsCache.status.in_(("ok", "partial"))).order_by(TickerFinancialsCache.symbol.asc())
        if normalized_symbols is not None:
            statement = statement.where(func.upper(TickerFinancialsCache.symbol).in_(normalized_symbols))
        rows = db.execute(statement).scalars().all()
        all_candidates = [
            candidate
            for row in rows
            for candidate in candidates_from_financials_row(row, lag_days=lag_days, provider=provider)
        ]
        future_candidates = [candidate for candidate in all_candidates if candidate.snapshot_date > as_of_date]
        candidates = [candidate for candidate in all_candidates if candidate.snapshot_date <= as_of_date]
        existing_keys = _existing_snapshot_keys(db, candidates)
        dates = [candidate.snapshot_date for candidate in candidates]
        period_dates = [candidate.period_date for candidate in candidates]
        symbol_counts: dict[str, int] = {}
        for candidate in candidates:
            symbol_counts[candidate.symbol] = symbol_counts.get(candidate.symbol, 0) + 1
        estimated_payload_bytes = sum(_candidate_json_size(candidate) for candidate in candidates)
        sample = [
            {
                **asdict(candidate),
                "source_fetched_at": candidate.source_fetched_at.isoformat(),
                "snapshot_date": candidate.snapshot_date.isoformat(),
                "period_date": candidate.period_date.isoformat(),
            }
            for candidate in candidates[: max(0, int(sample_limit))]
        ]
        return {
            "status": "ok",
            "mode": "dry_run",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology_version": METHODOLOGY_VERSION,
            "provider": provider,
            "as_of_date": as_of_date.isoformat(),
            "source": "ticker_financials_cache.quarterly_statement_rows",
            "availability_basis": f"statement period date plus {max(0, int(lag_days))} calendar days; proxy only",
            "warnings": [
                "No rows were written.",
                "Availability date is a conservative proxy because cached normalized financial payloads do not retain filing acceptance timestamps.",
                "Do not publish these as final historical fundamentals until filing/acceptance dates are backfilled or disclosed as proxy methodology.",
            ],
            "cache_rows_seen": len(rows),
            "raw_candidate_rows": len(all_candidates),
            "future_availability_candidate_rows_excluded": len(future_candidates),
            "candidate_rows": len(candidates),
            "candidate_symbols": len(symbol_counts),
            "existing_snapshot_key_conflicts": len(existing_keys),
            "new_snapshot_key_candidates": len(candidates) - len(existing_keys),
            "snapshot_date_range": {
                "start": min(dates).isoformat() if dates else None,
                "end": max(dates).isoformat() if dates else None,
            },
            "period_date_range": {
                "start": min(period_dates).isoformat() if period_dates else None,
                "end": max(period_dates).isoformat() if period_dates else None,
            },
            "estimated_candidate_json_bytes": estimated_payload_bytes,
            "estimated_candidate_json_megabytes": round(estimated_payload_bytes / 1_000_000, 4),
            "top_symbols_by_candidate_rows": [
                {"symbol": symbol, "candidate_rows": count}
                for symbol, count in sorted(symbol_counts.items(), key=lambda item: (-item[1], item[0]))[:20]
            ],
            "sample": sample,
        }


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None or not value.strip():
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run historical fundamentals snapshot backfill from cached financials only.")
    parser.add_argument("--symbols", help="Optional comma-separated symbols to inspect.")
    parser.add_argument("--lag-days", type=int, default=DEFAULT_QUARTERLY_AVAILABILITY_LAG_DAYS)
    parser.add_argument("--provider", default=DEFAULT_PROVIDER)
    parser.add_argument("--as-of-date", help="Exclude proxy snapshot dates after this YYYY-MM-DD date. Defaults to today UTC.")
    parser.add_argument("--sample-limit", type=int, default=12)
    args = parser.parse_args()
    print(
        json.dumps(
            dry_run_historical_fundamentals_snapshot_backfill(
                symbols=_parse_symbols(args.symbols),
                lag_days=args.lag_days,
                provider=args.provider,
                as_of=_parse_date(args.as_of_date),
                sample_limit=args.sample_limit,
            ),
            indent=2,
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
