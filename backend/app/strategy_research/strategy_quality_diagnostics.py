from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Security, TickerMeta
from app.strategy_research.congress_buys import Lot, Signal, _normalize_universe


def pct(part: int | float, total: int | float) -> float:
    return round((float(part) / float(total) * 100.0), 4) if total else 0.0


def top_counts(counter: Counter[str], *, total: int, limit: int) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count, "pct": pct(count, total)}
        for key, count in counter.most_common(limit)
    ]


def signal_actor(signal: Signal) -> str:
    return signal.member_bioguide_id or signal.member_name or "unknown"


def month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def load_current_sector_map(db: Session, symbols: Iterable[str]) -> dict[str, str]:
    normalized = set(_normalize_universe(symbols))
    if not normalized:
        return {}

    sectors: dict[str, str] = {}
    for symbol, sector in db.execute(
        select(func.upper(TickerMeta.symbol), TickerMeta.sector).where(func.upper(TickerMeta.symbol).in_(normalized))
    ).all():
        if sector:
            sectors[str(symbol).upper()] = str(sector)

    missing = normalized - set(sectors)
    if missing:
        for symbol, sector in db.execute(
            select(func.upper(Security.symbol), Security.sector).where(func.upper(Security.symbol).in_(missing))
        ).all():
            if sector:
                sectors[str(symbol).upper()] = str(sector)
    return sectors


def concentration_flags(summary: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    total_lots = int(summary.get("lots") or 0)
    unique_symbols = int(summary.get("unique_symbols") or 0)
    unique_actors = int(summary.get("unique_actors") or 0)
    top_symbol_pct = (summary.get("top_symbols") or [{}])[0].get("pct", 0.0) if summary.get("top_symbols") else 0.0
    top_actor_pct = (summary.get("top_actors") or [{}])[0].get("pct", 0.0) if summary.get("top_actors") else 0.0
    top_month_pct = (summary.get("top_disclosure_months") or [{}])[0].get("pct", 0.0) if summary.get("top_disclosure_months") else 0.0
    top_sector_pct = (summary.get("top_sectors") or [{}])[0].get("pct", 0.0) if summary.get("top_sectors") else 0.0
    amount_missing_pct = float(summary.get("amount_missing_pct") or 0.0)

    if total_lots < 100:
        flags.append("sample_size_below_100_lots")
    if unique_symbols < 20:
        flags.append("symbol_breadth_below_20")
    if unique_actors < 20:
        flags.append("actor_breadth_below_20")
    if top_symbol_pct >= 25.0:
        flags.append("top_symbol_exceeds_25pct_of_lots")
    if top_actor_pct >= 25.0:
        flags.append("top_actor_exceeds_25pct_of_lots")
    if top_month_pct >= 25.0:
        flags.append("top_month_exceeds_25pct_of_lots")
    if top_sector_pct >= 40.0:
        flags.append("top_sector_exceeds_40pct_of_lots")
    if amount_missing_pct >= 25.0:
        flags.append("amount_missing_for_at_least_25pct_of_signals")
    return flags


def summarize_strategy_quality(
    *,
    primary_signals: list[Signal],
    confirmed_signals: list[Signal],
    lots: list[Lot],
    skipped: dict[str, int],
    sector_by_symbol: dict[str, str] | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    sector_by_symbol = sector_by_symbol or {}
    lot_signals = [lot.signal for lot in lots]
    lot_symbols = Counter(signal.symbol for signal in lot_signals)
    lot_actors = Counter(signal_actor(signal) for signal in lot_signals)
    lot_filings = Counter(signal.source_filing_id or f"event:{signal.event_id}" for signal in lot_signals)
    lot_months = Counter(month_key(signal.disclosure_date) for signal in lot_signals)
    lot_sectors = Counter(sector_by_symbol.get(signal.symbol, "unknown") for signal in lot_signals)
    confirmed_symbols = {signal.symbol for signal in confirmed_signals}
    confirmed_actors = {signal_actor(signal) for signal in confirmed_signals}
    amount_missing = sum(1 for signal in confirmed_signals if not signal.amount_max or signal.amount_max <= 0)

    returns_by_symbol: dict[str, list[float]] = defaultdict(list)
    returns_by_actor: dict[str, list[float]] = defaultdict(list)
    returns_by_sector: dict[str, list[float]] = defaultdict(list)
    for lot in lots:
        returns_by_symbol[lot.signal.symbol].append(float(lot.net_return))
        returns_by_actor[signal_actor(lot.signal)].append(float(lot.net_return))
        returns_by_sector[sector_by_symbol.get(lot.signal.symbol, "unknown")].append(float(lot.net_return))

    def top_returns(values: dict[str, list[float]]) -> list[dict[str, Any]]:
        rows = [
            {
                "key": key,
                "lots": len(returns),
                "avg_net_return_pct": round(sum(returns) / len(returns) * 100.0, 4),
                "sum_net_return_pct": round(sum(returns) * 100.0, 4),
            }
            for key, returns in values.items()
            if returns
        ]
        return sorted(rows, key=lambda row: (row["sum_net_return_pct"], row["lots"]), reverse=True)[:limit]

    summary = {
        "primary_signals": len(primary_signals),
        "confirmed_signals": len(confirmed_signals),
        "lots": len(lots),
        "unique_symbols": len(confirmed_symbols),
        "unique_actors": len(confirmed_actors),
        "unique_owners": len(confirmed_actors),
        "unique_filings_in_lots": len(lot_filings),
        "amount_missing_signals": amount_missing,
        "amount_missing_pct": pct(amount_missing, len(confirmed_signals)),
        "skipped_lots": dict(sorted(skipped.items())),
        "top_symbols": top_counts(lot_symbols, total=len(lots), limit=limit),
        "top_actors": top_counts(lot_actors, total=len(lots), limit=limit),
        "top_owners": top_counts(lot_actors, total=len(lots), limit=limit),
        "top_filings": top_counts(lot_filings, total=len(lots), limit=limit),
        "top_disclosure_months": top_counts(lot_months, total=len(lots), limit=limit),
        "top_sectors": top_counts(lot_sectors, total=len(lots), limit=limit),
        "top_symbols_by_net_return": top_returns(returns_by_symbol),
        "top_actors_by_net_return": top_returns(returns_by_actor),
        "top_owners_by_net_return": top_returns(returns_by_actor),
        "top_sectors_by_net_return": top_returns(returns_by_sector),
        "sector_metadata_basis": "current ticker_meta/security metadata; not point-in-time",
    }
    summary["concentration_flags"] = concentration_flags(summary)
    summary["data_quality_confidence"] = "low" if summary["concentration_flags"] else "medium"
    return summary
