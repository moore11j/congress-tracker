from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import InsiderTransaction, InsiderTransactionNormalized
from app.services.backtesting.queries import first_text, parse_payload
from app.services.trade_outcome_display import normalize_trade_side
from app.strategy_research.congress_buys import (
    DEFAULT_UNIVERSE,
    METHODOLOGY_VERSION as BASE_ENGINE_VERSION,
    ResearchConfig,
    Signal,
    build_lots,
    compute_metrics,
    load_adjusted_price_histories,
    parse_iso_date,
    simulate_active_lot_portfolio,
    _normalize_universe,
)
from app.services.ticker_meta import normalize_cik
from app.utils.symbols import normalize_symbol

MethodologyVersion = Literal["insider_buys_research_v1"]
InsiderRole = Literal["all", "ceo", "cfo", "director", "officer", "ten_percent_owner"]
SignalSource = Literal["normalized", "legacy"]

METHODOLOGY_VERSION: MethodologyVersion = "insider_buys_research_v1"


def _role_text(row: InsiderTransactionNormalized) -> str:
    return " ".join(
        str(value or "").strip().lower()
        for value in (
            row.officer_title,
            "director" if row.is_director else "",
            "officer" if row.is_officer else "",
            "ten percent owner" if row.is_ten_percent_owner else "",
        )
        if str(value or "").strip()
    )


def _legacy_role_text(row: InsiderTransaction) -> str:
    payload = parse_payload(row.payload_json)
    return " ".join(
        str(value or "").strip().lower()
        for value in (
            row.role,
            first_text(payload, "role", "officer_title", "officerTitle", "relationship"),
        )
        if str(value or "").strip()
    )


def _matches_role(row: InsiderTransactionNormalized, role: InsiderRole) -> bool:
    if role == "all":
        return True
    text = _role_text(row)
    if role == "ceo":
        return "chief executive" in text or "ceo" in text or "president & ceo" in text
    if role == "cfo":
        return "chief financial" in text or "cfo" in text
    if role == "director":
        return bool(row.is_director)
    if role == "officer":
        return bool(row.is_officer)
    if role == "ten_percent_owner":
        return bool(row.is_ten_percent_owner)
    return False


def _matches_legacy_role(row: InsiderTransaction, role: InsiderRole) -> bool:
    if role == "all":
        return True
    text = _legacy_role_text(row)
    if role == "ceo":
        return "chief executive" in text or "ceo" in text or "president & ceo" in text
    if role == "cfo":
        return "chief financial" in text or "cfo" in text
    if role == "director":
        return "director" in text
    if role == "officer":
        return any(term in text for term in ("officer", "chief", "ceo", "cfo", "president"))
    if role == "ten_percent_owner":
        return "10%" in text or "ten percent" in text or "10 percent" in text
    return False


def _amount_value(row: InsiderTransactionNormalized) -> int | None:
    if row.value is not None and row.value > 0:
        return int(round(float(row.value)))
    if row.shares is not None and row.price is not None and row.shares > 0 and row.price > 0:
        return int(round(float(row.shares) * float(row.price)))
    return None


def _legacy_amount_value(row: InsiderTransaction, payload: dict[str, Any]) -> int | None:
    for key in ("value", "transaction_value", "transactionValue", "amount", "estimated_value"):
        raw_value = payload.get(key)
        if isinstance(raw_value, (int, float)) and raw_value > 0:
            return int(round(float(raw_value)))
        if isinstance(raw_value, str):
            try:
                parsed = float(raw_value.replace(",", "").replace("$", ""))
            except ValueError:
                continue
            if parsed > 0:
                return int(round(parsed))
    if row.shares is not None and row.price is not None and row.shares > 0 and row.price > 0:
        return int(round(float(row.shares) * float(row.price)))
    return None


def _payload_date(payload: dict[str, Any], *keys: str) -> date | None:
    for key in keys:
        value = first_text(payload, key)
        if value:
            parsed = parse_iso_date(value)
            if parsed is not None:
                return parsed
    return None


def load_insider_open_market_purchase_signals(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    role: InsiderRole,
) -> list[Signal]:
    normalized_universe = set(_normalize_universe(universe))
    rows = (
        db.execute(
            select(InsiderTransactionNormalized)
            .where(InsiderTransactionNormalized.is_duplicate.is_(False))
            .where(InsiderTransactionNormalized.ticker_normalized.is_not(None))
            .where(InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase")
            .where(InsiderTransactionNormalized.filing_date >= start_date)
            .where(InsiderTransactionNormalized.filing_date <= end_date)
            .order_by(InsiderTransactionNormalized.filing_date.asc(), InsiderTransactionNormalized.id.asc())
        )
        .scalars()
        .all()
    )
    signals: list[Signal] = []
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        symbol = normalize_symbol(row.ticker_normalized or row.ticker_raw)
        filing_date = row.filing_date
        if not symbol or symbol not in normalized_universe or filing_date is None:
            continue
        if not _matches_role(row, role):
            continue
        cik = normalize_cik(row.reporting_owner_cik)
        amount = _amount_value(row)
        dedupe_key = (
            row.normalized_hash
            or row.accession_number
            or symbol,
            cik or row.reporting_owner_name,
            filing_date,
            row.transaction_date,
            row.shares,
            row.price,
            amount,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(
            Signal(
                event_id=int(row.id),
                symbol=symbol,
                disclosure_date=filing_date,
                raw_entry_date=filing_date + timedelta(days=1),
                amount_min=amount,
                amount_max=amount,
                member_name=row.reporting_owner_name,
                member_bioguide_id=cik,
                chamber=row.officer_title,
                party=None,
                source_filing_id=row.normalized_hash,
                source_document_url=None,
                dedupe_key=dedupe_key,
            )
        )
    return signals


def load_legacy_insider_purchase_signals(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    role: InsiderRole,
) -> list[Signal]:
    normalized_universe = set(_normalize_universe(universe))
    rows = (
        db.execute(
            select(InsiderTransaction)
            .where(InsiderTransaction.filing_date >= start_date)
            .where(InsiderTransaction.filing_date <= end_date)
            .order_by(InsiderTransaction.filing_date.asc(), InsiderTransaction.id.asc())
        )
        .scalars()
        .all()
    )
    signals: list[Signal] = []
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        payload = parse_payload(row.payload_json)
        raw_type = row.transaction_type or first_text(
            payload,
            "trade_type",
            "tradeType",
            "transaction_type",
            "transactionType",
            "transaction_code",
            "transactionCode",
        )
        side = normalize_trade_side(raw_type)
        if side != "purchase":
            continue
        symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker", "ticker_normalized", "tickerNormalized"))
        filing_date = row.filing_date or _payload_date(payload, "filing_date", "filingDate", "report_date", "reportDate")
        transaction_date = row.transaction_date or _payload_date(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
        if not symbol or symbol not in normalized_universe or filing_date is None:
            continue
        if filing_date > end_date or (transaction_date is not None and transaction_date > end_date):
            continue
        if not _matches_legacy_role(row, role):
            continue
        cik = normalize_cik(row.reporting_cik) or normalize_cik(
            first_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
        )
        amount = _legacy_amount_value(row, payload)
        dedupe_key = (
            row.external_id or row.id,
            symbol,
            cik or row.insider_name,
            filing_date,
            transaction_date,
            row.shares,
            row.price,
            amount,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(
            Signal(
                event_id=int(row.id),
                symbol=symbol,
                disclosure_date=filing_date,
                raw_entry_date=filing_date + timedelta(days=1),
                amount_min=amount,
                amount_max=amount,
                member_name=row.insider_name or first_text(payload, "insider_name", "insiderName", "reporting_owner_name", "reportingOwnerName"),
                member_bioguide_id=cik,
                chamber=row.role or first_text(payload, "role", "officer_title", "officerTitle"),
                party=None,
                source_filing_id=row.external_id,
                source_document_url=None,
                dedupe_key=dedupe_key,
            )
        )
    return signals


def run_research(db: Session, config: ResearchConfig, *, role: InsiderRole, source: SignalSource = "normalized") -> dict[str, object]:
    price_start = config.start_date or date(1990, 1, 1)
    price_maps = load_adjusted_price_histories(
        db,
        (*config.universe, config.benchmark),
        start_date=price_start,
        end_date=config.end_date,
        require_adjusted=config.require_adjusted,
    )
    benchmark_prices = price_maps.get(config.benchmark, {})
    benchmark_dates = sorted(benchmark_prices)
    if not benchmark_dates:
        raise RuntimeError(f"Missing benchmark prices for {config.benchmark}.")

    universe_price_starts = [
        min(prices)
        for symbol, prices in price_maps.items()
        if symbol != config.benchmark and prices
    ]
    first_price_day = min(universe_price_starts) if universe_price_starts else (config.start_date or config.end_date)
    signal_start = config.start_date or first_price_day
    if source == "legacy":
        signals = load_legacy_insider_purchase_signals(
            db,
            universe=config.universe,
            start_date=signal_start,
            end_date=config.end_date,
            role=role,
        )
    else:
        signals = load_insider_open_market_purchase_signals(
            db,
            universe=config.universe,
            start_date=signal_start,
            end_date=config.end_date,
            role=role,
        )
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    runs: list[dict[str, object]] = []
    for hold_days in config.hold_days:
        lots, skipped = build_lots(
            signals,
            universe_price_maps,
            benchmark_dates=benchmark_dates,
            hold_days=hold_days,
            rebalance_frequency=config.rebalance_frequency,
            per_side_cost_rate=per_side_cost_rate,
        )
        simulation = simulate_active_lot_portfolio(
            lots,
            universe_price_maps,
            benchmark_prices,
            weighting=config.weighting,
            per_side_cost_rate=per_side_cost_rate,
        )
        metrics = compute_metrics(lots=lots, simulation=simulation, hold_days=hold_days, skipped=skipped)
        if metrics.get("status") == "ok" and metrics.get("lots", 0) < config.min_lots:
            metrics["status"] = "insufficient_lots"
        runs.append(metrics)

    role_counts = (
        _legacy_role_counts(db, universe=config.universe, start_date=signal_start, end_date=config.end_date)
        if source == "legacy"
        else _role_counts(db, universe=config.universe, start_date=signal_start, end_date=config.end_date)
    )
    return {
        "metadata": {
            "strategy_name": config.strategy_name,
            "plain_english_rule": (
                "Buy every eligible open-market insider purchase in the selected universe on the next trading day "
                "after the SEC Form 4 filing date; hold for the configured fixed period."
            ),
            "methodology_version": METHODOLOGY_VERSION,
            "base_engine_version": BASE_ENGINE_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "universe": list(config.universe),
            "benchmark": config.benchmark,
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "role": role,
            "source": source,
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after SEC Form 4 filing date",
            "fees_bps_per_side": config.fee_bps,
            "slippage_bps_per_side": config.slippage_bps,
            "require_adjusted_prices": config.require_adjusted,
            "price_source": "price_cache.adjusted_close",
            "signal_source": (
                "legacy insider_transactions purchase rows; lower confidence until normalized Form 4 repair"
                if source == "legacy"
                else "insider_transactions_normalized open_market_purchase rows"
            ),
            "data_quality_confidence": "lower" if source == "legacy" else "high",
            "data_state": "production PostgreSQL read-only research query",
        },
        "signal_count": len(signals),
        "role_signal_counts": role_counts,
        "price_coverage": {
            symbol: {
                "rows": len(prices),
                "start": min(prices).isoformat() if prices else None,
                "end": max(prices).isoformat() if prices else None,
            }
            for symbol, prices in sorted(price_maps.items())
        },
        "runs": runs,
    }


def _role_counts(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    normalized_universe = list(_normalize_universe(universe))
    rows = (
        db.execute(
            select(InsiderTransactionNormalized)
            .where(InsiderTransactionNormalized.is_duplicate.is_(False))
            .where(func.upper(InsiderTransactionNormalized.ticker_normalized).in_(normalized_universe))
            .where(InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase")
            .where(InsiderTransactionNormalized.filing_date >= start_date)
            .where(InsiderTransactionNormalized.filing_date <= end_date)
        )
        .scalars()
        .all()
    )
    roles: tuple[InsiderRole, ...] = ("all", "ceo", "cfo", "director", "officer", "ten_percent_owner")
    return {role: sum(1 for row in rows if _matches_role(row, role)) for role in roles}


def _legacy_role_counts(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    normalized_universe = set(_normalize_universe(universe))
    rows = (
        db.execute(
            select(InsiderTransaction)
            .where(InsiderTransaction.filing_date >= start_date)
            .where(InsiderTransaction.filing_date <= end_date)
        )
        .scalars()
        .all()
    )
    filtered = []
    for row in rows:
        payload = parse_payload(row.payload_json)
        side = normalize_trade_side(row.transaction_type or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType"))
        symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker", "ticker_normalized", "tickerNormalized"))
        transaction_date = row.transaction_date or _payload_date(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
        if side == "purchase" and symbol in normalized_universe and (transaction_date is None or transaction_date <= end_date):
            filtered.append(row)
    roles: tuple[InsiderRole, ...] = ("all", "ceo", "cfo", "director", "officer", "ten_percent_owner")
    return {role: sum(1 for row in filtered if _matches_legacy_role(row, role)) for role in roles}


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_UNIVERSE
    return _normalize_universe(part.strip() for part in value.split(","))


def _parse_holds(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _print_text_report(result: dict[str, object]) -> None:
    meta = result["metadata"]
    assert isinstance(meta, dict)
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"signals={result['signal_count']} role={meta['role']} universe={len(meta['universe'])} "
        f"weighting={meta['weighting']} rebalance={meta['rebalance_frequency']} "
        f"cost_bps_per_side={meta['fees_bps_per_side'] + meta['slippage_bps_per_side']}"
    )
    print(f"ROLE_COUNTS {json.dumps(result['role_signal_counts'], sort_keys=True)}")
    for row in result["runs"]:
        assert isinstance(row, dict)
        if row.get("status") != "ok":
            print(
                f"H{row.get('hold_days')} status={row.get('status')} "
                f"lots={row.get('lots')} skipped={row.get('skipped')}"
            )
            continue
        print(
            "H{hold_days} status={status} lots={lots} start={start_date} end={end_date} "
            "total={total_return_pct}% cagr={cagr_pct}% spy_cagr={benchmark_cagr_pct}% "
            "alpha={alpha_cagr_pct}% max_dd={max_drawdown_pct}% sharpe={sharpe} "
            "sortino={sortino} beta={beta} vol={annualized_volatility_pct}% "
            "win={win_rate_pct}% avg_active={avg_active_lots} trades={trade_count} "
            "roll12m_beat_spy={rolling_12m_beating_spy_pct}%"
            .format(**row)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only insider open-market buys strategy research runner.")
    parser.add_argument("--symbols", help="Comma-separated universe. Defaults to the approved 24-symbol research universe.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--hold-days", default="30,90,180,365")
    parser.add_argument("--role", choices=("all", "ceo", "cfo", "director", "officer", "ten_percent_owner"), default="all")
    parser.add_argument("--source", choices=("normalized", "legacy"), default="normalized")
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    role = args.role
    label = "Insider Open-Market Buys" if role == "all" else f"Insider Open-Market Buys - {role.replace('_', ' ').title()}"
    config = ResearchConfig(
        strategy_name=label,
        universe=_parse_symbols(args.symbols),
        benchmark=normalize_symbol(args.benchmark) or "SPY",
        start_date=parse_iso_date(args.start_date) if args.start_date else None,
        end_date=parse_iso_date(args.end_date) or date.today(),
        hold_days=_parse_holds(args.hold_days),
        weighting=args.weighting,
        rebalance_frequency=args.rebalance_frequency,
        slippage_bps=float(args.slippage_bps),
        fee_bps=float(args.fee_bps),
        require_adjusted=not args.allow_raw_prices,
        min_lots=int(args.min_lots),
    )
    with SessionLocal() as db:
        result = run_research(db, config, role=role, source=args.source)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
