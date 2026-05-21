from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    Event,
    ReplicatedPortfolioPoint,
    ReplicatedPortfolioPosition,
    ReplicatedPortfolioRun,
)
from app.services.backtesting.metrics import (
    compute_cagr_pct,
    compute_max_drawdown_pct,
    compute_sharpe_ratio,
    compute_volatility_pct_from_daily_returns,
    compute_win_rate_pct,
)
from app.services.backtesting.queries import (
    first_price_on_or_after,
    first_text,
    load_price_histories,
    parse_iso_date,
    parse_payload,
    price_on_or_before,
    sorted_price_dates,
)
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.ticker_meta import normalize_cik
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import classify_symbol, normalize_symbol

PORTFOLIO_METHODOLOGY_VERSION = "replicated_portfolio_v1"
DEFAULT_STARTING_VALUE = 100000.0
SUPPORTED_MODES = {"realistic_disclosure_lag", "theoretical_transaction_date"}
SUPPORTED_ENTITY_TYPES = {"congress_member", "insider"}
_REIT_TERMS = ("reit", "real estate investment trust")
_OPTION_TERMS = ("option", "stock option", "call option", "put option", "derivative")
_CORPORATE_BOND_TERMS = ("corporate bond", "corp bond", "debenture")
_MUNICIPAL_BOND_TERMS = ("municipal bond", "muni bond", "municipal")
_PRIVATE_FUND_TERMS = ("private fund", "private equity", "hedge fund", "limited partnership")


@dataclass(frozen=True)
class PortfolioTradeEvent:
    event_id: int | None
    entity_type: str
    entity_id: str
    symbol: str
    side: str
    transaction_date: date
    public_date: date
    amount_min: int | None = None
    amount_max: int | None = None
    issuer_cik: str | None = None
    issuer_symbol: str | None = None


@dataclass(frozen=True)
class PortfolioSkip:
    event_id: int | None
    symbol: str | None
    side: str | None
    reason: str
    detail: str | None = None


@dataclass
class PortfolioPositionState:
    event_id: int | None
    symbol: str
    side: str
    entry_date: date
    entry_price: float
    shares: float = 0.0
    amount_min: int | None = None
    amount_max: int | None = None
    exit_date: date | None = None
    exit_price: float | None = None
    status: str = "open"


@dataclass(frozen=True)
class PortfolioPoint:
    asof_date: date
    strategy_value: float
    benchmark_value: float | None
    strategy_return_pct: float
    benchmark_return_pct: float | None
    alpha_pct: float | None
    daily_return_pct: float
    active_positions: int
    exposure_pct: float
    cash_pct: float


@dataclass(frozen=True)
class PortfolioSummary:
    starting_value: float
    ending_value: float
    benchmark_ending_value: float | None
    total_return_pct: float
    benchmark_return_pct: float | None
    alpha_pct: float | None
    cagr_pct: float
    max_drawdown_pct: float
    volatility_pct: float
    sharpe_ratio: float | None
    win_rate_pct: float
    average_exposure_pct: float
    ending_cash_pct: float
    points_count: int
    positions_count: int
    skipped_events_count: int


@dataclass(frozen=True)
class PortfolioCoverage:
    requested_start_date: date
    requested_end_date: date
    actual_start_date: date | None
    actual_end_date: date | None
    calendar_points: int
    calendar_source: str
    benchmark_symbol: str
    benchmark_points_loaded: int
    benchmark_first_date: date | None
    benchmark_last_date: date | None
    symbols_loaded: int
    symbol_points_loaded: dict[str, int]
    symbol_first_dates: dict[str, str]
    symbol_last_dates: dict[str, str]
    limitations: list[str]


@dataclass(frozen=True)
class PortfolioSimulation:
    summary: PortfolioSummary
    points: list[PortfolioPoint]
    positions: list[PortfolioPositionState]
    skipped: list[PortfolioSkip]
    coverage: PortfolioCoverage


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return float(round(float(value), digits))


def _as_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _flatten_payload_text(payload: Any) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []

    def walk(value: Any, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                child_path = f"{key}.{child_key}" if key else str(child_key)
                walk(child_value, child_path)
        elif isinstance(value, list):
            for index, child_value in enumerate(value):
                child_path = f"{key}.{index}" if key else str(index)
                walk(child_value, child_path)
        elif value is not None:
            text = str(value).strip()
            if text:
                values.append((key, text))

    walk(payload)
    return values


def _key_token(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _first_nested_text(payload: dict[str, Any], *keys: str) -> str | None:
    wanted = [_key_token(key) for key in keys]
    for key, value in _flatten_payload_text(payload):
        token = _key_token(key)
        if any(token == item or token.endswith(item) or item in token for item in wanted):
            return value
    return None


def _nested_text_fields(payload: dict[str, Any], *keys: str) -> dict[str, str]:
    wanted = [_key_token(key) for key in keys]
    fields: dict[str, str] = {}
    for key, value in _flatten_payload_text(payload):
        token = _key_token(key)
        if any(token == item or token.endswith(item) or item in token for item in wanted):
            fields.setdefault(key, value)
    return fields


def _event_transaction_date(event: Event, payload: dict[str, Any]) -> date | None:
    return (
        parse_iso_date(first_text(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate"))
        or (event.event_date.date() if event.event_date is not None else None)
        or event.ts.date()
    )


def _event_public_date(event: Event, payload: dict[str, Any]) -> date | None:
    return (
        parse_iso_date(
            first_text(
                payload,
                "filing_date",
                "filingDate",
                "report_date",
                "reportDate",
                "disclosure_date",
                "disclosureDate",
            )
        )
        or (event.event_date.date() if event.event_date is not None else None)
        or event.ts.date()
    )


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _event_issuer_cik(payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_cik(
        first_text(payload, "company_cik", "companyCik", "issuer_cik", "issuerCik")
        or first_text(raw, "company_cik", "companyCik", "companyCIK", "issuer_cik", "issuerCik")
    )


def _event_reporting_cik(payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_cik(
        first_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
        or first_text(raw, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
    )


def _is_market_insider_trade(payload: dict[str, Any], side: str | None) -> bool:
    is_market_trade = payload.get("is_market_trade")
    if is_market_trade is False:
        return False
    return side in {"purchase", "sale"}


def _normalize_insider_side(event: Event, payload: dict[str, Any]) -> str | None:
    raw_side = (
        event.trade_type
        or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        or _first_nested_text(
            payload,
            "trade_type",
            "tradeType",
            "transaction_type",
            "transactionType",
            "transactionTypeCode",
            "transaction_type_code",
            "transactionCode",
            "transaction_code",
            "transactionCodingCode",
            "transactionCoding.code",
        )
    )
    side = normalize_trade_side(raw_side)
    if side in {"purchase", "sale"}:
        return side

    acquired_disposed = _insider_acquisition_disposition_code(payload)
    normalized_ad = (acquired_disposed or "").strip().lower()
    if normalized_ad in {"a", "acquired", "acquisition"}:
        return "purchase"
    if normalized_ad in {"d", "disposed", "disposition"}:
        return "sale"
    return None


def _insider_transaction_code(payload: dict[str, Any]) -> str | None:
    return _first_nested_text(
        payload,
        "transactionCode",
        "transaction_code",
        "transactionTypeCode",
        "transaction_type_code",
        "transactionCodingCode",
        "transactionCoding.code",
    )


def _insider_acquisition_disposition_code(payload: dict[str, Any]) -> str | None:
    return _first_nested_text(
        payload,
        "transactionAcquiredDisposedCode",
        "transaction_acquired_disposed_code",
        "acquiredDisposedCode",
        "acquisitionDispositionCode",
        "acquisition_or_disposition",
        "acquiredDisposed",
        "acquired_disposed",
    )


def inspect_replicated_portfolio_event(event: Event, *, entity_type: str, entity_id: str) -> dict[str, Any]:
    payload = parse_payload(event.payload_json)
    portfolio_event, skip = _portfolio_event_from_event(event, entity_type=entity_type, entity_id=entity_id)
    raw_side_fields = _nested_text_fields(
        payload,
        "trade_type",
        "tradeType",
        "transaction_type",
        "transactionType",
        "transactionTypeCode",
        "transaction_type_code",
        "transactionCode",
        "transaction_code",
        "transactionAcquiredDisposedCode",
        "transaction_acquired_disposed_code",
        "acquiredDisposedCode",
        "acquisitionDispositionCode",
        "acquisition_or_disposition",
        "acquiredDisposed",
        "acquired_disposed",
    )
    transaction_amount_fields = _nested_text_fields(
        payload,
        "shares",
        "securitiesTransacted",
        "transactionShares",
        "transactionAmount",
        "amount",
        "amount_min",
        "amountMax",
        "amount_max",
        "transactionValue",
        "value",
    )
    return {
        "event_id": event.id,
        "reporting_cik": _event_reporting_cik(payload),
        "issuer_cik": _event_issuer_cik(payload),
        "issuer_symbol": normalize_symbol(first_text(payload, "issuer_symbol", "issuerSymbol")),
        "symbol": normalize_symbol(event.symbol or first_text(payload, "symbol", "ticker")),
        "event_date": event.event_date.date().isoformat() if event.event_date is not None else None,
        "filed_at": _event_public_date(event, payload).isoformat() if _event_public_date(event, payload) else None,
        "transaction_date": _event_transaction_date(event, payload).isoformat() if _event_transaction_date(event, payload) else None,
        "raw_side_fields": raw_side_fields,
        "normalized_side": portfolio_event.side if portfolio_event is not None else _normalize_insider_side(event, payload),
        "transaction_code": _insider_transaction_code(payload),
        "acquisition_disposition_code": _insider_acquisition_disposition_code(payload),
        "transaction_amount_fields": transaction_amount_fields,
        "skip_reason": skip.reason if skip else None,
    }


def _asset_text(payload: dict[str, Any]) -> str:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    parts = [
        payload.get("asset_class"),
        payload.get("assetClass"),
        payload.get("instrument_type"),
        payload.get("instrumentType"),
        payload.get("security_description"),
        payload.get("securityDescription"),
        payload.get("security_name"),
        payload.get("securityName"),
        payload.get("description"),
        raw.get("asset_class"),
        raw.get("assetClass"),
        raw.get("securityTitle"),
        raw.get("securityName"),
        raw.get("transactionSecurityTitle"),
    ]
    return " ".join(str(part).strip().lower() for part in parts if part is not None and str(part).strip())


def _portfolio_asset_skip_reason(payload: dict[str, Any]) -> str | None:
    text = _asset_text(payload)
    if not text:
        return None
    if any(term in text for term in _OPTION_TERMS):
        return "options"
    if any(term in text for term in _MUNICIPAL_BOND_TERMS):
        return "municipal_bond"
    if any(term in text for term in _CORPORATE_BOND_TERMS):
        return "corporate_bond"
    if " bond" in f" {text}" or "bonds" in text:
        return "corporate_bond"
    if any(term in text for term in _PRIVATE_FUND_TERMS):
        return "private_fund"
    return None


def _is_public_reit(payload: dict[str, Any]) -> bool:
    text = _asset_text(payload)
    return any(term in text for term in _REIT_TERMS)


def normalize_skip_reason(skip: PortfolioSkip) -> str:
    reason = (skip.reason or "unknown").strip().lower()
    detail = (skip.detail or "").strip().lower()
    combined = f"{reason} {detail}"
    if reason in {"no_symbol", "invalid_symbol"}:
        return "no_symbol" if "missing" in combined or reason == "no_symbol" else "invalid_symbol"
    if reason in {"missing_price_history", "no_execution_price", "missing_trading_calendar"}:
        return "missing_price"
    if reason in {"unsupported_side", "missing_transaction_code_or_side"}:
        return reason
    if reason == "future_transaction_date":
        return "future_transaction_date"
    if "option" in combined:
        return "options"
    if "municipal" in combined:
        return "municipal_bond"
    if "corporate bond" in combined or "bond" in combined or "cusip" in combined:
        return "corporate_bond"
    if "private fund" in combined or "private equity" in combined:
        return "private_fund"
    if reason in {"not_equity_outcome_eligible", "non_equity_or_unpriced_asset"}:
        return "unsupported_asset_class"
    return reason


def skip_reason_summary(skips: list[PortfolioSkip]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for skip in skips:
        key = normalize_skip_reason(skip)
        summary[key] = summary.get(key, 0) + 1
    return dict(sorted(summary.items()))


def _portfolio_event_from_event(event: Event, *, entity_type: str, entity_id: str) -> tuple[PortfolioTradeEvent | None, PortfolioSkip | None]:
    payload = parse_payload(event.payload_json)
    raw_symbol = event.symbol or first_text(payload, "symbol", "ticker")
    status, symbol, symbol_error = classify_symbol(raw_symbol)
    if entity_type == "insider":
        side = _normalize_insider_side(event, payload)
        raw_side = side
    else:
        raw_side = event.trade_type or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        side = normalize_trade_side(raw_side)

    if side not in {"purchase", "sale"}:
        reason = "missing_transaction_code_or_side" if entity_type == "insider" else "unsupported_side"
        return None, PortfolioSkip(event.id, symbol, side, reason, raw_side)
    if status != "eligible" or not symbol:
        return None, PortfolioSkip(event.id, symbol, side, status, symbol_error)

    transaction_date = _event_transaction_date(event, payload)
    public_date = _event_public_date(event, payload)
    if transaction_date is None:
        return None, PortfolioSkip(event.id, symbol, side, "missing_transaction_date")
    if public_date is None:
        return None, PortfolioSkip(event.id, symbol, side, "missing_public_date")
    today = _today_utc()
    if transaction_date > today or (event.event_date is not None and event.event_date.date() > today):
        return None, PortfolioSkip(event.id, symbol, side, "future_transaction_date")
    if entity_type == "insider" and transaction_date > public_date + timedelta(days=7):
        return None, PortfolioSkip(
            event.id,
            symbol,
            side,
            "future_transaction_date",
            f"transaction_date={transaction_date.isoformat()} public_date={public_date.isoformat()}",
        )

    if entity_type == "congress_member":
        portfolio_asset_skip = _portfolio_asset_skip_reason(payload)
        if portfolio_asset_skip:
            return None, PortfolioSkip(event.id, symbol, side, portfolio_asset_skip)
        eligibility = congress_equity_outcome_eligibility(
            event_type="congress_trade",
            symbol=symbol,
            payload=payload,
            trade_date=transaction_date.isoformat(),
            side=side,
            amount_min=event.amount_min if event.amount_min is not None else 1,
            amount_max=event.amount_max if event.amount_max is not None else 1,
        )
        if not eligibility.eligible:
            if _is_public_reit(payload) and eligibility.skip_reason == "not_equity_outcome_eligible":
                pass
            else:
                skip_reason = (
                    "unsupported_asset_class"
                    if eligibility.skip_reason == "not_equity_outcome_eligible"
                    else eligibility.skip_reason or "not_eligible"
                )
                return None, PortfolioSkip(event.id, eligibility.symbol or symbol, side, skip_reason, eligibility.detail)

    if entity_type == "insider" and not _is_market_insider_trade(payload, side):
        return None, PortfolioSkip(event.id, symbol, side, "insider_non_market")

    issuer_symbol = normalize_symbol(first_text(payload, "issuer_symbol", "issuerSymbol") or symbol)
    return (
        PortfolioTradeEvent(
            event_id=event.id,
            entity_type=entity_type,
            entity_id=entity_id,
            symbol=symbol,
            side=side,
            transaction_date=transaction_date,
            public_date=public_date,
            amount_min=_as_int(event.amount_min if event.amount_min is not None else payload.get("amount_min") or payload.get("amountMin")),
            amount_max=_as_int(event.amount_max if event.amount_max is not None else payload.get("amount_max") or payload.get("amountMax")),
            issuer_cik=_event_issuer_cik(payload),
            issuer_symbol=issuer_symbol,
        ),
        None,
    )


def _coverage_from_inputs(
    *,
    benchmark_symbol: str,
    benchmark_history: dict[str, float],
    price_histories: dict[str, dict[str, float]],
    start_date: date,
    end_date: date,
    calendar: list[str],
    calendar_source: str,
) -> PortfolioCoverage:
    benchmark_dates = sorted_price_dates(benchmark_history)
    symbol_dates = {symbol: sorted_price_dates(history) for symbol, history in price_histories.items()}
    limitations: list[str] = []
    if not benchmark_dates:
        limitations.append(f"No cached benchmark history loaded for {benchmark_symbol}.")
    elif benchmark_dates[0] > start_date.isoformat():
        limitations.append(f"Benchmark coverage starts at {benchmark_dates[0]}, after requested start {start_date.isoformat()}.")
    if benchmark_dates and benchmark_dates[-1] < end_date.isoformat():
        limitations.append(f"Benchmark coverage ends at {benchmark_dates[-1]}, before requested end {end_date.isoformat()}.")
    if not calendar:
        limitations.append("No trading calendar could be built from cached benchmark or symbol prices.")
    elif calendar[0] > start_date.isoformat():
        limitations.append(f"Curve starts at {calendar[0]}, after requested start {start_date.isoformat()}.")
    if calendar and calendar[-1] < end_date.isoformat():
        limitations.append(f"Curve ends at {calendar[-1]}, before requested end {end_date.isoformat()}.")
    for symbol, dates in symbol_dates.items():
        if not dates:
            limitations.append(f"No cached price history loaded for {symbol}.")
        elif dates[0] > start_date.isoformat():
            limitations.append(f"{symbol} price coverage starts at {dates[0]}, after requested start {start_date.isoformat()}.")
        if dates and dates[-1] < end_date.isoformat():
            limitations.append(f"{symbol} price coverage ends at {dates[-1]}, before requested end {end_date.isoformat()}.")
    return PortfolioCoverage(
        requested_start_date=start_date,
        requested_end_date=end_date,
        actual_start_date=date.fromisoformat(calendar[0]) if calendar else None,
        actual_end_date=date.fromisoformat(calendar[-1]) if calendar else None,
        calendar_points=len(calendar),
        calendar_source=calendar_source,
        benchmark_symbol=benchmark_symbol,
        benchmark_points_loaded=len(benchmark_dates),
        benchmark_first_date=date.fromisoformat(benchmark_dates[0]) if benchmark_dates else None,
        benchmark_last_date=date.fromisoformat(benchmark_dates[-1]) if benchmark_dates else None,
        symbols_loaded=len(price_histories),
        symbol_points_loaded={symbol: len(dates) for symbol, dates in sorted(symbol_dates.items())},
        symbol_first_dates={symbol: dates[0] for symbol, dates in sorted(symbol_dates.items()) if dates},
        symbol_last_dates={symbol: dates[-1] for symbol, dates in sorted(symbol_dates.items()) if dates},
        limitations=limitations,
    )


def event_effective_date(event: PortfolioTradeEvent, mode: str) -> date:
    if mode == "theoretical_transaction_date":
        return event.transaction_date
    return event.public_date


def _trading_calendar(
    *,
    benchmark_history: dict[str, float],
    price_histories: dict[str, dict[str, float]],
    start_date: date,
    end_date: date,
) -> list[str]:
    start_key = start_date.isoformat()
    end_key = end_date.isoformat()
    benchmark_dates = [day for day in sorted_price_dates(benchmark_history) if start_key <= day <= end_key]
    if benchmark_dates:
        return benchmark_dates
    return sorted(
        {
            day
            for history in price_histories.values()
            for day in sorted_price_dates(history)
            if start_key <= day <= end_key
        }
    )


def _position_value(position: PortfolioPositionState, day: str, price_histories: dict[str, dict[str, float]], sorted_dates: dict[str, list[str]]) -> float:
    close = price_on_or_before(day, price_histories.get(position.symbol, {}), sorted_dates.get(position.symbol, []))
    if close is None or close <= 0:
        return 0.0
    return float(position.shares * close)


def _snapshot(
    *,
    cash: float,
    open_positions: list[PortfolioPositionState],
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
) -> tuple[float, float]:
    invested = sum(_position_value(position, day, price_histories, sorted_dates) for position in open_positions)
    return float(cash + invested), float(invested)


def _rebalance_equal_weight(
    *,
    cash: float,
    open_positions: list[PortfolioPositionState],
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
) -> float:
    if not open_positions:
        return cash
    total_value, _ = _snapshot(
        cash=cash,
        open_positions=open_positions,
        day=day,
        price_histories=price_histories,
        sorted_dates=sorted_dates,
    )
    target_value = total_value / len(open_positions)
    next_cash = total_value
    for position in open_positions:
        close = price_on_or_before(day, price_histories.get(position.symbol, {}), sorted_dates.get(position.symbol, []))
        if close is None or close <= 0:
            continue
        position.shares = target_value / float(close)
        next_cash -= target_value
    return 0.0 if abs(next_cash) < 0.000001 else float(next_cash)


def simulate_replicated_portfolio(
    *,
    events: list[PortfolioTradeEvent],
    price_histories: dict[str, dict[str, float]],
    benchmark_history: dict[str, float],
    start_date: date,
    end_date: date,
    mode: str,
    benchmark_symbol: str = "^GSPC",
    starting_value: float = DEFAULT_STARTING_VALUE,
) -> PortfolioSimulation:
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"Unsupported portfolio mode: {mode}")

    calendar = _trading_calendar(
        benchmark_history=benchmark_history,
        price_histories=price_histories,
        start_date=start_date,
        end_date=end_date,
    )
    calendar_source = "benchmark" if [day for day in sorted_price_dates(benchmark_history) if start_date.isoformat() <= day <= end_date.isoformat()] else "symbol_prices"
    coverage = _coverage_from_inputs(
        benchmark_symbol=benchmark_symbol,
        benchmark_history=benchmark_history,
        price_histories=price_histories,
        start_date=start_date,
        end_date=end_date,
        calendar=calendar,
        calendar_source=calendar_source,
    )
    if not calendar:
        skipped = [PortfolioSkip(None, None, None, "missing_trading_calendar")]
        summary = PortfolioSummary(
            starting_value=starting_value,
            ending_value=starting_value,
            benchmark_ending_value=None,
            total_return_pct=0.0,
            benchmark_return_pct=None,
            alpha_pct=None,
            cagr_pct=0.0,
            max_drawdown_pct=0.0,
            volatility_pct=0.0,
            sharpe_ratio=None,
            win_rate_pct=0.0,
            average_exposure_pct=0.0,
            ending_cash_pct=100.0,
            points_count=0,
            positions_count=0,
            skipped_events_count=1,
        )
        return PortfolioSimulation(summary=summary, points=[], positions=[], skipped=skipped, coverage=coverage)

    sorted_symbol_dates = {symbol: sorted_price_dates(history) for symbol, history in price_histories.items()}
    skipped: list[PortfolioSkip] = []
    events_by_day: dict[str, list[PortfolioTradeEvent]] = {}
    for event in sorted(events, key=lambda item: (event_effective_date(item, mode), item.event_id or 0)):
        history = price_histories.get(event.symbol, {})
        if not history:
            skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "missing_price_history"))
            continue
        resolved = first_price_on_or_after(event_effective_date(event, mode), history)
        if resolved is None or resolved.close <= 0:
            skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
            continue
        if resolved.date < start_date or resolved.date > end_date:
            continue
        events_by_day.setdefault(resolved.date.isoformat(), []).append(event)

    cash = float(starting_value)
    positions: list[PortfolioPositionState] = []
    points: list[PortfolioPoint] = []
    previous_value = float(starting_value)

    benchmark_base = first_price_on_or_after(start_date, benchmark_history)

    for day in calendar:
        day_events = events_by_day.get(day, [])
        open_positions = [position for position in positions if position.status == "open"]

        for event in [item for item in day_events if item.side == "sale"]:
            matching = [position for position in open_positions if position.symbol == event.symbol and position.status == "open"]
            if not matching:
                skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "unmatched_sell"))
                continue
            position = sorted(matching, key=lambda item: (item.entry_date, item.event_id or 0))[0]
            close = price_on_or_before(day, price_histories.get(position.symbol, {}), sorted_symbol_dates.get(position.symbol, []))
            if close is None or close <= 0:
                skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
                continue
            cash += position.shares * float(close)
            position.exit_date = date.fromisoformat(day)
            position.exit_price = float(close)
            position.status = "closed"
            open_positions = [item for item in open_positions if item.status == "open"]

        buy_events = [item for item in day_events if item.side == "purchase"]
        for event in buy_events:
            close = price_on_or_before(day, price_histories.get(event.symbol, {}), sorted_symbol_dates.get(event.symbol, []))
            if close is None or close <= 0:
                skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
                continue
            position = PortfolioPositionState(
                event_id=event.event_id,
                symbol=event.symbol,
                side=event.side,
                entry_date=date.fromisoformat(day),
                entry_price=float(close),
                amount_min=event.amount_min,
                amount_max=event.amount_max,
            )
            positions.append(position)
            open_positions.append(position)

        if buy_events:
            cash = _rebalance_equal_weight(
                cash=cash,
                open_positions=open_positions,
                day=day,
                price_histories=price_histories,
                sorted_dates=sorted_symbol_dates,
            )

        strategy_value, invested_value = _snapshot(
            cash=cash,
            open_positions=open_positions,
            day=day,
            price_histories=price_histories,
            sorted_dates=sorted_symbol_dates,
        )
        benchmark_value = None
        benchmark_return_pct = None
        if benchmark_base is not None and benchmark_base.close > 0:
            benchmark_close = price_on_or_before(day, benchmark_history, sorted_price_dates(benchmark_history))
            if benchmark_close is not None and benchmark_close > 0:
                benchmark_value = starting_value * (float(benchmark_close) / benchmark_base.close)
                benchmark_return_pct = ((benchmark_value / starting_value) - 1.0) * 100.0

        strategy_return_pct = ((strategy_value / starting_value) - 1.0) * 100.0
        alpha_pct = strategy_return_pct - benchmark_return_pct if benchmark_return_pct is not None else None
        daily_return_pct = 0.0 if not points or previous_value <= 0 else ((strategy_value / previous_value) - 1.0) * 100.0
        exposure_pct = 0.0 if strategy_value <= 0 else (invested_value / strategy_value) * 100.0
        cash_pct = 0.0 if strategy_value <= 0 else (cash / strategy_value) * 100.0
        points.append(
            PortfolioPoint(
                asof_date=date.fromisoformat(day),
                strategy_value=_round(strategy_value) or 0.0,
                benchmark_value=_round(benchmark_value),
                strategy_return_pct=_round(strategy_return_pct) or 0.0,
                benchmark_return_pct=_round(benchmark_return_pct),
                alpha_pct=_round(alpha_pct),
                daily_return_pct=_round(daily_return_pct) or 0.0,
                active_positions=len(open_positions),
                exposure_pct=_round(exposure_pct) or 0.0,
                cash_pct=_round(cash_pct) or 0.0,
            )
        )
        previous_value = strategy_value

    last_day = calendar[-1]
    for position in positions:
        close = price_on_or_before(last_day, price_histories.get(position.symbol, {}), sorted_symbol_dates.get(position.symbol, []))
        if close is None or close <= 0:
            continue
        if position.status == "open":
            position.exit_price = float(close)
        position.market_value = position.shares * float(close)  # type: ignore[attr-defined]

    ending_value = points[-1].strategy_value if points else starting_value
    benchmark_ending = points[-1].benchmark_value if points else None
    total_return_pct = ((ending_value / starting_value) - 1.0) * 100.0
    benchmark_return_pct = ((benchmark_ending / starting_value) - 1.0) * 100.0 if benchmark_ending is not None else None
    daily_returns = [point.daily_return_pct / 100.0 for point in points[1:]]
    years = max((points[-1].asof_date - points[0].asof_date).days / 365.25, 1 / 365.25) if len(points) >= 2 else 0.0
    position_returns = []
    for position in positions:
        end_price = position.exit_price
        if position.entry_price > 0 and end_price is not None and end_price > 0:
            position_returns.append(((end_price / position.entry_price) - 1.0) * 100.0)

    summary = PortfolioSummary(
        starting_value=starting_value,
        ending_value=ending_value,
        benchmark_ending_value=benchmark_ending,
        total_return_pct=_round(total_return_pct) or 0.0,
        benchmark_return_pct=_round(benchmark_return_pct),
        alpha_pct=_round(total_return_pct - benchmark_return_pct) if benchmark_return_pct is not None else None,
        cagr_pct=_round(compute_cagr_pct(total_return_pct, years)) or 0.0,
        max_drawdown_pct=_round(compute_max_drawdown_pct([point.strategy_value for point in points])) or 0.0,
        volatility_pct=_round(compute_volatility_pct_from_daily_returns(daily_returns)) or 0.0,
        sharpe_ratio=_round(compute_sharpe_ratio(daily_returns)),
        win_rate_pct=_round(compute_win_rate_pct(position_returns)) or 0.0,
        average_exposure_pct=_round(sum(point.exposure_pct for point in points) / len(points)) if points else 0.0,
        ending_cash_pct=points[-1].cash_pct if points else 100.0,
        points_count=len(points),
        positions_count=len(positions),
        skipped_events_count=len(skipped),
    )
    return PortfolioSimulation(summary=summary, points=points, positions=positions, skipped=skipped, coverage=coverage)


def load_replicated_portfolio_events(
    db: Session,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    issuer: str | None = None,
    end_date: date | None = None,
) -> tuple[list[PortfolioTradeEvent], list[PortfolioSkip]]:
    end = end_date or datetime.now(timezone.utc).date()
    window_start = datetime.combine(end - timedelta(days=max(lookback_days, 1) + 14), datetime.min.time(), tzinfo=timezone.utc)
    query = select(Event).where(Event.ts >= window_start)
    if entity_type == "congress_member":
        query = query.where(Event.event_type == "congress_trade")
        query = query.where(func.lower(func.coalesce(Event.member_bioguide_id, "")) == entity_id.strip().lower())
    elif entity_type == "insider":
        query = query.where(Event.event_type == "insider_trade")
    else:
        raise ValueError(f"Unsupported entity_type: {entity_type}")

    events = db.execute(query.order_by(Event.ts.asc(), Event.id.asc())).scalars().all()
    normalized_issuer_cik = normalize_cik(issuer)
    normalized_issuer_symbol = normalize_symbol(issuer) if not normalized_issuer_cik else None
    normalized_entity_cik = normalize_cik(entity_id) if entity_type == "insider" else None
    portfolio_events: list[PortfolioTradeEvent] = []
    skipped: list[PortfolioSkip] = []

    for event in events:
        payload = parse_payload(event.payload_json)
        if entity_type == "insider":
            reporting_cik = _event_reporting_cik(payload)
            if normalized_entity_cik and reporting_cik != normalized_entity_cik:
                continue
            issuer_cik = _event_issuer_cik(payload)
            event_symbol = normalize_symbol(event.symbol or first_text(payload, "symbol", "ticker"))
            if normalized_issuer_cik and issuer_cik != normalized_issuer_cik:
                continue
            if normalized_issuer_symbol and event_symbol != normalized_issuer_symbol:
                continue

        portfolio_event, skip = _portfolio_event_from_event(event, entity_type=entity_type, entity_id=entity_id)
        if portfolio_event is not None:
            portfolio_events.append(portfolio_event)
        elif skip is not None:
            skipped.append(skip)

    return portfolio_events, skipped


def run_replicated_portfolio_simulation(
    db: Session,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark: str = "^GSPC",
    issuer: str | None = None,
    end_date: date | None = None,
) -> PortfolioSimulation:
    end = end_date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(lookback_days, 1))
    events, loader_skips = load_replicated_portfolio_events(
        db,
        entity_type=entity_type,
        entity_id=entity_id,
        lookback_days=lookback_days,
        issuer=issuer,
        end_date=end,
    )
    symbols = sorted({event.symbol for event in events})
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"
    histories = load_price_histories(db, symbols + [benchmark_symbol], start, end)
    benchmark_history = histories.pop(benchmark_symbol, {})
    simulation = simulate_replicated_portfolio(
        events=events,
        price_histories=histories,
        benchmark_history=benchmark_history,
        start_date=start,
        end_date=end,
        mode=mode,
        benchmark_symbol=benchmark_symbol,
    )
    if not loader_skips:
        return simulation
    summary = PortfolioSummary(
        **{
            **simulation.summary.__dict__,
            "skipped_events_count": simulation.summary.skipped_events_count + len(loader_skips),
        }
    )
    return PortfolioSimulation(
        summary=summary,
        points=simulation.points,
        positions=simulation.positions,
        skipped=[*loader_skips, *simulation.skipped],
        coverage=simulation.coverage,
    )


def persist_replicated_portfolio_run(
    db: Session,
    *,
    simulation: PortfolioSimulation,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark: str = "^GSPC",
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
    start_date: date,
    end_date: date,
) -> ReplicatedPortfolioRun:
    summary = simulation.summary
    run = ReplicatedPortfolioRun(
        entity_type=entity_type,
        entity_id=entity_id,
        issuer_cik=issuer_cik,
        issuer_symbol=issuer_symbol,
        mode=mode,
        lookback_days=lookback_days,
        benchmark_symbol=normalize_symbol(benchmark) or "^GSPC",
        start_date=start_date,
        end_date=end_date,
        starting_value=summary.starting_value,
        ending_value=summary.ending_value,
        benchmark_ending_value=summary.benchmark_ending_value,
        total_return_pct=summary.total_return_pct,
        benchmark_return_pct=summary.benchmark_return_pct,
        alpha_pct=summary.alpha_pct,
        cagr_pct=summary.cagr_pct,
        max_drawdown_pct=summary.max_drawdown_pct,
        volatility_pct=summary.volatility_pct,
        sharpe_ratio=summary.sharpe_ratio,
        win_rate_pct=summary.win_rate_pct,
        average_exposure_pct=summary.average_exposure_pct,
        ending_cash_pct=summary.ending_cash_pct,
        points_count=summary.points_count,
        positions_count=summary.positions_count,
        skipped_events_count=summary.skipped_events_count,
        status="ok",
        methodology_version=PORTFOLIO_METHODOLOGY_VERSION,
    )
    db.add(run)
    db.flush()
    db.add_all(
        [
            ReplicatedPortfolioPoint(
                run_id=run.id,
                asof_date=point.asof_date,
                strategy_value=point.strategy_value,
                benchmark_value=point.benchmark_value,
                strategy_return_pct=point.strategy_return_pct,
                benchmark_return_pct=point.benchmark_return_pct,
                alpha_pct=point.alpha_pct,
                daily_return_pct=point.daily_return_pct,
                active_positions=point.active_positions,
                exposure_pct=point.exposure_pct,
                cash_pct=point.cash_pct,
            )
            for point in simulation.points
        ]
    )
    position_rows: list[ReplicatedPortfolioPosition] = []
    for position in simulation.positions:
        end_price = position.exit_price
        return_pct = ((end_price / position.entry_price) - 1.0) * 100.0 if end_price is not None and position.entry_price > 0 else None
        position_rows.append(
            ReplicatedPortfolioPosition(
                run_id=run.id,
                source_event_id=position.event_id,
                symbol=position.symbol,
                side=position.side,
                entry_date=position.entry_date,
                exit_date=position.exit_date,
                entry_price=position.entry_price,
                exit_price=end_price,
                shares=position.shares,
                market_value=getattr(position, "market_value", None),
                return_pct=_round(return_pct),
                amount_min=position.amount_min,
                amount_max=position.amount_max,
                status=position.status,
            )
        )
    for skip in simulation.skipped:
        position_rows.append(
            ReplicatedPortfolioPosition(
                run_id=run.id,
                source_event_id=skip.event_id,
                symbol=skip.symbol,
                side=skip.side,
                status="skipped",
                skip_reason=skip.reason,
            )
        )
    db.add_all(position_rows)
    db.commit()
    db.refresh(run)
    return run


def latest_replicated_portfolio_payload(
    db: Session,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark: str = "^GSPC",
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
) -> dict[str, Any]:
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"
    query = (
        select(ReplicatedPortfolioRun)
        .where(ReplicatedPortfolioRun.entity_type == entity_type)
        .where(ReplicatedPortfolioRun.entity_id == entity_id)
        .where(ReplicatedPortfolioRun.lookback_days == lookback_days)
        .where(ReplicatedPortfolioRun.mode == mode)
        .where(ReplicatedPortfolioRun.benchmark_symbol == benchmark_symbol)
    )
    if issuer_cik:
        query = query.where(ReplicatedPortfolioRun.issuer_cik == issuer_cik)
    elif issuer_symbol:
        query = query.where(ReplicatedPortfolioRun.issuer_symbol == issuer_symbol)
    else:
        query = query.where(ReplicatedPortfolioRun.issuer_cik.is_(None)).where(ReplicatedPortfolioRun.issuer_symbol.is_(None))

    run = db.execute(query.order_by(ReplicatedPortfolioRun.computed_at.desc(), ReplicatedPortfolioRun.id.desc())).scalars().first()
    if run is None:
        return {
            "status": "no_persisted_run",
            "persisted_only": True,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "issuer_cik": issuer_cik,
            "issuer_symbol": issuer_symbol,
            "lookback_days": lookback_days,
            "mode": mode,
            "benchmark_symbol": benchmark_symbol,
            "summary": None,
            "points": [],
            "positions": [],
        }

    points = db.execute(
        select(ReplicatedPortfolioPoint)
        .where(ReplicatedPortfolioPoint.run_id == run.id)
        .order_by(ReplicatedPortfolioPoint.asof_date.asc())
    ).scalars().all()
    positions = db.execute(
        select(ReplicatedPortfolioPosition)
        .where(ReplicatedPortfolioPosition.run_id == run.id)
        .order_by(ReplicatedPortfolioPosition.id.asc())
    ).scalars().all()
    return {
        "status": run.status,
        "persisted_only": True,
        "run_id": run.id,
        "entity_type": run.entity_type,
        "entity_id": run.entity_id,
        "issuer_cik": run.issuer_cik,
        "issuer_symbol": run.issuer_symbol,
        "lookback_days": run.lookback_days,
        "mode": run.mode,
        "benchmark_symbol": run.benchmark_symbol,
        "start_date": run.start_date.isoformat(),
        "end_date": run.end_date.isoformat(),
        "computed_at": run.computed_at.isoformat() if run.computed_at else None,
        "methodology_version": run.methodology_version,
        "summary": {
            "starting_value": run.starting_value,
            "ending_value": run.ending_value,
            "benchmark_ending_value": run.benchmark_ending_value,
            "total_return_pct": run.total_return_pct,
            "benchmark_return_pct": run.benchmark_return_pct,
            "alpha_pct": run.alpha_pct,
            "cagr_pct": run.cagr_pct,
            "max_drawdown_pct": run.max_drawdown_pct,
            "volatility_pct": run.volatility_pct,
            "sharpe_ratio": run.sharpe_ratio,
            "win_rate_pct": run.win_rate_pct,
            "average_exposure_pct": run.average_exposure_pct,
            "ending_cash_pct": run.ending_cash_pct,
            "points_count": run.points_count,
            "positions_count": run.positions_count,
            "skipped_events_count": run.skipped_events_count,
        },
        "points": [
            {
                "asof_date": point.asof_date.isoformat(),
                "strategy_value": point.strategy_value,
                "benchmark_value": point.benchmark_value,
                "strategy_return_pct": point.strategy_return_pct,
                "benchmark_return_pct": point.benchmark_return_pct,
                "alpha_pct": point.alpha_pct,
                "daily_return_pct": point.daily_return_pct,
                "active_positions": point.active_positions,
                "exposure_pct": point.exposure_pct,
                "cash_pct": point.cash_pct,
            }
            for point in points
        ],
        "positions": [
            {
                "source_event_id": position.source_event_id,
                "symbol": position.symbol,
                "side": position.side,
                "entry_date": position.entry_date.isoformat() if position.entry_date else None,
                "exit_date": position.exit_date.isoformat() if position.exit_date else None,
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "shares": position.shares,
                "market_value": position.market_value,
                "return_pct": position.return_pct,
                "status": position.status,
                "skip_reason": position.skip_reason,
            }
            for position in positions
        ],
    }
