from __future__ import annotations

import json
from bisect import bisect_right
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
    Security,
    TickerMeta,
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
    nearest_price_on_date,
    parse_iso_date,
    parse_payload,
    price_on_or_before,
    sorted_price_dates,
)
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.ticker_meta import normalize_cik
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import classify_symbol, normalize_symbol

PORTFOLIO_METHODOLOGY_VERSION = "replicated_portfolio_v2"
DEFAULT_STARTING_VALUE = 100000.0
DEFAULT_MAX_STALE_PRICE_TRADING_DAYS = 5
SUPPORTED_MODES = {"realistic_disclosure_lag", "theoretical_transaction_date"}
SUPPORTED_ENTITY_TYPES = {"congress_member", "insider"}
_REIT_TERMS = ("reit", "real estate investment trust")
_OPTION_TERMS = ("option", "stock option", "call option", "put option", "derivative")
_FUND_OR_INDEX_ASSET_CLASSES = {
    "etf",
    "etf fund",
    "etf_fund",
    "exchange traded fund",
    "exchange-traded fund",
    "fund",
    "index",
    "mutual fund",
}
_FUND_OR_INDEX_TERMS = (
    "exchange traded fund",
    "exchange-traded fund",
    "index fund",
    "money market fund",
    "mutual fund",
)
_CORPORATE_BOND_TERMS = (
    "corporate bond",
    "corp bond",
    "debenture",
    "treasury bill",
    "treasury note",
    " t bill",
    " u s t bill",
)
_MUNICIPAL_BOND_TERMS = ("municipal bond", "muni bond", "municipal", "housing dev auth", "rev bds", "go pub impt bds")
_PRIVATE_FUND_TERMS = ("private fund", "private equity", "hedge fund", "limited partnership")
_PRIVATE_FUND_SYMBOL_SECURITY_NAMES = {
    ("GLAS", "trimer capital partners i lp"),
    ("ICAPITAL", "sl partners vii"),
}
_DELISTED_OR_ACQUIRED_NO_HISTORY_SYMBOLS = {
    "GPORQ",
    "NCR",
    "RDS.B",
    "WLTW",
}
_PORTFOLIO_SAFE_SYMBOL_MAPPINGS = {
    "AMGEN": "AMGN",
    "BRKB": "BRK-B",
    "DUK.PA": "DUK-PA",
    "DUK/PA": "DUK-PA",
    "LBYAV": "LBTYA",
    "PDRDY": "PRNDY",
}


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
    source_type: str = "disclosed_trade"
    source_reason: str | None = None
    confidence: str | None = None
    estimated_opening_value: float | None = None
    estimated_source_event_id: int | None = None


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
class PortfolioFlatSegment:
    start_date: date
    end_date: date
    trading_days: int
    segment_type: str
    active_positions: int
    active_positions_count: int
    valued_positions_count: int
    zero_value_positions_count: int
    total_shares_nonzero_count: int
    total_market_value_nonzero_count: int
    active_symbols: list[str]
    stale_symbols: list[str]
    missing_symbols: list[str]
    portfolio_value_start: float | None = None
    portfolio_value_end: float | None = None
    cash_value_start: float | None = None
    cash_value_end: float | None = None
    invested_value_start: float | None = None
    invested_value_end: float | None = None
    exposure_pct_start: float | None = None
    exposure_pct_end: float | None = None
    top_positions_by_market_value_start: list[dict[str, Any]] | None = None
    top_positions_by_market_value_end: list[dict[str, Any]] | None = None
    top_zero_value_symbols: list[str] | None = None
    legitimate_no_holdings: bool = False


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
class PortfolioEffectiveWindow:
    requested_start_date: date
    effective_start_date: date | None
    effective_end_date: date | None
    effective_window_days: int
    effective_window_reason: str
    no_active_holdings: bool = False


@dataclass(frozen=True)
class PortfolioWarmupDiagnostics:
    warmup_start_date: date
    visible_start_date: date
    warmup_days: int
    opening_positions_count: int
    sale_without_position_before_warmup: int
    sale_without_position_after_warmup: int
    opening_position_estimated: bool = False
    estimated_opening_positions_count: int = 0
    estimated_opening_positions_symbols: list[str] | None = None
    estimated_opening_positions_value: float = 0.0
    sale_without_position_before_estimation: int = 0
    sale_without_position_after_estimation: int = 0


@dataclass(frozen=True)
class PortfolioCoverage:
    requested_start_date: date
    requested_end_date: date
    warmup_start_date: date | None
    warmup_days: int
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
class PortfolioCurveDiagnostics:
    flat_segment_count: int
    longest_flat_segment_days: int
    longest_problematic_flat_segment_days: int
    average_exposure_pct: float
    min_exposure_pct: float
    max_exposure_pct: float
    days_with_zero_exposure: int
    days_with_active_positions_but_zero_exposure: int
    days_with_active_positions_but_no_valued_positions: int
    pct_position_days_with_price_gaps: float
    pct_invested_value_with_price_gaps: float
    avg_priced_invested_value_pct: float
    min_priced_invested_value_pct: float
    days_below_90pct_priced_value: int
    days_below_75pct_priced_value: int
    days_below_50pct_priced_value: int
    stale_price_fill_count: int
    missing_price_fill_count: int
    positions_marked_to_market_count: int
    positions_using_stale_price_count: int
    pct_days_with_price_gaps: float
    curve_quality_status: str
    curve_quality_notes: list[str]
    flat_segments: list[PortfolioFlatSegment]
    suggested_backfill_symbols: list[str]
    suggested_backfill_start_date: date | None = None
    suggested_backfill_end_date: date | None = None


@dataclass(frozen=True)
class PortfolioSimulation:
    summary: PortfolioSummary
    points: list[PortfolioPoint]
    positions: list[PortfolioPositionState]
    skipped: list[PortfolioSkip]
    coverage: PortfolioCoverage
    curve_diagnostics: PortfolioCurveDiagnostics
    daily_quality: list[_DailyCurveQuality]
    effective_window: PortfolioEffectiveWindow | None = None
    warmup_diagnostics: PortfolioWarmupDiagnostics | None = None


@dataclass(frozen=True)
class _ResolvedValuationPrice:
    close: float
    price_date: str
    fill_type: str
    stale_trading_days: int = 0


@dataclass(frozen=True)
class _DailyCurveQuality:
    day: str
    active_symbols: list[str]
    stale_symbols: list[str]
    missing_symbols: list[str]
    marked_to_market_count: int
    active_positions_count: int = 0
    portfolio_value: float = 0.0
    cash_value: float = 0.0
    invested_value: float = 0.0
    exposure_pct: float = 0.0
    valued_positions_count: int = 0
    zero_value_positions_count: int = 0
    shares_nonzero_count: int = 0
    market_value_nonzero_count: int = 0
    priced_invested_value: float = 0.0
    stale_invested_value: float = 0.0
    missing_invested_value: float = 0.0
    price_gap_invested_value: float = 0.0
    priced_invested_value_pct: float = 100.0
    price_gap_value_by_symbol: dict[str, float] | None = None
    top_positions_by_market_value: list[dict[str, Any]] | None = None
    top_zero_value_symbols: list[str] | None = None
    stale_position_count: int = 0
    missing_position_count: int = 0


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return float(round(float(value), digits))


def effective_window_payload(window: PortfolioEffectiveWindow | None) -> dict[str, Any]:
    if window is None:
        return {
            "requested_start_date": None,
            "effective_start_date": None,
            "effective_end_date": None,
            "effective_window_days": 0,
            "effective_window_reason": "unknown",
            "no_active_holdings": False,
        }
    return {
        "requested_start_date": window.requested_start_date.isoformat(),
        "effective_start_date": window.effective_start_date.isoformat() if window.effective_start_date else None,
        "effective_end_date": window.effective_end_date.isoformat() if window.effective_end_date else None,
        "effective_window_days": window.effective_window_days,
        "effective_window_reason": window.effective_window_reason,
        "no_active_holdings": window.no_active_holdings,
    }


def warmup_diagnostics_payload(diagnostics: PortfolioWarmupDiagnostics | None) -> dict[str, Any] | None:
    if diagnostics is None:
        return None
    return {
        "warmup_start_date": diagnostics.warmup_start_date.isoformat(),
        "visible_start_date": diagnostics.visible_start_date.isoformat(),
        "warmup_days": diagnostics.warmup_days,
        "opening_positions_count": diagnostics.opening_positions_count,
        "sale_without_position_before_warmup": diagnostics.sale_without_position_before_warmup,
        "sale_without_position_after_warmup": diagnostics.sale_without_position_after_warmup,
        "opening_position_estimated": diagnostics.opening_position_estimated,
        "estimated_opening_positions_count": diagnostics.estimated_opening_positions_count,
        "estimated_opening_positions_symbols": diagnostics.estimated_opening_positions_symbols or [],
        "estimated_opening_positions_value": _round(diagnostics.estimated_opening_positions_value) or 0.0,
        "sale_without_position_before_estimation": diagnostics.sale_without_position_before_estimation,
        "sale_without_position_after_estimation": diagnostics.sale_without_position_after_estimation,
    }


def _as_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _trade_amount_midpoint(event: PortfolioTradeEvent) -> float | None:
    amount_min = float(event.amount_min) if event.amount_min is not None else None
    amount_max = float(event.amount_max) if event.amount_max is not None else None
    if amount_min is not None and amount_max is not None:
        return (amount_min + amount_max) / 2.0
    return amount_max if amount_max is not None else amount_min


def _find_effective_point_index(points: list[PortfolioPoint]) -> int | None:
    for index, point in enumerate(points):
        if int(point.active_positions or 0) > 0 and float(point.exposure_pct or 0.0) > 0.000001:
            return index
    return None


def _effective_window_from_points(
    *,
    points: list[PortfolioPoint],
    requested_start_date: date,
) -> PortfolioEffectiveWindow:
    effective_index = _find_effective_point_index(points)
    if effective_index is None:
        return PortfolioEffectiveWindow(
            requested_start_date=requested_start_date,
            effective_start_date=None,
            effective_end_date=None,
            effective_window_days=0,
            effective_window_reason="no_active_holdings",
            no_active_holdings=True,
        )
    effective_start_date = points[effective_index].asof_date
    effective_end_date = points[-1].asof_date
    return PortfolioEffectiveWindow(
        requested_start_date=requested_start_date,
        effective_start_date=effective_start_date,
        effective_end_date=effective_end_date,
        effective_window_days=max((effective_end_date - effective_start_date).days, 0),
        effective_window_reason="requested_start_active_holding" if effective_index == 0 else "first_active_holding",
        no_active_holdings=False,
    )


def _rebase_points_to_effective_window(
    *,
    points: list[PortfolioPoint],
    effective_window: PortfolioEffectiveWindow,
    starting_value: float,
) -> list[PortfolioPoint]:
    if effective_window.no_active_holdings or effective_window.effective_start_date is None:
        return []
    effective_points = [point for point in points if point.asof_date >= effective_window.effective_start_date]
    if not effective_points:
        return []

    strategy_base = float(effective_points[0].strategy_value or 0.0)
    benchmark_base = effective_points[0].benchmark_value
    if strategy_base <= 0:
        return []

    rebased: list[PortfolioPoint] = []
    previous_strategy_value: float | None = None
    for point in effective_points:
        strategy_value = float(starting_value) * (float(point.strategy_value or 0.0) / strategy_base)
        strategy_return_pct = ((strategy_value / float(starting_value)) - 1.0) * 100.0
        benchmark_value = None
        benchmark_return_pct = None
        if benchmark_base is not None and float(benchmark_base or 0.0) > 0 and point.benchmark_value is not None:
            benchmark_value = float(starting_value) * (float(point.benchmark_value) / float(benchmark_base))
            benchmark_return_pct = ((benchmark_value / float(starting_value)) - 1.0) * 100.0
        alpha_pct = strategy_return_pct - benchmark_return_pct if benchmark_return_pct is not None else None
        daily_return_pct = (
            0.0
            if previous_strategy_value is None or previous_strategy_value <= 0
            else ((strategy_value / previous_strategy_value) - 1.0) * 100.0
        )
        rebased.append(
            PortfolioPoint(
                asof_date=point.asof_date,
                strategy_value=_round(strategy_value) or 0.0,
                benchmark_value=_round(benchmark_value),
                strategy_return_pct=_round(strategy_return_pct) or 0.0,
                benchmark_return_pct=_round(benchmark_return_pct),
                alpha_pct=_round(alpha_pct),
                daily_return_pct=_round(daily_return_pct) or 0.0,
                active_positions=point.active_positions,
                exposure_pct=point.exposure_pct,
                cash_pct=point.cash_pct,
            )
        )
        previous_strategy_value = strategy_value
    return rebased


def _position_win_rate_pct(positions: list[PortfolioPositionState]) -> float:
    position_returns = []
    for position in positions:
        end_price = position.exit_price
        if position.entry_price > 0 and end_price is not None and end_price > 0:
            position_returns.append(((end_price / position.entry_price) - 1.0) * 100.0)
    return _round(compute_win_rate_pct(position_returns)) or 0.0


def _summary_from_effective_points(
    *,
    points: list[PortfolioPoint],
    positions: list[PortfolioPositionState],
    skipped_events_count: int,
    starting_value: float,
) -> PortfolioSummary:
    ending_value = points[-1].strategy_value if points else starting_value
    benchmark_ending = points[-1].benchmark_value if points else None
    total_return_pct = ((ending_value / starting_value) - 1.0) * 100.0 if starting_value > 0 else 0.0
    benchmark_return_pct = ((benchmark_ending / starting_value) - 1.0) * 100.0 if benchmark_ending is not None and starting_value > 0 else None
    daily_returns = [point.daily_return_pct / 100.0 for point in points[1:]]
    years = max((points[-1].asof_date - points[0].asof_date).days / 365.25, 1 / 365.25) if len(points) >= 2 else 0.0
    return PortfolioSummary(
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
        win_rate_pct=_position_win_rate_pct(positions),
        average_exposure_pct=_round(sum(point.exposure_pct for point in points) / len(points)) if points else 0.0,
        ending_cash_pct=points[-1].cash_pct if points else 100.0,
        points_count=len(points),
        positions_count=len(positions),
        skipped_events_count=skipped_events_count,
    )


def _summary_for_no_active_holdings(
    *,
    starting_value: float,
    positions: list[PortfolioPositionState],
    skipped_events_count: int,
) -> PortfolioSummary:
    return PortfolioSummary(
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
        win_rate_pct=_position_win_rate_pct(positions),
        average_exposure_pct=0.0,
        ending_cash_pct=100.0,
        points_count=0,
        positions_count=len(positions),
        skipped_events_count=skipped_events_count,
    )


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


def _security_lookup_name(payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return first_text(
        payload,
        "company_name",
        "companyName",
        "issuer_name",
        "issuerName",
        "security_name",
        "securityName",
        "security_description",
        "securityDescription",
        "description",
    ) or first_text(
        raw,
        "companyName",
        "issuerName",
        "securityName",
        "securityTitle",
        "transactionSecurityTitle",
    )


def _normalized_lookup_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(str(value).strip().lower().replace("&", "and").split())
    return cleaned or None


def _safe_symbol_from_rows(rows: list[tuple[str | None]]) -> str | None:
    symbols = sorted({normalize_symbol(row[0]) for row in rows if row and normalize_symbol(row[0])})
    eligible = [symbol for symbol in symbols if classify_symbol(symbol)[0] == "eligible"]
    return eligible[0] if len(eligible) == 1 else None


def _resolve_symbol_from_security_name(db: Session | None, payload: dict[str, Any]) -> tuple[str | None, str | None]:
    if db is None:
        return None, None
    lookup_name = _security_lookup_name(payload)
    normalized_name = _normalized_lookup_text(lookup_name)
    if not normalized_name:
        return None, None

    security_rows = db.execute(
        select(Security.symbol).where(func.lower(Security.name) == normalized_name).limit(3)
    ).all()
    resolved = _safe_symbol_from_rows(security_rows)
    if resolved:
        return resolved, f"security_name_exact:{lookup_name}"

    meta_rows = db.execute(
        select(TickerMeta.symbol).where(func.lower(TickerMeta.company_name) == normalized_name).limit(3)
    ).all()
    resolved = _safe_symbol_from_rows(meta_rows)
    if resolved:
        return resolved, f"ticker_meta_company_exact:{lookup_name}"

    return None, f"no_exact_symbol_match security_name={lookup_name}"


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


def _is_known_private_fund_symbol(symbol: str | None, text: str) -> bool:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol or not text:
        return False
    return any(
        normalized_symbol == known_symbol and security_name in text
        for known_symbol, security_name in _PRIVATE_FUND_SYMBOL_SECURITY_NAMES
    )


def _portfolio_asset_skip_reason(payload: dict[str, Any], *, symbol: str | None = None) -> str | None:
    text = _asset_text(payload)
    if not text:
        return None
    if any(term in text for term in _OPTION_TERMS):
        return "options"
    if _looks_like_index_option_contract(text):
        return "non_simulatable_fund_or_index"
    if _is_fund_or_index_payload(payload, text):
        return "non_simulatable_fund_or_index"
    if any(term in text for term in _MUNICIPAL_BOND_TERMS):
        return "municipal_bond"
    if any(term in text for term in _CORPORATE_BOND_TERMS):
        return "corporate_bond"
    if " bond" in f" {text}" or "bonds" in text:
        return "corporate_bond"
    if _is_known_private_fund_symbol(symbol, text):
        return "private_fund"
    if any(term in text for term in _PRIVATE_FUND_TERMS):
        return "private_fund"
    return None


def _looks_like_index_option_contract(text: str) -> bool:
    padded = f" {text} "
    if "put/" in text or "call/" in text:
        return True
    return bool((" put " in padded or " call " in padded) and (" exp " in padded or "@" in text))


def _is_fund_or_index_payload(payload: dict[str, Any], text: str) -> bool:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    class_values = [
        payload.get("asset_class"),
        payload.get("assetClass"),
        payload.get("instrument_type"),
        payload.get("instrumentType"),
        raw.get("asset_class"),
        raw.get("assetClass"),
        raw.get("instrument_type"),
        raw.get("instrumentType"),
    ]
    for value in class_values:
        normalized = str(value).strip().lower().replace("-", " ").replace("_", " ") if value is not None else ""
        if normalized in _FUND_OR_INDEX_ASSET_CLASSES:
            return True
    padded = f" {text} "
    if any(term in text for term in _FUND_OR_INDEX_TERMS):
        return True
    return " etf " in padded or padded.endswith(" etf ") or " fund " in padded or padded.endswith(" fund ")


def _portfolio_symbol_skip_reason(symbol: str | None) -> str | None:
    normalized_symbol = normalize_symbol(symbol)
    if normalized_symbol in _DELISTED_OR_ACQUIRED_NO_HISTORY_SYMBOLS:
        return "delisted_or_acquired_no_history"
    return None


def _safe_portfolio_execution_symbol(symbol: str | None, payload: dict[str, Any]) -> str | None:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return normalized_symbol
    mapped = _PORTFOLIO_SAFE_SYMBOL_MAPPINGS.get(normalized_symbol)
    if not mapped:
        return normalized_symbol

    text = _asset_text(payload)
    if normalized_symbol == "AMGEN" and "amgen" in text:
        return mapped
    if normalized_symbol == "BRKB" and "berkshire hathaway" in text and "class b" in text:
        return mapped
    if normalized_symbol in {"DUK.PA", "DUK/PA"} and "duke energy" in text:
        return mapped
    if normalized_symbol == "LBYAV" and "liberty global" in text:
        return mapped
    if normalized_symbol == "PDRDY" and "pernod" in text and "ricard" in text:
        return mapped
    return normalized_symbol


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
    if reason in {"non_simulatable_fund_or_index", "delisted_or_acquired_no_history", "adr_provider_gap", "price_missing_equity"}:
        return reason
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
    if reason in {"unmatched_sell", "sale_without_known_prior_position", "sale_without_position_during_warmup"}:
        return "sale_without_position"
    return reason


def skip_diagnostic_category(skip: PortfolioSkip) -> str:
    reason = normalize_skip_reason(skip)
    if reason in {"no_symbol", "invalid_symbol", "unsupported_symbol"}:
        return "unresolved_symbol"
    if reason in {"missing_price", "missing_price_history", "no_execution_price", "missing_trading_calendar", "no_data"}:
        return "missing_execution_price"
    if reason in {"options", "municipal_bond", "corporate_bond", "private_fund", "unsupported_asset_class", "non_simulatable_fund_or_index"}:
        return "non_equity_asset"
    if reason == "sale_without_position":
        return "sale_without_position"
    if reason in {"missing_mark_price", "no_mark_price"}:
        return "missing_mark_price"
    return "other"


def skip_reason_summary(skips: list[PortfolioSkip]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for skip in skips:
        key = normalize_skip_reason(skip)
        summary[key] = summary.get(key, 0) + 1
    return dict(sorted(summary.items()))


def skip_diagnostic_summary(skips: list[PortfolioSkip]) -> dict[str, int]:
    summary = {
        "skipped_total": len(skips),
        "missing_execution_price": 0,
        "unresolved_symbol": 0,
        "non_equity_asset": 0,
        "sale_without_position": 0,
        "missing_mark_price": 0,
        "other": 0,
    }
    for skip in skips:
        category = skip_diagnostic_category(skip)
        summary[category] = summary.get(category, 0) + 1
    return summary


def _portfolio_event_from_event(
    event: Event,
    *,
    entity_type: str,
    entity_id: str,
    db: Session | None = None,
) -> tuple[PortfolioTradeEvent | None, PortfolioSkip | None]:
    payload = parse_payload(event.payload_json)
    raw_symbol = event.symbol or first_text(payload, "symbol", "ticker")
    status, symbol, symbol_error = classify_symbol(raw_symbol)
    symbol_resolution_detail = None
    if status != "eligible":
        resolved_symbol, resolution_detail = _resolve_symbol_from_security_name(db, payload)
        symbol_resolution_detail = resolution_detail
        if resolved_symbol:
            status, symbol, symbol_error = "eligible", resolved_symbol, None
    if entity_type == "congress_member" and status == "eligible":
        symbol = _safe_portfolio_execution_symbol(symbol, payload)
    if entity_type == "insider":
        side = _normalize_insider_side(event, payload)
        raw_side = side
    else:
        raw_side = event.trade_type or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        side = normalize_trade_side(raw_side)

    if side not in {"purchase", "sale"}:
        reason = "missing_transaction_code_or_side" if entity_type == "insider" else "unsupported_side"
        return None, PortfolioSkip(event.id, symbol, side, reason, raw_side)
    if entity_type == "congress_member":
        portfolio_asset_skip = _portfolio_asset_skip_reason(payload, symbol=symbol)
        if portfolio_asset_skip:
            return None, PortfolioSkip(event.id, symbol, side, portfolio_asset_skip)
        portfolio_symbol_skip = _portfolio_symbol_skip_reason(symbol)
        if portfolio_symbol_skip:
            return None, PortfolioSkip(event.id, symbol, side, portfolio_symbol_skip)
    if status != "eligible" or not symbol:
        detail_parts = [
            part
            for part in (
                f"raw_symbol={raw_symbol}" if raw_symbol else None,
                f"security_name={_security_lookup_name(payload)}" if _security_lookup_name(payload) else None,
                symbol_error,
                symbol_resolution_detail,
            )
            if part
        ]
        return None, PortfolioSkip(event.id, symbol, side, status, "; ".join(detail_parts) or symbol_error)

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
    warmup_start_date: date | None = None,
) -> PortfolioCoverage:
    benchmark_dates = sorted_price_dates(benchmark_history)
    symbol_dates = {symbol: sorted_price_dates(history) for symbol, history in price_histories.items()}
    requested_calendar = [day for day in calendar if start_date.isoformat() <= day <= end_date.isoformat()]
    limitations: list[str] = []
    if not benchmark_dates:
        limitations.append(f"No cached benchmark history loaded for {benchmark_symbol}.")
    elif benchmark_dates[0] > start_date.isoformat():
        limitations.append(f"Benchmark coverage starts at {benchmark_dates[0]}, after requested start {start_date.isoformat()}.")
    if benchmark_dates and benchmark_dates[-1] < end_date.isoformat():
        limitations.append(f"Benchmark coverage ends at {benchmark_dates[-1]}, before requested end {end_date.isoformat()}.")
    if not calendar:
        limitations.append("No trading calendar could be built from cached benchmark or symbol prices.")
    elif not requested_calendar:
        limitations.append("No requested-window trading days could be built from cached benchmark or symbol prices.")
    elif requested_calendar[0] > start_date.isoformat():
        limitations.append(f"Curve starts at {requested_calendar[0]}, after requested start {start_date.isoformat()}.")
    if requested_calendar and requested_calendar[-1] < end_date.isoformat():
        limitations.append(f"Curve ends at {requested_calendar[-1]}, before requested end {end_date.isoformat()}.")
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
        warmup_start_date=warmup_start_date,
        warmup_days=max((start_date - warmup_start_date).days, 0) if warmup_start_date else 0,
        actual_start_date=date.fromisoformat(requested_calendar[0]) if requested_calendar else None,
        actual_end_date=date.fromisoformat(requested_calendar[-1]) if requested_calendar else None,
        calendar_points=len(requested_calendar),
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


def _visible_sale_without_position_count(
    events: list[PortfolioTradeEvent],
    *,
    start_date: date,
    end_date: date,
    mode: str,
) -> int:
    open_counts: dict[str, int] = {}
    count = 0
    for event in sorted(events, key=lambda item: (event_effective_date(item, mode), item.event_id or 0)):
        effective_date = event_effective_date(event, mode)
        if effective_date < start_date or effective_date > end_date:
            continue
        if event.side == "purchase":
            open_counts[event.symbol] = open_counts.get(event.symbol, 0) + 1
            continue
        if event.side == "sale":
            open_count = open_counts.get(event.symbol, 0)
            if open_count <= 0:
                count += 1
            else:
                open_counts[event.symbol] = open_count - 1
    return count


def _visible_start_mark_price(
    *,
    symbol: str,
    visible_start_date: date,
    price_histories: dict[str, dict[str, float]],
    max_stale_price_trading_days: int,
) -> float | None:
    history = price_histories.get(symbol, {})
    if not history:
        return None
    resolved = nearest_price_on_date(
        visible_start_date,
        history,
        prefer_previous=True,
        max_backward_trading_days=max_stale_price_trading_days,
        max_forward_trading_days=max_stale_price_trading_days,
    )
    if resolved is None or resolved.close <= 0:
        return None
    return float(resolved.close)


def default_warmup_days_for_lookback(lookback_days: int) -> int:
    if lookback_days <= 90:
        return 365
    if lookback_days <= 180:
        return 730
    if lookback_days <= 365:
        return 1095
    if lookback_days <= 1095:
        return 1825
    return 1825


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


def _calendar_index_for_price_date(day: str, calendar: list[str], calendar_indexes: dict[str, int]) -> int | None:
    if day in calendar_indexes:
        return calendar_indexes[day]
    index = bisect_right(calendar, day) - 1
    if index < 0:
        return None
    return index


def _resolve_valuation_price(
    *,
    symbol: str,
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
    calendar: list[str],
    calendar_indexes: dict[str, int],
    max_stale_price_trading_days: int,
) -> _ResolvedValuationPrice | None:
    history = price_histories.get(symbol, {})
    dates = sorted_dates.get(symbol, [])
    if not dates:
        return None
    target_key = day[:10]
    exact = history.get(target_key)
    if exact is not None and exact > 0:
        return _ResolvedValuationPrice(close=float(exact), price_date=target_key, fill_type="exact")

    index = bisect_right(dates, target_key) - 1
    if index < 0:
        return None
    price_day = dates[index]
    close = history.get(price_day)
    if close is None or close <= 0:
        return None
    target_calendar_index = calendar_indexes.get(target_key)
    price_calendar_index = _calendar_index_for_price_date(price_day, calendar, calendar_indexes)
    stale_trading_days = 0
    if target_calendar_index is not None and price_calendar_index is not None:
        stale_trading_days = max(target_calendar_index - price_calendar_index, 0)
    else:
        stale_trading_days = max((date.fromisoformat(target_key) - date.fromisoformat(price_day)).days, 0)
    if stale_trading_days > max(max_stale_price_trading_days, 0):
        return _ResolvedValuationPrice(
            close=float(close),
            price_date=price_day,
            fill_type="stale_beyond_tolerance",
            stale_trading_days=stale_trading_days,
        )
    return _ResolvedValuationPrice(
        close=float(close),
        price_date=price_day,
        fill_type="stale",
        stale_trading_days=stale_trading_days,
    )


def _position_value(
    position: PortfolioPositionState,
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
    calendar: list[str],
    calendar_indexes: dict[str, int],
    max_stale_price_trading_days: int,
) -> tuple[float, _ResolvedValuationPrice | None]:
    resolved = _resolve_valuation_price(
        symbol=position.symbol,
        day=day,
        price_histories=price_histories,
        sorted_dates=sorted_dates,
        calendar=calendar,
        calendar_indexes=calendar_indexes,
        max_stale_price_trading_days=max_stale_price_trading_days,
    )
    if resolved is None:
        return 0.0, None
    return float(position.shares * resolved.close), resolved


def _snapshot(
    *,
    cash: float,
    open_positions: list[PortfolioPositionState],
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
    calendar: list[str],
    calendar_indexes: dict[str, int],
    max_stale_price_trading_days: int,
) -> tuple[float, float, _DailyCurveQuality]:
    invested = 0.0
    stale_symbols: set[str] = set()
    missing_symbols: set[str] = set()
    active_symbols = sorted({position.symbol for position in open_positions})
    marked_to_market_count = 0
    stale_position_count = 0
    missing_position_count = 0
    zero_value_symbols: set[str] = set()
    shares_nonzero_count = 0
    market_value_nonzero_count = 0
    priced_invested_value = 0.0
    stale_invested_value = 0.0
    missing_invested_value = 0.0
    market_value_by_symbol: dict[str, float] = {}
    shares_by_symbol: dict[str, float] = {}
    price_gap_value_by_symbol: dict[str, float] = {}
    for position in open_positions:
        value, resolved = _position_value(
            position,
            day,
            price_histories,
            sorted_dates,
            calendar,
            calendar_indexes,
            max_stale_price_trading_days,
        )
        if abs(float(position.shares or 0.0)) > 0.000001:
            shares_nonzero_count += 1
            shares_by_symbol[position.symbol] = shares_by_symbol.get(position.symbol, 0.0) + float(position.shares or 0.0)
        if resolved is None:
            missing_symbols.add(position.symbol)
            missing_position_count += 1
            zero_value_symbols.add(position.symbol)
            price_gap_value_by_symbol.setdefault(position.symbol, 0.0)
            continue
        market_value_by_symbol[position.symbol] = market_value_by_symbol.get(position.symbol, 0.0) + value
        invested += value
        marked_to_market_count += 1
        if abs(value) > 0.000001:
            market_value_nonzero_count += 1
        else:
            zero_value_symbols.add(position.symbol)
        if resolved.fill_type == "stale":
            stale_symbols.add(position.symbol)
            stale_position_count += 1
            stale_invested_value += value
            price_gap_value_by_symbol[position.symbol] = price_gap_value_by_symbol.get(position.symbol, 0.0) + abs(value)
        elif resolved.fill_type == "stale_beyond_tolerance":
            missing_symbols.add(position.symbol)
            missing_position_count += 1
            missing_invested_value += value
            price_gap_value_by_symbol[position.symbol] = price_gap_value_by_symbol.get(position.symbol, 0.0) + abs(value)
        else:
            priced_invested_value += value
    portfolio_value = float(cash + invested)
    exposure_pct = 0.0 if portfolio_value <= 0 else (invested / portfolio_value) * 100.0
    price_gap_invested_value = stale_invested_value + missing_invested_value
    if invested > 0:
        priced_invested_value_pct = (priced_invested_value / invested) * 100.0
    elif stale_position_count or missing_position_count:
        priced_invested_value_pct = 0.0
    else:
        priced_invested_value_pct = 100.0
    top_positions = [
        {
            "symbol": symbol,
            "market_value": _round(market_value),
            "shares": _round(shares_by_symbol.get(symbol, 0.0)),
        }
        for symbol, market_value in sorted(
            market_value_by_symbol.items(),
            key=lambda item: (-abs(item[1]), item[0]),
        )
        if abs(market_value) > 0.000001
    ][:10]
    quality = _DailyCurveQuality(
        day=day,
        active_symbols=active_symbols,
        stale_symbols=sorted(stale_symbols),
        missing_symbols=sorted(missing_symbols),
        marked_to_market_count=marked_to_market_count,
        active_positions_count=len(open_positions),
        portfolio_value=_round(portfolio_value) or 0.0,
        cash_value=_round(cash) or 0.0,
        invested_value=_round(invested) or 0.0,
        exposure_pct=_round(exposure_pct) or 0.0,
        valued_positions_count=marked_to_market_count,
        zero_value_positions_count=len([position for position in open_positions if position.symbol in zero_value_symbols]),
        shares_nonzero_count=shares_nonzero_count,
        market_value_nonzero_count=market_value_nonzero_count,
        priced_invested_value=_round(priced_invested_value) or 0.0,
        stale_invested_value=_round(stale_invested_value) or 0.0,
        missing_invested_value=_round(missing_invested_value) or 0.0,
        price_gap_invested_value=_round(price_gap_invested_value) or 0.0,
        priced_invested_value_pct=_round(priced_invested_value_pct) or 0.0,
        price_gap_value_by_symbol={symbol: _round(value) or 0.0 for symbol, value in sorted(price_gap_value_by_symbol.items())},
        top_positions_by_market_value=top_positions,
        top_zero_value_symbols=sorted(zero_value_symbols)[:10],
        stale_position_count=stale_position_count,
        missing_position_count=missing_position_count,
    )
    return portfolio_value, float(invested), quality


def _rebalance_equal_weight(
    *,
    cash: float,
    open_positions: list[PortfolioPositionState],
    day: str,
    price_histories: dict[str, dict[str, float]],
    sorted_dates: dict[str, list[str]],
    calendar: list[str],
    calendar_indexes: dict[str, int],
    max_stale_price_trading_days: int,
) -> float:
    if not open_positions:
        return cash
    total_value, _, _ = _snapshot(
        cash=cash,
        open_positions=open_positions,
        day=day,
        price_histories=price_histories,
        sorted_dates=sorted_dates,
        calendar=calendar,
        calendar_indexes=calendar_indexes,
        max_stale_price_trading_days=max_stale_price_trading_days,
    )
    priceable_positions = [
        position
        for position in open_positions
        if _resolve_valuation_price(
            symbol=position.symbol,
            day=day,
            price_histories=price_histories,
            sorted_dates=sorted_dates,
            calendar=calendar,
            calendar_indexes=calendar_indexes,
            max_stale_price_trading_days=max_stale_price_trading_days,
        )
        is not None
    ]
    if not priceable_positions:
        return cash
    if total_value <= 0:
        return cash
    target_value = total_value / len(priceable_positions)
    next_cash = total_value
    for position in priceable_positions:
        resolved = _resolve_valuation_price(
            symbol=position.symbol,
            day=day,
            price_histories=price_histories,
            sorted_dates=sorted_dates,
            calendar=calendar,
            calendar_indexes=calendar_indexes,
            max_stale_price_trading_days=max_stale_price_trading_days,
        )
        if resolved is None or resolved.close <= 0:
            continue
        position.shares = target_value / float(resolved.close)
        next_cash -= target_value
    return 0.0 if abs(next_cash) < 0.000001 else float(next_cash)


def _empty_curve_diagnostics(note: str, *, status: str = "good") -> PortfolioCurveDiagnostics:
    return PortfolioCurveDiagnostics(
        flat_segment_count=0,
        longest_flat_segment_days=0,
        longest_problematic_flat_segment_days=0,
        average_exposure_pct=0.0,
        min_exposure_pct=0.0,
        max_exposure_pct=0.0,
        days_with_zero_exposure=0,
        days_with_active_positions_but_zero_exposure=0,
        days_with_active_positions_but_no_valued_positions=0,
        pct_position_days_with_price_gaps=0.0,
        pct_invested_value_with_price_gaps=0.0,
        avg_priced_invested_value_pct=100.0,
        min_priced_invested_value_pct=100.0,
        days_below_90pct_priced_value=0,
        days_below_75pct_priced_value=0,
        days_below_50pct_priced_value=0,
        stale_price_fill_count=0,
        missing_price_fill_count=0,
        positions_marked_to_market_count=0,
        positions_using_stale_price_count=0,
        pct_days_with_price_gaps=0.0,
        curve_quality_status=status,
        curve_quality_notes=[note][:5],
        flat_segments=[],
        suggested_backfill_symbols=[],
    )


def _build_curve_diagnostics(
    *,
    points: list[PortfolioPoint],
    daily_quality: list[_DailyCurveQuality],
    positions_count: int,
    stale_price_fill_count: int,
    missing_price_fill_count: int,
    positions_marked_to_market_count: int,
    stale_position_keys: set[tuple[int | None, str, date]],
) -> PortfolioCurveDiagnostics:
    quality_by_day = {item.day: item for item in daily_quality}
    flat_segments: list[PortfolioFlatSegment] = []
    current_start_index: int | None = None

    for index in range(1, len(points)):
        unchanged = abs(points[index].strategy_value - points[index - 1].strategy_value) <= 0.000001
        if unchanged:
            if current_start_index is None:
                current_start_index = index - 1
            continue
        if current_start_index is not None:
            flat_segments.append(
                _flat_segment_from_points(points, quality_by_day, current_start_index, index - 1)
            )
            current_start_index = None
    if current_start_index is not None:
        flat_segments.append(
            _flat_segment_from_points(points, quality_by_day, current_start_index, len(points) - 1)
        )

    gap_days = {
        item.day
        for item in daily_quality
        if item.stale_symbols or item.missing_symbols
    }
    exposure_values = [float(point.exposure_pct or 0.0) for point in points]
    recorded_days = {point.asof_date.isoformat() for point in points}
    quality_by_recorded_day = {item.day: item for item in daily_quality if item.day in recorded_days}
    days_with_active_positions_but_zero_exposure = sum(
        1 for point in points if int(point.active_positions or 0) > 0 and abs(float(point.exposure_pct or 0.0)) <= 0.000001
    )
    days_with_active_positions_but_no_valued_positions = sum(
        1
        for point in points
        if int(point.active_positions or 0) > 0
        and (quality_by_recorded_day.get(point.asof_date.isoformat()).valued_positions_count if quality_by_recorded_day.get(point.asof_date.isoformat()) else 0)
        == 0
    )
    pct_days_with_price_gaps = _round((len(gap_days) / len(points)) * 100.0, 3) if points else 0.0
    active_position_days = sum(int(point.active_positions or 0) for point in points)
    position_gap_days = stale_price_fill_count + missing_price_fill_count
    pct_position_days_with_price_gaps = _round((position_gap_days / active_position_days) * 100.0, 3) if active_position_days else 0.0
    total_invested_value = sum(max(float(item.invested_value or 0.0), 0.0) for item in daily_quality)
    total_gap_value = sum(max(float(item.price_gap_invested_value or 0.0), 0.0) for item in daily_quality)
    total_missing_value = sum(max(float(item.missing_invested_value or 0.0), 0.0) for item in daily_quality)
    pct_invested_value_with_price_gaps = _round((total_gap_value / total_invested_value) * 100.0, 3) if total_invested_value else 0.0
    pct_missing_invested_value = _round((total_missing_value / total_invested_value) * 100.0, 3) if total_invested_value else 0.0
    priced_value_pcts = [float(item.priced_invested_value_pct or 0.0) for item in daily_quality if item.day in recorded_days]
    avg_priced_invested_value_pct = _round(sum(priced_value_pcts) / len(priced_value_pcts)) if priced_value_pcts else 100.0
    min_priced_invested_value_pct = _round(min(priced_value_pcts)) if priced_value_pcts else 100.0
    days_below_90pct_priced_value = sum(1 for value in priced_value_pcts if value < 90.0)
    days_below_75pct_priced_value = sum(1 for value in priced_value_pcts if value < 75.0)
    days_below_50pct_priced_value = sum(1 for value in priced_value_pcts if value < 50.0)
    longest_flat_segment_days = max((segment.trading_days for segment in flat_segments), default=0)
    problematic_flat_types = {"stale_prices", "missing_prices", "mixed"}
    longest_problematic_flat_segment_days = max(
        (segment.trading_days for segment in flat_segments if segment.segment_type in problematic_flat_types),
        default=0,
    )
    gap_symbol_value: dict[str, float] = {}
    for item in daily_quality:
        for symbol, value in (item.price_gap_value_by_symbol or {}).items():
            gap_symbol_value[symbol] = gap_symbol_value.get(symbol, 0.0) + float(value or 0.0)
    suggested_symbols = [
        symbol
        for symbol, _ in sorted(gap_symbol_value.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]

    notes: list[str] = []
    if positions_count == 0:
        notes.append("No simulated holdings were active in this window.")
    if missing_price_fill_count:
        notes.append(f"{missing_price_fill_count} position-day valuations lacked a bounded prior close.")
    if stale_price_fill_count:
        notes.append(f"{stale_price_fill_count} position-day valuations used a bounded stale close.")
    if longest_problematic_flat_segment_days >= 5:
        notes.append(f"Longest stale/missing-price flat segment spans {longest_problematic_flat_segment_days} trading days.")
    if days_with_active_positions_but_zero_exposure:
        notes.append(f"{days_with_active_positions_but_zero_exposure} curve days had active positions but zero exposure.")
    if days_with_active_positions_but_no_valued_positions:
        notes.append(f"{days_with_active_positions_but_no_valued_positions} curve days had active positions but no valued positions.")
    if pct_days_with_price_gaps and pct_days_with_price_gaps >= 5:
        notes.append(
            f"{pct_days_with_price_gaps:.1f}% of curve days had stale or missing holding prices; "
            f"value-weighted gap impact was {pct_invested_value_with_price_gaps:.1f}%."
        )
    if avg_priced_invested_value_pct < 95:
        notes.append(f"Average priced invested value was {avg_priced_invested_value_pct:.1f}%.")
    if not notes:
        notes.append("Curve pricing coverage looks adequate for this run.")

    status = "good"
    if positions_count > 0:
        if (
            longest_problematic_flat_segment_days >= 20
            or ((avg_priced_invested_value_pct or 100.0) < 75.0 and (pct_missing_invested_value or 0.0) >= 25.0)
            or (days_below_50pct_priced_value > 0 and (pct_missing_invested_value or 0.0) >= 10.0)
            or (days_below_75pct_priced_value > max(5, int(len(points) * 0.10)) and (pct_missing_invested_value or 0.0) >= 10.0)
            or days_with_active_positions_but_no_valued_positions
        ):
            status = "poor"
        elif (
            longest_problematic_flat_segment_days >= 5
            or (avg_priced_invested_value_pct or 100.0) < 95.0
            or days_below_90pct_priced_value > 0
            or days_with_active_positions_but_zero_exposure
        ):
            status = "warning"

    gap_dates = [date.fromisoformat(item.day) for item in daily_quality if item.missing_symbols or item.stale_symbols]
    return PortfolioCurveDiagnostics(
        flat_segment_count=len(flat_segments),
        longest_flat_segment_days=longest_flat_segment_days,
        longest_problematic_flat_segment_days=longest_problematic_flat_segment_days,
        average_exposure_pct=_round(sum(exposure_values) / len(exposure_values)) if exposure_values else 0.0,
        min_exposure_pct=_round(min(exposure_values)) if exposure_values else 0.0,
        max_exposure_pct=_round(max(exposure_values)) if exposure_values else 0.0,
        days_with_zero_exposure=sum(1 for value in exposure_values if abs(value) <= 0.000001),
        days_with_active_positions_but_zero_exposure=days_with_active_positions_but_zero_exposure,
        days_with_active_positions_but_no_valued_positions=days_with_active_positions_but_no_valued_positions,
        pct_position_days_with_price_gaps=pct_position_days_with_price_gaps or 0.0,
        pct_invested_value_with_price_gaps=pct_invested_value_with_price_gaps or 0.0,
        avg_priced_invested_value_pct=avg_priced_invested_value_pct or 0.0,
        min_priced_invested_value_pct=min_priced_invested_value_pct or 0.0,
        days_below_90pct_priced_value=days_below_90pct_priced_value,
        days_below_75pct_priced_value=days_below_75pct_priced_value,
        days_below_50pct_priced_value=days_below_50pct_priced_value,
        stale_price_fill_count=stale_price_fill_count,
        missing_price_fill_count=missing_price_fill_count,
        positions_marked_to_market_count=positions_marked_to_market_count,
        positions_using_stale_price_count=len(stale_position_keys),
        pct_days_with_price_gaps=pct_days_with_price_gaps or 0.0,
        curve_quality_status=status,
        curve_quality_notes=notes[:5],
        flat_segments=flat_segments[:20],
        suggested_backfill_symbols=suggested_symbols,
        suggested_backfill_start_date=min(gap_dates) if gap_dates else None,
        suggested_backfill_end_date=max(gap_dates) if gap_dates else None,
    )


def _flat_segment_from_points(
    points: list[PortfolioPoint],
    quality_by_day: dict[str, _DailyCurveQuality],
    start_index: int,
    end_index: int,
) -> PortfolioFlatSegment:
    active_symbols: set[str] = set()
    stale_symbols: set[str] = set()
    missing_symbols: set[str] = set()
    zero_value_symbols: set[str] = set()
    max_active_positions = 0
    max_valued_positions = 0
    max_zero_value_positions = 0
    max_shares_nonzero = 0
    max_market_value_nonzero = 0
    for point in points[start_index : end_index + 1]:
        day_quality = quality_by_day.get(point.asof_date.isoformat())
        if day_quality is not None:
            active_symbols.update(day_quality.active_symbols)
            stale_symbols.update(day_quality.stale_symbols)
            missing_symbols.update(day_quality.missing_symbols)
            zero_value_symbols.update(day_quality.top_zero_value_symbols or [])
            max_valued_positions = max(max_valued_positions, int(day_quality.valued_positions_count or 0))
            max_zero_value_positions = max(max_zero_value_positions, int(day_quality.zero_value_positions_count or 0))
            max_shares_nonzero = max(max_shares_nonzero, int(day_quality.shares_nonzero_count or 0))
            max_market_value_nonzero = max(max_market_value_nonzero, int(day_quality.market_value_nonzero_count or 0))
        max_active_positions = max(max_active_positions, int(point.active_positions or 0))
    start_quality = quality_by_day.get(points[start_index].asof_date.isoformat())
    end_quality = quality_by_day.get(points[end_index].asof_date.isoformat())
    segment_type = _classify_flat_segment(
        max_active_positions=max_active_positions,
        stale_symbols=stale_symbols,
        missing_symbols=missing_symbols,
        start_quality=start_quality,
        end_quality=end_quality,
        start_point=points[start_index],
        end_point=points[end_index],
    )
    return PortfolioFlatSegment(
        start_date=points[start_index].asof_date,
        end_date=points[end_index].asof_date,
        trading_days=end_index - start_index + 1,
        segment_type=segment_type,
        active_positions=max_active_positions,
        active_positions_count=max_active_positions,
        valued_positions_count=max_valued_positions,
        zero_value_positions_count=max_zero_value_positions,
        total_shares_nonzero_count=max_shares_nonzero,
        total_market_value_nonzero_count=max_market_value_nonzero,
        active_symbols=sorted(active_symbols)[:25],
        stale_symbols=sorted(stale_symbols)[:25],
        missing_symbols=sorted(missing_symbols)[:25],
        portfolio_value_start=start_quality.portfolio_value if start_quality else points[start_index].strategy_value,
        portfolio_value_end=end_quality.portfolio_value if end_quality else points[end_index].strategy_value,
        cash_value_start=start_quality.cash_value if start_quality else None,
        cash_value_end=end_quality.cash_value if end_quality else None,
        invested_value_start=start_quality.invested_value if start_quality else None,
        invested_value_end=end_quality.invested_value if end_quality else None,
        exposure_pct_start=start_quality.exposure_pct if start_quality else points[start_index].exposure_pct,
        exposure_pct_end=end_quality.exposure_pct if end_quality else points[end_index].exposure_pct,
        top_positions_by_market_value_start=(start_quality.top_positions_by_market_value or []) if start_quality else [],
        top_positions_by_market_value_end=(end_quality.top_positions_by_market_value or []) if end_quality else [],
        top_zero_value_symbols=sorted(zero_value_symbols)[:10],
        legitimate_no_holdings=max_active_positions == 0,
    )


def _material_value_change(start: float | None, end: float | None, *, portfolio_value: float | None) -> bool:
    if start is None or end is None:
        return False
    threshold = max(abs(float(portfolio_value or 0.0)) * 0.001, 1.0)
    return abs(float(end) - float(start)) > threshold


def _classify_flat_segment(
    *,
    max_active_positions: int,
    stale_symbols: set[str],
    missing_symbols: set[str],
    start_quality: _DailyCurveQuality | None,
    end_quality: _DailyCurveQuality | None,
    start_point: PortfolioPoint,
    end_point: PortfolioPoint,
) -> str:
    if max_active_positions == 0:
        return "no_holdings"

    has_stale = bool(stale_symbols)
    has_missing = bool(missing_symbols)
    if has_stale and has_missing:
        return "mixed"
    if has_missing:
        return "missing_prices"
    if has_stale:
        return "stale_prices"

    portfolio_value = start_quality.portfolio_value if start_quality else start_point.strategy_value
    cash_changed = _material_value_change(
        start_quality.cash_value if start_quality else None,
        end_quality.cash_value if end_quality else None,
        portfolio_value=portfolio_value,
    )
    invested_changed = _material_value_change(
        start_quality.invested_value if start_quality else None,
        end_quality.invested_value if end_quality else None,
        portfolio_value=portfolio_value,
    )
    exposure_changed = abs(float(end_point.exposure_pct or 0.0) - float(start_point.exposure_pct or 0.0)) > 1.0
    if cash_changed or invested_changed or exposure_changed:
        return "portfolio_build_up"
    return "true_flat_value"


def curve_diagnostics_payload(diagnostics: PortfolioCurveDiagnostics) -> dict[str, Any]:
    return {
        "flat_segment_count": diagnostics.flat_segment_count,
        "longest_flat_segment_days": diagnostics.longest_flat_segment_days,
        "longest_problematic_flat_segment_days": diagnostics.longest_problematic_flat_segment_days,
        "average_exposure_pct": diagnostics.average_exposure_pct,
        "min_exposure_pct": diagnostics.min_exposure_pct,
        "max_exposure_pct": diagnostics.max_exposure_pct,
        "days_with_zero_exposure": diagnostics.days_with_zero_exposure,
        "days_with_active_positions_but_zero_exposure": diagnostics.days_with_active_positions_but_zero_exposure,
        "days_with_active_positions_but_no_valued_positions": diagnostics.days_with_active_positions_but_no_valued_positions,
        "pct_position_days_with_price_gaps": diagnostics.pct_position_days_with_price_gaps,
        "pct_invested_value_with_price_gaps": diagnostics.pct_invested_value_with_price_gaps,
        "avg_priced_invested_value_pct": diagnostics.avg_priced_invested_value_pct,
        "min_priced_invested_value_pct": diagnostics.min_priced_invested_value_pct,
        "days_below_90pct_priced_value": diagnostics.days_below_90pct_priced_value,
        "days_below_75pct_priced_value": diagnostics.days_below_75pct_priced_value,
        "days_below_50pct_priced_value": diagnostics.days_below_50pct_priced_value,
        "stale_price_fill_count": diagnostics.stale_price_fill_count,
        "missing_price_fill_count": diagnostics.missing_price_fill_count,
        "positions_marked_to_market_count": diagnostics.positions_marked_to_market_count,
        "positions_using_stale_price_count": diagnostics.positions_using_stale_price_count,
        "pct_days_with_price_gaps": diagnostics.pct_days_with_price_gaps,
        "curve_quality_status": diagnostics.curve_quality_status,
        "curve_quality_notes": diagnostics.curve_quality_notes[:5],
        "data_coverage_notes": diagnostics.curve_quality_notes[:5],
        "flat_segments": [
            {
                "start_date": segment.start_date.isoformat(),
                "end_date": segment.end_date.isoformat(),
                "trading_days": segment.trading_days,
                "segment_type": segment.segment_type,
                "active_positions": segment.active_positions,
                "portfolio_value_start": segment.portfolio_value_start,
                "portfolio_value_end": segment.portfolio_value_end,
                "cash_value_start": segment.cash_value_start,
                "cash_value_end": segment.cash_value_end,
                "invested_value_start": segment.invested_value_start,
                "invested_value_end": segment.invested_value_end,
                "exposure_pct_start": segment.exposure_pct_start,
                "exposure_pct_end": segment.exposure_pct_end,
                "active_positions_count": segment.active_positions_count,
                "valued_positions_count": segment.valued_positions_count,
                "zero_value_positions_count": segment.zero_value_positions_count,
                "total_shares_nonzero_count": segment.total_shares_nonzero_count,
                "total_market_value_nonzero_count": segment.total_market_value_nonzero_count,
                "top_positions_by_market_value_start": segment.top_positions_by_market_value_start or [],
                "top_positions_by_market_value_end": segment.top_positions_by_market_value_end or [],
                "top_zero_value_symbols": segment.top_zero_value_symbols or [],
                "active_symbols": segment.active_symbols,
                "stale_symbols": segment.stale_symbols,
                "missing_symbols": segment.missing_symbols,
                "legitimate_no_holdings": segment.legitimate_no_holdings,
            }
            for segment in diagnostics.flat_segments
        ],
        "suggested_backfill_symbols": diagnostics.suggested_backfill_symbols,
        "suggested_backfill_start_date": diagnostics.suggested_backfill_start_date.isoformat()
        if diagnostics.suggested_backfill_start_date
        else None,
        "suggested_backfill_end_date": diagnostics.suggested_backfill_end_date.isoformat()
        if diagnostics.suggested_backfill_end_date
        else None,
    }


def curve_debug_daily_payload(
    simulation: PortfolioSimulation,
    *,
    start_date: date,
    end_date: date,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = []
    for item in simulation.daily_quality:
        day = date.fromisoformat(item.day)
        if day < start_date or day > end_date:
            continue
        rows.append(
            {
                "date": item.day,
                "portfolio_value": item.portfolio_value,
                "cash_value": item.cash_value,
                "invested_value": item.invested_value,
                "exposure_pct": item.exposure_pct,
                "priced_invested_value": item.priced_invested_value,
                "price_gap_invested_value": item.price_gap_invested_value,
                "stale_invested_value": item.stale_invested_value,
                "missing_invested_value": item.missing_invested_value,
                "priced_invested_value_pct": item.priced_invested_value_pct,
                "active_positions_count": item.active_positions_count,
                "valued_positions_count": item.valued_positions_count,
                "stale_symbols": item.stale_symbols[:10],
                "missing_symbols": item.missing_symbols[:10],
                "top_price_gap_symbols": [
                    {"symbol": symbol, "market_value_impact": value}
                    for symbol, value in sorted(
                        (item.price_gap_value_by_symbol or {}).items(),
                        key=lambda entry: (-float(entry[1] or 0.0), entry[0]),
                    )[:10]
                ],
            }
        )
        if len(rows) >= max(limit, 1):
            break
    return rows


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
    max_stale_price_trading_days: int = DEFAULT_MAX_STALE_PRICE_TRADING_DAYS,
    warmup_start_date: date | None = None,
) -> PortfolioSimulation:
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"Unsupported portfolio mode: {mode}")

    simulation_start_date = min(warmup_start_date or start_date, start_date)
    calendar = _trading_calendar(
        benchmark_history=benchmark_history,
        price_histories=price_histories,
        start_date=simulation_start_date,
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
        warmup_start_date=simulation_start_date if simulation_start_date < start_date else None,
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
        return PortfolioSimulation(
            summary=summary,
            points=[],
            positions=[],
            skipped=skipped,
            coverage=coverage,
            curve_diagnostics=_empty_curve_diagnostics("No trading calendar could be built.", status="poor"),
            daily_quality=[],
            effective_window=PortfolioEffectiveWindow(
                requested_start_date=start_date,
                effective_start_date=None,
                effective_end_date=None,
                effective_window_days=0,
                effective_window_reason="no_chart_points",
                no_active_holdings=True,
            ),
            warmup_diagnostics=PortfolioWarmupDiagnostics(
                warmup_start_date=simulation_start_date,
                visible_start_date=start_date,
                warmup_days=max((start_date - simulation_start_date).days, 0),
                opening_positions_count=0,
                sale_without_position_before_warmup=_visible_sale_without_position_count(
                    events,
                    start_date=start_date,
                    end_date=end_date,
                    mode=mode,
                ),
                sale_without_position_after_warmup=0,
                opening_position_estimated=False,
                estimated_opening_positions_count=0,
                estimated_opening_positions_symbols=[],
                estimated_opening_positions_value=0.0,
                sale_without_position_before_estimation=0,
                sale_without_position_after_estimation=0,
            ),
        )

    sorted_symbol_dates = {symbol: sorted_price_dates(history) for symbol, history in price_histories.items()}
    calendar_indexes = {day: index for index, day in enumerate(calendar)}
    skipped: list[PortfolioSkip] = []
    events_by_day: dict[str, list[PortfolioTradeEvent]] = {}
    sale_without_position_before_warmup = _visible_sale_without_position_count(
        events,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
    )
    for event in sorted(events, key=lambda item: (event_effective_date(item, mode), item.event_id or 0)):
        history = price_histories.get(event.symbol, {})
        if not history:
            skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "missing_price_history"))
            continue
        resolved = nearest_price_on_date(
            event_effective_date(event, mode),
            history,
            prefer_previous=True,
            max_backward_trading_days=max_stale_price_trading_days,
            max_forward_trading_days=max_stale_price_trading_days,
        )
        if resolved is None or resolved.close <= 0:
            skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
            continue
        if resolved.date < simulation_start_date or resolved.date > end_date:
            continue
        events_by_day.setdefault(resolved.date.isoformat(), []).append(event)

    cash = float(starting_value)
    positions: list[PortfolioPositionState] = []
    points: list[PortfolioPoint] = []
    previous_value = float(starting_value)
    daily_quality: list[_DailyCurveQuality] = []
    stale_price_fill_count = 0
    missing_price_fill_count = 0
    positions_marked_to_market_count = 0
    stale_position_keys: set[tuple[int | None, str, date]] = set()
    rebased_at_recording_start = False
    opening_positions_count = 0
    opening_positions_captured = False
    sale_without_position_after_warmup = 0
    sale_without_position_before_estimation = 0
    sale_without_position_after_estimation = 0
    estimated_opening_positions_count = 0
    estimated_opening_positions_value = 0.0
    estimated_opening_positions_symbols: set[str] = set()

    benchmark_base = first_price_on_or_after(start_date, benchmark_history)

    def estimated_opening_candidates(initial_open_positions: list[PortfolioPositionState]) -> list[PortfolioTradeEvent]:
        open_counts: dict[str, int] = {}
        for position in initial_open_positions:
            open_counts[position.symbol] = open_counts.get(position.symbol, 0) + 1
        candidates: list[PortfolioTradeEvent] = []
        seen_candidate_keys: set[tuple[str, str, float | None, float | None]] = set()
        visible_days = [day for day in calendar if start_date.isoformat() <= day <= end_date.isoformat()]
        for visible_day in visible_days:
            for event in [item for item in events_by_day.get(visible_day, []) if item.side == "sale"]:
                open_count = open_counts.get(event.symbol, 0)
                if open_count <= 0:
                    candidate_key = (event.symbol, visible_day, event.amount_min, event.amount_max)
                    if candidate_key not in seen_candidate_keys:
                        candidates.append(event)
                        seen_candidate_keys.add(candidate_key)
                else:
                    open_counts[event.symbol] = open_count - 1
            for event in [item for item in events_by_day.get(visible_day, []) if item.side == "purchase"]:
                open_counts[event.symbol] = open_counts.get(event.symbol, 0) + 1
        return candidates

    for day in calendar:
        day_events = events_by_day.get(day, [])
        open_positions = [position for position in positions if position.status == "open"]
        is_recorded_day = day >= start_date.isoformat()
        if is_recorded_day and not opening_positions_captured:
            opening_positions_count = len(open_positions)
            candidates = estimated_opening_candidates(open_positions)
            sale_without_position_before_estimation = len(candidates)
            for event in candidates:
                opening_value = _trade_amount_midpoint(event)
                basis_price = _visible_start_mark_price(
                    symbol=event.symbol,
                    visible_start_date=start_date,
                    price_histories=price_histories,
                    max_stale_price_trading_days=max_stale_price_trading_days,
                )
                if opening_value is None or opening_value <= 0 or basis_price is None or basis_price <= 0:
                    continue
                position = PortfolioPositionState(
                    event_id=event.event_id,
                    symbol=event.symbol,
                    side="estimated_opening_position",
                    entry_date=start_date,
                    entry_price=float(basis_price),
                    shares=float(opening_value) / float(basis_price),
                    amount_min=event.amount_min,
                    amount_max=event.amount_max,
                    source_type="estimated_opening_position",
                    source_reason="prior_acquisition_not_found_in_available_disclosures",
                    confidence="estimated",
                    estimated_opening_value=float(opening_value),
                    estimated_source_event_id=event.event_id,
                )
                positions.append(position)
                open_positions.append(position)
                estimated_opening_positions_count += 1
                estimated_opening_positions_value += float(opening_value)
                estimated_opening_positions_symbols.add(event.symbol)
            opening_positions_captured = True

        for event in [item for item in day_events if item.side == "sale"]:
            matching = [position for position in open_positions if position.symbol == event.symbol and position.status == "open"]
            if not matching:
                if is_recorded_day:
                    sale_without_position_after_warmup += 1
                    sale_without_position_after_estimation += 1
                    skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "sale_without_known_prior_position"))
                else:
                    # Warmup-only unmatched sales explain why no opening holding can be reconstructed,
                    # but they are outside the selected visible window and should not inflate public skips.
                    pass
                continue
            position = sorted(matching, key=lambda item: (item.entry_date, item.event_id or 0))[0]
            resolved = _resolve_valuation_price(
                symbol=position.symbol,
                day=day,
                price_histories=price_histories,
                sorted_dates=sorted_symbol_dates,
                calendar=calendar,
                calendar_indexes=calendar_indexes,
                max_stale_price_trading_days=max_stale_price_trading_days,
            )
            if resolved is None or resolved.close <= 0:
                skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
                continue
            cash += position.shares * float(resolved.close)
            position.exit_date = date.fromisoformat(day)
            position.exit_price = float(resolved.close)
            position.status = "closed"
            open_positions = [item for item in open_positions if item.status == "open"]

        buy_events = [item for item in day_events if item.side == "purchase"]
        for event in buy_events:
            resolved = _resolve_valuation_price(
                symbol=event.symbol,
                day=day,
                price_histories=price_histories,
                sorted_dates=sorted_symbol_dates,
                calendar=calendar,
                calendar_indexes=calendar_indexes,
                max_stale_price_trading_days=max_stale_price_trading_days,
            )
            if resolved is None or resolved.close <= 0:
                skipped.append(PortfolioSkip(event.event_id, event.symbol, event.side, "no_execution_price"))
                continue
            position = PortfolioPositionState(
                event_id=event.event_id,
                symbol=event.symbol,
                side=event.side,
                entry_date=date.fromisoformat(day),
                entry_price=float(resolved.close),
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
                calendar=calendar,
                calendar_indexes=calendar_indexes,
                max_stale_price_trading_days=max_stale_price_trading_days,
            )

        strategy_value, invested_value, day_quality = _snapshot(
            cash=cash,
            open_positions=open_positions,
            day=day,
            price_histories=price_histories,
            sorted_dates=sorted_symbol_dates,
            calendar=calendar,
            calendar_indexes=calendar_indexes,
            max_stale_price_trading_days=max_stale_price_trading_days,
        )
        if is_recorded_day and not rebased_at_recording_start:
            if strategy_value > 0:
                scale = float(starting_value) / float(strategy_value)
                cash *= scale
                for position in open_positions:
                    position.shares *= scale
                strategy_value, invested_value, day_quality = _snapshot(
                    cash=cash,
                    open_positions=open_positions,
                    day=day,
                    price_histories=price_histories,
                    sorted_dates=sorted_symbol_dates,
                    calendar=calendar,
                    calendar_indexes=calendar_indexes,
                    max_stale_price_trading_days=max_stale_price_trading_days,
                )
            previous_value = float(strategy_value)
            rebased_at_recording_start = True
        if not is_recorded_day:
            continue
        daily_quality.append(day_quality)
        stale_price_fill_count += day_quality.stale_position_count
        missing_price_fill_count += day_quality.missing_position_count
        positions_marked_to_market_count += day_quality.marked_to_market_count
        for position in open_positions:
            if position.symbol in day_quality.stale_symbols:
                stale_position_keys.add((position.event_id, position.symbol, position.entry_date))
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
        resolved = _resolve_valuation_price(
            symbol=position.symbol,
            day=last_day,
            price_histories=price_histories,
            sorted_dates=sorted_symbol_dates,
            calendar=calendar,
            calendar_indexes=calendar_indexes,
            max_stale_price_trading_days=max_stale_price_trading_days,
        )
        if resolved is None or resolved.close <= 0:
            continue
        if position.status == "open":
            position.exit_price = float(resolved.close)
        position.market_value = position.shares * float(resolved.close)  # type: ignore[attr-defined]

    raw_points = points
    effective_window = _effective_window_from_points(points=raw_points, requested_start_date=start_date)
    effective_points = _rebase_points_to_effective_window(
        points=raw_points,
        effective_window=effective_window,
        starting_value=starting_value,
    )
    summary = (
        _summary_for_no_active_holdings(
            starting_value=starting_value,
            positions=positions,
            skipped_events_count=len(skipped),
        )
        if effective_window.no_active_holdings
        else _summary_from_effective_points(
            points=effective_points,
            positions=positions,
            skipped_events_count=len(skipped),
            starting_value=starting_value,
        )
    )
    curve_diagnostics = _build_curve_diagnostics(
        points=raw_points,
        daily_quality=daily_quality,
        positions_count=len(positions),
        stale_price_fill_count=stale_price_fill_count,
        missing_price_fill_count=missing_price_fill_count,
        positions_marked_to_market_count=positions_marked_to_market_count,
        stale_position_keys=stale_position_keys,
    )
    warmup_diagnostics = PortfolioWarmupDiagnostics(
        warmup_start_date=simulation_start_date,
        visible_start_date=start_date,
        warmup_days=max((start_date - simulation_start_date).days, 0),
        opening_positions_count=opening_positions_count,
        sale_without_position_before_warmup=sale_without_position_before_warmup,
        sale_without_position_after_warmup=sale_without_position_after_estimation,
        opening_position_estimated=estimated_opening_positions_count > 0,
        estimated_opening_positions_count=estimated_opening_positions_count,
        estimated_opening_positions_symbols=sorted(estimated_opening_positions_symbols),
        estimated_opening_positions_value=estimated_opening_positions_value,
        sale_without_position_before_estimation=sale_without_position_before_estimation,
        sale_without_position_after_estimation=sale_without_position_after_estimation,
    )
    return PortfolioSimulation(
        summary=summary,
        points=effective_points,
        positions=positions,
        skipped=skipped,
        coverage=coverage,
        curve_diagnostics=curve_diagnostics,
        daily_quality=daily_quality,
        effective_window=effective_window,
        warmup_diagnostics=warmup_diagnostics,
    )


def load_replicated_portfolio_events(
    db: Session,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    issuer: str | None = None,
    end_date: date | None = None,
    warmup_days: int = 0,
) -> tuple[list[PortfolioTradeEvent], list[PortfolioSkip]]:
    end = end_date or datetime.now(timezone.utc).date()
    window_start = datetime.combine(
        end - timedelta(days=max(lookback_days, 1) + max(warmup_days, 0) + 14),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
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

        portfolio_event, skip = _portfolio_event_from_event(event, entity_type=entity_type, entity_id=entity_id, db=db)
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
    warmup_days: int | None = None,
) -> PortfolioSimulation:
    end = end_date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(lookback_days, 1))
    effective_warmup_days = default_warmup_days_for_lookback(lookback_days) if warmup_days is None else max(warmup_days, 0)
    warmup_start = start - timedelta(days=effective_warmup_days) if effective_warmup_days else start
    events, loader_skips = load_replicated_portfolio_events(
        db,
        entity_type=entity_type,
        entity_id=entity_id,
        lookback_days=lookback_days,
        issuer=issuer,
        end_date=end,
        warmup_days=effective_warmup_days,
    )
    symbols = sorted({event.symbol for event in events})
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"
    histories = load_price_histories(db, symbols + [benchmark_symbol], warmup_start, end)
    benchmark_history = histories.pop(benchmark_symbol, {})
    simulation = simulate_replicated_portfolio(
        events=events,
        price_histories=histories,
        benchmark_history=benchmark_history,
        start_date=start,
        end_date=end,
        mode=mode,
        benchmark_symbol=benchmark_symbol,
        warmup_start_date=warmup_start if effective_warmup_days else None,
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
        curve_diagnostics=simulation.curve_diagnostics,
        daily_quality=simulation.daily_quality,
        effective_window=simulation.effective_window,
        warmup_diagnostics=simulation.warmup_diagnostics,
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
        status_message=json.dumps(
            {
                "curve_diagnostics": curve_diagnostics_payload(simulation.curve_diagnostics),
                "data_coverage_notes": simulation.curve_diagnostics.curve_quality_notes[:5],
                "effective_window": effective_window_payload(simulation.effective_window),
                "warmup_diagnostics": warmup_diagnostics_payload(simulation.warmup_diagnostics),
            },
            sort_keys=True,
        ),
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


def _fallback_curve_diagnostics_from_persisted_points(
    *,
    points: list[ReplicatedPortfolioPoint],
    positions_count: int,
) -> dict[str, Any]:
    portfolio_points = [
        PortfolioPoint(
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
        for point in points
    ]
    diagnostics = _build_curve_diagnostics(
        points=portfolio_points,
        daily_quality=[
            _DailyCurveQuality(
                day=point.asof_date.isoformat(),
                active_symbols=[],
                stale_symbols=[],
                missing_symbols=[],
                marked_to_market_count=int(point.active_positions or 0),
                active_positions_count=int(point.active_positions or 0),
                portfolio_value=float(point.strategy_value or 0.0),
                cash_value=float(point.strategy_value or 0.0) * float(point.cash_pct or 0.0) / 100.0,
                invested_value=float(point.strategy_value or 0.0) * float(point.exposure_pct or 0.0) / 100.0,
                exposure_pct=float(point.exposure_pct or 0.0),
                valued_positions_count=int(point.active_positions or 0),
                zero_value_positions_count=int(point.active_positions or 0) if abs(float(point.exposure_pct or 0.0)) <= 0.000001 else 0,
                shares_nonzero_count=int(point.active_positions or 0) if abs(float(point.exposure_pct or 0.0)) > 0.000001 else 0,
                market_value_nonzero_count=int(point.active_positions or 0) if abs(float(point.exposure_pct or 0.0)) > 0.000001 else 0,
                priced_invested_value=float(point.strategy_value or 0.0) * float(point.exposure_pct or 0.0) / 100.0,
                priced_invested_value_pct=100.0,
            )
            for point in points
        ],
        positions_count=positions_count,
        stale_price_fill_count=0,
        missing_price_fill_count=0,
        positions_marked_to_market_count=sum(int(point.active_positions or 0) for point in points),
        stale_position_keys=set(),
    )
    payload = curve_diagnostics_payload(diagnostics)
    if positions_count > 0 and payload["curve_quality_status"] == "good" and payload["longest_flat_segment_days"] >= 5:
        payload["curve_quality_status"] = "warning"
        payload["curve_quality_notes"] = [
            f"Persisted curve has a flat segment spanning {payload['longest_flat_segment_days']} trading days; rerun with curve diagnostics for price-gap details."
        ][:5]
        payload["data_coverage_notes"] = payload["curve_quality_notes"]
    return payload


def _curve_diagnostics_from_status_message(
    status_message: str | None,
    *,
    points: list[ReplicatedPortfolioPoint],
    positions_count: int,
) -> dict[str, Any]:
    parsed = _status_message_payload(status_message)
    diagnostics = parsed.get("curve_diagnostics") if isinstance(parsed, dict) else None
    if isinstance(diagnostics, dict):
        return diagnostics
    return _fallback_curve_diagnostics_from_persisted_points(points=points, positions_count=positions_count)


def _status_message_payload(status_message: str | None) -> dict[str, Any]:
    if not status_message:
        return {}
    try:
        parsed = json.loads(status_message)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _persisted_effective_window_payload(
    run: ReplicatedPortfolioRun,
    points: list[ReplicatedPortfolioPoint],
    status_payload: dict[str, Any],
) -> dict[str, Any]:
    payload = status_payload.get("effective_window")
    if isinstance(payload, dict):
        return {
            "requested_start_date": payload.get("requested_start_date") or (run.start_date.isoformat() if run.start_date else None),
            "effective_start_date": payload.get("effective_start_date"),
            "effective_end_date": payload.get("effective_end_date"),
            "effective_window_days": int(payload.get("effective_window_days") or 0),
            "effective_window_reason": payload.get("effective_window_reason") or "unknown",
            "no_active_holdings": bool(payload.get("no_active_holdings")),
        }
    return {
        "requested_start_date": run.start_date.isoformat() if run.start_date else None,
        "effective_start_date": points[0].asof_date.isoformat() if points else None,
        "effective_end_date": points[-1].asof_date.isoformat() if points else None,
        "effective_window_days": max((points[-1].asof_date - points[0].asof_date).days, 0) if points else 0,
        "effective_window_reason": "legacy_persisted_run",
        "no_active_holdings": not points,
    }


def _persisted_warmup_diagnostics_payload(
    run: ReplicatedPortfolioRun,
    status_payload: dict[str, Any],
    positions: list[ReplicatedPortfolioPosition],
) -> dict[str, Any]:
    payload = status_payload.get("warmup_diagnostics")
    if isinstance(payload, dict):
        return {
            "warmup_start_date": payload.get("warmup_start_date"),
            "visible_start_date": payload.get("visible_start_date") or (run.start_date.isoformat() if run.start_date else None),
            "warmup_days": int(payload.get("warmup_days") or 0),
            "opening_positions_count": int(payload.get("opening_positions_count") or 0),
            "sale_without_position_before_warmup": int(payload.get("sale_without_position_before_warmup") or 0),
            "sale_without_position_after_warmup": int(payload.get("sale_without_position_after_warmup") or 0),
            "opening_position_estimated": bool(payload.get("opening_position_estimated")),
            "estimated_opening_positions_count": int(payload.get("estimated_opening_positions_count") or 0),
            "estimated_opening_positions_symbols": list(payload.get("estimated_opening_positions_symbols") or []),
            "estimated_opening_positions_value": float(payload.get("estimated_opening_positions_value") or 0.0),
            "sale_without_position_before_estimation": int(payload.get("sale_without_position_before_estimation") or 0),
            "sale_without_position_after_estimation": int(payload.get("sale_without_position_after_estimation") or payload.get("sale_without_position_after_warmup") or 0),
        }
    opening_positions_count = sum(
        1
        for position in positions
        if position.status != "skipped"
        and position.entry_date is not None
        and run.start_date is not None
        and position.entry_date <= run.start_date
        and (position.exit_date is None or position.exit_date >= run.start_date)
    )
    return {
        "warmup_start_date": None,
        "visible_start_date": run.start_date.isoformat() if run.start_date else None,
        "warmup_days": 0,
        "opening_positions_count": opening_positions_count,
        "sale_without_position_before_warmup": 0,
        "sale_without_position_after_warmup": 0,
        "opening_position_estimated": False,
        "estimated_opening_positions_count": 0,
        "estimated_opening_positions_symbols": [],
        "estimated_opening_positions_value": 0.0,
        "sale_without_position_before_estimation": 0,
        "sale_without_position_after_estimation": 0,
    }


def _skip_from_persisted_position(position: ReplicatedPortfolioPosition) -> PortfolioSkip:
    return PortfolioSkip(
        event_id=position.source_event_id,
        symbol=position.symbol,
        side=position.side,
        reason=position.skip_reason or "unknown",
    )


def _skip_reason_summary_from_positions(positions: list[ReplicatedPortfolioPosition]) -> dict[str, int]:
    return skip_reason_summary(
        [_skip_from_persisted_position(position) for position in positions if position.status == "skipped"]
    )


def _skip_diagnostic_summary_from_positions(positions: list[ReplicatedPortfolioPosition]) -> dict[str, int]:
    return skip_diagnostic_summary(
        [_skip_from_persisted_position(position) for position in positions if position.status == "skipped"]
    )


def _event_context_by_id(db: Session, positions: list[ReplicatedPortfolioPosition]) -> dict[int, dict[str, str | None]]:
    event_ids = sorted(
        {
            int(position.source_event_id)
            for position in positions
            if position.source_event_id is not None
        }
    )
    if not event_ids:
        return {}
    events = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
    context: dict[int, dict[str, str | None]] = {}
    for event in events:
        payload = parse_payload(event.payload_json)
        transaction_date = _event_transaction_date(event, payload)
        public_date = _event_public_date(event, payload)
        context[int(event.id)] = {
            "trade_date": transaction_date.isoformat() if transaction_date else None,
            "report_date": public_date.isoformat() if public_date else None,
        }
    return context


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
            "requested_start_date": None,
            "effective_start_date": None,
            "effective_end_date": None,
            "effective_window_days": 0,
            "effective_window_reason": "no_persisted_run",
            "no_active_holdings": False,
            "summary": None,
            "points": [],
            "positions": [],
            "skip_reason_summary": {},
            "skip_diagnostics": skip_diagnostic_summary([]),
            "warmup_diagnostics": {
                "warmup_start_date": None,
                "visible_start_date": None,
                "warmup_days": 0,
                "opening_positions_count": 0,
                "sale_without_position_before_warmup": 0,
                "sale_without_position_after_warmup": 0,
                "opening_position_estimated": False,
                "estimated_opening_positions_count": 0,
                "estimated_opening_positions_symbols": [],
                "estimated_opening_positions_value": 0.0,
                "sale_without_position_before_estimation": 0,
                "sale_without_position_after_estimation": 0,
            },
            "curve_quality_status": "good",
            "longest_flat_segment_days": 0,
            "pct_days_with_price_gaps": 0.0,
            "data_coverage_notes": [],
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
    event_context = _event_context_by_id(db, positions)
    curve_diagnostics = _curve_diagnostics_from_status_message(
        run.status_message,
        points=points,
        positions_count=run.positions_count,
    )
    status_payload = _status_message_payload(run.status_message)
    effective_window = _persisted_effective_window_payload(run, points, status_payload)
    warmup_diagnostics = _persisted_warmup_diagnostics_payload(run, status_payload, positions)
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
        "requested_start_date": effective_window["requested_start_date"],
        "effective_start_date": effective_window["effective_start_date"],
        "effective_end_date": effective_window["effective_end_date"],
        "effective_window_days": effective_window["effective_window_days"],
        "effective_window_reason": effective_window["effective_window_reason"],
        "no_active_holdings": effective_window["no_active_holdings"],
        "warmup_diagnostics": warmup_diagnostics,
        "warmup_start_date": warmup_diagnostics["warmup_start_date"],
        "visible_start_date": warmup_diagnostics["visible_start_date"],
        "opening_positions_count": warmup_diagnostics["opening_positions_count"],
        "sale_without_position_before_warmup": warmup_diagnostics["sale_without_position_before_warmup"],
        "sale_without_position_after_warmup": warmup_diagnostics["sale_without_position_after_warmup"],
        "opening_position_estimated": warmup_diagnostics["opening_position_estimated"],
        "estimated_opening_positions_count": warmup_diagnostics["estimated_opening_positions_count"],
        "estimated_opening_positions_symbols": warmup_diagnostics["estimated_opening_positions_symbols"],
        "estimated_opening_positions_value": warmup_diagnostics["estimated_opening_positions_value"],
        "sale_without_position_before_estimation": warmup_diagnostics["sale_without_position_before_estimation"],
        "sale_without_position_after_estimation": warmup_diagnostics["sale_without_position_after_estimation"],
        "computed_at": run.computed_at.isoformat() if run.computed_at else None,
        "methodology_version": run.methodology_version,
        "flat_segment_count": curve_diagnostics.get("flat_segment_count", 0),
        "longest_flat_segment_days": curve_diagnostics.get("longest_flat_segment_days", 0),
        "longest_problematic_flat_segment_days": curve_diagnostics.get("longest_problematic_flat_segment_days", 0),
        "average_exposure_pct": curve_diagnostics.get("average_exposure_pct", run.average_exposure_pct),
        "min_exposure_pct": curve_diagnostics.get("min_exposure_pct", 0.0),
        "max_exposure_pct": curve_diagnostics.get("max_exposure_pct", 0.0),
        "days_with_zero_exposure": curve_diagnostics.get("days_with_zero_exposure", 0),
        "days_with_active_positions_but_zero_exposure": curve_diagnostics.get("days_with_active_positions_but_zero_exposure", 0),
        "days_with_active_positions_but_no_valued_positions": curve_diagnostics.get("days_with_active_positions_but_no_valued_positions", 0),
        "pct_position_days_with_price_gaps": curve_diagnostics.get("pct_position_days_with_price_gaps", 0.0),
        "pct_invested_value_with_price_gaps": curve_diagnostics.get("pct_invested_value_with_price_gaps", 0.0),
        "avg_priced_invested_value_pct": curve_diagnostics.get("avg_priced_invested_value_pct", 100.0),
        "min_priced_invested_value_pct": curve_diagnostics.get("min_priced_invested_value_pct", 100.0),
        "days_below_90pct_priced_value": curve_diagnostics.get("days_below_90pct_priced_value", 0),
        "days_below_75pct_priced_value": curve_diagnostics.get("days_below_75pct_priced_value", 0),
        "days_below_50pct_priced_value": curve_diagnostics.get("days_below_50pct_priced_value", 0),
        "stale_price_fill_count": curve_diagnostics.get("stale_price_fill_count", 0),
        "missing_price_fill_count": curve_diagnostics.get("missing_price_fill_count", 0),
        "positions_marked_to_market_count": curve_diagnostics.get("positions_marked_to_market_count", 0),
        "positions_using_stale_price_count": curve_diagnostics.get("positions_using_stale_price_count", 0),
        "pct_days_with_price_gaps": curve_diagnostics.get("pct_days_with_price_gaps", 0.0),
        "curve_quality_status": curve_diagnostics.get("curve_quality_status", "good"),
        "curve_quality_notes": curve_diagnostics.get("curve_quality_notes", [])[:5],
        "data_coverage_notes": curve_diagnostics.get("data_coverage_notes", curve_diagnostics.get("curve_quality_notes", []))[:5],
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
            "skip_reason_summary": _skip_reason_summary_from_positions(positions),
            "skip_diagnostics": _skip_diagnostic_summary_from_positions(positions),
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
                "trade_date": event_context.get(int(position.source_event_id or 0), {}).get("trade_date"),
                "report_date": event_context.get(int(position.source_event_id or 0), {}).get("report_date"),
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "shares": position.shares,
                "market_value": position.market_value,
                "return_pct": position.return_pct,
                "amount_min": position.amount_min,
                "amount_max": position.amount_max,
                "status": position.status,
                "skip_reason": position.skip_reason,
                "skip_category": skip_diagnostic_category(_skip_from_persisted_position(position))
                if position.status == "skipped"
                else None,
                "source_type": "estimated_opening_position"
                if position.side == "estimated_opening_position"
                else "disclosed_trade",
                "source_reason": "prior_acquisition_not_found_in_available_disclosures"
                if position.side == "estimated_opening_position"
                else None,
                "confidence": "estimated" if position.side == "estimated_opening_position" else None,
                "estimated_opening_value": (
                    ((position.amount_min or 0) + (position.amount_max or 0)) / 2.0
                    if position.side == "estimated_opening_position" and position.amount_min is not None and position.amount_max is not None
                    else position.amount_max if position.side == "estimated_opening_position" and position.amount_max is not None
                    else position.amount_min if position.side == "estimated_opening_position" and position.amount_min is not None
                    else None
                ),
            }
            for position in positions
        ],
        "skip_reason_summary": _skip_reason_summary_from_positions(positions),
        "skip_diagnostics": _skip_diagnostic_summary_from_positions(positions),
    }
