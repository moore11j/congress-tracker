from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator

StrategyType = Literal["watchlist", "saved_screen", "congress", "insider", "custom_tickers"]
SourceScope = Literal["all_congress", "house", "senate", "member", "member_list", "all_insiders", "insider"]
ContributionFrequency = Literal["none", "monthly", "quarterly", "annually"]
RebalancingFrequency = Literal["monthly", "quarterly", "semi_annually", "annually"]
WeightingMode = Literal["equal"]

LOOKBACK_PRESET_DAYS: tuple[int, ...] = (30, 90, 180, 365, 1095)
HOLD_DAY_OPTIONS: tuple[int, ...] = (30, 60, 90, 180, 365)
DEFAULT_BENCHMARK = "^GSPC"
MAX_CUSTOM_TICKERS = 25
MAX_CUSTOM_ALLOCATED_TICKERS = 10


class BacktestTickerInput(BaseModel):
    symbol: str
    allocation_pct: float | None = None


class BacktestStrategyConfig(BaseModel):
    strategy_type: StrategyType
    watchlist_id: int | None = None
    saved_screen_id: int | None = None
    tickers: list[str] = Field(default_factory=list)
    custom_allocations: dict[str, float] | None = None
    source_label: str | None = None
    source_scope: SourceScope | None = None
    member_id: str | None = None
    member_ids: list[str] = Field(default_factory=list)
    insider_cik: str | None = None
    start_date: date
    end_date: date
    hold_days: int = Field(default=90)
    start_balance: float = Field(default=10000.0, gt=0)
    contribution_amount: float = Field(default=0.0, ge=0)
    contribution_frequency: ContributionFrequency = "none"
    rebalancing_frequency: RebalancingFrequency = "monthly"
    max_position_weight: float = Field(default=1.0, gt=0, le=1)
    weighting: WeightingMode = "equal"
    benchmark: str = DEFAULT_BENCHMARK

    @model_validator(mode="before")
    @classmethod
    def _normalize_ticker_payloads(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        raw_tickers = data.get("tickers")
        if not isinstance(raw_tickers, list):
            return data

        normalized_symbols: list[str] = []
        allocations: dict[str, float] = {}
        has_object_inputs = False

        for raw_item in raw_tickers:
            if isinstance(raw_item, str):
                normalized_symbols.append(raw_item)
                continue
            if not isinstance(raw_item, dict):
                continue
            has_object_inputs = True
            symbol = raw_item.get("symbol")
            if isinstance(symbol, str):
                normalized_symbols.append(symbol)
                allocation_pct = raw_item.get("allocation_pct")
                if isinstance(allocation_pct, (int, float)):
                    allocations[symbol] = float(allocation_pct)

        next_data = dict(data)
        next_data["tickers"] = normalized_symbols
        if has_object_inputs and allocations:
            next_data["custom_allocations"] = allocations
        return next_data

    @model_validator(mode="after")
    def _validate_config(self) -> "BacktestStrategyConfig":
        self.tickers = list(dict.fromkeys(symbol.strip().upper() for symbol in self.tickers if symbol and symbol.strip()))
        self.source_label = (self.source_label or "").strip() or None
        self.member_id = (self.member_id or "").strip() or None
        self.member_ids = list(
            dict.fromkeys(member_id.strip().upper() for member_id in self.member_ids if member_id and member_id.strip())
        )
        self.insider_cik = (self.insider_cik or "").strip() or None
        if self.custom_allocations:
            normalized_allocations: dict[str, float] = {}
            for raw_symbol, allocation_pct in self.custom_allocations.items():
                symbol = (raw_symbol or "").strip().upper()
                if not symbol or symbol not in self.tickers:
                    continue
                normalized_allocations[symbol] = float(allocation_pct)
            self.custom_allocations = normalized_allocations or None
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date.")
        if self.hold_days not in HOLD_DAY_OPTIONS:
            raise ValueError(f"hold_days must be one of {', '.join(str(value) for value in HOLD_DAY_OPTIONS)}.")
        if (self.benchmark or DEFAULT_BENCHMARK).strip().upper() != DEFAULT_BENCHMARK:
            raise ValueError("benchmark must be ^GSPC in v1.")
        if self.contribution_frequency == "none" and self.contribution_amount > 0:
            raise ValueError("contribution_frequency must be set when contribution_amount is greater than zero.")
        if self.contribution_frequency != "none" and self.contribution_amount < 0:
            raise ValueError("contribution_amount must be zero or greater.")

        if self.strategy_type == "watchlist":
            if self.watchlist_id is None:
                raise ValueError("watchlist_id is required for watchlist strategies.")
        elif self.strategy_type == "saved_screen":
            if self.saved_screen_id is None:
                raise ValueError("saved_screen_id is required for saved_screen strategies.")
        elif self.strategy_type == "custom_tickers":
            if not self.tickers:
                raise ValueError("tickers must contain at least one symbol for custom_tickers strategies.")
            if len(self.tickers) > MAX_CUSTOM_TICKERS:
                raise ValueError(f"tickers may contain at most {MAX_CUSTOM_TICKERS} symbols in v1.")
            if self.custom_allocations:
                if len(self.tickers) > MAX_CUSTOM_ALLOCATED_TICKERS:
                    raise ValueError(
                        f"tickers may contain at most {MAX_CUSTOM_ALLOCATED_TICKERS} symbols when custom allocations are enabled in v1."
                    )
                if set(self.custom_allocations) != set(self.tickers):
                    raise ValueError("custom allocations must be provided for every selected ticker.")
                total_allocation = sum(self.custom_allocations.values())
                if any(allocation <= 0 for allocation in self.custom_allocations.values()):
                    raise ValueError("custom allocations must be greater than zero for every selected ticker.")
                if abs(total_allocation - 100.0) > 0.02:
                    raise ValueError("custom allocations must total 100%.")
        elif self.strategy_type == "congress":
            if self.source_scope not in {"all_congress", "house", "senate", "member", "member_list"}:
                raise ValueError("source_scope must be one of all_congress, house, senate, member, or member_list.")
            if self.source_scope == "member" and not self.member_id:
                raise ValueError("member_id is required when source_scope=member.")
            if self.source_scope == "member_list" and not self.member_ids:
                raise ValueError("member_ids is required when source_scope=member_list.")
        elif self.strategy_type == "insider":
            if self.source_scope not in {"all_insiders", "insider"}:
                raise ValueError("source_scope must be all_insiders or insider for insider strategies.")
            if self.source_scope == "insider" and not self.insider_cik:
                raise ValueError("insider_cik is required when source_scope=insider.")
        return self


@dataclass(frozen=True)
class BacktestSignal:
    symbol: str
    signal_date: date
    source_event_id: int | None = None
    source_label: str | None = None


@dataclass(frozen=True)
class ResolvedPosition:
    symbol: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    return_pct: float
    source_event_id: int | None = None
    source_label: str | None = None
    truncated_at_end: bool = False
    price_fallback_used: bool = False


class BacktestSummary(BaseModel):
    start_balance: float
    ending_balance: float
    benchmark_ending_balance: float
    total_contributions: float
    net_profit: float
    strategy_return_pct: float
    time_weighted_return_pct: float
    benchmark_return_pct: float
    alpha_pct: float
    cagr_pct: float
    sharpe_ratio: float | None = None
    win_rate: float
    max_drawdown_pct: float
    volatility_pct: float
    trade_count: int
    positions_count: int
    skipped_positions_count: int = 0
    skipped_reasons: list[str] = Field(default_factory=list)
    price_fallback_positions_count: int = 0


class BacktestTimelinePoint(BaseModel):
    date: str
    strategy_value: float
    benchmark_value: float
    strategy_return_pct: float
    benchmark_return_pct: float
    active_positions: int
    invested_pct: float
    cash: float
    daily_return_pct: float


class BacktestDiagnostics(BaseModel):
    average_active_positions: float
    max_active_positions: int
    average_invested_pct: float
    max_invested_pct: float
    max_position_weight_observed: float
    skipped_positions_count: int = 0
    skipped_reasons: list[str] = Field(default_factory=list)
    price_fallback_positions_count: int = 0


class BacktestPositionPoint(BaseModel):
    symbol: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    return_pct: float
    source_event_id: int | None = None
    source_label: str | None = None
    price_fallback_used: bool = False


class BacktestRunResponse(BaseModel):
    summary: BacktestSummary
    timeline: list[BacktestTimelinePoint]
    positions: list[BacktestPositionPoint]
    assumptions: list[str]
    diagnostics: BacktestDiagnostics | None = None
