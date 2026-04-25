from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator

StrategyType = Literal["watchlist", "saved_screen", "congress", "insider", "custom_tickers"]
SourceScope = Literal["all_congress", "house", "senate", "member", "all_insiders", "insider"]
ContributionFrequency = Literal["none", "monthly", "quarterly", "annually"]
RebalancingFrequency = Literal["monthly", "quarterly", "semi_annually", "annually"]
WeightingMode = Literal["equal"]

LOOKBACK_PRESET_DAYS: tuple[int, ...] = (30, 90, 180, 365, 1095)
HOLD_DAY_OPTIONS: tuple[int, ...] = (30, 60, 90, 180, 365)
DEFAULT_BENCHMARK = "^GSPC"
MAX_CUSTOM_TICKERS = 25


class BacktestStrategyConfig(BaseModel):
    strategy_type: StrategyType
    watchlist_id: int | None = None
    saved_screen_id: int | None = None
    tickers: list[str] = Field(default_factory=list)
    source_scope: SourceScope | None = None
    member_id: str | None = None
    insider_cik: str | None = None
    start_date: date
    end_date: date
    hold_days: int = Field(default=90)
    start_balance: float = Field(default=10000.0, gt=0)
    contribution_amount: float = Field(default=0.0, ge=0)
    contribution_frequency: ContributionFrequency = "none"
    rebalancing_frequency: RebalancingFrequency = "monthly"
    max_position_weight: float = Field(default=0.25, gt=0, le=1)
    weighting: WeightingMode = "equal"
    benchmark: str = DEFAULT_BENCHMARK

    @model_validator(mode="after")
    def _validate_config(self) -> "BacktestStrategyConfig":
        self.tickers = list(dict.fromkeys(symbol.strip().upper() for symbol in self.tickers if symbol and symbol.strip()))
        self.member_id = (self.member_id or "").strip() or None
        self.insider_cik = (self.insider_cik or "").strip() or None
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
        elif self.strategy_type == "congress":
            if self.source_scope not in {"all_congress", "house", "senate", "member"}:
                raise ValueError("source_scope must be one of all_congress, house, senate, or member.")
            if self.source_scope == "member" and not self.member_id:
                raise ValueError("member_id is required when source_scope=member.")
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


class BacktestPositionPoint(BaseModel):
    symbol: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    return_pct: float
    source_event_id: int | None = None
    source_label: str | None = None


class BacktestRunResponse(BaseModel):
    summary: BacktestSummary
    timeline: list[BacktestTimelinePoint]
    positions: list[BacktestPositionPoint]
    assumptions: list[str]
    diagnostics: BacktestDiagnostics | None = None
