from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator

StrategyType = Literal["watchlist", "saved_screen", "congress", "insider"]
SourceScope = Literal["all_congress", "house", "senate", "member", "all_insiders", "insider"]
RebalanceMode = Literal["on_signal"]
WeightingMode = Literal["equal"]

LOOKBACK_PRESET_DAYS: tuple[int, ...] = (30, 90, 180, 365, 1095)
HOLD_DAY_OPTIONS: tuple[int, ...] = (30, 60, 90, 180, 365)
DEFAULT_BENCHMARK = "^GSPC"


class BacktestStrategyConfig(BaseModel):
    strategy_type: StrategyType
    watchlist_id: int | None = None
    saved_screen_id: int | None = None
    source_scope: SourceScope | None = None
    member_id: str | None = None
    insider_cik: str | None = None
    start_date: date
    end_date: date
    hold_days: int = Field(default=90)
    rebalance: RebalanceMode = "on_signal"
    weighting: WeightingMode = "equal"
    benchmark: str = DEFAULT_BENCHMARK

    @model_validator(mode="after")
    def _validate_config(self) -> "BacktestStrategyConfig":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date.")
        if self.hold_days not in HOLD_DAY_OPTIONS:
            raise ValueError(f"hold_days must be one of {', '.join(str(value) for value in HOLD_DAY_OPTIONS)}.")
        if (self.benchmark or DEFAULT_BENCHMARK).strip().upper() != DEFAULT_BENCHMARK:
            raise ValueError("benchmark must be ^GSPC in v1.")

        if self.strategy_type == "watchlist":
            if self.watchlist_id is None:
                raise ValueError("watchlist_id is required for watchlist strategies.")
        elif self.strategy_type == "saved_screen":
            if self.saved_screen_id is None:
                raise ValueError("saved_screen_id is required for saved_screen strategies.")
        elif self.strategy_type == "congress":
            if self.source_scope not in {"all_congress", "house", "senate", "member"}:
                raise ValueError("source_scope must be one of all_congress, house, senate, or member.")
            if self.source_scope == "member" and not (self.member_id or "").strip():
                raise ValueError("member_id is required when source_scope=member.")
        elif self.strategy_type == "insider":
            if self.source_scope not in {"all_insiders", "insider"}:
                raise ValueError("source_scope must be all_insiders or insider for insider strategies.")
            if self.source_scope == "insider" and not (self.insider_cik or "").strip():
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
    strategy_return_pct: float
    benchmark_return_pct: float
    alpha_pct: float
    win_rate: float
    max_drawdown_pct: float
    volatility_pct: float
    trade_count: int
    positions_count: int


class BacktestTimelinePoint(BaseModel):
    date: str
    strategy_value: float
    benchmark_value: float
    active_positions: int


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
