from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models import Event

BUY_TRADE_TYPES = {"purchase", "buy", "p-purchase"}
SELL_TRADE_TYPES = {"sale", "sell", "s-sale"}


@dataclass(frozen=True)
class ConfirmationMetrics:
    congress_active_30d: bool
    insider_active_30d: bool
    congress_trade_count_30d: int
    insider_trade_count_30d: int
    insider_buy_count_30d: int
    insider_sell_count_30d: int
    cross_source_confirmed_30d: bool
    repeat_congress_30d: bool
    repeat_insider_30d: bool

    def as_dict(self) -> dict[str, bool | int]:
        return {
            "congress_active_30d": self.congress_active_30d,
            "insider_active_30d": self.insider_active_30d,
            "congress_trade_count_30d": self.congress_trade_count_30d,
            "insider_trade_count_30d": self.insider_trade_count_30d,
            "insider_buy_count_30d": self.insider_buy_count_30d,
            "insider_sell_count_30d": self.insider_sell_count_30d,
            "cross_source_confirmed_30d": self.cross_source_confirmed_30d,
            "repeat_congress_30d": self.repeat_congress_30d,
            "repeat_insider_30d": self.repeat_insider_30d,
        }


def _empty_metrics() -> ConfirmationMetrics:
    return ConfirmationMetrics(
        congress_active_30d=False,
        insider_active_30d=False,
        congress_trade_count_30d=0,
        insider_trade_count_30d=0,
        insider_buy_count_30d=0,
        insider_sell_count_30d=0,
        cross_source_confirmed_30d=False,
        repeat_congress_30d=False,
        repeat_insider_30d=False,
    )


def get_confirmation_metrics_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    window_days: int = 30,
) -> dict[str, ConfirmationMetrics]:
    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()})
    if not normalized_symbols:
        return {}

    since = datetime.now(timezone.utc) - timedelta(days=window_days)
    trade_ts = func.coalesce(Event.event_date, Event.ts)
    normalized_trade_type = func.lower(func.trim(func.coalesce(Event.trade_type, "")))
    normalized_symbol = func.upper(Event.symbol)

    congress_count = func.sum(
        case((Event.event_type == "congress_trade", 1), else_=0)
    ).label("congress_trade_count_30d")
    insider_count = func.sum(
        case((Event.event_type == "insider_trade", 1), else_=0)
    ).label("insider_trade_count_30d")
    insider_buy_count = func.sum(
        case(
            (
                (Event.event_type == "insider_trade")
                & normalized_trade_type.in_(BUY_TRADE_TYPES),
                1,
            ),
            else_=0,
        )
    ).label("insider_buy_count_30d")
    insider_sell_count = func.sum(
        case(
            (
                (Event.event_type == "insider_trade")
                & normalized_trade_type.in_(SELL_TRADE_TYPES),
                1,
            ),
            else_=0,
        )
    ).label("insider_sell_count_30d")

    rows = db.execute(
        select(
            normalized_symbol.label("symbol"),
            congress_count,
            insider_count,
            insider_buy_count,
            insider_sell_count,
        )
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(Event.symbol.is_not(None))
        .where(normalized_symbol.in_(normalized_symbols))
        .where(trade_ts >= since)
        .group_by(normalized_symbol)
    ).all()

    metrics_by_symbol: dict[str, ConfirmationMetrics] = {
        symbol: _empty_metrics() for symbol in normalized_symbols
    }

    for row in rows:
        symbol = (row.symbol or "").strip().upper()
        if not symbol:
            continue

        congress_trade_count_30d = int(row.congress_trade_count_30d or 0)
        insider_trade_count_30d = int(row.insider_trade_count_30d or 0)
        insider_buy_count_30d = int(row.insider_buy_count_30d or 0)
        insider_sell_count_30d = int(row.insider_sell_count_30d or 0)

        congress_active_30d = congress_trade_count_30d > 0
        insider_active_30d = insider_trade_count_30d > 0

        metrics_by_symbol[symbol] = ConfirmationMetrics(
            congress_active_30d=congress_active_30d,
            insider_active_30d=insider_active_30d,
            congress_trade_count_30d=congress_trade_count_30d,
            insider_trade_count_30d=insider_trade_count_30d,
            insider_buy_count_30d=insider_buy_count_30d,
            insider_sell_count_30d=insider_sell_count_30d,
            cross_source_confirmed_30d=congress_active_30d and insider_active_30d,
            repeat_congress_30d=congress_trade_count_30d >= 2,
            repeat_insider_30d=insider_trade_count_30d >= 2,
        )

    return metrics_by_symbol
