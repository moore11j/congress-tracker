"""Cache-only regular-session comparisons for minute charts.

Minute bars stay on the chart, but never serve as adjacent daily closes.
"""
from datetime import datetime, time
import math
from zoneinfo import ZoneInfo

from app.models import PriceCache
from app.services.price_lookup import is_market_trading_day, previous_market_trading_day

MARKET_TZ = ZoneInfo("America/New_York")


def _positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError):
        return None


def _close_time(day):
    # NYSE/Nasdaq cash-equity calendar: Thanksgiving Friday, trading Christmas
    # Eve and trading July 3 close at 13:00 ET. July 2 is not an early close.
    # https://www.nyse.com/trade/hours-calendars
    early = ((day.month == 11 and day.weekday() == 4 and 23 <= day.day <= 29)
             or (day.month == 12 and day.day == 24 and day.weekday() < 4)
             or (day.month == 7 and day.day == 3 and day.weekday() < 4))
    return time(13 if early else 16)


def intraday_session_returns(db, symbol, point, *, selected_massive):
    result = dict(previous_close=None, previous_close_date=None,
                  regular_session_price=None, regular_session_date=None,
                  market_session=None, day_change=None, day_change_pct=None,
                  extended_hours_change=None, extended_hours_change_pct=None)
    try:
        observed = datetime.fromisoformat(str(point.get("date") or "").replace("Z", "+00:00"))
        if observed.tzinfo is None:
            return result
        local = observed.astimezone(MARKET_TZ)
    except (ValueError, TypeError, OverflowError):
        return result
    day = local.date()
    if not is_market_trading_day(day):
        return {**result, "market_session": "closed"}
    previous_day = previous_market_trading_day(day)

    def close_for(session):
        row = db.get(PriceCache, (symbol, session.isoformat()))
        if selected_massive and (row is None or row.price_source not in {"massive", "massive+corporate_actions"}
                                 or row.adjustment_status != "split_adjusted_price_return"):
            return None
        # Match the daily chart's split-adjusted price-return close, not a total
        # return series or the snapshot's potentially different price basis.
        return _positive(row.close) if row is not None else None

    previous = close_for(previous_day)
    latest = _positive(point.get("close"))
    session = ("pre_market" if local.time() < time(9, 30) else
               "after_hours" if local.time() >= _close_time(day) else "regular")
    regular = latest if session == "regular" else close_for(day) if session == "after_hours" else None
    result.update(previous_close=previous, previous_close_date=previous_day.isoformat(),
                  regular_session_price=regular, regular_session_date=day.isoformat(), market_session=session)
    if regular is not None and previous is not None:
        result.update(day_change=regular - previous, day_change_pct=(regular - previous) / previous * 100)
    extended_base = regular if session == "after_hours" else previous if session == "pre_market" else None
    if latest is not None and extended_base is not None:
        result.update(extended_hours_change=latest - extended_base,
                      extended_hours_change_pct=(latest - extended_base) / extended_base * 100)
    return result
