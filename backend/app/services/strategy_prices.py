"""Resolve scheduled model prices from stored canonical daily bars only."""
from datetime import date, timedelta

from sqlalchemy import select

from app.models import PriceCache
from app.services.outcome_integrity import adjusted_price
from app.services.price_lookup import is_market_trading_day


def position_prices(db, *, symbol: str, entry_date: date | None, as_of: date) -> dict:
    session_date = entry_date or as_of
    while not is_market_trading_day(session_date):
        session_date += timedelta(days=1)
    rows = db.execute(select(PriceCache).where(
        PriceCache.symbol == symbol,
        PriceCache.date <= as_of.isoformat(),
        PriceCache.date >= (entry_date or as_of).isoformat(),
    ).order_by(PriceCache.date.asc())).scalars().all()
    first = next((row for row in rows if row.date == session_date.isoformat()), None)
    latest = rows[-1] if rows else None
    # Never substitute a later day's open for a missing execution-session open.
    entry = adjusted_price(first, "open") if first and first.adjustment_status == "split_adjusted_price_return" else None
    close = adjusted_price(latest, "close") if latest and latest.adjustment_status == "split_adjusted_price_return" else None
    return {
        "entryPrice": float(entry) if entry and entry > 0 else None,
        "entryDate": session_date,
        "lastPrice": float(close) if close and close > 0 else None,
        "priceAsOfDate": latest.date if close and close > 0 else None,
    }
