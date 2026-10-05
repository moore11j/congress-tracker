"""Bounded, rotating canonical price repair for current model portfolios."""
import json

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import AppSetting, StrategyLiveHolding
from app.services.price_lookup import get_expected_latest_market_date, hydrate_split_adjusted_ohlc
from app.services.strategy_prices import position_prices

CURSOR_KEY = "strategy_price_repair_cursor"


def refresh_strategy_prices(db, *, limit=25):
    end = get_expected_latest_market_date()
    cursor = db.get(AppSetting, CURSOR_KEY)
    rows = db.execute(select(StrategyLiveHolding.symbol, func.min(StrategyLiveHolding.entry_date))
        .where(StrategyLiveHolding.entry_date <= end, StrategyLiveHolding.rank <= 50)
        .group_by(StrategyLiveHolding.symbol).order_by(StrategyLiveHolding.symbol)).all()
    after = cursor.value if cursor else ""
    pending = [row for row in rows if row[0] > after] or rows
    result = {"checked": 0, "refreshed": 0, "failed": 0}
    for symbol, start in pending[:limit]:
        try:
            marks = position_prices(db, symbol=symbol, entry_date=start, as_of=end)
            if marks["entryPrice"] is None or marks["priceAsOfDate"] != end.isoformat():
                result["refreshed"] += bool(hydrate_split_adjusted_ohlc(db, symbol, start.isoformat(), end.isoformat()))
        except Exception:
            db.rollback()
            result["failed"] += 1
        cursor = db.get(AppSetting, CURSOR_KEY)
        if cursor is None:
            cursor = AppSetting(key=CURSOR_KEY)
            db.add(cursor)
        cursor.value = symbol
        db.commit()
        result["checked"] += 1
    return result


if __name__ == "__main__":
    with SessionLocal() as db:
        print(json.dumps(refresh_strategy_prices(db)))
