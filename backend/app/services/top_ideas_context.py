"""Scheduled-only context from canonical clusters and published strategy entries."""
from datetime import timedelta

from sqlalchemy import func, inspect, select

from app.models import StrategyDefinition, StrategyEvent


def load_idea_context(db, symbols, now):
    if not symbols:
        return {}
    available = set(inspect(db.get_bind()).get_table_names())
    result = {symbol: {} for symbol in symbols}
    since = now - timedelta(days=30)
    if "insider_transactions_normalized" in available:
        # Reuse the canonical purchase/deduplication rules used on profiles.
        from app.services.profile_overviews import _cluster_buying
        for row in _cluster_buying(db, since=since, before=now.date() + timedelta(days=1), symbols=set(symbols), limit=len(symbols)):
            result[row["symbol"]]["insider_cluster_count"] = row["unique_insiders"]
    if {"strategy_events", "strategy_definitions"}.issubset(available):
        rows = db.execute(select(StrategyEvent.symbol, func.count(StrategyEvent.id))
            .join(StrategyDefinition, StrategyDefinition.id == StrategyEvent.strategy_id)
            .where(StrategyEvent.symbol.in_(symbols), StrategyEvent.event_type == "trade_added",
                   StrategyEvent.occurred_at >= since, StrategyEvent.occurred_at <= now,
                   StrategyDefinition.status == "published")
            .group_by(StrategyEvent.symbol)).all()
        for symbol, count in rows:
            result[symbol]["strategy_entries"] = count
    return result
