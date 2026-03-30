from __future__ import annotations

from sqlalchemy import func, or_

from app.models import Event

VISIBLE_INSIDER_TRADE_TYPES = {"purchase", "sale", "p-purchase", "s-sale"}


def insider_visibility_clause():
    normalized_trade_type = func.lower(func.trim(func.coalesce(Event.trade_type, "")))
    return or_(
        Event.event_type != "insider_trade",
        normalized_trade_type.in_(VISIBLE_INSIDER_TRADE_TYPES),
    )
