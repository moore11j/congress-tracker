from datetime import datetime, timezone
from types import SimpleNamespace

from app.routers.events import VISIBLE_INSIDER_TRADE_TYPES, _insider_trade_row


def _event(*, event_id: int, trade_type: str = "sale", amount_max: int = 10000):
    return SimpleNamespace(
        id=event_id,
        symbol="USNA",
        trade_type=trade_type,
        amount_min=amount_max,
        amount_max=amount_max,
        member_name=None,
        ts=datetime(2026, 1, 15, tzinfo=timezone.utc),
    )


def test_visible_insider_trade_types_include_aliases_used_by_filers():
    assert "sale" in VISIBLE_INSIDER_TRADE_TYPES
    assert "purchase" in VISIBLE_INSIDER_TRADE_TYPES
    assert "s-sale" in VISIBLE_INSIDER_TRADE_TYPES
    assert "p-purchase" in VISIBLE_INSIDER_TRADE_TYPES


def test_unscored_recent_trade_does_not_emit_pnl_or_smart_signal():
    row = _insider_trade_row(
        _event(event_id=1),
        {
            "symbol": "USNA",
            "transaction_date": "2026-01-14",
            "price": 95.0,
            "smart_score": 28,
            "smart_band": "mild",
        },
        outcome=None,
        fallback_pnl_pct=0.5,
    )

    assert row["pnl_pct"] is None
    assert row["pnl_source"] is None
    assert row["smart_score"] is None
    assert row["smart_band"] is None
