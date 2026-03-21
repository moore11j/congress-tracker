from datetime import date, datetime, timezone

from app.models import TradeOutcome
from app.services.trade_outcomes import dedupe_member_trade_outcomes


def _row(
    *,
    event_id: int,
    trade_type: str,
    computed_at: datetime,
    return_pct: float,
) -> TradeOutcome:
    row = TradeOutcome(event_id=event_id)
    row.member_id = "FMP_SENATE_XX_MITCH_MCCONNELL"
    row.symbol = "LAZR"
    row.trade_type = trade_type
    row.trade_date = date(2025, 6, 26)
    row.amount_min = 100001
    row.amount_max = 250000
    row.benchmark_symbol = "^GSPC"
    row.computed_at = computed_at
    row.return_pct = return_pct
    row.scoring_status = "ok"
    return row


def test_dedupe_member_trade_outcomes_prefers_newest_row_per_logical_trade():
    older = _row(
        event_id=65040,
        trade_type="sale",
        computed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        return_pct=-97.89,
    )
    newer = _row(
        event_id=107776,
        trade_type="sale",
        computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        return_pct=97.89,
    )

    deduped = dedupe_member_trade_outcomes([older, newer])
    assert len(deduped) == 1
    assert deduped[0].event_id == 107776
    assert deduped[0].return_pct == 97.89


def test_dedupe_member_trade_outcomes_normalizes_trade_side_aliases():
    s_sale = _row(
        event_id=1,
        trade_type="s-sale",
        computed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        return_pct=20.0,
    )
    sale = _row(
        event_id=2,
        trade_type="sale",
        computed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        return_pct=25.0,
    )

    deduped = dedupe_member_trade_outcomes([s_sale, sale])
    assert len(deduped) == 1
    assert deduped[0].event_id == 2


def test_dedupe_member_trade_outcomes_ignores_member_id_aliases():
    legacy = _row(
        event_id=65040,
        trade_type="sale",
        computed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        return_pct=-97.89,
    )
    legacy.member_id = "FMP_SENATE_XX_MITCH_MCCONNELL"

    canonical = _row(
        event_id=107776,
        trade_type="sale",
        computed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        return_pct=97.89,
    )
    canonical.member_id = "M000355"

    deduped = dedupe_member_trade_outcomes([legacy, canonical])
    assert len(deduped) == 1
    assert deduped[0].event_id == 107776
