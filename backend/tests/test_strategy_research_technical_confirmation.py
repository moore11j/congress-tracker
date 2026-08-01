from __future__ import annotations

from datetime import date, timedelta

from app.strategy_research.congress_buys import PriceBar, Signal
from app.strategy_research.technical_confirmation import (
    filter_signals_by_technical_rule,
    technical_rule_matches,
    technical_state_as_of,
)


def _prices(start: date, values: list[float]) -> dict[date, PriceBar]:
    return {
        start + timedelta(days=index): PriceBar(day=start + timedelta(days=index), close=value, dollar_volume=None)
        for index, value in enumerate(values)
    }


def _signal(symbol: str = "AAPL", disclosure_date: date = date(2024, 8, 1)) -> Signal:
    return Signal(
        event_id=1,
        symbol=symbol,
        disclosure_date=disclosure_date,
        raw_entry_date=disclosure_date + timedelta(days=1),
        amount_min=1000,
        amount_max=1000,
        member_name=None,
        member_bioguide_id=None,
        chamber=None,
        party=None,
        source_filing_id=None,
        source_document_url=None,
    )


def test_technical_state_uses_only_prices_on_or_before_as_of_date():
    start = date(2024, 1, 1)
    disclosure_date = start + timedelta(days=209)
    values = [100.0] * 210 + [1_000.0] * 40
    prices = _prices(start, values)

    state = technical_state_as_of(prices, disclosure_date)

    assert state.status == "ok"
    assert state.close == 100.0
    assert not technical_rule_matches(state, "price_above_sma50_sma200")


def test_filter_signals_by_technical_alignment_excludes_non_confirmed_signal():
    start = date(2024, 1, 1)
    disclosure_date = start + timedelta(days=209)
    prices = _prices(start, [100.0] * 250)

    filtered, skipped = filter_signals_by_technical_rule(
        [_signal(disclosure_date=disclosure_date)],
        {"AAPL": prices},
        rule="technical_alignment",
    )

    assert filtered == []
    assert skipped == {"rule_not_matched": 1}


def test_filter_signals_by_macd_bullish_keeps_confirmed_signal():
    start = date(2024, 1, 1)
    disclosure_date = start + timedelta(days=249)
    values = [100.0 + (index * 0.2) for index in range(250)]
    prices = _prices(start, values)

    filtered, skipped = filter_signals_by_technical_rule(
        [_signal(disclosure_date=disclosure_date)],
        {"AAPL": prices},
        rule="macd_bullish",
    )

    assert len(filtered) == 1
    assert skipped == {}
