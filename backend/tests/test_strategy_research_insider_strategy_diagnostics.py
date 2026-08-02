from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from app.strategy_research.congress_buys import Signal
from app.strategy_research.insider_strategy_diagnostics import summarize_insider_strategy_quality


def _signal(
    symbol: str,
    *,
    event_id: int,
    owner: str,
    filing_id: str,
    amount: int | None = 1000,
    disclosure_date: date = date(2026, 1, 15),
) -> Signal:
    return Signal(
        event_id=event_id,
        symbol=symbol,
        disclosure_date=disclosure_date,
        raw_entry_date=disclosure_date + timedelta(days=1),
        amount_min=amount,
        amount_max=amount,
        member_name=owner,
        member_bioguide_id=owner,
        chamber=None,
        party=None,
        source_filing_id=filing_id,
        source_document_url=None,
        dedupe_key=(filing_id, event_id),
    )


def _lot(signal: Signal, net_return: float) -> SimpleNamespace:
    return SimpleNamespace(signal=signal, net_return=net_return)


def test_summarize_insider_strategy_quality_flags_concentration_and_missing_amounts():
    dominant = [_signal("AAPL", event_id=index, owner="owner-1", filing_id=f"filing-{index}", amount=None) for index in range(6)]
    others = [_signal(f"SYM{index}", event_id=100 + index, owner=f"other-owner-{index}", filing_id=f"other-{index}") for index in range(4)]
    signals = dominant + others
    lots = [_lot(signal, 0.10) for signal in dominant] + [_lot(signal, -0.02) for signal in others]

    summary = summarize_insider_strategy_quality(
        primary_signals=signals,
        confirmed_signals=signals,
        lots=lots,
        skipped={"missing_exit_price": 2},
        limit=3,
    )

    assert summary["lots"] == 10
    assert summary["unique_symbols"] == 5
    assert summary["amount_missing_pct"] == 60.0
    assert summary["top_symbols"][0] == {"key": "AAPL", "count": 6, "pct": 60.0}
    assert summary["top_owners"][0] == {"key": "owner-1", "count": 6, "pct": 60.0}
    assert "top_symbol_exceeds_25pct_of_lots" in summary["concentration_flags"]
    assert "top_owner_exceeds_25pct_of_lots" in summary["concentration_flags"]
    assert "amount_missing_for_at_least_25pct_of_signals" in summary["concentration_flags"]
    assert summary["data_quality_confidence"] == "low"


def test_summarize_insider_strategy_quality_can_report_medium_proxy_without_flags():
    signals = [
        _signal(
            f"SYM{index}",
            event_id=index,
            owner=f"owner-{index}",
            filing_id=f"filing-{index}",
            disclosure_date=date(2026, (index % 12) + 1, 15),
        )
        for index in range(120)
    ]
    lots = [_lot(signal, 0.01) for signal in signals]

    summary = summarize_insider_strategy_quality(
        primary_signals=signals,
        confirmed_signals=signals,
        lots=lots,
        skipped={},
        limit=5,
    )

    assert summary["lots"] == 120
    assert summary["unique_symbols"] == 120
    assert summary["unique_owners"] == 120
    assert summary["concentration_flags"] == []
    assert summary["data_quality_confidence"] == "medium_proxy"
