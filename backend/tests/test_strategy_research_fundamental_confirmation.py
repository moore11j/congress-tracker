from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_fundamentals_snapshot_schema
from app.models import FundamentalsSnapshot
from app.strategy_research.congress_buys import Signal
from app.strategy_research.fundamental_confirmation import (
    FundamentalState,
    _overall_snapshot_confidence,
    filter_signals_by_fundamental_rule,
    fundamental_rule_matches,
    fundamental_state_from_snapshot,
    snapshot_provenance_summary,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_snapshot_schema(engine)
    return SessionLocal


def _snapshot(
    symbol: str,
    snapshot_date: date,
    *,
    revenue_growth: float = 12.0,
    eps_growth: float | None = 10.0,
    roe: float = 18.0,
    free_cash_flow: float = 1_000_000.0,
    forward_pe: float | None = 22.0,
    net_debt_to_ebitda: float | None = 1.5,
    source_kind: str | None = None,
    data_quality_confidence: str | None = None,
) -> FundamentalsSnapshot:
    observed_at = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=timezone.utc)
    return FundamentalsSnapshot(
        symbol=symbol,
        provider="fmp",
        snapshot_date=snapshot_date,
        observed_at=observed_at,
        source_fetched_at=observed_at,
        period_date=snapshot_date,
        status="ok",
        revenue_growth=revenue_growth,
        eps_growth=eps_growth,
        roe=roe,
        free_cash_flow=free_cash_flow,
        forward_pe=forward_pe,
        net_debt_to_ebitda=net_debt_to_ebitda,
        source_kind=source_kind,
        data_quality_confidence=data_quality_confidence,
    )


def _signal(symbol: str = "AAPL", disclosure_date: date = date(2026, 8, 1)) -> Signal:
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


def test_fundamental_rules_cover_growth_value_and_leverage():
    state = FundamentalState(
        status="ok",
        revenue_growth=14.0,
        eps_growth=12.0,
        roe=20.0,
        free_cash_flow=10_000_000.0,
        forward_pe=24.0,
        net_debt_to_ebitda=1.0,
        debt_to_equity=0.5,
    )

    assert fundamental_rule_matches(state, "quality_growth")
    assert fundamental_rule_matches(state, "reasonable_growth_value")
    assert fundamental_rule_matches(state, "garp")
    assert fundamental_rule_matches(state, "low_leverage")
    assert not fundamental_rule_matches(FundamentalState(status="ok"), "low_leverage")


def test_fundamental_state_uses_snapshot_fields():
    snapshot = _snapshot("MSFT", date(2026, 8, 1), revenue_growth=9.0, roe=16.0, forward_pe=18.0)

    state = fundamental_state_from_snapshot(snapshot)

    assert state.status == "ok"
    assert state.revenue_growth == 9.0
    assert state.roe == 16.0
    assert state.forward_pe == 18.0
    assert state.snapshot_date == date(2026, 8, 1)


def test_filter_signals_uses_only_snapshot_on_or_before_disclosure_date():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(
            _snapshot(
                "AAPL",
                date(2026, 8, 1),
                revenue_growth=-5.0,
                roe=5.0,
                free_cash_flow=-1.0,
                forward_pe=80.0,
            )
        )
        db.add(_snapshot("AAPL", date(2026, 8, 2), revenue_growth=20.0, roe=30.0, free_cash_flow=10_000_000.0))
        db.commit()

        filtered, skipped = filter_signals_by_fundamental_rule(
            db,
            [_signal(disclosure_date=date(2026, 8, 1))],
            rule="quality_growth",
        )

        assert filtered == []
        assert skipped == {"rule_not_matched": 1}
    finally:
        db.close()


def test_current_cache_proxy_mode_is_explicitly_separate_from_snapshot_mode():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(
            _snapshot(
                "AAPL",
                date(2026, 8, 1),
                revenue_growth=-5.0,
                roe=5.0,
                free_cash_flow=-1.0,
                forward_pe=80.0,
            )
        )
        db.add(_snapshot("AAPL", date(2026, 8, 2), revenue_growth=20.0, roe=30.0, free_cash_flow=10_000_000.0))
        db.commit()

        filtered, skipped = filter_signals_by_fundamental_rule(
            db,
            [_signal(disclosure_date=date(2026, 8, 1))],
            rule="quality_growth",
            data_mode="current_cache_proxy",
        )

        assert len(filtered) == 1
        assert skipped == {}
    finally:
        db.close()


def test_snapshot_provenance_downgrades_proxy_strategy_confidence():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(
            _snapshot(
                "AAPL",
                date(2026, 8, 1),
                source_kind="ticker_financials_cache_statement_proxy",
                data_quality_confidence="medium_proxy",
            )
        )
        db.commit()

        provenance = snapshot_provenance_summary(
            db,
            [_signal(disclosure_date=date(2026, 8, 1))],
        )

        assert provenance["source_kind_counts"] == {"ticker_financials_cache_statement_proxy": 1}
        assert provenance["data_quality_confidence_counts"] == {"medium_proxy": 1}
        assert _overall_snapshot_confidence(provenance, "snapshots") == "medium_proxy"
    finally:
        db.close()
