from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_fundamentals_snapshot_schema
from app.models import FundamentalsSnapshot
from app.strategy_research.fundamental_confirmation_sweep import (
    flatten_result,
    load_fundamentals_snapshot_universe,
    run_sweep,
    sort_sweep_rows,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_snapshot_schema(engine)
    return SessionLocal


def _snapshot(symbol: str, snapshot_date: date, *, source_kind: str | None) -> FundamentalsSnapshot:
    observed_at = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=timezone.utc)
    return FundamentalsSnapshot(
        symbol=symbol,
        provider="fmp",
        snapshot_date=snapshot_date,
        observed_at=observed_at,
        source_fetched_at=observed_at,
        period_date=snapshot_date,
        status="ok",
        source_kind=source_kind,
        data_quality_confidence="medium_proxy",
    )


def test_load_fundamentals_snapshot_universe_filters_source_kind_date_and_exclusions():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_snapshot("AAPL", date(2025, 1, 1), source_kind="ticker_financials_cache_statement_proxy"))
        db.add(_snapshot("MSFT", date(2025, 1, 1), source_kind="fundamentals_cache_current_snapshot"))
        db.add(_snapshot("NVDA", date(2026, 1, 1), source_kind="ticker_financials_cache_statement_proxy"))
        db.commit()

        universe = load_fundamentals_snapshot_universe(
            db,
            end_date=date(2025, 12, 31),
            source_kind="ticker_financials_cache_statement_proxy",
            exclude_symbols=("AAPL",),
        )

        assert universe == ()

        universe = load_fundamentals_snapshot_universe(
            db,
            end_date=date(2025, 12, 31),
            source_kind=None,
        )

        assert universe == ("AAPL", "MSFT")
    finally:
        db.close()


def test_flatten_and_sort_sweep_rows_prioritizes_ok_metric_then_sample_size():
    result = {
        "metadata": {
            "strategy_name": "Congress + Low Leverage",
            "source": "congress",
            "insider_role": None,
            "fundamental_rule": "low_leverage",
            "data_quality_confidence": "medium_proxy",
            "fundamental_snapshot_provenance": {"data_quality_confidence_counts": {"medium_proxy": 2}},
            "universe": ["AAPL", "MSFT"],
            "methodology_version": "fundamental_confirmation_research_v1",
            "base_engine_version": "congress_buys_research_v1",
            "run_timestamp": "2026-08-01T00:00:00+00:00",
            "start_date": "2025-01-01",
            "end_date": "2026-07-31",
        },
        "primary_signal_count": 10,
        "signal_count": 2,
        "aligned_symbol_count": 2,
        "filtered_out": {"rule_not_matched": 8},
        "runs": [
            {"hold_days": 90, "status": "insufficient_lots", "lots": 1, "alpha_cagr_pct": 200.0},
            {"hold_days": 180, "status": "ok", "lots": 4, "alpha_cagr_pct": 1.0, "sharpe": 0.4},
        ],
    }

    rows = flatten_result(result, universe_source="fundamentals_snapshots")
    ranked = sort_sweep_rows(rows)

    assert ranked[0]["status"] == "ok"
    assert ranked[0]["hold_days"] == 180
    assert ranked[0]["confirmed_signals"] == 2
    assert ranked[0]["data_quality_confidence"] == "medium_proxy"


def test_run_sweep_calls_research_for_congress_once_and_each_insider_role(monkeypatch):
    calls = []

    def fake_load_prices(db, symbols, *, start_date, end_date, require_adjusted):
        return {
            "AAPL": {date(2025, 1, 1): object()},
            "SPY": {date(2025, 1, 1): object()},
        }

    def fake_load_signals(db, source, *, universe, start_date, end_date, insider_role):
        return []

    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep.load_adjusted_price_histories", fake_load_prices)
    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep._load_primary_signals", fake_load_signals)
    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep.FundamentalsSnapshotLookup.load", lambda *args, **kwargs: object())

    def fake_run_research(db, config, *, source, rule, insider_role, provider, **kwargs):
        calls.append((source, insider_role, rule, tuple(config.hold_days), provider))
        assert kwargs["shared_price_maps"]
        assert kwargs["shared_primary_signals"] == []
        assert kwargs["shared_snapshot_lookup"] is not None
        return {
            "metadata": {
                "strategy_name": f"{source}-{insider_role}-{rule}",
                "source": source,
                "insider_role": insider_role if source == "insider" else None,
                "fundamental_rule": rule,
                "data_quality_confidence": "medium_proxy",
                "fundamental_snapshot_provenance": {},
                "universe": list(config.universe),
                "methodology_version": "fundamental_confirmation_research_v1",
                "base_engine_version": "congress_buys_research_v1",
                "run_timestamp": "2026-08-01T00:00:00+00:00",
                "start_date": "2025-01-01",
                "end_date": "2026-07-31",
            },
            "primary_signal_count": 1,
            "signal_count": 1,
            "aligned_symbol_count": 1,
            "filtered_out": {},
            "runs": [{"hold_days": 90, "status": "ok", "lots": 1, "alpha_cagr_pct": 3.0}],
        }

    monkeypatch.setattr(
        "app.strategy_research.fundamental_confirmation_sweep.run_research",
        fake_run_research,
    )

    result = run_sweep(
        object(),
        universe=("AAPL",),
        universe_source="explicit",
        start_date=date(2025, 1, 1),
        end_date=date(2026, 7, 31),
        sources=("congress", "insider"),
        rules=("low_leverage",),
        insider_roles=("all", "director"),
        hold_days=(90,),
        weighting="equal",
        rebalance_frequency="event",
        benchmark="SPY",
        slippage_bps=5.0,
        fee_bps=0.0,
        require_adjusted=True,
        min_lots=1,
        provider="fmp",
    )

    assert calls == [
        ("congress", "all", "low_leverage", (90,), "fmp"),
        ("insider", "all", "low_leverage", (90,), "fmp"),
        ("insider", "director", "low_leverage", (90,), "fmp"),
    ]
    assert len(result["rows"]) == 3
    assert result["errors"] == []


def test_run_sweep_collects_variant_timings(monkeypatch):
    def fake_load_prices(db, symbols, *, start_date, end_date, require_adjusted):
        return {
            "AAPL": {date(2025, 1, 1): object()},
            "SPY": {date(2025, 1, 1): object()},
        }

    def fake_load_signals(db, source, *, universe, start_date, end_date, insider_role):
        return []

    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep.load_adjusted_price_histories", fake_load_prices)
    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep._load_primary_signals", fake_load_signals)
    monkeypatch.setattr("app.strategy_research.fundamental_confirmation_sweep.FundamentalsSnapshotLookup.load", lambda *args, **kwargs: object())

    def fake_run_research(db, config, *, source, rule, insider_role, provider, collect_timings=False, **kwargs):
        assert collect_timings is True
        assert kwargs["shared_price_maps"]
        assert kwargs["shared_primary_signals"] == []
        assert kwargs["shared_snapshot_lookup"] is not None
        return {
            "metadata": {
                "strategy_name": f"{source}-{rule}",
                "source": source,
                "insider_role": insider_role if source == "insider" else None,
                "fundamental_rule": rule,
                "data_quality_confidence": "medium_proxy",
                "fundamental_snapshot_provenance": {},
                "universe": list(config.universe),
                "methodology_version": "fundamental_confirmation_research_v1",
                "base_engine_version": "congress_buys_research_v1",
                "run_timestamp": "2026-08-01T00:00:00+00:00",
                "start_date": "2025-01-01",
                "end_date": "2026-07-31",
            },
            "primary_signal_count": 2,
            "signal_count": 1,
            "aligned_symbol_count": 1,
            "filtered_out": {},
            "timings": {
                "load_adjusted_price_histories_seconds": 0.1,
                "load_primary_signals_seconds": 0.2,
                "filter_fundamentals_seconds": 0.3,
                "snapshot_provenance_seconds": 0.4,
                "total_seconds": 1.0,
            },
            "runs": [{"hold_days": 90, "status": "ok", "lots": 1, "alpha_cagr_pct": 3.0, "timings": {"build_lots_seconds": 0.01}}],
        }

    monkeypatch.setattr(
        "app.strategy_research.fundamental_confirmation_sweep.run_research",
        fake_run_research,
    )

    result = run_sweep(
        object(),
        universe=("AAPL",),
        universe_source="explicit",
        start_date=date(2025, 1, 1),
        end_date=date(2026, 7, 31),
        sources=("congress",),
        rules=("low_leverage",),
        insider_roles=("all",),
        hold_days=(90,),
        weighting="equal",
        rebalance_frequency="event",
        benchmark="SPY",
        slippage_bps=5.0,
        fee_bps=0.0,
        require_adjusted=True,
        min_lots=1,
        provider="fmp",
        collect_timings=True,
    )

    assert result["timings"]["total_seconds"] >= 0
    assert result["timings"]["context"]["load_adjusted_price_histories_seconds"] >= 0
    assert result["timings"]["primary_signal_timings"][0]["signals"] == 0
    assert result["timings"]["variant_timings"][0]["confirmed_signals"] == 1
    assert result["rows"][0]["elapsed_seconds"] == 1.0
    assert result["rows"][0]["hold_timings"] == {"build_lots_seconds": 0.01}
