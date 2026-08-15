from __future__ import annotations

import json
import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.entitlements import ENTITLEMENTS
from app.models import (
    HouseAnnualDisclosureDocument,
    HouseAnnualDisclosureHolding,
    ReplicatedPortfolioPosition,
    ReplicatedPortfolioPoint,
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyEvaluationRun,
    StrategyEvent,
    StrategyEventDelivery,
    StrategyHoldingsSnapshot,
    StrategyHistoricalTransaction,
    StrategyLiveHolding,
    StrategyPerformanceSnapshot,
    StrategySubscription,
    StrategyTrade,
    StrategyVersion,
)
from app.services.strategies import list_strategy_cards, set_strategy_publication, strategy_detail
from app.services.replicated_portfolio_strategy_refresh import _curve_points


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return SessionLocal, engine


def test_ensure_strategy_storage_schema_creates_expected_tables_and_indexes():
    _, engine = _session()
    inspector = inspect(engine)

    expected_tables = {
        "strategy_definitions",
        "strategy_backtest_runs",
        "strategy_performance_snapshots",
        "strategy_equity_curve_points",
        "strategy_holdings_snapshots",
        "strategy_holding_rows",
        "strategy_current_holdings",
        "strategy_versions",
        "strategy_evaluation_runs",
        "strategy_live_holdings",
        "strategy_trades",
        "strategy_historical_transactions",
        "strategy_events",
        "strategy_subscriptions",
        "strategy_event_deliveries",
    }
    assert expected_tables <= set(inspector.get_table_names())
    indexes = {index["name"] for index in inspector.get_indexes("strategy_definitions")}
    assert "ix_strategy_definitions_slug" in indexes


def test_strategy_service_lists_persisted_cards_and_sorts_lowest_drawdown_first():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        conservative = StrategyDefinition(
            slug="low-drawdown",
            name="Low Drawdown",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
            sort_order=2,
        )
        aggressive = StrategyDefinition(
            slug="high-return",
            name="High Return",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
            sort_order=1,
        )
        db.add_all([conservative, aggressive])
        db.flush()
        now = datetime.now(timezone.utc)
        run_one = StrategyBacktestRun(
            strategy_id=conservative.id,
            run_key="run-low",
            status="ok",
            completed_at=now,
            methodology_version="v1",
            walnut_strategy_score=55,
            diagnostics_json=json.dumps({"validation": {"walnut_strategy_score": {"score": 55, "score_version": "walnut_strategy_score_v2"}}}),
        )
        run_two = StrategyBacktestRun(
            strategy_id=aggressive.id,
            run_key="run-high",
            status="ok",
            completed_at=now,
            methodology_version="v1",
            walnut_strategy_score=70,
            diagnostics_json=json.dumps({"validation": {"walnut_strategy_score": {"score": 70, "score_version": "walnut_strategy_score_v2"}}}),
        )
        db.add_all([run_one, run_two])
        db.flush()
        db.add_all(
            [
                StrategyPerformanceSnapshot(
                    strategy_id=conservative.id,
                    run_id=run_one.id,
                    as_of_date=date(2026, 7, 31),
                    period="max",
                    cagr_pct=12,
                    max_drawdown_pct=-8,
                    walnut_strategy_score=55,
                ),
                StrategyPerformanceSnapshot(
                    strategy_id=aggressive.id,
                    run_id=run_two.id,
                    as_of_date=date(2026, 7, 31),
                    period="max",
                    cagr_pct=25,
                    max_drawdown_pct=-35,
                    walnut_strategy_score=70,
                ),
            ]
        )
        db.commit()

        by_drawdown = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"], sort="drawdown")
        assert [item["slug"] for item in by_drawdown["items"]] == ["low-drawdown", "high-return"]

        by_score = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"], sort="walnut_score")
        assert [item["slug"] for item in by_score["items"]] == ["high-return", "low-drawdown"]

        by_default = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])
        assert [item["slug"] for item in by_default["items"]] == ["high-return", "low-drawdown"]
    finally:
        db.close()


def test_strategy_detail_keeps_public_performance_but_withholds_current_positions_for_free_users():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="public-performance",
            name="Public Performance",
            category="walnut",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(strategy_id=strategy.id, run_key="public-run", status="ok", methodology_version="v1")
        db.add(run)
        db.flush()
        db.add_all([
            StrategyPerformanceSnapshot(strategy_id=strategy.id, run_id=run.id, as_of_date=date(2026, 8, 1), period="max", cagr_pct=18),
            StrategyEquityCurvePoint(
                strategy_id=strategy.id,
                run_id=run.id,
                date=date(2026, 8, 1),
                strategy_value=110,
                benchmark_value=105,
                active_holdings=7,
            ),
            StrategyCurrentHolding(strategy_id=strategy.id, run_id=run.id, as_of_date=date(2026, 8, 1), symbol="NVDA", company_name="NVIDIA"),
        ])
        db.commit()

        free = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["free"])
        assert free["access"]["locked"] is False
        assert free["performance"]["cagrPct"] == 18
        assert len(free["equityCurve"]) == 1
        assert free["equityCurve"][0]["activeHoldings"] == 1
        assert free["equityCurve"][0]["activeLots"] == 7
        assert free["currentHoldings"] == []
        assert free["currentHoldingsCount"] == 1
        assert free["strategyAccess"]["canViewCurrentHoldings"] is False

        premium = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"])
        assert premium["currentHoldings"][0]["symbol"] == "NVDA"
        assert premium["strategyAccess"]["canFollow"] is True
    finally:
        db.close()


def test_replicated_curve_counts_unique_active_tickers_not_open_lots():
    points = [
        ReplicatedPortfolioPoint(asof_date=date(2026, 1, 2), strategy_value=100, active_positions=2),
        ReplicatedPortfolioPoint(asof_date=date(2026, 1, 3), strategy_value=101, active_positions=3),
    ]
    positions = [
        ReplicatedPortfolioPosition(symbol="WFC", status="open", entry_date=date(2026, 1, 1)),
        ReplicatedPortfolioPosition(symbol="WFC", status="open", entry_date=date(2026, 1, 2)),
        ReplicatedPortfolioPosition(symbol="SPG", status="closed", entry_date=date(2026, 1, 1), exit_date=date(2026, 1, 3)),
    ]

    curve = _curve_points(strategy_id=1, strategy_run_id=1, points=points, positions=positions)

    assert [point.active_holdings for point in curve] == [2, 3]
    assert [point.active_tickers for point in curve] == [2, 1]


def test_strategy_detail_curve_matches_selected_period_and_preserves_endpoints():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="curve-window",
            name="Curve Window",
            category="walnut",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(strategy_id=strategy.id, run_key="curve-window-run", status="ok", methodology_version="v1")
        db.add(run)
        db.flush()
        start = date(2023, 1, 1)
        for offset in range(800):
            db.add(
                StrategyEquityCurvePoint(
                    strategy_id=strategy.id,
                    run_id=run.id,
                    date=start + timedelta(days=offset),
                    strategy_value=100.0 + offset,
                    benchmark_value=100.0 + offset * 0.5,
                )
            )
        db.commit()

        all_period = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], equity_limit=25)
        one_year = strategy_detail(
            db,
            slug=strategy.slug,
            entitlements=ENTITLEMENTS["premium"],
            period="1y",
            equity_limit=25,
        )

        assert len(all_period["equityCurve"]) == 25
        assert all_period["equityCurve"][0]["date"] == start.isoformat()
        assert all_period["equityCurve"][-1]["date"] == (start + timedelta(days=799)).isoformat()
        assert len(one_year["equityCurve"]) == 25
        assert one_year["equityCurve"][0]["date"] >= (start + timedelta(days=434)).isoformat()
        assert one_year["equityCurve"][-1]["date"] == (start + timedelta(days=799)).isoformat()
    finally:
        db.close()


def test_strategy_publication_requires_a_completed_run_and_max_snapshot():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="reviewed-strategy",
            name="Reviewed Strategy",
            category="congress",
            status="draft",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.commit()

        try:
            set_strategy_publication(
                db,
                slug=strategy.slug,
                published=True,
                entitlements=ENTITLEMENTS["premium"],
            )
            raise AssertionError("Expected publication to require a completed run and snapshot")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 422

        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="reviewed-run",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
        )
        db.add(run)
        db.flush()
        db.add(
            StrategyPerformanceSnapshot(
                strategy_id=strategy.id,
                run_id=run.id,
                as_of_date=date(2026, 7, 31),
                period="max",
                cagr_pct=12,
            )
        )
        db.add(StrategyHoldingsSnapshot(strategy_id=strategy.id, run_id=run.id, as_of_date=date(2026, 7, 31), holdings_count=0))
        db.commit()

        published = set_strategy_publication(
            db,
            slug=strategy.slug,
            published=True,
            entitlements=ENTITLEMENTS["premium"],
        )
        assert published["status"] == "published"
        assert published["publishedAt"] is not None
        assert [item["slug"] for item in list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])["items"]] == [strategy.slug]

        unpublished = set_strategy_publication(
            db,
            slug=strategy.slug,
            published=False,
            entitlements=ENTITLEMENTS["premium"],
        )
        assert unpublished["status"] == "draft"
        assert unpublished["publishedAt"] is None
        assert list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])["items"] == []
    finally:
        db.close()


def test_strategy_detail_keeps_research_public_and_allows_premium_followers_to_view_holdings():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="pro-only",
            name="Pro Only",
            category="cross_source",
            status="published",
            access_tier="pro",
            methodology_version="v1",
            rule_json=json.dumps({"kind": "test"}),
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="run-pro",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
            walnut_strategy_score=80,
        )
        db.add(run)
        db.flush()
        db.add(
            StrategyEquityCurvePoint(
                strategy_id=strategy.id,
                run_id=run.id,
                date=date(2026, 7, 31),
                strategy_value=140,
                benchmark_value=120,
            )
        )
        db.add(
            StrategyCurrentHolding(
                strategy_id=strategy.id,
                run_id=run.id,
                as_of_date=date(2026, 7, 31),
                symbol="NVDA",
                rank=1,
                weight_pct=10,
            )
        )
        db.commit()

        premium = strategy_detail(db, slug="pro-only", entitlements=ENTITLEMENTS["premium"])
        assert premium["access"]["locked"] is False
        assert premium["equityCurve"][0]["strategyValue"] == 140
        assert premium["currentHoldings"][0]["symbol"] == "NVDA"
        assert premium["strategyAccess"]["canFollow"] is True

        free = strategy_detail(db, slug="pro-only", entitlements=ENTITLEMENTS["free"])
        assert free["equityCurve"][0]["strategyValue"] == 140
        assert free["currentHoldings"] == []
        assert free["currentHoldingsCount"] == 1
    finally:
        db.close()


def test_strategy_detail_pages_current_holdings_without_loading_the_full_portfolio():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="many-holdings",
            name="Many Holdings",
            category="insider",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="many-holdings-run",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
        )
        db.add(run)
        db.flush()
        db.add_all(
            [
                StrategyCurrentHolding(
                    strategy_id=strategy.id,
                    run_id=run.id,
                    as_of_date=date(2026, 8, 10),
                    symbol=f"T{index:03d}",
                    rank=index,
                    weight_pct=1,
                )
                for index in range(1, 24)
            ]
        )
        db.commit()

        first_page = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], holdings_limit=20)
        final_page = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], holdings_offset=20, holdings_limit=20)

        assert first_page["currentHoldingsTotal"] == 23
        assert first_page["currentHoldingsOffset"] == 0
        assert len(first_page["currentHoldings"]) == 20
        assert first_page["currentHoldings"][0]["symbol"] == "T001"
        assert final_page["currentHoldingsOffset"] == 20
        assert [holding["symbol"] for holding in final_page["currentHoldings"]] == ["T021", "T022", "T023"]
    finally:
        db.close()


def test_strategy_detail_pages_persisted_transaction_history_and_hides_it_when_locked():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="history-strategy",
            name="History Strategy",
            category="insider",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="history-run",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
        )
        db.add(run)
        db.flush()
        db.add_all(
            [
                StrategyTrade(
                    strategy_id=strategy.id,
                    strategy_version_id=1,
                    strategy_run_id=run.id,
                    symbol=f"T{index:03d}",
                    ticker_at_time=f"T{index:03d}",
                    action="buy",
                    status="open",
                    effective_date=date(2026, 8, index),
                    entry_price=100 + index,
                )
                for index in range(1, 4)
            ]
        )
        db.commit()

        first_page = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], history_limit=2)
        final_page = strategy_detail(
            db,
            slug=strategy.slug,
            entitlements=ENTITLEMENTS["premium"],
            history_offset=2,
            history_limit=2,
        )
        locked = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["free"], history_limit=2)

        assert first_page["transactionHistoryTotal"] == 3
        assert len(first_page["transactionHistory"]) == 2
        assert first_page["transactionHistory"][0]["recordType"] == "model_trade"
        assert final_page["transactionHistory"][0]["symbol"] == "T001"
        assert locked["transactionHistory"] == []
        assert locked["transactionHistoryTotal"] == 0
    finally:
        db.close()


def test_strategy_detail_exposes_replicated_portfolio_source_history():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="replicated-history",
            name="Replicated History",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="replicated-history-run",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
            dataset_versions_json=json.dumps({"source": "replicated_portfolio_runs", "source_run_id": 92}),
        )
        db.add(run)
        db.add_all(
            [
                ReplicatedPortfolioPosition(
                    run_id=92,
                    symbol="NVDA",
                    side="buy",
                    entry_date=date(2026, 8, 5),
                    entry_price=170,
                    return_pct=8.5,
                    status="open",
                    source_type="reported_purchase",
                    confidence="high",
                ),
                ReplicatedPortfolioPosition(
                    run_id=92,
                    symbol="MSFT",
                    side="buy",
                    entry_date=date(2026, 7, 1),
                    entry_price=500,
                    return_pct=2.5,
                    status="closed",
                ),
            ]
        )
        db.commit()

        payload = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], history_limit=1)

        assert payload["transactionHistoryTotal"] == 2
        assert payload["transactionHistory"][0]["recordType"] == "reconstructed_position"
        assert payload["transactionHistory"][0]["symbol"] == "NVDA"
        assert payload["transactionHistory"][0]["sourceType"] == "reported_purchase"
    finally:
        db.close()


def test_individual_congress_strategy_exposes_latest_official_reported_holdings():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="congress-portfolio-m000001-1095d",
            name="Test Member Portfolio",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        document = HouseAnnualDisclosureDocument(
            member_name="Test Member",
            member_bioguide_id="M000001",
            filing_year=2025,
            filing_type="O",
            document_id="report-2025",
            filing_date=date(2026, 5, 1),
            report_url="https://example.test/report",
        )
        db.add_all([strategy, document])
        db.flush()
        db.add_all(
            [
                HouseAnnualDisclosureHolding(
                    document_row_id=document.id,
                    member_name="Test Member",
                    member_bioguide_id="M000001",
                    filing_year=2025,
                    document_id="report-2025",
                    asset_name=f"Asset {index:02d}",
                    symbol=f"T{index:02d}",
                    value_range="$15,001 - $50,000",
                    value_min=15_001,
                    value_max=50_000,
                )
                for index in range(1, 22)
            ]
        )
        db.commit()

        first_page = strategy_detail(
            db,
            slug=strategy.slug,
            entitlements=ENTITLEMENTS["premium"],
            reported_limit=20,
        )
        final_page = strategy_detail(
            db,
            slug=strategy.slug,
            entitlements=ENTITLEMENTS["premium"],
            reported_offset=20,
            reported_limit=20,
        )

        assert first_page["reportedHoldings"]["status"] == "ok"
        assert first_page["reportedHoldings"]["report"]["filingYear"] == 2025
        assert first_page["reportedHoldings"]["total"] == 21
        assert len(first_page["reportedHoldings"]["items"]) == 20
        assert final_page["reportedHoldings"]["offset"] == 20
        assert [item["symbol"] for item in final_page["reportedHoldings"]["items"]] == ["T21"]
    finally:
        db.close()


def test_strategy_detail_falls_back_to_persisted_holdings_until_live_monitoring_has_positions():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(slug="live-fallback", name="Live fallback", category="congress", status="published", methodology_version="v1")
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(strategy_id=strategy.id, run_key="live-fallback-run", status="ok", completed_at=datetime.now(timezone.utc), methodology_version="v1")
        version = StrategyVersion(strategy_id=strategy.id, version=1, status="active")
        db.add_all([run, version])
        db.flush()
        db.add(StrategyCurrentHolding(strategy_id=strategy.id, run_id=run.id, as_of_date=date(2026, 8, 14), symbol="NVDA", rank=1))
        db.commit()

        fallback = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"])
        assert fallback["holdingsSource"] == "historical_backtest"
        assert fallback["currentHoldings"][0]["symbol"] == "NVDA"

        db.add(StrategyLiveHolding(strategy_id=strategy.id, strategy_version_id=version.id, strategy_run_id=1, opening_trade_id=1, symbol="MSFT", ticker_at_time="MSFT", as_of_date=date(2026, 8, 15), rank=1))
        db.commit()
        live = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"])
        assert live["holdingsSource"] == "prospective_monitor"
        assert live["currentHoldings"][0]["symbol"] == "MSFT"
    finally:
        db.close()


def test_strategy_detail_prefers_persisted_three_year_transaction_history():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(slug="persisted-history", name="Persisted history", category="congress", status="published", methodology_version="v1")
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(strategy_id=strategy.id, run_key="persisted-history-run", status="ok", completed_at=datetime.now(timezone.utc), backtest_end_date=date(2026, 8, 14), methodology_version="v1")
        db.add(run)
        db.flush()
        db.add_all([
            StrategyHistoricalTransaction(strategy_id=strategy.id, strategy_run_id=run.id, source_key="one", record_type="backtest_lot", symbol="NVDA", action="buy", effective_date=date(2026, 8, 1)),
            StrategyHistoricalTransaction(strategy_id=strategy.id, strategy_run_id=run.id, source_key="old", record_type="backtest_lot", symbol="OLD", action="buy", effective_date=date(2020, 8, 1)),
        ])
        db.commit()
        payload = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"])
        assert payload["transactionHistoryTotal"] == 1
        assert payload["transactionHistory"][0]["symbol"] == "NVDA"
        assert payload["transactionHistoryStartDate"] == "2023-08-15"
    finally:
        db.close()
