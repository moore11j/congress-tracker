from datetime import date, datetime, timezone

from sqlalchemy import select

from app.entitlements import ENTITLEMENTS
from app.models import StrategyBacktestRun, PriceCache, StrategyCurrentHolding, StrategyEvaluationRun, StrategyHistoricalTransaction, StrategyTrade, StrategyLiveHolding
from app.services.strategies import strategy_detail
from app.services.strategy_evaluations import StrategyEvaluationCandidate as Candidate, evaluate_strategy_candidates
from app.services.strategy_portfolio_policy import select_candidates
from app.services.strategy_prices import position_prices
from app.utils.symbols import symbol_variants
from app.services.strategy_subscriptions import queue_recent_strategy_event_deliveries, upsert_strategy_subscription
from test_strategy_evaluations import _session, _strategy
from test_strategy_subscriptions import _strategy_and_user, _event


def test_position_caps_preserve_deterministic_newest_disclosures():
    candidates = [Candidate(f"S{i:03}", 1, qualification_snapshot={"publicDate": "2026-10-02" if i >= 75 else "2026-09-01"}) for i in range(100)]
    chosen = select_candidates(candidates, {})
    assert len(chosen) == 25
    assert {c.symbol for c in chosen} == {f"S{i:03}" for i in range(75, 100)}
    assert sum(c.weight_pct for c in chosen) == 100
    assert len(select_candidates(candidates, {"max_positions": 999})) == 50
    assert len(select_candidates(candidates, {"max_positions": 12})) == 12


def test_missing_ticker_cannot_become_share_class_or_priced_position():
    assert symbol_variants("N/A") == []
    chosen = select_candidates([Candidate("N/A", 50), Candidate("AAPL", 50)], {})
    assert [(c.symbol, c.weight_pct) for c in chosen] == [("AAPL", 100)]
    with _session()() as db:
        db.add(PriceCache(symbol="N/A", date="2026-10-02", close=110, open_price=100, adjustment_status="split_adjusted_price_return"))
        db.commit()
        marks = position_prices(db, symbol="N/A", entry_date=date(2026, 10, 2), as_of=date(2026, 10, 2))
        assert marks["entryPrice"] is None and marks["lastPrice"] is None


def test_rebalance_is_not_repeated_against_original_buy_weight():
    with _session()() as db:
        strategy, version = _strategy(db)
        def run(day, candidates):
            return evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 9, day), universe_count=2, candidates=candidates)
        run(1, [Candidate("AAPL", 100)])
        assert run(2, [Candidate("AAPL", 50), Candidate("MSFT", 50)])["changes"]["rebalanced"] == 1
        assert run(3, [Candidate("AAPL", 50), Candidate("MSFT", 50)])["changes"]["rebalanced"] == 0


def test_entry_is_filled_after_weekend_and_return_uses_same_price_basis():
    with _session()() as db:
        strategy, version = _strategy(db)
        evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 2), universe_count=1, candidates=[Candidate("AAPL", 100, effective_date=date(2026, 10, 3))])
        db.add(PriceCache(symbol="AAPL", date="2026-10-05", close=110, raw_close=110, adjusted_close=110, open_price=100, adjustment_status="split_adjusted_price_return"))
        db.commit()
        assert position_prices(db, symbol="AAPL", entry_date=date(2026, 10, 3), as_of=date(2026, 10, 4))["entryPrice"] is None
        evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 5), universe_count=1, candidates=[Candidate("AAPL", 100)])
        holding = db.execute(select(StrategyLiveHolding)).scalar_one()
        assert holding.entry_price == 100
        assert holding.entry_date == date(2026, 10, 5)
        marks = position_prices(db, symbol="AAPL", entry_date=holding.entry_date, as_of=date(2026, 10, 5))
        assert marks["lastPrice"] == 110


def test_missing_execution_session_does_not_use_later_open():
    with _session()() as db:
        db.add(PriceCache(symbol="AAPL", date="2026-10-06", close=110, open_price=100, adjustment_status="split_adjusted_price_return"))
        db.commit()
        assert position_prices(db, symbol="AAPL", entry_date=date(2026, 10, 5), as_of=date(2026, 10, 6))["entryPrice"] is None


def test_alert_batch_progresses_past_unsubscribed_and_previously_queued_events():
    with _session()() as db:
        strategy, user = _strategy_and_user(db)
        upsert_strategy_subscription(db, user_id=user.id, slug=strategy.slug, email_enabled=True, delivery_mode="daily", event_types=["trade_added"])
        for i in range(105):
            _event(db, strategy.id + 999, key=f"unrelated-{i}")
        first = _event(db, strategy.id, key="eligible-1")
        second = _event(db, strategy.id, key="eligible-2")
        assert queue_recent_strategy_event_deliveries(db, limit=1)["queued"] == 1
        assert queue_recent_strategy_event_deliveries(db, limit=1)["queued"] == 1
        assert queue_recent_strategy_event_deliveries(db, limit=1)["events"] == 0


def test_empty_evaluated_portfolio_does_not_resurrect_backtest_positions():
    with _session()() as db:
        strategy, version = _strategy(db)
        strategy.status = "published"
        version.status = "active"
        db.add(StrategyCurrentHolding(strategy_id=strategy.id, run_id=1, symbol="OLD", as_of_date=date(2026, 8, 1)))
        db.commit()
        evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 2), universe_count=0, candidates=[])
        payload = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"])
        assert payload["holdingsSource"] == "prospective_monitor"
        assert payload["currentHoldingsTotal"] == 0
        assert payload["currentHoldings"] == []
        assert payload["monitoring"]["lastEvaluatedDate"] == "2026-10-02"


def test_evaluation_persists_only_capped_positions_and_records_policy():
    with _session()() as db:
        strategy, version = _strategy(db)
        result = evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 2), universe_count=1000, candidates=[Candidate(f"S{i:03}", 0.1) for i in range(1000)])
        assert result["qualifyingCount"] == 25
        assert len(db.execute(select(StrategyLiveHolding)).scalars().all()) == 25
        run = db.execute(select(StrategyEvaluationRun)).scalar_one()
        assert 'focused_portfolio_v1' in run.metadata_json


def test_transaction_history_includes_new_model_changes_before_old_backtest():
    with _session()() as db:
        strategy, version = _strategy(db)
        strategy.status = "published"
        version.status = "active"
        backtest = StrategyBacktestRun(strategy_id=strategy.id, run_key="old", status="ok", methodology_version="test", completed_at=datetime(2026, 8, 1, tzinfo=timezone.utc))
        db.add(backtest)
        db.flush()
        db.add(StrategyHistoricalTransaction(strategy_id=strategy.id, strategy_run_id=backtest.id, source_key="old", record_type="backtest", symbol="OLD", action="buy", effective_date=date(2026, 8, 1)))
        db.commit()
        evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 2), universe_count=1, candidates=[Candidate("NEW", 100)])
        payload = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], history_limit=1)
        assert payload["transactionHistoryTotal"] == 2
        assert payload["transactionHistory"][0]["symbol"] == "NEW"
        payload = strategy_detail(db, slug=strategy.slug, entitlements=ENTITLEMENTS["premium"], history_limit=1, history_offset=1)
        assert payload["transactionHistory"][0]["symbol"] == "OLD"


def test_price_repair_rotates_and_uses_bounded_provider_requests(monkeypatch):
    from app.jobs import refresh_strategy_prices as repair
    monkeypatch.setattr(repair, "get_expected_latest_market_date", lambda: date(2026, 10, 2))
    calls = []
    monkeypatch.setattr(repair, "hydrate_split_adjusted_ohlc", lambda db, symbol, start, end: calls.append(symbol) or 1)
    with _session()() as db:
        strategy, version = _strategy(db)
        evaluate_strategy_candidates(db, strategy_id=strategy.id, strategy_version_id=version.id, evaluation_date=date(2026, 10, 1), universe_count=2, candidates=[Candidate("AAPL", 50), Candidate("MSFT", 50)])
        assert repair.refresh_strategy_prices(db, limit=1)["refreshed"] == 1
        assert repair.refresh_strategy_prices(db, limit=1)["refreshed"] == 1
        assert calls == ["AAPL", "MSFT"]


def test_oversized_transition_records_capacity_without_sale_alerts():
    from app.models import StrategyEvent
    from app.services.strategy_scheduler import run_active_strategy_evaluations
    from app.services.strategy_candidate_resolver import StrategyCandidateResolution
    from unittest.mock import patch
    with _session()() as db:
        strategy, version = _strategy(db)
        strategy.status = "published"
        version.status = "active"
        version.rules_json = '{"candidate_source":"disclosure_portfolio","trade_source":"insider"}'
        for i in range(60):
            db.add(StrategyTrade(strategy_id=strategy.id, strategy_version_id=version.id, strategy_run_id=1, symbol=f"S{i:03}", ticker_at_time=f"S{i:03}", action="buy", status="open", weight_pct=100/60))
        db.add(StrategyEvaluationRun(strategy_id=strategy.id, strategy_version_id=version.id, idempotency_key="previous", evaluation_date=date(2026, 10, 1), status="completed"))
        db.commit()
        candidates = [Candidate(f"S{i:03}", 100/60) for i in range(60)]
        now = datetime(2026, 10, 2, 23, tzinfo=timezone.utc)
        with patch.dict("os.environ", {"STRATEGY_EVALUATIONS_ENABLED":"true"}), patch("app.services.strategy_scheduler.resolve_strategy_candidates", return_value=StrategyCandidateResolution("disclosure_portfolio", candidates, 60, now)):
            result = run_active_strategy_evaluations(db, scheduled_for=now)
        assert result["failed"] == 0
        types = [e.event_type for e in db.execute(select(StrategyEvent)).scalars()]
        assert types.count("position_removed_by_policy") == 35
        assert "trade_exited" not in types
        assert types.count("rebalance_completed") == 1
