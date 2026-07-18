import json
import logging
import pytest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.ingest_run import (
    _build_parser,
    _market_data_refresh_symbols,
    _payload_exit_code,
    _payload_json,
    _run_enrichment_queue_job,
    _run_institutional_ingest,
    _run_portfolio_methodology_guard_job,
    _run_portfolio_simulation_refresh_job,
    _run_priority_ticker_prewarm_job,
    _run_recent_congress_job,
)
from app.models import Base, Event, IndexMembership, PriceCache, SavedScreenSnapshot, Security, WatchlistItem


def test_institutional_ingest_provider_error_is_non_fatal(monkeypatch, caplog) -> None:
    def fake_scheduled_latest_once():
        return {"status": "retryable", "error": "latest endpoint timed out"}

    monkeypatch.setattr("app.ingest_run.run_scheduled_latest_once", fake_scheduled_latest_once)

    with caplog.at_level(logging.INFO, logger="app.ingest_run"):
        result = _run_institutional_ingest(pages=3, limit=200, days=30)

    assert result == {"status": "retryable", "error": "latest endpoint timed out"}
    assert "institutional_daily_ingest_delegating_to_latest_scheduler" in caplog.text
    assert "Traceback" not in caplog.text


def test_payload_json_serializes_nested_non_json_values(monkeypatch) -> None:
    monkeypatch.setenv("FMP_API_KEY", "sk_test_should_not_appear")
    payload = {
        "job": "core",
        "timestamp": datetime(2026, 6, 12, 20, 30, tzinfo=timezone.utc),
        "nested": {
            "created_at": datetime(2026, 6, 12, 21, 30, tzinfo=timezone.utc),
            "trade_date": date(2026, 6, 12),
            "amount": Decimal("12.50"),
            "symbols": {"MSFT", "AAPL"},
        },
    }

    encoded = _payload_json(payload)
    decoded = json.loads(encoded)

    assert decoded["timestamp"] == "2026-06-12T20:30:00+00:00"
    assert decoded["nested"]["created_at"] == "2026-06-12T21:30:00+00:00"
    assert decoded["nested"]["trade_date"] == "2026-06-12"
    assert decoded["nested"]["amount"] == 12.5
    assert decoded["nested"]["symbols"] == ["AAPL", "MSFT"]
    assert "sk_test_should_not_appear" not in encoded


def test_partial_daily_repair_payload_exits_successfully() -> None:
    payload = {
        "job": "daily-repair",
        "status": "partial",
        "partial_reason": "price_lookup_budget_exceeded",
    }

    assert _payload_exit_code(payload) == 0
    assert _payload_exit_code({"job": "daily-repair", "status": "failed"}) == 1


def test_scheduled_ingest_workflow_calls_ingest_module_directly() -> None:
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "daily_ingest.yml"
    contents = workflow.read_text()

    assert "scripts/run_ingest_job.sh" not in contents
    assert "REMOTE_COMMAND=\"cd /app && python -m app.ingest_run --job $JOB_MODE\"" in contents
    assert "Remote command: $REMOTE_COMMAND" in contents


def test_scheduled_ingest_workflow_retries_transient_fly_ssh_failures() -> None:
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "daily_ingest.yml"
    contents = workflow.read_text()

    assert "for attempt in 1 2 3" in contents
    assert "Fly SSH transport failed; retrying" in contents
    assert "tunnel unavailable|Error contacting Fly.io API|context deadline exceeded" in contents
    assert "has no started VMs|not have been deployed yet" in contents


def test_recent_congress_job_uses_small_recent_window(monkeypatch) -> None:
    seen = {}

    def fake_recent_ingest(*, days, pages, limit, sleep_s):
        seen.update({"days": days, "pages": pages, "limit": limit, "sleep_s": sleep_s})
        return {"status": "ok", "events_inserted": 3}

    monkeypatch.setenv("CONGRESS_RECENT_DAYS", "7")
    monkeypatch.setenv("CONGRESS_RECENT_PAGES", "12")
    monkeypatch.setenv("CONGRESS_RECENT_LIMIT", "50")
    monkeypatch.setenv("CONGRESS_RECENT_SLEEP_S", "0")
    monkeypatch.setattr("app.ingest_run.run_recent_congress_ingest", fake_recent_ingest)

    result = _run_recent_congress_job()

    assert result["job"] == "recent-congress"
    assert result["congress_recent"]["events_inserted"] == 3
    assert seen == {"days": 7, "pages": 12, "limit": 50, "sleep_s": 0.0}


def test_market_data_refresh_symbols_skips_old_cache_only_symbols_but_keeps_active_sources(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(
        bind=engine,
        tables=[
            Event.__table__,
            IndexMembership.__table__,
            PriceCache.__table__,
            SavedScreenSnapshot.__table__,
            Security.__table__,
            WatchlistItem.__table__,
        ],
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    monkeypatch.setenv("INGEST_SIGNALS_BENCHMARK", "SPY")
    monkeypatch.setenv("MARKET_DATA_REFRESH_PRIORITY_SYMBOLS", "NVDA")
    monkeypatch.setenv("MARKET_DATA_REFRESH_STALE_CACHE_MAX_AGE_DAYS", "730")

    with Session() as db:
        db.add_all(
            [
                PriceCache(symbol="TWTR", date="2021-01-04", close=40.0),
                PriceCache(symbol="XLNX", date="2021-01-04", close=140.0),
                PriceCache(symbol="WATCH", date="2021-01-04", close=10.0),
                PriceCache(symbol="SNAP", date="2021-01-04", close=12.0),
                PriceCache(symbol="INDEX", date="2021-01-04", close=14.0),
                PriceCache(symbol="EVENT", date="2021-01-04", close=16.0),
                PriceCache(symbol="RECENT", date="2026-06-01", close=18.0),
                Event(
                    event_type="insider_trade",
                    ts=datetime.now(timezone.utc),
                    event_date=datetime.now(timezone.utc),
                    symbol="EVENT",
                    source="test",
                    payload_json="{}",
                ),
                SavedScreenSnapshot(
                    user_id=1,
                    saved_screen_id=1,
                    ticker="SNAP",
                    observed_at=datetime.now(timezone.utc),
                ),
                IndexMembership(
                    index_code="sp500",
                    symbol="INDEX",
                    effective_from=date(2026, 1, 1),
                    effective_to=None,
                    source="test",
                    source_as_of=date(2026, 7, 17),
                    refreshed_at=datetime.now(timezone.utc),
                    is_active=True,
                ),
            ]
        )
        watch_security = Security(symbol="WATCH", name="Watchlist Inc.", asset_class="stock", sector=None)
        db.add(watch_security)
        db.flush()
        db.add(WatchlistItem(watchlist_id=1, security_id=watch_security.id))
        db.commit()

        symbols = _market_data_refresh_symbols(db, expected_date=date(2026, 7, 17), limit=20)

    assert symbols[:2] == ["NVDA", "SPY"]
    assert {"EVENT", "WATCH", "SNAP", "INDEX", "RECENT"}.issubset(symbols)
    assert "TWTR" not in symbols
    assert "XLNX" not in symbols


def test_enrichment_queue_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "enrichment-queue"])

    assert args.job == "enrichment-queue"


def test_priority_ticker_prewarm_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "priority-ticker-prewarm"])

    assert args.job == "priority-ticker-prewarm"


def test_market_data_refresh_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "market-data-refresh-daily"])

    assert args.job == "market-data-refresh-daily"


def test_portfolio_simulation_refresh_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "portfolio-simulation-refresh"])

    assert args.job == "portfolio-simulation-refresh"


def test_portfolio_methodology_guard_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "portfolio-methodology-guard"])

    assert args.job == "portfolio-methodology-guard"


def test_institutional_latest_daily_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "institutional-latest-daily"])

    assert args.job == "institutional-latest-daily"


def test_scheduled_ingest_workflow_includes_market_data_refresh() -> None:
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "daily_ingest.yml"
    contents = workflow.read_text()
    crontab = Path(__file__).resolve().parents[1] / "crontab"

    assert "market-data-refresh-daily" in contents
    assert 'JOB_MODE="market-data-refresh-daily"' in contents
    assert "market-data-refresh-daily" in crontab.read_text()


def test_scheduled_ingest_workflow_includes_portfolio_refresh() -> None:
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "daily_ingest.yml"
    contents = workflow.read_text()
    crontab = Path(__file__).resolve().parents[1] / "crontab"

    assert "portfolio-simulation-refresh" in contents
    assert 'JOB_MODE="portfolio-simulation-refresh"' in contents
    assert "portfolio-methodology-guard" in contents
    assert "portfolio-simulation-refresh" in crontab.read_text()


def test_crontab_runs_institutional_latest_through_ingest_dispatcher() -> None:
    crontab = Path(__file__).resolve().parents[1] / "crontab"
    contents = crontab.read_text()

    assert "python -m app.ingest_run --job institutional-latest-daily" in contents
    assert "scripts/run_institutional_latest_job.sh" not in contents


def test_portfolio_simulation_refresh_runs_standard_backfill_commands(monkeypatch) -> None:
    calls = []

    def fake_run(command, *, check, capture_output, text):
        calls.append(command)
        assert check is True
        assert capture_output is True
        assert text is True
        return SimpleNamespace(stdout=json.dumps({"summary": {"created": 2, "failed": 0}}))

    monkeypatch.setenv("PORTFOLIO_REFRESH_LOOKBACKS", "365,30")
    monkeypatch.setenv("PORTFOLIO_REFRESH_BENCHMARK", "SPY")
    monkeypatch.setenv("PORTFOLIO_REFRESH_BATCH_SIZE", "25")
    monkeypatch.setenv("PORTFOLIO_REFRESH_REPLACE_EXISTING", "1")
    monkeypatch.setattr(
        "app.ingest_run.check_background_job_guard",
        lambda job: SimpleNamespace(proceed=True, reason="ok", to_dict=lambda: {"job": job}),
    )
    monkeypatch.setattr("app.ingest_run.subprocess.run", fake_run)
    monkeypatch.setattr("app.ingest_run._portfolio_current_methodology_counts", lambda **_kwargs: {365: 2, 30: 2})

    result = _run_portfolio_simulation_refresh_job()

    assert result["job"] == "portfolio-simulation-refresh"
    assert result["lookbacks"] == [365, 30]
    assert result["counts"] == {365: 2, 30: 2}
    assert len(calls) == 2
    assert all("--replace-existing" in command for command in calls)
    assert calls[0][calls[0].index("--lookback-days") + 1] == "365"
    assert calls[1][calls[1].index("--lookback-days") + 1] == "30"
    assert calls[0][calls[0].index("--benchmark") + 1] == "SPY"
    assert calls[0][calls[0].index("--batch-size") + 1] == "25"


def test_portfolio_methodology_guard_fails_without_current_runs_or_scheduled_backfill(monkeypatch) -> None:
    monkeypatch.setenv("PORTFOLIO_REFRESH_LOOKBACKS", "365,30")
    monkeypatch.delenv("PORTFOLIO_METHODOLOGY_BACKFILL_SCHEDULED", raising=False)
    monkeypatch.setattr("app.ingest_run._portfolio_current_methodology_counts", lambda **_kwargs: {365: 0, 30: 2})

    with pytest.raises(RuntimeError, match="Current portfolio methodology has no persisted runs"):
        _run_portfolio_methodology_guard_job()


def test_portfolio_methodology_guard_allows_explicit_scheduled_backfill(monkeypatch) -> None:
    monkeypatch.setenv("PORTFOLIO_REFRESH_LOOKBACKS", "365,30")
    monkeypatch.setenv("PORTFOLIO_METHODOLOGY_BACKFILL_SCHEDULED", "1")
    monkeypatch.setattr("app.ingest_run._portfolio_current_methodology_counts", lambda **_kwargs: {365: 0, 30: 2})

    result = _run_portfolio_methodology_guard_job()

    assert result["status"] == "scheduled"
    assert result["missing_lookbacks"] == [365]
    assert result["backfill_scheduled"] is True


def test_enrichment_queue_job_uses_bounded_env(monkeypatch) -> None:
    seen = {}

    def fake_process_data_enrichment_jobs(*, limit, max_seconds):
        seen.update({"limit": limit, "max_seconds": max_seconds})
        return {"processed": 0, "succeeded": 0, "failed": 0, "skipped": 0}

    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")
    monkeypatch.setenv("DATA_ENRICHMENT_QUEUE_BATCH_SIZE", "50")
    monkeypatch.setenv("DATA_ENRICHMENT_QUEUE_MAX_SECONDS", "45")
    monkeypatch.setattr("app.ingest_run.process_data_enrichment_jobs", fake_process_data_enrichment_jobs)

    result = _run_enrichment_queue_job()

    assert result == {
        "job": "enrichment-queue",
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
    }
    assert seen == {"limit": 50, "max_seconds": 45}


def test_enrichment_queue_job_defaults_to_disabled(monkeypatch) -> None:
    monkeypatch.delenv("ENRICHMENT_QUEUE_ENABLED", raising=False)
    monkeypatch.setattr(
        "app.ingest_run.process_data_enrichment_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("queue should not run")),
    )

    result = _run_enrichment_queue_job()

    assert result["job"] == "enrichment-queue"
    assert result["reason"] == "enrichment_queue_disabled"
    assert result["skipped"] == 1


def test_enrichment_queue_job_skips_when_background_guard_blocks(monkeypatch) -> None:
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")
    monkeypatch.setattr(
        "app.ingest_run.check_background_job_guard",
        lambda job: SimpleNamespace(proceed=False, reason="db_active_connection_pressure", to_dict=lambda: {"job": job}),
    )
    monkeypatch.setattr(
        "app.ingest_run.process_data_enrichment_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("queue should not run")),
    )

    result = _run_enrichment_queue_job()

    assert result["job"] == "enrichment-queue"
    assert result["status"] == "skipped"
    assert result["reason"] == "db_active_connection_pressure"
    assert result["processed"] == 0


def test_data_enrichment_queue_processes_trade_outcome_jobs(monkeypatch) -> None:
    from app.services.data_enrichment_queue import _process_one

    calls = []

    def fake_run_compute(**kwargs):
        calls.append(kwargs)
        return {"inserted": 1}

    monkeypatch.setattr("app.compute_trade_outcomes.run_compute", fake_run_compute)
    job = SimpleNamespace(
        job_type="trade_outcomes",
        symbol=None,
        window_key="feed:insider_trade:30d:25",
        payload_json=json.dumps(
            {
                "event_type": "insider_trade",
                "lookback_days": 30,
                "limit": 25,
                "retry_failed_statuses": "no_data,no_current_price",
            }
        ),
    )

    _process_one(SimpleNamespace(), job)

    assert calls == [
        {
            "replace": False,
            "limit": 25,
            "member_id": None,
            "event_type": "insider_trade",
            "benchmark_symbol": "SPY",
            "lookback_days": 30,
            "trade_date_after": None,
            "only_missing": True,
            "retry_failed_status": None,
            "retry_failed_statuses": "no_data,no_current_price",
        }
    ]


def test_priority_ticker_prewarm_job_uses_bounded_env(monkeypatch) -> None:
    seen = {}

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_enqueue_priority_ticker_prewarm_jobs(db, *, symbol_limit, popular_limit, per_user_limit, source, mode):
        seen.update(
            {
                "db": db,
                "symbol_limit": symbol_limit,
                "popular_limit": popular_limit,
                "per_user_limit": per_user_limit,
                "source": source,
                "mode": mode,
            }
        )
        return {
            "symbol_count": 2,
            "enqueued": 10,
            "attempted": 18,
            "symbols": ["BMNR", "MSTR"],
            "watchlist_symbol_count": 1,
            "recently_viewed_symbol_count": 0,
            "popular_symbol_count": 1,
            "landing_symbol_count": 0,
            "enqueued_by_type": {"quote": 2},
            "attempted_by_type": {"quote": 2},
            "skipped_budget": 0,
        }

    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_ENABLED", "true")
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_SYMBOL_LIMIT", "2")
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_POPULAR_LIMIT", "1")
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_PER_USER_LIMIT", "1")
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_MODE", "core")
    monkeypatch.setattr("app.ingest_run.SessionLocal", FakeSession)
    monkeypatch.setattr("app.ingest_run.enqueue_priority_ticker_prewarm_jobs", fake_enqueue_priority_ticker_prewarm_jobs)

    result = _run_priority_ticker_prewarm_job()

    assert result["job"] == "priority-ticker-prewarm"
    assert result["symbols"] == ["BMNR", "MSTR"]
    assert seen["symbol_limit"] == 2
    assert seen["popular_limit"] == 1
    assert seen["per_user_limit"] == 1
    assert seen["source"] == "priority_ticker_prewarm"
    assert seen["mode"] == "core"


def test_priority_ticker_prewarm_job_defaults_to_disabled(monkeypatch) -> None:
    monkeypatch.delenv("PRIORITY_TICKER_PREWARM_ENABLED", raising=False)
    monkeypatch.setattr(
        "app.ingest_run.enqueue_priority_ticker_prewarm_jobs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("prewarm should not run")),
    )

    result = _run_priority_ticker_prewarm_job()

    assert result["job"] == "priority-ticker-prewarm"
    assert result["status"] == "skipped"
    assert result["reason"] == "priority_ticker_prewarm_disabled"


def test_priority_ticker_prewarm_job_skips_when_background_guard_blocks(monkeypatch) -> None:
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_ENABLED", "true")
    monkeypatch.setattr(
        "app.ingest_run.check_background_job_guard",
        lambda job: SimpleNamespace(proceed=False, reason="db_total_connection_pressure", to_dict=lambda: {"job": job}),
    )
    monkeypatch.setattr(
        "app.ingest_run.enqueue_priority_ticker_prewarm_jobs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("prewarm should not run")),
    )

    result = _run_priority_ticker_prewarm_job()

    assert result["job"] == "priority-ticker-prewarm"
    assert result["status"] == "skipped"
    assert result["reason"] == "db_total_connection_pressure"
    assert result["attempted"] == 0
    assert result["enqueued"] == 0
