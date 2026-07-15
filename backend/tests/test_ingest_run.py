import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.ingest_run import (
    _build_parser,
    _payload_exit_code,
    _payload_json,
    _run_enrichment_queue_job,
    _run_institutional_ingest,
    _run_priority_ticker_prewarm_job,
    _run_recent_congress_job,
)


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


def test_enrichment_queue_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "enrichment-queue"])

    assert args.job == "enrichment-queue"


def test_priority_ticker_prewarm_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "priority-ticker-prewarm"])

    assert args.job == "priority-ticker-prewarm"


def test_market_data_refresh_job_is_accepted_by_parser() -> None:
    args = _build_parser().parse_args(["--job", "market-data-refresh-daily"])

    assert args.job == "market-data-refresh-daily"


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


def test_crontab_runs_institutional_latest_through_ingest_dispatcher() -> None:
    crontab = Path(__file__).resolve().parents[1] / "crontab"
    contents = crontab.read_text()

    assert "python -m app.ingest_run --job institutional-latest-daily" in contents
    assert "scripts/run_institutional_latest_job.sh" not in contents


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
