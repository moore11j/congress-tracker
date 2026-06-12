import json
from types import SimpleNamespace

from app.clients.fmp import FMPClientError
from app.ingest_run import (
    _build_parser,
    _run_enrichment_queue_job,
    _run_institutional_ingest,
    _run_priority_ticker_prewarm_job,
    _run_recent_congress_job,
)


def test_institutional_ingest_provider_error_is_non_fatal(monkeypatch) -> None:
    def fail_institutional_ingest(*, pages, limit, days):
        raise FMPClientError("FMP institutional API request failed: 402: Restricted Endpoint")

    monkeypatch.setattr("app.ingest_run.institutional_ingest_run", fail_institutional_ingest)

    result = _run_institutional_ingest(pages=3, limit=200, days=30)

    assert result["status"] == "skipped_provider_error"
    assert "Restricted Endpoint" in result["error"]


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


def test_enrichment_queue_job_uses_bounded_env(monkeypatch) -> None:
    seen = {}

    def fake_process_data_enrichment_jobs(*, limit, max_seconds):
        seen.update({"limit": limit, "max_seconds": max_seconds})
        return {"processed": 0, "succeeded": 0, "failed": 0, "skipped": 0}

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
            "benchmark_symbol": "^GSPC",
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

    def fake_enqueue_priority_ticker_prewarm_jobs(db, *, symbol_limit, popular_limit, source):
        seen.update({"db": db, "symbol_limit": symbol_limit, "popular_limit": popular_limit, "source": source})
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

    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_SYMBOL_LIMIT", "2")
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_POPULAR_LIMIT", "1")
    monkeypatch.setattr("app.ingest_run.SessionLocal", FakeSession)
    monkeypatch.setattr("app.ingest_run.enqueue_priority_ticker_prewarm_jobs", fake_enqueue_priority_ticker_prewarm_jobs)

    result = _run_priority_ticker_prewarm_job()

    assert result["job"] == "priority-ticker-prewarm"
    assert result["symbols"] == ["BMNR", "MSTR"]
    assert seen["symbol_limit"] == 2
    assert seen["popular_limit"] == 1
    assert seen["source"] == "priority_ticker_prewarm"
