from app.clients.fmp import FMPClientError
from app.ingest_run import (
    _build_parser,
    _run_enrichment_queue_job,
    _run_institutional_ingest,
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
