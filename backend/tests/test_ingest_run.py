from app.clients.fmp import FMPClientError
from app.ingest_run import _run_institutional_ingest, _run_recent_congress_job


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
