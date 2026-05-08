from app.clients.fmp import FMPClientError
from app.ingest_run import _run_institutional_ingest


def test_institutional_ingest_provider_error_is_non_fatal(monkeypatch) -> None:
    def fail_institutional_ingest(*, pages, limit, days):
        raise FMPClientError("FMP institutional API request failed: 402: Restricted Endpoint")

    monkeypatch.setattr("app.ingest_run.institutional_ingest_run", fail_institutional_ingest)

    result = _run_institutional_ingest(pages=3, limit=200, days=30)

    assert result["status"] == "skipped_provider_error"
    assert "Restricted Endpoint" in result["error"]
