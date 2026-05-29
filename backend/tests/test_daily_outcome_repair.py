from __future__ import annotations

from app import ingest_run


def test_daily_outcome_repair_uses_only_missing_safe_retry_statuses(monkeypatch):
    calls: list[dict] = []

    def fake_run_compute(**kwargs):
        calls.append(kwargs)
        return {
            "event_type": kwargs["event_type"],
            "inserted": 1,
            "updated": 2,
            "skipped": 3,
            "status_counts": {"ok": 1},
        }

    monkeypatch.setattr(ingest_run, "run_compute", fake_run_compute)
    monkeypatch.setattr(
        ingest_run,
        "_daily_outcome_coverage_report",
        lambda *, lookback_days: {"lookback_days": lookback_days, "failed_statuses": {}},
    )
    monkeypatch.delenv("OUTCOME_REPAIR_LIMIT", raising=False)

    report = ingest_run._run_daily_outcome_repair()

    assert report["job"] == "daily-repair"
    assert [call["event_type"] for call in calls] == ["congress_trade", "insider_trade"]
    assert all(call["only_missing"] is True for call in calls)
    assert all(call["replace"] is False for call in calls)
    assert all(call["lookback_days"] == 1095 for call in calls)
    assert all(call["retry_failed_statuses"] == ingest_run.SAFE_OUTCOME_RETRY_STATUSES for call in calls)
