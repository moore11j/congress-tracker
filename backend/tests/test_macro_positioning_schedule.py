from __future__ import annotations

from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_macro_positioning_refresh_runs_weekly_after_cot_release_window():
    crontab = (BACKEND_ROOT / "crontab").read_text()

    assert "CRON_TZ=America/Los_Angeles" in crontab
    assert "45 13 * * 5 cd /app && python -m app.jobs.refresh_macro_positioning" in crontab
    assert "45 5 * * 5 cd /app && python -m app.jobs.refresh_macro_positioning" not in crontab


def test_outcome_ledger_hydrator_runs_during_market_window_and_after_close():
    crontab = (BACKEND_ROOT / "crontab").read_text()
    ingest_run = (BACKEND_ROOT / "app" / "ingest_run.py").read_text()
    fly_config = (BACKEND_ROOT / "fly.toml").read_text()

    assert "10 7-13 * * 1-5 cd /app && python -m app.ingest_run --job outcome-ledger-hydrator" in crontab
    assert "35 16 * * 1-5 cd /app && python -m app.ingest_run --job outcome-ledger-hydrator" in crontab
    assert "20 17 * * 1-5 cd /app && python -m app.ingest_run --job outcome-ledger-history-backfill" in crontab
    assert '"outcome-ledger-hydrator"' in ingest_run
    assert '"outcome-ledger-history-backfill"' in ingest_run
    assert 'OUTCOME_LEDGER_HYDRATOR_ENABLED = "true"' in fly_config
    assert 'OUTCOME_LEDGER_HISTORY_BACKFILL_ENABLED = "true"' in fly_config
