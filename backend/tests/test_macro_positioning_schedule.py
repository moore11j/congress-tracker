from __future__ import annotations

from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_macro_positioning_refresh_runs_weekly_after_cot_release_window():
    crontab = (BACKEND_ROOT / "crontab").read_text()

    assert "CRON_TZ=America/Los_Angeles" in crontab
    assert "45 13 * * 5 cd /app && python -m app.jobs.refresh_macro_positioning" in crontab
    assert "45 5 * * 5 cd /app && python -m app.jobs.refresh_macro_positioning" not in crontab
