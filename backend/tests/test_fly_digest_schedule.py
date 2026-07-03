from __future__ import annotations

import tomllib
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_fly_cron_process_is_separate_from_web_process():
    fly_config = tomllib.loads((BACKEND_ROOT / "fly.toml").read_text())

    assert fly_config["processes"]["app"] == "uvicorn app.main:app --host 0.0.0.0 --port 8080"
    assert fly_config["processes"]["cron"] == "supercronic /app/crontab"
    assert fly_config["http_service"]["processes"] == ["app"]
    assert fly_config["env"]["DATA_ENRICHMENT_QUEUE_ENABLED"] == "true"
    assert fly_config["env"]["DATA_ENRICHMENT_QUEUE_BATCH_SIZE"] == "50"
    assert fly_config["env"]["DATA_ENRICHMENT_QUEUE_MAX_SECONDS"] == "45"
    assert fly_config["env"]["FEED_PNL_REPAIR_ENABLED"] == "true"
    assert fly_config["env"]["FEED_PNL_REPAIR_DAYS"] == "3"
    assert fly_config["env"]["FEED_PNL_REPAIR_LIMIT"] == "500"
    assert fly_config["env"]["FEED_PNL_REPAIR_MAX_SECONDS"] == "60"
    assert fly_config["env"]["INSTITUTIONAL_SCHEDULED_INGEST_ENABLED"] == "false"
    assert fly_config["env"]["INSTITUTIONAL_SCHEDULED_INGEST_START_PAGE"] == "9"
    assert fly_config["env"]["INSTITUTIONAL_SCHEDULED_INGEST_MAX_SECONDS"] == "900"


def test_dockerfile_lets_fly_process_groups_override_commands():
    dockerfile = (BACKEND_ROOT / "Dockerfile").read_text()

    assert "ENTRYPOINT" not in dockerfile
    assert 'CMD ["uvicorn", "app.main:app"' in dockerfile
    assert "supercronic -version" in dockerfile


def test_crontab_schedules_bounded_daily_digest_and_intraday_jobs():
    crontab = (BACKEND_ROOT / "crontab").read_text()

    assert "CRON_TZ=America/Los_Angeles" in crontab
    assert "0 7 * * * cd /app && sh /app/scripts/run_email_digest_schedule.sh monitoring" in crontab
    assert "5 7 * * * cd /app && sh /app/scripts/run_email_digest_schedule.sh watchlist_activity" in crontab
    assert "10 7 * * * cd /app && sh /app/scripts/run_email_digest_schedule.sh signals" in crontab
    assert "*/5 * * * * cd /app && sh /app/scripts/run_feed_pnl_repair.sh" in crontab
    assert "*/5 * * * * cd /app && sh /app/scripts/run_enrichment_queue.sh" in crontab
    assert "17 * * * * cd /app && sh /app/scripts/run_institutional_latest_job.sh" in crontab
    assert "20 5,12 * * 1-5 cd /app && python -m app.jobs.refresh_fred_macro_cache" in crontab
    assert "*/15 6-13 * * 1-5 cd /app && python -m app.jobs.refresh_insights_snapshot --kind all" in crontab
    assert "30 6 * * 1-5 cd /app && sh /app/scripts/run_email_intraday_alert_sweep.sh" in crontab
    assert "0,30 7-12 * * 1-5 cd /app && sh /app/scripts/run_email_intraday_alert_sweep.sh" in crontab
    assert "0 13 * * 1-5 cd /app && sh /app/scripts/run_email_intraday_alert_sweep.sh" in crontab
    assert "billing" not in crontab.lower()
    assert "monthly" not in crontab.lower()


def test_digest_schedule_wrapper_is_gated_and_bounded():
    script = (BACKEND_ROOT / "scripts" / "run_email_digest_schedule.sh").read_text()

    assert 'EMAIL_DIGEST_SCHEDULE_ENABLED:-0' in script
    assert "email_digest_schedule_disabled" in script
    assert 'EMAIL_DIGEST_SCHEDULE_LIMIT:-100' in script
    assert 'EMAIL_DIGEST_SCHEDULE_LOOKBACK_DAYS:-1' in script
    assert 'EMAIL_DIGEST_SCHEDULE_DRY_RUN:-0' in script
    assert "--dry-run" in script
    assert 'set -- python -m app.jobs.send_email_digests --kind "$kind"' in script
    assert "monitoring|watchlist_activity|signals" in script
    assert "billing" not in script.lower()
    assert "monthly" not in script.lower()


def test_intraday_schedule_wrapper_defaults_to_dry_run_and_is_bounded():
    script = (BACKEND_ROOT / "scripts" / "run_email_intraday_alert_sweep.sh").read_text()

    assert 'EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES:-60' in script
    assert 'EMAIL_ALERT_SWEEP_LIMIT:-100' in script
    assert 'EMAIL_ALERT_SCHEDULE_DRY_RUN:-true' in script
    assert "--dry-run" in script
    assert "python -m app.jobs.send_intraday_email_alerts" in script


def test_enrichment_queue_wrapper_is_gated_bounded_and_non_overlapping():
    script = (BACKEND_ROOT / "scripts" / "run_enrichment_queue.sh").read_text()

    assert "FMP_BACKGROUND_REFRESH_ENABLED:-true" in script
    assert "DATA_ENRICHMENT_QUEUE_ENABLED:-true" in script
    assert "DATA_ENRICHMENT_QUEUE_BATCH_SIZE:-50" in script
    assert "DATA_ENRICHMENT_QUEUE_MAX_SECONDS:-45" in script
    assert "mkdir \"$lock_dir\"" in script
    assert "worker_already_running" in script
    assert "timeout \"$hard_timeout\" python -m app.ingest_run --job enrichment-queue" in script
    assert "processed=%s succeeded=%s failed=%s skipped=%s" in script


def test_feed_pnl_repair_wrapper_is_gated_bounded_and_non_overlapping():
    script = (BACKEND_ROOT / "scripts" / "run_feed_pnl_repair.sh").read_text()

    assert "FEED_PNL_REPAIR_ENABLED:-true" in script
    assert "FEED_PNL_REPAIR_DAYS:-3" in script
    assert "FEED_PNL_REPAIR_LIMIT:-500" in script
    assert "FEED_PNL_REPAIR_MAX_SECONDS:-60" in script
    assert "mkdir \"$lock_dir\"" in script
    assert "repair_already_running" in script
    assert "timeout \"$max_seconds\" python -m app.repair_recent_feed_pnl" in script


def test_institutional_latest_job_wrapper_is_disabled_bounded_and_non_overlapping():
    script = (BACKEND_ROOT / "scripts" / "run_institutional_latest_job.sh").read_text()

    assert "INSTITUTIONAL_SCHEDULED_INGEST_ENABLED:-false" in script
    assert "institutional_latest_job_disabled" in script
    assert "INSTITUTIONAL_SCHEDULED_INGEST_MAX_SECONDS:-900" in script
    assert "mkdir \"$lock_dir\"" in script
    assert "worker_already_running" in script
    assert "timeout \"$max_seconds\" python -m app.ingest_institutional_activity --scheduled-latest-once --log-level INFO" in script
