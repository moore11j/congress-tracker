"""Refresh, evaluate and deliver daily price thresholds without screener work."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from app.db import SessionLocal
from app.services.confirmation_monitoring import refresh_all_monitored_watchlist_confirmation_monitoring
from app.services.email_intraday import (
    intraday_alerts_enabled, intraday_schedule_dry_run_default,
    run_intraday_alert_sweep, summarize_intraday_alert_results,
)
from app.services.price_lookup import is_market_trading_day


def run_price_alert_cycle(*, now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    local = current.astimezone(ZoneInfo("America/New_York"))
    # The close sweep has a short grace period so the 16:00 observation is not
    # rejected by the generic intraday sweep's strict market-hours cutoff.
    if not is_market_trading_day(local.date()) or not time(9, 30) <= local.time() <= time(16, 10):
        return {"status": "skipped", "reason": "outside_price_alert_window"}
    if not intraday_alerts_enabled():
        return {"status": "skipped", "reason": "intraday_disabled"}
    if os.getenv("BACKGROUND_JOBS_PAUSED", "").lower() in {"true", "1", "yes", "on"}:
        return {"status": "skipped", "reason": "background_jobs_paused"}
    evaluation = refresh_all_monitored_watchlist_confirmation_monitoring(
        SessionLocal, refresh_quotes=True, price_rules_only=True,
    )
    # Recover unsent same-session alerts after a delayed/restarted sweep. Sent
    # recipients are protected by durable price-session delivery identities.
    delivery_now = now or datetime.now(timezone.utc)
    session_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    lookback_minutes = max(1, int((delivery_now - session_start).total_seconds() / 60) + 1)
    with SessionLocal() as db:
        results = run_intraday_alert_sweep(
            db, lookback_minutes=lookback_minutes, limit=500,
            dry_run=intraday_schedule_dry_run_default(), now=delivery_now,
            market_hours_only=False, custom_price_only=True,
        )
    delivery = summarize_intraday_alert_results(results)
    failed = evaluation.get("failures", 0) or delivery["failed_count"]
    return {"status": "failed" if failed else "ok", "evaluation": evaluation, "delivery": delivery}


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    result = run_price_alert_cycle()
    print(json.dumps(result, default=str), flush=True)
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
