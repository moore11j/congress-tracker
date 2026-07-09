# Email Digest and Intraday Alert Delivery

Walnut email notifications are delivered only by explicit admin actions or background job CLIs. Page reads, feed loads, screener loads, leaderboard loads, and ticker loads do not send email.

There are two user-facing notification modes:

- Intraday Alerts: high-priority watchlist activity and high-conviction signal matches during market hours only.
- Daily Digests: daily summaries of watchlist activity and ranked monitoring candidates, including lower and medium-priority items that did not qualify for intraday alerts.

## Current Automatic Scheduling State

The repository schedules daily digest delivery with a separate Fly `cron` process group in `backend/fly.toml`. The web `app` process serves requests only; it does not run email jobs in request threads.

Fly Machines scheduled jobs were considered, but Fly's built-in scheduled Machine interval is coarse (`hourly`, `daily`, `weekly`, or `monthly`) rather than a precise Pacific wall-clock time. The checked-in schedule uses Supercronic in a single `cron` Machine so the 7:00/7:05 AM Pacific times are explicit and deployable with the app image.

Scheduled digest sends are gated by `EMAIL_DIGEST_SCHEDULE_ENABLED=1`. Without that Fly secret, the cron Machine logs a disabled message and exits without calling the digest CLI. Use the admin run-now endpoint or CLI dry-runs before enabling scheduled sends.

Intraday alert sends are separately gated by `EMAIL_ALERT_INTRADAY_ENABLED=false` by default. The intraday scheduler defaults to dry-run with `EMAIL_ALERT_SCHEDULE_DRY_RUN=true`, so scheduled sweeps can be observed before production sending is enabled.

## Daily Windows

Scheduled digest jobs use the previous midnight-to-midnight window in America/Los_Angeles. A run at 7:00 AM Pacific on June 5 sends the June 4 00:00 through June 5 00:00 Pacific window.

## CLI Commands

Dry-run first:

```powershell
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind monitoring --lookback-days 1 --limit 100 --dry-run"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind watchlist_activity --lookback-days 1 --limit 100 --dry-run"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_intraday_email_alerts --lookback-minutes 60 --limit 100 --dry-run"
```

Send:

```powershell
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind monitoring --lookback-days 1 --limit 100"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind watchlist_activity --lookback-days 1 --limit 100"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_intraday_email_alerts --lookback-minutes 60 --limit 100"
```

Suggested external schedule:

- `monitoring`: ranked monitoring candidates daily around 7:00 AM Pacific.
- `watchlist_activity`: daily around 7:05 AM Pacific.
- intraday sweep: every 30 minutes during market hours on weekdays.

## Admin Endpoint

Admins can run the same bounded job engine:

`POST /api/admin/email/digests/run-now`

```json
{
  "kind": "watchlist_activity",
  "lookback_days": 1,
  "limit": 100,
  "force": false,
  "dry_run": true
}
```

Use `dry_run: false` only after reviewing the summary. Billing statements remain admin-triggered test sends and are not included in scheduled jobs.

Admins can also dry-run or run the intraday sweep:

`POST /api/admin/email/intraday/run-now`

```json
{
  "lookback_minutes": 60,
  "limit": 100,
  "dry_run": true,
  "market_hours_only": true
}
```

Review `candidate_count`, `sent_count`, `skipped_count`, and `skip_reasons` before turning off dry-run. Expected skip reasons include `low_priority`, `low_conviction`, `monitoring_digest_only`, `outside_market_hours`, `intraday_disabled`, and `duplicate_alert_already_sent`.

## Fly Scheduled Delivery

The production schedule lives in `backend/crontab`:

- `monitoring`: `0 7 * * *` Pacific.
- `watchlist_activity`: `5 7 * * *` Pacific.
- intraday sweep: every 30 minutes during market hours on weekdays.

Each scheduled command calls `scripts/run_email_digest_schedule.sh`, which validates the digest kind and then calls `python -m app.jobs.send_email_digests` with `--lookback-days 1 --limit 100` by default. The digest engine remains idempotent for the midnight-to-midnight Pacific window, and the per-run limit bounds sends if a large backlog appears.

The intraday schedule calls `scripts/run_email_intraday_alert_sweep.sh`, which validates `EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES` and `EMAIL_ALERT_SWEEP_LIMIT`, then calls `python -m app.jobs.send_intraday_email_alerts`. The intraday engine uses event-level idempotency keys so duplicate sweeps do not resend the same alert.

Suggested intraday defaults:

```powershell
EMAIL_ALERT_INTRADAY_ENABLED=false
EMAIL_ALERT_MIN_SCORE=80
EMAIL_ALERT_MIN_FLOW_USD=250000
EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES=60
EMAIL_ALERT_SCHEDULE_DRY_RUN=true
```

Deploy from the backend root:

```powershell
cd backend
fly deploy
fly scale count app=1 cron=1 -a congress-tracker-api
```

Keep exactly one `cron` Machine running. More than one `cron` Machine can cause duplicate attempts; idempotency should skip already-delivered digests, but the intended operational shape is a single scheduler.

Before enabling or after schedule changes, run the dry-run CLI commands above or call the admin run-now endpoint with `"dry_run": true`. Review totals, skipped rows, and failures before sending manually or enabling the active schedule.

To observe scheduled dry-runs without sending email:

```powershell
fly secrets set EMAIL_DIGEST_SCHEDULE_ENABLED=1 EMAIL_DIGEST_SCHEDULE_DRY_RUN=1 -a congress-tracker-api
fly secrets set EMAIL_ALERT_INTRADAY_ENABLED=false EMAIL_ALERT_SCHEDULE_DRY_RUN=true -a congress-tracker-api
```

To enable scheduled sends after dry-runs pass:

```powershell
fly secrets set EMAIL_DIGEST_SCHEDULE_ENABLED=1 EMAIL_DIGEST_SCHEDULE_DRY_RUN=0 EMAIL_DIGEST_SCHEDULE_LIMIT=100 -a congress-tracker-api
fly secrets set EMAIL_ALERT_INTRADAY_ENABLED=true EMAIL_ALERT_SCHEDULE_DRY_RUN=false EMAIL_ALERT_SWEEP_LIMIT=100 -a congress-tracker-api
```

To disable scheduled sends without removing the cron Machine:

```powershell
fly secrets unset EMAIL_DIGEST_SCHEDULE_ENABLED -a congress-tracker-api
```

Monthly billing statements are intentionally excluded from `backend/crontab`; keep them admin-triggered test sends until billing statement automation is explicitly approved.
