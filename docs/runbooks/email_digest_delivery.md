# Email Digest Delivery

Walnut digest emails are delivered only by explicit admin actions or the digest job CLI. Page reads, feed loads, screener loads, leaderboard loads, and ticker loads do not send email.

## Current Automatic Scheduling State

The repository has a Fly web process in `backend/fly.toml`, but no checked-in Fly cron, scheduled machine, or separate digest worker process. Until one is provisioned, run scheduled delivery through an external scheduler that invokes the CLI or the admin run-now endpoint.

## Daily Windows

Scheduled digest jobs use the previous midnight-to-midnight window in America/Los_Angeles. A run at 7:00 AM Pacific on June 5 sends the June 4 00:00 through June 5 00:00 Pacific window.

## CLI Commands

Dry-run first:

```powershell
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind monitoring --lookback-days 1 --limit 100 --dry-run"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind watchlist_activity --lookback-days 1 --limit 100 --dry-run"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind signals --lookback-days 1 --limit 100 --dry-run"
```

Send:

```powershell
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind monitoring --lookback-days 1 --limit 100"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind watchlist_activity --lookback-days 1 --limit 100"
flyctl ssh console -a congress-tracker-api --command "python -m app.jobs.send_email_digests --kind signals --lookback-days 1 --limit 100"
```

Suggested external schedule:

- `monitoring`: daily around 7:00 AM Pacific.
- `watchlist_activity`: daily around 7:05 AM Pacific.
- `signals`: daily around 7:10 AM Pacific.

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
