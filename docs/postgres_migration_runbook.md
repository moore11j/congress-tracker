# SQLite to PostgreSQL Migration Runbook

This migration is copy-first and rollback-ready:

`COPY -> VERIFY -> COMPARE -> STAGE -> CUT OVER -> MONITOR -> KEEP SQLITE AS ROLLBACK`

Do not delete, compact, overwrite, or replace `/data/app.db`. SQLite remains the source of truth until the PostgreSQL copy has passed verification, backend endpoint comparison, frontend smoke testing, and manual production cutover approval.

## Database Audit Summary

- DB engine/session is created in `backend/app/db.py`.
- `DATABASE_URL` defaults to `sqlite:////data/app.db`. PostgreSQL URLs are now supported through `postgresql+psycopg://`, with modest pooling and `pool_pre_ping`.
- Tables are declared in `backend/app/models.py`; some legacy schema repair is still in `backend/app/db.py`.
- Startup in `backend/app/main.py` currently calls `Base.metadata.create_all`, `ensure_event_columns`, `ensure_government_contracts_schema`, `seed_plan_config`, event repair, optional ingest autoheal, and optional event backfill. These startup writes are a cutover risk and should be disabled or tightly controlled during final migration.
- SQLite-specific code found:
  - SQLite PRAGMAs in `backend/app/db.py`.
  - SQLite-only legacy schema repair in `ensure_event_columns`.
  - SQLite dialect upserts in price, quote, ticker metadata, and government contract ingestion. Price/quote/ticker metadata now choose PostgreSQL upsert syntax by dialect.
  - SQLite JSON function usage exists in historical backfill scripts, not normal request paths.
- Read endpoint write risks found:
  - `/api/monitoring/inbox` refreshed alerts during GET; this patch changed it to read existing alerts only.
  - Quote, price, ticker metadata, and CIK metadata lookup helpers could write caches when called by read paths; this patch defaults those request-path helpers to no cache writes.
  - Startup remains write-capable by design. Pause or disable startup write toggles during final migration and comparison.
- Background/ingestion writers include congressional ingest, insider/institutional ingest, government contracts ingest, trade outcome computation, monitoring refresh jobs, notification delivery, backfills, and admin/account/billing writes.
- Foreign-key-like relationships are mostly integer/text columns without enforced `ForeignKey` constraints, for example transactions to filings/members/securities, watchlist items to watchlists/securities, events to source tables, monitoring alerts to events/users, saved screens to users, and contract actions to events.
- No ORM-declared table was found without a primary key; reflected production SQLite should still be checked by the verifier because legacy/placeholder tables may exist outside `models.py`.
- Tables with composite/string primary keys exist (`price_cache`, `app_settings`, `plan_limits`, `plan_prices`, `stripe_webhook_events`, `congress_member_aliases`). The migration scripts verify primary key summaries and duplicate primary keys.

Model/table inventory from `backend/app/models.py`:

`members`, `securities`, `filings`, `transactions`, `insider_transactions`, `institutional_transactions`, `watchlists`, `watchlist_items`, `watchlist_view_states`, `confirmation_monitoring_snapshots`, `confirmation_monitoring_events`, `user_accounts`, `app_settings`, `feature_gates`, `plan_limits`, `plan_prices`, `stripe_webhook_events`, `billing_transactions`, `notification_subscriptions`, `government_contracts`, `government_contract_actions`, `saved_screens`, `saved_screen_snapshots`, `saved_screen_events`, `monitoring_alerts`, `notification_deliveries`, `events`, `quotes_cache`, `price_cache`, `ticker_meta`, `cik_meta`, `trade_outcomes`, `congress_member_aliases`.

## Backup Before Migration

On Fly, create a timestamped backup without altering the source DB:

```bash
python backend/scripts/backup_sqlite.py --sqlite-path /data/app.db
```

Expected backup path:

```text
/data/app.backup.pre-postgres.YYYYMMDD-HHMMSS.db
```

The script writes a `.sha256.json` manifest next to the backup. Treat both files as sensitive production data.

Download the backup locally before proceeding:

```bash
fly ssh sftp shell -a congress-tracker-api
get /data/app.backup.pre-postgres.YYYYMMDD-HHMMSS.db
get /data/app.backup.pre-postgres.YYYYMMDD-HHMMSS.db.sha256.json
```

Do not commit downloaded database backups, manifests containing operational paths, or database credentials.

## Staging Migration Flow

1. Create a PostgreSQL database for staging.
2. Do not change production `DATABASE_URL`.
3. Run schema setup by starting a staging backend pointed at PostgreSQL. The migration script also creates any source SQLite tables missing from the target so placeholder or legacy tables are not skipped.
4. Back up SQLite:

```bash
python backend/scripts/backup_sqlite.py --sqlite-path /data/app.db
```

5. Run the copy migration:

```bash
python backend/scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path /data/app.db \
  --postgres-url "$POSTGRES_DATABASE_URL" \
  --log-path postgres-migration-run.json
```

For a likely production PostgreSQL target, the script refuses to run unless:

```bash
export POSTGRES_MIGRATION_TARGET_APPROVED=copy-sqlite-to-postgres
```

6. Verify the copy:

```bash
python backend/scripts/verify_postgres_migration.py \
  --sqlite-path /data/app.db \
  --postgres-url "$POSTGRES_DATABASE_URL" \
  --json-report postgres-verification-report.json
```

7. Run a second backend pointed at PostgreSQL. Keep the SQLite-backed backend running.
8. Compare GET endpoints only:

```bash
python backend/scripts/compare_backends.py \
  --sqlite-backend-url "$SQLITE_BACKEND_URL" \
  --postgres-backend-url "$POSTGRES_BACKEND_URL" \
  --json-report backend-comparison-report.json
```

Add `--admin-token` only for protected admin/test-user checks. Do not expose credentials to the frontend.
By default, mismatch reports include endpoint status and normalized response hashes only. Use `--include-bodies` only in a protected local environment because response bodies can contain sensitive user or admin data.

9. Point a staging or local frontend at the PostgreSQL-backed backend.
10. Smoke test the UI manually.

Required manual smoke tests:

- Main feed
- Feed filters
- Ticker page
- Signals
- Leaderboards
- Watchlists
- Saved views
- Saved screens
- Monitoring
- Inbox/alerts
- Admin users
- Admin pricing/settings
- Pricing page
- Login/session behavior
- Premium/Pro/Admin entitlements

## Production Cutover Checklist

Production cutover is manual. This patch does not automate or imply cutover.

1. Pause ingestion/background jobs if possible.
2. Confirm no long-running writes.
3. Create a fresh SQLite backup.
4. Generate and save the backup checksum.
5. Run final migration to PostgreSQL.
6. Run verification script and require `PASS`.
7. Run backend endpoint comparison and require `PASS`.
8. Only if all checks pass, manually update production backend `DATABASE_URL` to PostgreSQL.
9. Restart backend.
10. Smoke test backend.
11. Smoke test frontend.
12. Monitor logs.
13. Keep SQLite DB untouched as rollback.

Recommended cutover environment hardening:

- Disable startup autoheal/backfill/repair toggles unless explicitly needed:
  - `AUTOHEAL_ON_STARTUP=0`
  - `AUTO_REPAIR_EVENTS_ON_STARTUP=0`
  - `AUTO_BACKFILL_EVENTS_ON_STARTUP=0`
- Keep PostgreSQL pool settings modest:
  - `DB_POOL_SIZE=5`
  - `DB_MAX_OVERFLOW=5`
  - `DB_POOL_TIMEOUT=30`

## Rollback Checklist

1. Set `DATABASE_URL` back to `sqlite:////data/app.db`.
2. Restart backend.
3. Confirm backend health.
4. Confirm frontend core flows.
5. Investigate PostgreSQL separately.
6. Do not emergency-patch production data unless the root cause is known.

## Post-Cutover Safety

Keep `/data/app.db` and the pre-Postgres backup for at least several weeks. Keep migration run logs, verification reports, and endpoint comparison reports.

Monitor:

- DB errors
- Connection pool exhaustion
- Slow queries
- Failed requests
- Admin/user entitlement issues
- Ingestion job failures
- Row count drift between expected new writes and actual writes

## Security Notes

- Do not print or commit `DATABASE_URL` values.
- Do not expose database credentials to the frontend.
- Do not expose admin endpoints publicly.
- Do not weaken auth for comparison.
- Migration and verification reports should prefer counts and hashes over raw private data.
- Downloaded SQLite backups contain sensitive production data; protect and delete local copies according to the retention policy after rollback risk has passed.
