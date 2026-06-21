# Production Performance Indexes

Walnut web startup must not create performance-only indexes on large or hot
tables. Required tables and columns are still checked during FastAPI startup;
optional expression indexes are created through this maintenance command.

Run from the Fly app shell or a one-off release/maintenance machine:

```bash
python -m app.maintenance_indexes
```

For a single index:

```bash
python -m app.maintenance_indexes --index ix_events_member_name_lower
```

On Postgres this uses `CREATE INDEX CONCURRENTLY IF NOT EXISTS` outside a
transaction and applies short lock/statement timeouts. Lock timeout, duplicate
index races, missing optional tables, or missing optional columns are logged and
skipped; they must not block the web process from serving `/health`.

Equivalent SQL for the hot-table expression indexes:

```sql
SET lock_timeout = '2s';
SET statement_timeout = '30s';

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_securities_symbol_lower
  ON securities ((lower(symbol)));
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_securities_name_lower
  ON securities ((lower(name)));
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ticker_meta_symbol_lower
  ON ticker_meta ((lower(symbol)));
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ticker_meta_company_name_lower
  ON ticker_meta ((lower(company_name)));
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_members_name_lower
  ON members ((lower(first_name)), (lower(last_name)));
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_events_member_name_lower
  ON events ((lower(member_name)));
```

Do not run `CREATE INDEX CONCURRENTLY` inside an explicit transaction block.
