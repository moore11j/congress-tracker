# Insider profile freshness repair — 2026-09-14

## Cause

The scheduled FMP importer wrote `insider_transactions` and feed `events`, but
never wrote `insider_transactions_normalized`. Both the Insiders dashboard and
the insider card on Profiles aggregate the normalized table. The existing
normalization backfill was a one-time operation, not part of recurring ingest.

Read-only production checks found:

- Last normalized write: 2026-08-12 17:13 UTC.
- Latest normalized purchase/sale transaction date: 2026-08-11.
- September normalized purchases/sales: zero.
- Feed records dated September 1 through the audit date: 4,758, latest September 11.
- Raw records arriving since normalization stopped: 14,993.

The feed count includes transaction types that the profile's open-market filters
exclude; it is not an expected one-to-one dashboard trade count.

## Fix

`ingest_insider_trades` now stages normalized records in the same database
transaction as each new raw trade and feed event. Repeated raw records also run
normalization to repair a missing profile row. The shared backfill hash prevents
duplicates across retries and historical backfills. The ingest result reports
`inserted_normalized` separately from feed inserts.

## Production recovery

Deployed backend image: `deployment-01M2F25GJJKMTRTM0Y72K09EF8`.

The existing backfill was first previewed with `--dry-run`, then applied to the
confirmed missing ID range:

```sh
python -m app.backfill_legacy_insider_normalized --apply --min-id 161506 --max-id 176705 --batch-size 500
```

Result: 14,993 normalized transactions and 6,686 filing records inserted, zero
errors and zero unusable rows. This range records this incident only; a future
repair must determine its own missing range.

Rebuilt the 365-day and 90-day insider overview caches, and public/entitled
Profiles summaries. Repaired September aggregates contain 2,412 qualifying
open-market trades from 803 insiders, $367,918,762.66 in purchases, and
$2,364,079,376.34 in sales. The recent notable trades reach September 11.
The Profiles insider card contains 38,590 trailing-year trades and 7,890
active insiders.

Restarted the two API instances sequentially after cache warming, waiting for
each to become healthy, to clear their old in-memory responses. Public API
verification then returned 2,412 September trades on both `/api/profiles/summary`
and the default 365-day `/api/profiles/insiders/overview` endpoint.

## Validation

16 focused insider tests passed, including ingestion-to-dashboard values,
current-month chart points, non-market exclusion, retry repair, rollback, and
idempotency in both backfill/live-ingest orders.

The broader run passed 32 tests and failed four unchanged profile overview tests.
All four failures were reproduced with the original ingestion/backfill code:
an obsolete cache-version assertion, an obsolete prewarm ordering assertion,
and two institution fixtures without the currently required filing data.
