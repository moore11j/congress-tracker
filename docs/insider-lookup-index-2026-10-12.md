# Insider lookup index, October 12, 2026

Production plans confirm that case-insensitive insider history queries scan the full normalized table. AAPL takes 823.993 ms (11,760 blocks read), NVDA 123.637 ms and BRK.B 122.570 ms in the bounded pre-repair samples. Each query visits approximately 189,000 rows. The existing symbol/date index is case-sensitive. Three older invalid indexes exist on this table and remain untouched.

The database counters report 24,718 sequential scans and 1.557 billion rows visited, but did not increase in the short diagnostic interval. These cumulative counters do not establish the current scan rate or attribute all database load to this lookup. A separate 24-hour capacity sample shows recent two-hour API mean 0.156 CPU and DB mean 0.165 CPU. Moving the performance CPU from DB to API would therefore risk relocating the bottleneck; no hardware change is proposed here.

## Repair

Add one nonunique index on upper(ticker_normalized), transaction_date. Keep the existing queries, case-insensitive matching, canonical rows, provenance and alert identities unchanged. An explicit manual request uses the existing serialized data lane; there is no recurring trigger or startup migration. CREATE INDEX CONCURRENTLY uses a 90-second statement timeout, two-second lock timeout, 32 MB maintenance memory and zero parallel maintenance workers. A valid exact index repeats without DDL; an invalid or conflicting existing index fails for inspection, never auto-drops or rebuilds.

## Validation and state

25 focused index/scheduler checks pass on Python 3.14.2; five Linux-only checks skip on Windows. Actual isolated PostgreSQL 16.15 builds the exact index over 20,005 synthetic rows, preserves complete mixed-case/dotted/absent-symbol query results and repeats without DDL. The latest-row plan uses the index. The disposable database was removed. These tests prove the access-path repair, not sustained production capacity or complete source migration. Release and live application remain pending.

Local receipts: insider-index-read-1791772232.json, insider-index-before-1791772277.json, insider-index-postgres-oct12.json and role-cpu-1791771999.json in artifacts/direct-feeds/free-replacements-2026-10-09. The first diagnostic attempt lacked local Fly configuration access and was rerun with scoped access. No production data writes, provider calls, email, source activation or subscription changes occurred during diagnostics.
