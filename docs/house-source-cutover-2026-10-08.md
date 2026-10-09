# House direct-source cutover

October 8, 2026 Pacific time. Continued owner-authorized execution toward FMP shutdown. This report distinguishes source ownership from remaining historical reconciliation and the full product migration.

## Current production evidence and rehearsal

An enforced read-only export at 23:51:21 UTC captures all 13 staged House reports for October 1–8, complete affected-member populations (2,203 transactions and 3,932 events), ten relevant filings, 403 securities and 58 existing outcomes. No customer records are exported. Snapshot SHA-256: `f91b66e89fa36c7847cc838fe9ddae9a02b35b248cf4bb2850e69f231aa2ecce`.

Fresh evidence adds Gottheimer's two trades to the prior date-only candidates. Four current-window filings qualify for the already deployed guarded correction: documents 412 (2 trades), 416 (4), 423 (3), 425 (4). All 13 event IDs, arrival timestamps and ingestion times are retained; the correction records official filing dates separately from observed availability. The older Sessions fixture remains outside the current cutover window.

The full-member offline worker rehearsal corrects those dates, adopts seven filings with no inserted trades, and repeats with identical state. The Fong Treasury bill already has a correctly classified `congress_treasury_trade` event; an explicit source/member/instrument/economics/date check now permits adoption. It remains non-equity, with the same ID, no new outcome work and no new stock event. Ten altered-field cases stay held. Unknown non-stock events remain held.

Three digital reports remain held: Salazar 413 (42 source rows versus 37 stored, repeated lots and BRK-B versus stored BRK-A), Fields 417 (5 versus 4, repeated MSFT and GOOG versus GOOGL), and Taylor 424 (5 versus 3, repeated HD/MPC). Harshbarger's three scanned reports 419–421 remain unresolved. Existing legacy records are retained while these source-bound discrepancies are reviewed. They are not asserted to be reconciled or complete. Holds prevent inserting ambiguous replacements beside them.

## Monitoring and rankings

A separate current public snapshot supplies complete recent Congress activity for all 13 affected symbols (76 events), 13 fresh canonical score bundles, exact quote/history inputs and the current Top Stocks universe. An initial export failed before database access because a test-only HTTP library was absent; the corrected read-only export succeeded with HTTP requests forbidden.

Offline execution reproduces the captured live Congress cards and scores, then applies the date corrections. Watchlist activity remains 18 items, monitoring 12, daily alerts 8. All 27 synthetic monitoring records remain byte-for-byte unchanged, including IDs/read state, and repeat creation adds zero alerts. Daily and weekly Top Stocks dry runs both return `would_send`; the same ten stock items and all price inputs are retained. No emails or network calls are made.

Correct official dates change ten latest-evidence dates and lower NOW's score from 14 to 13; the other twelve scores are unchanged in the fixed-time comparison. This is an evidence correction using the existing score version, not a new methodology or a rewrite of saved strategy results. The check holds non-Congress source/price inputs fixed and excludes calendar acquisition. It does not establish complete FMP-free freshness across the product.

The first alert replay incorrectly assumed only the 13 explicitly seeded alerts would exist; the actual digest builder creates additional applicable watchlist alerts. The final replay compares the complete 27-record before/after state and still requires zero repeat growth.

## Rollout and boundaries

106 targeted tests pass in both primary and isolated release on Python 3.14.2, including real Treasury and new Gottheimer PDF fixtures, source conflict rejection, date/history preservation, source ownership and scheduling. The release will add House publication at minute 29, guarded by explicit `official_house` ownership. Before ownership selection the scheduled command skips. Existing hourly official collection remains unchanged.

Production repair and source selection are pending at this preparation checkpoint. The intended transition pauses the legacy House writer, applies only the four reviewed date corrections, selects the official source at an immutable October 1 boundary and adopts the seven verified filings. Resuming FMP after correction is prohibited by the ownership interface. The remaining known historical holds remain explicit; they do not authorize guessing source lots or changing stock classes. Current evidence views must then be refreshed and the deployed source/legacy skip, repeated publication and subsequent scheduled runs verified.

House collection is one part of the exit. Form 4/13F publication, Massive Starter and live prices, fresh replacement-driven universe/content/alerts, zero FMP runtime requests and billing cancellation remain open. No subscription operation is included here.

Local evidence: `artifacts/direct-feeds/house-cutover-2026-10-08/production-baseline.json`, `downstream.json`, `cutover-rehearsal.json` and captured live receipts. Public fixtures, exporter and replay scripts are included in the scoped release.

## Production cutover verified, October 9 00:16 UTC (October 8 Pacific)

Release `a93232dc68d2ab2f3f420a96acd9022260b3ace2` and [workflow 37863194783](https://github.com/moore11j/congress-tracker/actions/runs/37863194783) succeeded. All four workers are started on the exact image; readiness/database and anonymous Premium-access checks pass. Worker/date/availability/cron file hashes are checked before mutation. The House schedule is deployed at minute 29 and still requires database ownership.

The initial preflight stopped before writes because the SQLite rehearsal removed timezone offsets from its fingerprint. A fresh enforced read-only PostgreSQL inspection proves every field and complete population unchanged for all four filings, with the same source hashes and bindings. Removing timezone offsets reproduces all four original rehearsal hashes exactly. The apply was then bound to the reviewed PostgreSQL-native hashes, without weakening state checks.

At 00:13:06–11 UTC the operation wrote an immutable public-state backup, selected paused House generation 1, applied and repeated each reviewed correction, then selected `official_house` generation 2 from October 1. All 13 events preserve their IDs, arrival timestamps and creation times. Thirty archived original rows cover four filings, thirteen transactions and thirteen events. The live before/after hashes retain all 42 linked saved portfolio positions, thirteen outcomes, thirteen enrichment jobs and the one existing alert record. Seven filings are adopted with no new events or transactions, including Fong's unchanged Treasury event. Three digital conflicts and three scanned reports remain held. Exact publication repeat creates nothing.

The actual legacy House ingestion entry point runs under forbidden FMP/metadata transport and returns `source_ownership_skipped`. A separate read-only receipt at 00:16:33 verifies both House and Senate official generation 2 ownership and zero recorded legacy ingestion requests since their respective switches. All seven successful publication fingerprints still match; source dates, original availability and the Treasury classification verify. This does not claim zero FMP requests for other product areas.

A bounded view refresh backs up the current leaderboard/ticker/market views, rebuilds thirteen affected current score bundles, expires five ticker caches and updates fourteen market tiles. The other 1,736 candidates, Top 10 items, price fields and history tables are unchanged. Live cached-to-current score differences also include normal freshness changes and previously stale cached inputs; they must not all be attributed to the House date correction. The fixed-time source-only comparison above isolates that effect.

All four public member trade APIs return the thirteen original IDs and the corrected official report dates. Two initial concurrent requests returned 503; `/ready` remained healthy and bounded individual retries succeeded. Record this as a transient concurrency/reliability observation, not a full load-test pass. No email was sent by the cutover or validation. No billing changes.

Backup receipts: `/data/ops/house-cutover-20261008/before-20261009T001306152087Z.json`, SHA-256 `d4954e446ccaeb2783f567711b0aa82bf9b8f68414947bffd584a357b093fdab`; current views `current-views-20261009T001343654535Z.json`, SHA-256 `e0f7f074d06ecffca0bc765adb820880e29e54f986ab8e3e49bed8286f029034`. No private customer rows were exported; protected history was compared by server-side counts and hashes.

First scheduled House publication remains due at 00:29 UTC. Continue observing collection/publication and legacy suppression, address held coverage, then proceed with Form 4/13F, fresh replacement prices/fundamentals/content and whole-product alerts before global FMP shutdown and billing cancellation.

At 00:19:10 UTC, read-only verification observes actual post-cutover Congress collection run 15 completed at 00:17:06, with zero new work/public writes and the existing holds retained. Both ownership controls, all successful publication fingerprints, original event times and zero recorded legacy ingestion calls remain verified. First House publication at :29 is still pending.
