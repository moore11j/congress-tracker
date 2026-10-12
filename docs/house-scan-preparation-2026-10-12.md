# House scan row preparation

October 12, 2026. Deployed and runtime verified as described below. Not eligible for publication.

The no-text-layer House filing9116376 previously retained its PDF but no rows. Its [economic review](house-scan-economic-review-2026-10-12.md) now supplies459 exact-source rows, including the original notification date missing from the secondary transcription and all separate trust lots.

## Change

A separate `house_ptr_preparations.json` registry and parser match the exact PDF hash, all seven discovery fields, page count, complete page/row population and review scope. Economic fields are checked for valid owner code, dates, amount brackets, action and checkbox consistency. Visual page/row references are stable source identities; they are not described as printed row numbers. Cover pages with zero transactions are explicit.

This registry only approves the economic fields. Asset descriptions remain explicitly unverified candidates; third-party ticker fields are forbidden. The normalizer clears even its existing known-issuer ticker guesses, leaves classification unresolved and returns a mandatory hold. The collector retains every row in quarantine and skips reconciliation. The publisher reparses the original and cannot bypass the hold even if a document is erroneously marked parsed. This preserves the next identity-review input without publishing unverified data.

The existing TonyWied reviewed-stock registry and output are unchanged. Unknown or revised scan bytes retain the previous review-required path. Due rechecks use the existing daily quarantine interval; no new trigger or schedule is added. An already retained PDF can gain prepared rows without rewriting or duplicating its immutable original revision.

## Validation

Python3.14.2:117 focused tests pass across scan preparation, previous House review, scan quarantine, Congress publication, direct feeds and SEC collection. Initial fixture-copy commands used a duplicated backend path and ran no tests; corrected before these results.

Checks use the actual31-page public PDF with SHA256 `9112dbf52d805f97500c314cbfacf58b114c9448d0f9a81091bafdff63da1a95`. They verify459 distinct stable hashes, repeat equality, four separate-trust lot pairs, exact ranges/source dates, the missing notification-date correction, unknown rather than invented owner, changed-source rejection, seven metadata mismatches, incomplete population and unexpected ticker injection rejection. Actual original collection/recheck/upgrade preserves one immutable revision, zero canonical transactions/events/emails, and a mandatory publication hold. This is not a full-suite or live scheduled-operation claim.

Source fixture and registry are tracked; raw secondary ticker mappings are not imported. No provider requests occur during these tests. Earlier live receipts remain dated evidence: source retention's next ordinary run and sustained API capacity remain unverified.

## Remaining acceptance

Complete the original description/security mapping review, support every retained instrument appropriately, reconcile the entire existing filing population and check affected consumers before any publication. Three other scanned forms also remain under review. This preparation does not replace those gates or complete Congress coverage.

Full FMP exit still requires financial definitions/coverage, institutional publication/capacity, transcripts, monitoring/watchlist/daily/weekly/ranking operation, finalMassive and zero-FMP/billing verification. No source selection, subscription, host, schedule, email, landing/video change or cancellation is included.

## Deployed release and runtime verification, October 12 at 19:59 UTC

[PR622](https://github.com/moore11j/congress-tracker/pull/622), reviewed source1759c247, merged asf00f56f9727ebab26fce68f7373927b0f40891a0. Backend workflow38300848501 completed successfully with readiness and Premium privacy checks. Dependency audits still fail: Pillow10.4.0 has33 reported advisory entries; the production frontend audit reports five vulnerable packages/groups. Dependency manifests are unchanged by this release; secret scan and Vercel pass. This is not a full CI pass.

Deployed Python3.12.15 replay uses the actual bundled31-page PDF in an isolated in-memory database with network and reconciliation blocked. It verifies459 rows/459 distinct source hashes, the corrected notification date, null inferred tickers, unresolved classifications, exact repeated parsed output and one immutable original after a daily retry. A two-hour retry does no fetch. Even an incorrectly parsed document remains held on publication. Transactions, events and emails remain zero. All78 expected runtime hashes match. Two earlier SSH attempts failed during certificate/tunnel setup before remote execution; the explicit-organization retry succeeded. Source changes and fixture match merged main.

The combined API host remains shared2CPU/2048MB and database remains performance1CPU/2048MB. All checks pass in the19:59 filtered receipt. An earlier DB VM check briefly reported CPU pressure (5.92seconds in10seconds); the fresh check reports5.47seconds in60seconds and passes. Do not infer a persistent database failure or a need for larger hardware from that sample. The API two-hour mean0.18548CPU still exceeds0.125 sustained allowance; capacity remains open.

Postrelease public samples: ready0.471s, feeds0.380/0.533s, AAPL1.428s, NVDA1.216s and AZO press0.253s, all HTTP200. These are immediate samples, not whole-universe or sustained proof. Core fundamentals remainsFMP and institutional publicationfalse.

At19:55 the four scanned source rows still show their19:20 attempts and no originals, before their next retry is due around20:20. No collector was forced, so ordinary retention/preparation remains to be observed. Separate read-only19:57 warming receipts prove institutional references are advancing two scopes per run (including a verified and an unmatched identity), while free research and financial partial results retain missing/wrong-symbol/access-denied coverage. Nonzero scheduler exits here reflect those partial statuses, not execution crashes. Existing price-alert job19:55 exits0 at3.1203CPU seconds; this alone does not verify every consumer's data or delivery.

Local receipts under artifacts/direct-feeds/free-replacements-2026-10-09: house-preparation-release-1791835148.json, issuer-staging-public-1791835062.json, house-preparation-machines-oct12.json, house-scan-holds-1791834957.json, role-cpu-1791834950.json, scheduled-job-cpu-1791834982.json and warming-receipts-1791835041.json. No new subscription, source selection, schedule, host size, public data correction or customer email was introduced. All command handles are terminal. Exact security descriptions/classification, scheduled collection, reconciliation and consumer proof remain unfinished.
