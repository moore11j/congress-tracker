# House scan row preparation

October 12, 2026. Implemented and tested locally. Not yet deployed or eligible for publication.

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
