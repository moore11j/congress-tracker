# Alphabet free financial identity, October 12, 2026

## Observed issue and repair

The ordinary00:16 UTC metric refresh holds GOOG with metric_symbol_mismatch. Two bounded provider requests at00:18:17 through the existing shared free limiter verify that GOOG returns symbolGOOGL with130 metrics; the GOOGL request returns its own identity. This is a different share class, not a spelling alias.

[Alphabet's June30,2026 Form10-Q](https://www.sec.gov/Archives/edgar/data/1652044/000165204426000071/goog-20260630.htm) identifies GOOGL as ClassA and GOOG as ClassC, both under CIK0001652044. The existing reviewed-issuer policy now includes only the observed GOOG-to-GOOGL direction. It permits nine company-level ratios/growth fields with original source ticker/provenance and independently requires matching SEC CIK at combination. Prices, beta, valuations and per-share metrics remain excluded. Other classes and the unobserved reverse mapping remain rejected. Exact-security GOOGL remains unchanged.

No global alias, cache-version change, source selection, schedule or quota change is needed: failed normalization never wrote a usable GOOG cache. Existing cadence can now retry this scope normally. Public core remains FMP.

## Verification

Python3.14.2:116 affected identity, metric/growth, SEC preparation/priority and cache tests pass. One local pytest-cache write warning does not affect the test results. Independent Alphabet CIK rejection, preferred/unreviewed classes and beta injection are covered alongside existing provenance and stale-period guards.

Actual read-only SEC evidence export at00:19:33 confirms both prepared issuers have CIK0001652044 andJune30 periods. An isolated SQLite replay of both actual Finnhub responses and SEC panels preserves every existing SEC value/date. GOOG recovers debt/equity, ROIC, gross margin and annual EBITDA growth; it admits no valuation, beta or per-share fields. GOOGL exact-security normalization matches the unchanged baseline. Repeated upserts retain timestamps and summary/screener consumers retain the evidence. External transport is prohibited; zero production writes/emails. First replay failed before execution because its git subprocess lacked a safe.directory argument; corrected scoped invocation passes.

Source receipts and hashes:

- `alphabet-metric-identity-oct12.json`: `2551c3aa98cd649b58c93c07bf36c11671440f15537a09f424a40dc8b1954351`.
- `alphabet-sec-evidence-oct12.json`: `7802648b008cdc1313d6ce2a8a52f106ac074d8603cecdb58329d42d7e9ca90e`.

Replay: `alphabet-issuer-replay-oct12.json` under primary artifacts/direct-feeds/free-replacements-2026-10-09. Detailed artifacts are local; durable findings are recorded here.

## Release state

Implementation and local actual-source validation are complete; PR review, deployment, runtime hash and live cache/repeat acceptance remain pending. Broader provider coverage, issuer leverage definitions, numerical/score/ranking/all-alert rebaseline, institutions/Senate/transcripts and finalMassive/globalFMP/billing gates remain. No subscription or cancellation.
