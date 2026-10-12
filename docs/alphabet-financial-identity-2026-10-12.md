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

PR592 is merged as6fdebc851d1fd357e3c5b3193eccd9546040cf9b and deployed through successful workflow38187860807. Runtime hash and live cache/repeat acceptance are complete below. Broader provider coverage, issuer leverage definitions, numerical/score/ranking/all-alert rebaseline, institutions/Senate/transcripts and finalMassive/globalFMP/billing gates remain. No subscription or cancellation.


## Live release acceptance, 00:27 UTC

[PR592](https://github.com/moore11j/congress-tracker/pull/592), exact head d2d1e7ab, passes Vercel preview and secret scan. Automated code-review quota is exhausted; no automated approval is claimed. Frontend/backend dependency audits fail on unchanged dependencies (including Next.js and Pillow10.4.0); runtime diff is only the two-line reviewed association, with no dependency edits. Scoped code review and116 tests plus actual-source replay support this release. PR attachment was attempted but the chat's100-artifact limit prevented attachment.

[Backend deployment38187860807](https://github.com/moore11j/congress-tracker/actions/runs/38187860807) succeeds with readiness and privacy checks. All38 frozen hashes match. Approved APIshared2CPU/2GB and DBperformance1CPU/2GB remain; institutionsfalse. Queue2pending/1running, feeds0.472/0.564seconds, freshAAPL1.497/NVDA1.085seconds; eight public requests200 including50news items. This is recovery sampling, not sustained capacity acceptance. Receipt `two-service-verify-1791764774.json`.

On production Python3.12.15, both saved responses normalize and combine identically to the local replay using read-only SEC access, zero provider requests and zero database writes. GOOG adds the same four fields; GOOGL retains its existing exact-security fields. Receipt `alphabet-runtime-1791764770.json`.

Normal guarded GOOG preparation at00:26:34 produces nine issuer-only fields with sourceGOOGL and matching SEC identity. Its immediate repeat forbids provider transport and returns identical payload. A before/after hash of all canonical GOOG financial rows is unchanged. Public core remainsFMP. Receipt `alphabet-preparation-1791764795.json`. Subsequent ordinary GOOG refresh has not yet been observed; no schedule acceleration was introduced. All new local processes are terminal; no local PostgreSQL. Broader financial coverage/definitions and consumer validation remain required before source selection and FMP retirement.
