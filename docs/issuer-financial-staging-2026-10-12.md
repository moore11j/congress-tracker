# Issuer financial collection integration, October 12, 2026

## Scope and source

The existing SEC earnings collector now prepares the reviewed AutoZone FY2026 financial statement from its retained complete submission. This extends [the standalone parser](issuer-release-financials-2026-10-12.md). It adds no HTTP requests, cron entries, public financial selection or canonical events. The reviewed accession is `0001171843-26-006159`; other issuers and future periods are not implicitly accepted.

The actual source used “reported net sales”, which the prior earnings classifier held. The added match requires quarter-end language, a condensed consolidated statement of operations, the existing release description and financial evidence, and the existing exclusion/length guards. Only AZO's cache version is invalidated; other six-hour caches stay valid.

Three captured SEC originals validate issuer identity, accession, source hashes and availability before extracting the exhibit. The source index/header agree on September22 06:55:14 New York time, while submissions JSON reports14:55:14Z. Both are preserved and observation waits until the later clock. This does not assign a new press-publication timestamp.

| Original | SHA256 |
|---|---|
| Company submissions | `3cec158e2f8bded670849b8a8cbeeab494afadd7874c8024becb360cfdb9ebd8` |
| Complete submission | `8c4a288599743511ad3979bdda25874c2e206ca6d4124b91d0bbd3ffefcd9820` |
| Filing index | `d7313b1a1b39b204f1b3127a342a5e8d42cbfdad446448a38a1a10903a79df5a` |

The SGML exhibit hash `12f392561396036b9c65a93e3ca147fa7f9f9f7e4d27290c526d64aa659d6f0b` differs from the standalone HTML transport hash. Its six metrics, inputs and period dates match exactly. Transport differences are not represented as identical original bytes.

## Implementation and checks

`issuer_financial_staging.py` writes one source-bound `InsightsSnapshot` under a stable accession key, inside the existing issuer lock/transaction. It revalidates retained originals through the shared `read_staged_earnings` function. Unchanged replay preserves the full saved payload and timestamp. A later invalid financial format replaces prepared metrics with a held, empty-values receipt; valid earnings-release evidence is retained. Hash/identity tampering is rejected. `publication_eligible=false` throughout.

Python3.14.2:153 focused earnings, staging, press-reader/research/event and financial-parser checks pass. Existing FMP financial and research rows are explicitly verified unchanged. Earlier synthetic fixtures were below the existing1000-character minimum; the fixture was extended without weakening that guard. No full-suite claim.

Actual isolated SQLite replay through `refresh_sec_releases` twice produces exactly3 source documents,3 source revisions,1 financial snapshot,0 events,0 research documents and0 fundamental-cache rows. Both press items and full snapshot state repeat identically. Six reads use saved originals, zero network requests or production writes. Latest receipt `issuer-financials-oct12/staging-replay.json`, snapshot hash `488555a9921f90a9105e993b567129c2c8b5425ff9795dcf2ca33dd1d0b870d6`. The original replay harness initially omitted its one-filing fixture limit; corrected `filing_limit=1` matches the captured bundle. Production remains bounded at its existing five filings.

## Release status and remaining gates

Implemented and locally verified; release/runtime and ordinary scheduled observation pending. No new subscription, host size, schedule, customer email, landing edit or FMP cancellation. Public core financials remain FMP. This is one reviewed annual statement, not broad financial coverage. Source freshness, broad issuer/period coverage, public metric semantics, score and all-consumer parity remain activation gates; institutional publication, transcripts and final Massive price validation also remain open.


## Release and scheduled-path observation, October 12 16:13 UTC

[PR608](https://github.com/moore11j/congress-tracker/pull/608), exact head0022f58e466db4e9dcca7606757d171e3479feb2, merged as ea6d7874bdb0897108a87148927d62b7624eba6c. Backend workflow38275188451 succeeds. Secret scan passes; exact-head audits fail on unchanged Pillow10.4.0 and frontend dependencies (Next/PostCSS/Sharp). No dependency manifests changed. Direct diff reviewed; no fresh automated code-review claim. PR attachment failed at the100-artifact limit.

All66 runtime hashes match. The ordinary16:12 SEC warmer completes five scopes and retains explicit partial coverage; AZO is not among its49 scopes. Its normal public press endpoint enqueues job21840. One explicitly requested run of the existing serialized enrichment worker (174, terminalexit0) completes it at16:12:22. No cron entry or host size changes. Production retains original document8774 with all three hashes identical to the captured bundle; six prepared metrics, inputs, calculations and annual period dates match both standalone evidence and isolated deployed replay. The preparation remains publication-ineligible; core provider remainsFMP.

Read-only production evidence plus isolated SQLite replay on Python3.12.15 verifies identical repeat and3documents/3revisions/1snapshot/zero event/research/fundamental rows. Two collector replays take0.361856CPU seconds excluding fixture setup; no new source requests or production writes in the verifier. Receipt `issuer-staging-release-1791821562.json`. A first runtime check found no saved AZO source, which correctly delayed replay until the actual collector completed.

Public AZO now serves four unique SEC earnings links, four filings checked/no source failures, in0.229s. The new annual financial receipt is still preparation only. Final feed samples0.366/0.417s and cached AAPL/NVDA contexts0.390/0.533s all return200. Earlier initial contexts1.619/1.989s; these samples do not prove sustained capacity or comprehensive coverage. Receipt `issuer-staging-public-1791821580.json`.

This closes deployed integration and demand-queue validation for the reviewed source. A future ordinary AZO refresh, broad issuer/period discovery and financial/cache/score/all-consumer acceptance remain open. No customer email, public core source switch, purchase or FMP cancellation.
