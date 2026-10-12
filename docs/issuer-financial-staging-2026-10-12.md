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
