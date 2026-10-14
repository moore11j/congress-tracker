# SEC depreciation reconciliation evidence, October 14, 2026

The current leverage audit finds 408 complete EBITDA preparations among 1,745 fresh records. Of the incomplete records, 510 lack usable interest expense and 263 lack usable D&A. Those counts are preparation coverage, not public readiness. Receipt: `artifacts/leverage-readiness-1791997485.json` at 17:04 UTC. FMP remains selected for core financials.

## Microsoft source review

The [June 30, 2026 annual filing](https://www.sec.gov/Archives/edgar/data/789019/000119312526323660/msft-20260630.htm) reports these distinct amounts in USD billions:

| Period | Depreciation, Note 6 | Intangible amortization, Note 9 | Sum of those two lines |
| --- | ---: | ---: | ---: |
| 2024 | 15.2 | 4.8 | 20.0 |
| 2025 | 22.0 | 6.0 | 28.0 |
| 2026 | 34.3 | 4.7 | 39.0 |

The 2026 lease note separately reports 5.403 billion finance-lease right-of-use asset amortization. The cash-flow adjustment is 38.534 billion and explicitly includes other items, tagged with the issuer extension `msft:DepreciationAmortizationAndOther`. The software accounting policy describes capitalized software amortization, but no current standard companyfacts observation establishes its amount or overlap with the other lines. Neither the two-line sum nor the cash-flow adjustment is proven to be aggregate D&A. The dated FMP capture used 43.448 billion D&A; it is not reconciled by these disclosures alone. Missing components must not be set to zero and lease amortization must not be added without overlap proof.

Fresh source hashes in `artifacts/da-components-oct14/manifest.json`:

- Annual original, 8,585,611 bytes: `f5248cff45f5296701cd5b77065ca2f85598bcbc6eaf13883823dcc9bc6e88ec`.
- Companyfacts: `f8aae2965b20ad0df44bdf7ccbedf797d275b6b8dc030154a7a311361bb7246f`.
- Company submissions: `2484a2a3b5677d892a1b25af573ce4eda7b6f449f5eda41bac6b3f106dc3139c`.

## Implementation and checks

The candidate preserves the two-line sum, original component observations and separately reported lease amortization in preparation evidence. It remains `reconciliation_required`. It never fills `leverage_da`, changes EBITDA or creates a leverage ratio. Acceptance is limited to the exact Microsoft ticker/CIK, reviewed accession and filing date, three annual periods, USD, compatible filing identities and finite nonnegative component values. Existing totals/conflict markers remain intact. Only Microsoft's preparation version changes; other issuers and future periods receive no inferred policy. Optional inline components remain isolated from core parsing.

Final Linux Python 3.12.3 checks: **214 passed**. Offline current-source replay verifies the three sums and separate 5.403 billion lease amount, preserves all previously parsed facts and provenance, and confirms the combined issuer-extension cash-flow line is not substituted. A separate 20-company, hash-verified saved-source baseline/candidate/repeat comparison preserves every canonical value and existing leverage result. No network requests or production writes occur in replay. Both the original and prepared result still mark Microsoft EBITDA incomplete. Initial speculative substitution was removed before release after the overlap review; it is not part of the final implementation. Initial replay harness failures concerned Windows Git paths and an incorrect standard-tag assumption; final Linux replay passes.

The release and ordinary persistence are now verified below; aggregate D&A reconciliation remains open. Public scores/alerts stay on their existing source. Broad definition/coverage and all consumer checks, institutional publication, transcripts, sustained capacity, final Massive activation and global FMP/billing gates remain open. No subscription, source selection, schedule, host-size or customer-email change.

API CPU credits at 17:08 were 8,803 with zero current throttling, down from the earlier peak. Sustained capacity is still unproven; `cpu-quota-1791997697.json` retains the sample.

## Release and scheduled verification, 17:22 UTC

[PR635](https://github.com/moore11j/congress-tracker/pull/635), source `f7d6be4af0c6b68c6f25350f288157f7ab70d12f`, merged as `3f69704dfb3d3ec59e9e16b6c3e417ccd8b8fb4d`. Backend workflow38605514062 succeeded, including readiness/privacy checks. Nineteen normalized source hashes and deployed pure scope/hold checks match (`da-runtime-1791998394.json`). The unchanged dependency audits fail (Pillow33 advisories; frontend4 high/1 critical), with exact-head logs inspected through the required CI tool; secret scan passed. Manifests and lockfile are unchanged. No full-suite/all-CI or new automated code-review approval is claimed. The app's100-attachment limit rejected the PR attachment; the PR URL works.

The final offline replay also executes real isolated SQLite preparation and repeats the complete cache payload with no extra source requests; zero canonical financial/event rows. The scheduled17:21 run then persists the same Microsoft reconciliation evidence on production. Its ten prior prepared financial values and every pre-existing leverage result are exactly unchanged. Receipts `da-receipt-1791998371.json`, `da-receipt-1791998486.json` and `da-live-parity-oct14.json` record the comparison. Microsoft EBITDA remains incomplete; no public ratio is inferred.

That same ordinary run refreshes Arm's previously unavailable core preparation to ten metrics forJune30. Every prepared value exactly matches the previous annual/quarterly-original replay (`arm-ordinary-core-oct14.json`). This closes ordinary preparation propagation for PR633; public core remainsFMP and Arm's missing interest expense still prevents complete EBITDA.

At17:20, production has the same API shared2CPU/2GB and database performance1CPU/2GB (`preload-receipts-1791998414.json`). Postrelease public samples return200: Feed0.518s, Insights0.789s with research present, AAPL2.248s/NVDA0.516s (`insights-incident-1791998394.json`). News42/42 caches are fresh in the observed scope; institutional reference scope remains truncated and preparation-only. None of this proves sustained capacity, all-source coverage or all alert-consumer readiness. No source activation, subscription, schedule/host change, customer email or FMP cancellation.
