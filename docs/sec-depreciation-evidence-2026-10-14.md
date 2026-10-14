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

This is locally implemented and validated, not deployed. Ordinary persistence and release verification remain pending. Public scores/alerts stay on their existing source. Broad definition/coverage and all consumer checks, institutional publication, transcripts, sustained capacity, final Massive activation and global FMP/billing gates remain open. No subscription, source selection, schedule, host-size or customer-email change.

API CPU credits at 17:08 were 8,803 with zero current throttling, down from the earlier peak. Sustained capacity is still unproven; `cpu-quota-1791997697.json` retains the sample.
