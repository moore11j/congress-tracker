# Debt subtotal and lease reconciliation

October 14, 2026. Implemented and locally verified; release verification is pending. Public core fundamentals remains FMP.

## Source findings

The read-only 18:37 UTC audit covers 1,748 cached scopes: 1,102 partial, 643 unavailable and three expired/version mismatches. All 1,100 leverage panels use v4. There are 408 complete EBITDA panels and 71 scopes with at least one ratio candidate. Missing interest affects 514 panels and missing D&A affects 265; counts overlap. These are preparation counts, not completed replacement coverage.

Two apparently equivalent debt calculations disagree in the actual candidate cohort:

- [Goodyear June 30 Form 10-Q](https://www.sec.gov/Archives/edgar/data/42582/000162828026054032/gt-20260630.htm): the tagged $6.831 billion subtotal is long-term debt and finance leases, including their current portion. It excludes $359 million in notes payable and overdrafts. Total current obligations of $1.418 billion plus noncurrent debt and finance leases of $5.772 billion establish $7.190 billion, before $1.023 billion operating leases. The lower subtotal must not masquerade as total debt.
- [Sonoco June 28 Form 10-Q](https://www.sec.gov/Archives/edgar/data/91767/000009176726000038/son-20260628.htm): the $3.484464 billion noncurrent debt line already contains $41.044 million of finance leases. It is tagged both as noncurrent borrowing and finance-inclusive debt. Adding the lease amount again double-counts it. The debt note and lease classification table independently establish inclusion. Current plus noncurrent debt and all operating leases total $4.765630 billion.

Six SEC documents were captured with identified, paced requests and size bounds. Original hashes: GT `fbf86066dcae3067a503ebe4aab70b65c2bb7caf682d81f4fe60cd19057c9ba8`; SON `6e8aa56029c6002285d4696ce2816f59d1b445be4db7908ec78f1cc8f032d258`. Facts/submission hashes and complete originals are in the local `artifacts/debt-reconciliation-oct14/manifest.json` and companion files. Web-rendered original tables independently confirm the interpretation.

## Repair and validation

Leverage evidence v5 checks a reported finance-inclusive debt total against the sum of compatible, disjoint current and noncurrent obligations. It holds an undersized subtotal while retaining independent complete maturity calculations. Separately, a proposed borrowing-plus-finance-lease sum must reconcile with a compatible reported inclusive total when available. Contradictory additions and their operating-lease variants are held; reported totals remain independent candidates. Missing components are never zero, and mismatched periods, filing identities, currencies or CIKs cannot invalidate independent observations. No canonical ratio is selected by this repair.

Linux Python 3.12.3 passes 175 focused debt, financial and original-inline parser checks. The two new regression fixtures reproduce the exact omission and overlap, with guards for incompatible identities, missing versus explicit zero, both conflict directions, rounding and nonfinite sums.

All 71 retained positive panels reproduce their original candidates and EBITDA exactly under the baseline. The repaired replay removes only two GT and two SON alternatives. Every retained candidate and all EBITDA values remain identical; full repeated results are identical and inputs remain immutable. The initial replay omitted previously derived parent-income facts and failed on BSX; the harness was corrected to retain those exact selected observations before the successful full run. This replay uses retained facts, not a new whole-universe source acquisition.

A separate 22-company original-source replay verifies every source hash, unchanged canonical fields and non-leverage evidence, unchanged EBITDA and identical full repeats with network access prohibited. Seven original-companyfacts coverage holds remain, including SON, whose current live preparation depends on retained inline filings; its actual prepared panel is covered by the 71-panel replay. GT changes four candidates to two. No production writes or validation emails occurred.

Evidence: `leverage-universe-1792003058.json` (SHA256 `7a56292abcf52daa80fe2860749d5170fc939ef56e4c0caf9cbe531eac55b68d`), `debt-representations-replay-oct14.json`, `debt-originals-replay-oct14.json`, and `debt-representation-fixtures-oct14.json` in the primary artifacts directories. Initial optional BeautifulSoup inspection was unavailable; lxml and the web reader completed original-table inspection. No dependency was installed.

## Remaining gates

Deploy and verify the exact repair, then observe ordinary preparation of v5. Existing v4 caches retain their version until normal refresh; no broad forced update is required. Correct debt alternatives do not establish complete EBITDA semantics or select the final public ratio policy. Broader financial coverage, scores/rankings/all alerts, institutions, transcripts, sustained heavy-workload capacity, final Massive, zero global FMP requests and billing cancellation remain open. No host, schedule, subscription, public provider, landing-page or video change.
