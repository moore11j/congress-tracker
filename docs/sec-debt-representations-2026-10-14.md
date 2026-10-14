# Debt subtotal and lease reconciliation

October 14, 2026. Deployed and runtime/ordinary propagation verified through PR639 (`6b2f680d`). Public core fundamentals remains FMP.

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

## Release and ordinary operation

PR639 source `d8c74e88` merged as `6b2f680d`; backend workflow38618338377/job116039102020 completed successfully. At18:52:47UTC twelve runtime hashes match and both actual GT/SON prepared fixtures produce exactly two candidates with identical repeats on Python3.12.15. Transport was prohibited and no production writes or emails occurred. Receipt: `artifacts/debt-representations-runtime-1792003967.json`.

The ordinary18:51 preparation run completes10 SEC scopes and8 metric scopes. Five newly prepared leverage panels use v5 (BETR, BEN, BEEP, BDX, BCPC); other scopes retain explicit coverage holds. This verifies ordinary version propagation, not GT/SON-specific unattended refresh or complete replacement coverage. Receipt: `artifacts/debt-rollout-1792003983.json`. Core remains FMP and institutional publication remains false. Existing v4 entries rotate normally; no forced universe refresh.

Secret scanning passed; audits still fail on unchanged frontend dependencies and Pillow. No full-suite/all-CI success is claimed. The owner's Codex remote429 request was recovered with current execution and normal usage allowed (50% weekly consumed); its historical cause and permanent repair remain unverified. No new deployment was dispatched during recovery.

## Remaining gates

Observe issuer-specific GT/SON ordinary refresh and broader v5 rotation. Existing v4 caches retain their version until normal refresh; no broad forced update is required. Correct debt alternatives do not establish complete EBITDA semantics or select the final public ratio policy. Broader financial coverage, scores/rankings/all alerts, institutions, transcripts, sustained heavy-workload capacity, final Massive, zero global FMP requests and billing cancellation remain open. No host, schedule, subscription, public provider, landing-page or video change.
