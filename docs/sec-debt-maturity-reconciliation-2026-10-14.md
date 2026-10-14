# Debt maturity and lease reconciliation

October 14, 2026. Implemented and source-replayed; release and ordinary operation pending. Public core fundamentals remains FMP.

## Confirmed source issue

A broad debt tag does not establish that finance leases are excluded. Three complete original filings establish that the current and noncurrent carrying amounts already include them:

| Issuer and report date | Current debt | Noncurrent debt | Reported total | Extra leases incorrectly added |
| --- | ---: | ---: | ---: | ---: |
| [Campbell, August 2](https://www.sec.gov/Archives/edgar/data/16732/000001673226000026/cpb-20260802.htm) | 977 | 6,160 | 7,137 | 84 |
| [Cooper, July 31](https://www.sec.gov/Archives/edgar/data/711404/000162828026061160/coo-20260731.htm) | 628.1 | 1,916.1 | 2,544.2 | 2.2 |
| [Verisk, June 30](https://www.sec.gov/Archives/edgar/data/1442145/000143774926024749/vrsk20260630_10q.htm) | 257.7 | 4,215.8 | 4,473.5 | 18.1 |

Amounts are USD millions. Campbell's debt notes include finance leases in both maturities; the fair-value narrative explicitly identifies 7,137 as carrying value. Its 84 lease liability disclosure differs from the two debt-note lease lines totaling 78; no cause for that difference is inferred. The independent current/noncurrent total plus 313 operating leases remains 7,450. Cooper and Verisk debt tables explicitly reconcile their finance lease portions to each carrying total.

Original SHA256 values, respectively: `1f64b0b1feaee02003b9ddb7b896ee1237d619542351e5ed391c358413857411`, `c6305c4fb9efd5f3f7358e372e74e28c8e1e2d12cc4816515d08a076eeaee995`, `6bf6d752163c5344fdb5fcde31f8e3b093c67b1211b399d6da5a2547fd8ec32b`. Nine identified, paced SEC requests captured company records, companyfacts and exact original filings. Production inline parsing independently reproduces all 14 debt/lease input points from those checksum-verified originals.

## Repair and scope

Leverage evidence v7 compares a proposed borrowing-plus-finance-lease sum against an independent complete current/noncurrent finance-inclusive debt pair when all inputs share issuer, period, filing, currency and availability identity. Contradictory additions are held, including their operating-lease variants. Independent reported totals remain candidates. Missing or incompatible references never become zero or invalidate other observations. Both aggregate and maturity proof are retained when available; one matching reference cannot erase another conflicting reference. Invalid/negative/nonfinite inputs and overflowing sums retain explicit guards.

No canonical net-debt/EBITDA policy is selected. Core values, score inputs, alerts, provider selectors, cache schedule, host sizes and subscriptions are unchanged. Existing v6 panels rotate through the normal bounded warmer; this does not force a universe refresh.

## Validation

The 71 dated positive panels were replayed under current baseline v6 and candidate v7 using identical retained observations. Only COO (4 to 3 candidates), CPB (5 to 3), and VRSK (5 to 3) change: five contradictory alternatives are removed. Every surviving candidate and all EBITDA values are identical, repeated full results match, and inputs remain immutable. This is not fresh current-version universe completeness evidence.

A separate 25-company checksum-verified companyfacts/submission replay preserves all canonical values, all non-leverage evidence and EBITDA, with identical repeats and network prohibited. Eight original-companyfacts coverage holds remain, including VRSK and SON that need inline historical support. Their prepared panels are covered separately; unavailable companyfacts-only results are not called complete source coverage.

Focused debt, leverage, core and inline-parser regression checks include three issuer cases, mismatched identity, missing versus explicit zero, rounding, conflicting references, nonfinite values and overflow. All 217 selected checks pass on Linux Python 3.12.3. No production writes, customer emails or paid requests occurred in these replays.

Local evidence: `artifacts/cpb-debt-oct14/`, `artifacts/maturity-debt-oct14/`, `cpb-maturity-replay-oct14.json`, `cpb-maturity-fixtures-oct14.json`, `cpb-originals-replay-oct14.json`, `debt-maturity-original-inputs-oct14.json`. The original 71-panel source SHA256 is `7a56292abcf52daa80fe2860749d5170fc939ef56e4c0caf9cbe531eac55b68d`.

## Readiness limits

Fresh 22:58 UTC audit has 1,746 fresh cache rows, 1,587 old/missing-version leverage panels, and only 32 complete EBITDA panels under current v6, with 126 incomplete and one negative-expense hold. Only four current v6 scopes have ratio candidates (CMCSA, CPB, CRH, CXW). These counts reflect version rotation, not a loss of all retained financial data. Comcast's two totals still differ and require source review; do not call them equivalent without evidence.

Broader financial definitions and coverage, full score/ranking/monitoring/watchlist/daily/weekly consumers, institutions, transcripts, sustained capacity, final Massive activation, zero global FMP requests and billing cancellation remain open. The remote Codex 429 is currently recovered with normal usage allowed; its historical cause/permanent repair is unverified. No model/retry/account configuration was changed.
