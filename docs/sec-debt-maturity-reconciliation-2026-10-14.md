# Debt maturity and lease reconciliation

October 14, 2026. Deployed through PR650 (`ae4931ff`); exact runtime verified. Ordinary v7 propagation verified at23:16UTC; three issuer-specific cache refreshes remain pending. Public core fundamentals remains FMP.

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

## Release receipt and further source review

PR650 source `ad3595d3473de06af1777c379abe5089c47aad85` merged as `ae4931ff4b316b9eb6aa14a148b31840f13f024e`. Workflow38652456260/job116154270977 completed successfully at23:11:59UTC. At23:12:18 all12 runtime hashes match; the deployed Python3.12.15 processor reproduces all three fixtures exactly, including EBITDA935.6m COO,1271m CPB and1616m VRSK, with three candidates each and identical full repeats. These are isolated retained-input replays with no network, database writes or emails. Receipt: `artifacts/debt-maturity-runtime-1792019539.json`.

Exact image and the unchanged API shared2CPU/2GB and database performance1CPU/2GB hosts verify in `preload-receipts-1792019555.json`. Public samples all200: Feed0.388s, Insights0.412s with research, news0.274s, research0.498s, AAPL0.462s andNVDA0.376s. These immediate cached samples do not prove sustained reliability or full data completeness. The23:11 financial job completed10 SEC/eight metric scopes before the new image, so its v6 results do not demonstrate v7 scheduled propagation. Ordinary23:12 press preparation completed5/5. News23/23 and42fresh company caches are verified in the captured scope; institutions950scopes remain partial/truncated and publicationfalse.

At23:11 the sampled two-hour throttling is zero; CPU credits decline13018.7 to9870.0 over the interval, with intervening deployments. Sustained heavy-workload capacity remains open. Receipt: `cpu-quota-1792019480.json`. No hardware or schedule change.

CI secret scanning passes. Frontend production dependency audit still reports5 groups(4high/1critical); backend Pillow10.4.0 still reports33 advisory rows. Dependency manifests are unchanged. No all-CI/full-suite claim. The app PR attachment failed because this thread has100 attachment identities; the PR remains available at https://github.com/moore11j/congress-tracker/pull/650. One CI-log read returned502; the same job subsequently returned terminal success without redispatch.

A separate three-request capture of [Comcast's June30 original](https://www.sec.gov/Archives/edgar/data/1166691/000162828026049360/cmcsa-20260630.htm), SHA256 `4ed89cc5104d722b9f3a3c112995b4ae1589704d99a91e759f7b49bf54277a69`, resolves the previously uncertain19m difference: the90.4b tagged total is explicitly rounded to100m (`decimals=-8`), whereas6117m current and84264m noncurrent are each rounded to1m (`decimals=-6`). They share the same context `c-22`. The maturity sum90.381b is compatible with that coarser narrative total. This is source evidence only: no generalized rounding tolerance, selection policy or canonical value change was introduced. A future policy must preserve original precision rather than infer equivalence from a small numeric difference. Complete capture/hash/precision evidence is in `artifacts/cmcsa-debt-oct14/`.


## First ordinary v7 preparation

At23:16:26UTC the ordinary23:16:03 batch has completed10 SEC scopes and8 metric scopes, with no canonical writes or emails. Five stored panels (FDS, FCX, FCNCA, FCN, FCFS) use v7 and retain their explicit partial coverage. This confirms scheduler/worker propagation, not individual CPB/COO/VRSK cache rotation or whole-universe readiness. Receipt: `artifacts/debt-maturity-ordinary-1792019786.json`. No forced preparation or changed schedule. All local handles are terminal; global FMP cutover goal remains active.
