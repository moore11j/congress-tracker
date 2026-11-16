# Financial period reconciliation

Prepared November 15, 2026, America/Denver. The owner approved continuing the financial-period batch after PR814. This candidate does not activate a provider, change scoring methodology or pricing, or constitute full financial coverage.

## Result

The 43 retained current-revenue/TTM holds now have complete actual-reader inputs and exact output/request repeats. The candidate changes one issuer, HRB: revenue growth, net margin and free-cash-flow margin become source-supported. The other42 complete outputs and all43 request lists remain identical. HRB remains `unavailable` under the existing fundamentals classification because only one of the required scored metrics is available. No score-policy relaxation is included.

HRB's annual and prior-quarter statements put the title inside a table and leave the reported revenue subtotal unlabeled. The latest quarter uses `Revenues` for the primary total while the complete contract-revenue series has the same current-period amounts. Existing complete-series selection requires two exact overlapping periods, matching filing/accession/units, a complete TTM decomposition and primary-statement proof for every selected input. Those requirements remain intact.

The shared layout verifier accepts a visible, exact consolidated income-statement heading in leading table header cells. It accepts an unlabeled total only when the revenue section consists of the two reported service/royalty components followed by their reported total, before operating expenses. Standard product/service-axis members, matching periods, USD units and the arithmetic equation are retained in the proof. The latest labeled total must equal the selected entity-wide contract fact in the same original. No source JSON is altered, no quarters are summed, and no missing amount is treated as zero.

Current TTM revenue is $3,966,177,000; prior TTM revenue is $3,770,736,000. The three recovered ratios are5.183099532823292% revenue growth,17.9249942703011% net margin and18.83108091242524% free-cash-flow margin. Existing current ratio and free cash flow are exact. The existing full and partial projectors evaluate equivalent FCF-growth arithmetic in a different order, producing29.63434171267698 versus29.634341712676985 percentage points; the difference is below1e-12, and is disclosed rather than reported as byte parity.

## Complete originals and remaining batch

All87 requested/research filing originals are retained with complete-byte hashes, totaling230,243,074 bytes, with87 successful public reads and no access holds. Each of the43 issuers also has its retained complete SEC company/companyfacts records. The five committed HRB originals total8,886,632 bytes and cover company identity, companyfacts, annual2026, current-quarter2026 and prior-quarter2025 filings.

The annual/transition/registration diagnosis covers every43 issuer:

| Finding | Cases | Remaining requirement |
|---|---:|---|
| Annual entity-wide revenue exists | 12 | Match current concept, periods and primary-statement evidence; HRB is the candidate repair |
| Transition or registration document | 16 | Qualify the reporting period and entity scope separately |
| Annual original needs additional qualification | 8 | Resolve namespace/date/document-set or missing-current-revenue holds |
| No supported standard annual revenue point | 7 | Review original disclosure semantics; absence never establishes zero |

Only four standard alternate series are complete at the retained current period: HRB, PACB, KULR and PLBY. PACB has matching tagged overlaps but its primary statement and segment note use different concepts; its proof is not equivalent to HRB's existing total/contract path. KULR and PLBY retain conflicting overlaps. None of those three is admitted by this candidate.

Local artifacts: `period-reader-nov16/replay-1794803737.json`, `period-reader-nov16/candidate-1794803739.json`, `period-batch-comparison-nov16.json`, `period-annual-original-diagnosis-nov16.json`, `period-alternate-series-nov16.json` and `period-original-filings-nov16/manifest.json` under `artifacts/` in the main workspace. These are local evidence, not production recoveries.

## Validation and release gates

The new tests cover complete original acquisition/projection and repeat, source-proof tampering, hidden/late/guarantor/note titles, changed components, namespaces, missing/duplicate rows, cache/screener behavior and watchlist transitions. The standalone fixture retains the existing10/12 CPU-second and22-wall-second Linux limits. Local Windows3.14 completes the five-original acquisition/projection repeat in11.797CPU seconds; Linux capacity is a separate required CI gate and its bounds are not increased.

One pre-existing local Windows3.14 exact-equality assertion in `test_sec_revenue_unused_periods.py` fails on both the unchanged deployed base and this candidate. It is not weakened here. Linux3.12 CI, new focused tests, the49 retained-output replay and the prepared installed-release proof are recorded separately before release approval. No full test pass or production installation is claimed by this draft checkpoint.

The last full preparation audit remains737 gaps/2,622 symbols. Ordinary recovery, all consumers, sustained capacity, remaining source/identity/sector cases, institutional acceptance and Massive-last gates remain. FMP core/prices/transcripts and all production selectors stay unchanged.
