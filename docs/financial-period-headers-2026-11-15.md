# Explicit statement duration headers, November 15, 2026

The owner approved continuing the financial coverage work after PR812. Complete Safe Bulkers statements print headers such as `Three-Month Period Ended` and `Nine-Month Period Ended`. The existing reader only recognizes `three months ended`, so it rejects income and cash-flow columns and then reports a balance/income alignment hold.

The shared repair recognizes explicit three-, six-, nine- and twelve-month period captions, in words or numerals. Column geometry, original header text, actual dates, month-end requirements and filing availability remain intact. Unsupported durations, ambiguous years, contradictory columns, projected periods and future periods remain held. Only legacy statement-alignment holds receive a one-time policy retry; qualified and unrelated receipts retain their behavior. No issuer allowlist, scoring formula, market-data source or worker limit changes.

Two complete SB originals now normalize twelve observations each, for June30 and September30,2026. The current original binds quarterly operating income45,868,000USD to July1–September30 and year-to-date operating cash flow136,196,000USD to January1–September30. Four gross/net revenue conflicts remain unavailable. Consolidated income does not become parent/common-holder income, and equity remains absent. These are recovered source records, not a complete financial classification.

The actual discovery reader qualifies the current filing from three complete originals (SEC company identity, companyfacts and the complete filed statement), repeats exactly, and retains financial publication eligibility=false. Disabling only the new duration wording restores the prior alignment hold. The original-based consumer validator re-parses and verifies the same observations.

## Batch scope and validation

The complete161-original retained wording inventory finds this form in16documents across nine issuers: BUD, ENLT, FVRR, GASS, KMDA, NIVF, ONON, SB and TU. Eighteen complete company/facts originals are retained for that cohort:17public requests and one verified reuse,7,825,067bytes total. Caption matches do not imply qualification. Full individual-source baseline/candidate/repeat comparison is complete: two SB documents recover twelve supported observations each, fourteen other documents retain identical holds, and every repeat matches. The complete offline comparison took311.75CPU seconds across all three passes; this is not production-capacity acceptance. Other basis, currency and source holds are not waived.

All49 retained full-reader outputs and request lists remain identical to the accepted baseline and exact repeat. The broader focused suite passes231tests; the final34header tests also pass (the suites overlap). The prepared bounded complete-source script passes a Windows rehearsal in4.172CPU seconds; Linux resource and full regression checks remain pending. Tests retain both complete SB originals and add the complete1,056,127-byte SEC companyfacts fixture. No fixture slicing or synthetic financial values replace originals.

Evidence is retained in `artifacts/financial-period-parity-nov15`, `financial-period-wording-inventory-nov15.json`, `financial-period-sources-nov15`, and `financial-period-source-comparison-nov15` in the primary workspace. Worktree `artifacts/release-financial-period-nov15`, branch `codex/financial-period-headers-nov15`, starts from deployedPR812/e85385d3.

## Remaining gates

AtNovember16 01:53UTC, all93prior accounting-basis receipts still require ordinary PR812 retry; no forced refresh was performed. Whole-issuer discovery, actual source selection, complete financial coverage, downstream consumers and sustained capacity remain separate gates. This next repair is not merged or deployed at this checkpoint. Core/provider selections, prices, entitlements and subscription state remain unchanged. Massive subscription remains held. The separate financial-industry scoring preference is still unresolved.
