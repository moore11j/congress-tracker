# Consolidated earnings basis for leverage preparation

October 14, 2026. Implemented and locally verified; release and ordinary propagation pending. Public core remains FMP and no leverage candidate is selected.

## Source mismatch

Leverage preparation combined parent-attributable `NetIncomeLoss` with consolidated income taxes, interest and D&A, while comparing against consolidated debt. The parent-income helper could also fill this leverage input with consolidated profit minus NCI, intentionally appropriate for shareholder metrics but unsuitable for a consolidated debt basis.

HCA's [June 2026 Form 10-Q](https://www.sec.gov/Archives/edgar/data/860730/000119312526321077/hca-20260630.htm) distinguishes consolidated income3.787billion, NCI0.468billion and parent income3.319billion for first-half2026. First-half2025 is3.716/0.453/3.263billion. Its [2025 annual report](https://www.sec.gov/Archives/edgar/data/860730/000119312526044769/hca-20251231.htm) separately reports7.782/0.998/6.784billion. These original statements, not a vendor equality target, establish the distinction.

Trailing consolidated income is7.782+3.787-3.716=7.853billion; parent income remains6.840billion. With consolidated tax2.018, interest2.316 and D&A3.674, the explicitly consolidated calculation is15.861billion, compared with14.848billion under the parent-income basis. The retained debt/cash numerator is unchanged; its borrowing-only candidate changes3.28023976x to3.07073955x. This is a candidate basis, not the issuer's adjusted EBITDA, an FMP-equivalent definition or a selected public debt policy.

SEC [non-GAAP guidance](https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/non-gaap-financial-measures) distinguishes EBITDA from differently adjusted measures and requires reconciliation to GAAP net income. It does not make issuer definitions interchangeable or resolve every debt/lease policy.

## Repair

Evidence version6 uses reported consolidated `ProfitLoss` for leverage. If that observation is absent, it permits only explicit parent income plus compatible reported NCI, retaining both originals and the calculation. Explicit consolidated conflicts remain held. Missing NCI is never zero; nonfinite values, incompatible filing/period/currency/CIK, unreported or derived parent inputs, and ambiguous redeemable allocations cannot fill the gap. A present redeemable zero must have matching source identity. Reported consolidated observations are independently sufficient and do not depend on availability of a parent allocation.

The parent-income helper now serves shareholder metrics only. It cannot overwrite consolidated leverage inputs. Existing core values, shareholder income definitions, debt/lease inputs and overlap holds remain unchanged. Source evidence identifies the consolidated income basis and any derivation. Public net-debt/EBITDA remains unselected; prepared evidence version6 rotates under existing schedules and cache lifetimes without a forced full-universe rebuild.

## Checks and limits

228 affected checks pass on Linux Python3.12.3, covering explicit positive/zero/negative NCI, missing and conflicting observations, source identity, redevelopment of consolidated income, preserved shareholder values, debt/lease guards, inline recovery and cache persistence/repeats. Initial collection used one nonexistent test filename and ran nothing; corrected selection ran. Two old tests mutated parent income while intending to change the leverage input; they now mutate explicit consolidated fixtures. No unresolved test failures in this selection; no full-suite claim.

Actual HCA complete-original replay verifies the original current/comparative allocation lines, all four trailing components and exact new arithmetic. Twenty saved company-source controls retain every canonical field, all non-leverage evidence and debt/cash/lease inputs/holds; repeats are identical and source inputs unchanged. In these dated controls, BRK-B EBITDA changes125.822to126.136billion and LIN13.406to13.579billion. JNJ and NVDA now correctly remain incomplete for a consolidated basis because the captured sources do not establish all required income legs. Six existing top-level coverage holds remain. These are not fresh whole-universe coverage counts; missing consolidated evidence requires further source review, not an inferred zero allocation. The real current HCA files include an inline supplement; the older controls intentionally use their saved companyfacts alone.

Receipts: `artifacts/consolidated-income-oct14/{baseline-hashes.json,HCA-replay.json,source-replay.json}`; baseline92312ba8. HCA originals and hashes are retained in `artifacts/yahoo-gaps-oct14/HCA-source-manifest.json`; current raw companyfacts/submissions in `artifacts/operating-income-coverage-oct14/sources.json`. Replay is offline, without production writes or email. The initial comparison harness caught a subclass exception from an unchanged foreign issuer adapter; the corrected harness preserves that existing hold.

## Remaining gates

Exact release/runtime and ordinary version6 propagation; broader consolidated-income source coverage, debt-policy selection, all financial/score/ranking/alert consumer acceptance, institutions, transcripts, sustained capacity, final Massive, zero global FMP traffic and billing remain. No source selector, schedule, host size, subscription, video, landing-page, customer-email or cancellation change.
