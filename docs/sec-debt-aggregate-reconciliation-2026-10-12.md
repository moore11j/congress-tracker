# SEC debt aggregate reconciliation

October 12, 2026. Implemented and locally validated; release pending. Public core remains FMP.

## Current coverage and discovered error

Read-only production audit `leverage-universe-1791828421.json` at18:06:59 covers1748 cached preparation scopes:1097partial,649unavailable and2expired/version-mismatched. Among1096 leverage panels,397 have complete EBITDA components,689 incomplete,2 incompatible filing components and8 negative expense holds. Missing fields overlap: interest510,D&A264,tax43,net income86. These are cached evidence counts, not current-version equivalence:1095 panels still use leveragev1 and onev3 while normal daily refresh advances. No broad forced refresh or public selection occurred.

Sixty-six scopes contain a ratio candidate of any debt definition. Only eight contain the reported-finance-inclusive-debt-plus-operating-leases variant. ON's candidate is negative2.099124992 despite FMP's cachedpositive0.764952200. Dates/definitions differ, so the sign difference alone cannot identify an error; inspecting the original does.

In the [ON July3 Form10-Q](https://www.sec.gov/Archives/edgar/data/1097864/000109786426000017/on-20260703.htm), the entity-wide `DebtAndCapitalLeaseObligations` fact is1.6million. Its surrounding narrative identifies subsidiary guarantees, not aggregate balance-sheet debt. The same period/filing reports `LongTermDebtNoncurrent`3,657.3million. The tag's broad taxonomy description therefore cannot justify treating1.6million as total debt. The prior generic preparation produced two false negative-net-debt candidates; both remained outside public values/scores/alerts.

Three identified public SEC requests captured originals at18:08:08. No production financial writes occurred:

| Source | SHA-256 |
|---|---|
| Companyfacts,4,623,403bytes | d9996c2b958223c9b3342fe2733bfefa877d8afc22beb803742ae1ba050fd3dd |
| Company submissions,146,727bytes | 2f98631575a144f55f834fc43a00c5e60d3ea88b60f3023756c755c834725d26 |
| Original filing,1,733,756bytes | e7e0fd40523d01a35979e176053975ffdd69f0c59ffb3e9e0f4a5b10b83bc790 |

## Repair

Leverage evidencev4 checks reported debt aggregates against compatible nonnegative components they must include. Period, filing date/accession, unit and sourceCIK must match. Aggregate borrowings must cover noncurrent borrowings; debt including finance leases must cover compatible borrowings/current/noncurrent/finance-lease components. Current and noncurrent finance-inclusive debt each cover the corresponding finance-lease maturity component. Operating leases are not assumed included in finance debt; finance leases are not assumed included in borrowings.

Contradictory aggregates and every candidate depending on them are held with original values and explicit reasons. Independent valid candidates may remain preparation-only. Bounds never infer missing zeros, reconstruct an unreported total, choose another value merely because it is larger or select any canonical ratio. Existing lease-sum evidence is reused, with no additional tags, source requests or full parsing pass. Main preparation cache/timing/budgets remain unchanged; older evidence retains its own version until ordinary refresh. A future selector must not treat an old candidate as a v4-validated value.

## Validation

Python3.14.2:140 focused tests pass across debt bounds, leverage, financial projection and original-inline parsing. They cover all included-component relationships, actual projector routing, period/accession/currency/CIK mismatches, missing/negative/nonfinite/subcent inputs, lease exclusion, independent candidate preservation and immutable source data.

An offline21-issuer replay includes20 hash-verified October9 source pairs plus freshON facts/company. Baseline is deployedc710534a. Every canonical value and every non-leverage evidence field remains identical; complete repeated candidate results are identical, with sockets prohibited. The20 older issuers retain their exact candidate lists. ON goes from two false candidates to zero, holding the1.6million aggregate against both3,657.3million noncurrent debt and23.3million reconstructed finance leases. Its1,565.7million EBITDA preparation remains unchanged. This is a proven regression and repair, not a whole-universe correctness claim.

`on-debt-oct12/runtime-fixture.json` captures137 source-bound observations and reproduces the entire full-source candidate panel exactly; it is prepared for deployedPython3.12 validation. Local artifacts are under primary `artifacts/direct-feeds/free-replacements-2026-10-09/`: audit, `on-debt-oct12` originals/manifest, `debt-bounds-replay-oct12.json`. Older manifest SHA052495c6b28b8f7b3837cdbafbadbe5e67ca51ef78c2d1576124c9ab3487acf8; ON manifest SHAee4ca5b8c9b20924964433796ed6c91402938dc4d58101dace4fda16130d5267. Initial audit query incorrectly treated a colon in its LIKE literal as a bind; corrected parameter binding completed the read-only audit.

## Remaining migration gates

Review/release/runtime verification are next. Broad missing debt/interest/D&A coverage, source-specific debt definitions, score/ranking/all-alert compatibility and ordinary freshness remain required; this guard does not fill1364 previously populated FMP leverage fields. ACN releasePR616 is live with74hashes, but its ordinary earnings-to-financial handoff remains pending. Institutions capacity/publication, transcripts, all retained consumers, finalMassive validation, globalzeroFMP traffic and subscription cancellation remain. No public provider switch, host/schedule/subscription/landing change or validation email.
