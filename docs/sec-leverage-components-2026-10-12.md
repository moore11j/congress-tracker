# Complete lease and debt components, October 12, 2026

## Problem and implementation

The free financial adapter misses lease totals reported as separate current and noncurrent liabilities. It now combines a complete pair only when period, filing accession/date, CIK and currency match, with both original inputs and the formula retained. A reported aggregate is checked against a complete pair when both exist. Contradictory, negative, nonfinite or incompatible evidence is held, including component paths that could otherwise bypass the conflicting aggregate. A missing maturity component is never zero.

Three additional debt candidates preserve disjoint components. SEC companyfacts descriptions explicitly define DebtCurrent as including current capital lease obligations, while LongTermDebtNoncurrent excludes capital lease obligations. Consequently only noncurrent finance leases may be added to that pair. Operating leases remain separately identified. No candidate is selected into canonical net_debt_to_ebitda; reconciled debt and EBITDA definitions, coverage and consumer transition remain required.

The existing inline parser reads the shared tag registry, so the added standard tags also flow through that parser. Leverage evidence version2 distinguishes new preparations. Main preparation cache/version and existing warming budget/timing are unchanged; existing observations may remain version1 until normal expiry. No forced broad refresh.

## Actual evidence

Read-only production receipt leverage-preparations-1791824169.json samples20 current ticker preparations. Ten have complete EBITDA components; only V, XOM and LIN have ratio candidates. Other failures include missing interest/D&A/net income, incompatible filing components and incomplete debt representations. An initial commentary count of11 was corrected to10 after counting the receipt. These are presence/definition diagnostics, not full numerical parity.

Independent offline replay uses the20 saved companyfacts/submissions pairs captured October9. Manifest SHA256:052495c6b28b8f7b3837cdbafbadbe5e67ca51ef78c2d1576124c9ab3487acf8. Each file hash is verified, sockets prohibited, baseline module loaded from ace765f6 and candidate run twice. Existing canonical values remain identical; complete payload repeats are identical. Debt candidates rise4 to6, ratio candidates stay1. This older cohort is distinct from today's live preparations and does not include later inline/continuity coverage.

WMT gains complete maturity sums: finance leases6.832billion and operating leases16.512billion. Its reported borrowings39.932billion therefore support separately labelled debt candidates46.764billion including finance leases and63.276billion including both lease types. EBITDA remains incomplete. No public ratio, score or alert is manufactured from those amounts. Receipt:leverage-components-replay-oct12.json.

## Checks and remaining work

Python3.14.2:108 tests pass across test_sec_leverage.py, test_sec_fundamentals.py and test_sec_inline_financials.py. Coverage includes actual projector routing, maturity reconstruction, reported-total contradiction, no-double-counting, missing/explicit-zero distinction, period/accession conflicts, negative inputs and source immutability. The actual20-source replay was repeated after strengthening contradiction handling. Direct diff and whitespace checked; no full-suite claim.

October12: Complete debt/lease component preparation implemented locally, not deployed or selected. 108 focused SEC projector/inline/leverage checks pass. All20 checksum-verified saved-source projections preserve canonical fields and full repeated results. Debt candidates increase4 to6 through verified WMT finance leases6.832billion and operating leases16.512billion; usable ratios remain1 in that dated October9 fixture. Current production20-scope read separately finds10 complete EBITDA preparations and3 ratio-candidate scopes. Neither cohort proves full coverage or FMP numerical equivalence. Source totals/maturity pairs are reconciled, contradictions held, current finance leases never added twice, missing noncurrent finance leases never assumedzero. Production remainsPR611/ace765f6, coreFMP/institutionsfalse. No public financial/provider/schedule/host/subscription/email/landing/video/billing change. All local handles terminal/no localPG; prior accounting release and current implementation/source replay are progress, goalactive. Next review/release this preparation and close EBITDA/issuer debt reconciliation, broad coverage and consumer checks, institutions/transcripts, then finalMassive/globalFMP/cancellation.

## Release verified

PR612 head190beff0 merged f51b2902; workflow38281622041 succeeded. Runtime receipt leverage-release-1791824742.json verifies69 matching source hashes, Python3.12.15 and isolated debt maturity/contradiction/repeat checks with no network or production writes. Core remainsFMP. Secret scanpasses; exact-head frontend and backend logs show unchanged dependency audit failures. No full-suite claim. Existing version1 preparations may remain until normal refresh; no forced broad refresh or public ratio activation.
