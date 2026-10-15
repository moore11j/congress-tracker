# Magna SEC financial statements, October 15, 2026

## Problem and implementation

Magna (MGA, CIK0000749098) reports USD US GAAP financial statements through annual 40-F filings and quarterly 6-K exhibits. Its saved companyfacts feed contains annual 2025 observations but no June 2026 quarterly facts. The ordinary domestic-form reader cannot prepare current financials from this source. The reviewed 2025 interim directory reportDate is August 1, the publication day, while the actual statement period ends June 30. Treating directory reportDate as the financial period would misalign the calculation.

The new sec_mga_financials reader verifies the exact current/prior interim and annual original hashes, issuer identity, statement headings, USD million scale, column order and periods. It reconciles each income statement, operating cash-flow bridge, parent/minority equity and assets/liabilities. Same-period observations must agree across retained reports. Seven values enter the existing SEC preparation and selected-core dispatches: revenue growth, parent net margin, current ratio, parent ROE, free cash flow, FCF margin and FCF growth. Only MGA receives the new preparation cache version. The transport uses the existing identified, serial client, three bounded original requests per uncached preparation and existing 24-hour cache. No new schedule or provider selector.

Parent earnings are not consolidated earnings. Pretax income includes interest and equity income, so it is not mapped to operating income. Adjusted EBIT, gross/operating margins, EBITDA/debt ratios and reconstructed EPS remain absent. Free cash flow is operating cash flow less fixed asset additions, excluding the separately mixed investments/other-assets/intangibles line; this is the existing generic definition, not an assertion that it equals an issuer or FMP adjusted FCF definition. The missing classification inputs are not replaced with zero or a proxy.

New or amended filings require review. A separately reviewed same-day companion 6-K contains financial-review slides, webcast slides and a transcript; it does not replace the primary statements. Unrecognized same-day filings, subsequent filings, annual amendments, stale periods and future/incomplete filing days hold preparation before transport. The hard-pinned current cohort is intentionally not proof of future rollover support.

## Public original evidence

All sources were retrieved with the existing identified, rate-limited SEC client. No paid provider, customer or private production record was read. The full originals are retained under artifacts/mga-financials-oct15; condensed unaltered statement cells and original hashes are committed as the test fixture.

| Original | SHA256 |
|---|---|
| [tm2618646d1_6k.htm](https://www.sec.gov/Archives/edgar/data/749098/000110465926088923/tm2618646d1_6k.htm) | 9bbf3f5f7a65c08dc8f5a39c4a9c862a655bb369cb3cdfd3b8294683da74213b |
| [tm2618646d1_ex99-2.htm](https://www.sec.gov/Archives/edgar/data/749098/000110465926088923/tm2618646d1_ex99-2.htm) | 6fe6b3979c762cff19a0e1a5a5258fb36e064400040d624713b39fc00a8d85ab |
| [tm2519795d1_6k.htm](https://www.sec.gov/Archives/edgar/data/749098/000110465925072844/tm2519795d1_6k.htm) | 126105640351ffb3c3484fc0c4752d39790b2d460cb83f99b25f252f834f8e5f |
| [tm2519795d1_ex99-2.htm](https://www.sec.gov/Archives/edgar/data/749098/000110465925072844/tm2519795d1_ex99-2.htm) | c56fdd9b85ecdcb2902837577e2ac9d400307aa25505b2f42970316e3c6a42d8 |
| [d20215d40f.htm](https://www.sec.gov/Archives/edgar/data/749098/000119312526128771/d20215d40f.htm) | c15b6db1d6b1e331481e629012d091f7344d7a7bbf93fd3ed7c3e88f67464930 |
| [d20215dex993.htm](https://www.sec.gov/Archives/edgar/data/749098/000119312526128771/d20215dex993.htm) | e15ff384e3db06c25fc0c5897143e06c2397d5dd12d7b214375aa94e585cebe0 |
| [tm2618646d2_6k.htm](https://www.sec.gov/Archives/edgar/data/749098/000110465926089231/tm2618646d2_6k.htm) | 2f02a7760add0e7fc9430f0f10167a17c4bc6b4d56f25b1008591996e5e98bae |

## Exact original replay and consumer result

Three complete statement originals plus their covers and the companion cover are retained. Sixteen annual values independently agree with standard companyfacts observations from the same 40-F accession. The current/prior overlap reconciles. Trailing June 2026 revenue is USD42.671b, versus USD41.608b previously; parent earnings are USD761m, separately from consolidated USD814m. Operating cash is USD4.525b less USD1.287b fixed asset additions, producing USD3.238b free cash flow. Parent equity is USD11.766b versus USD12.482b a year earlier.

The resulting values are revenue growth2.554797%, parent net margin1.783413%, current ratio1.215218, ROE6.276806%, free cash flow USD3.238b, FCF margin7.588292% and FCF growth97.198538%. Existing consumer projection has only two scored inputs and correctly remains unavailable for a complete fundamentals classification. This does not close the MGA classification gap.

The offline full-original replay exercises real preparation, isolated SQLite persistence, cached repeat, the actual summary/confirmation converter and the actual watchlist evaluator. The availability transition sends no alert; an independent subsequent synthetic ROE threshold crossing produces one alert, repeat produces no duplicate, rollback produces none. Preparation writes no canonical cache or events. Twenty other saved-source ticker projections match a8e5ae84 exactly. No external network, production writes or emails during replay. Receipt: artifacts/mga-financials-oct15/full-replay.json.

Initial Linux Python3.12.3 selection:250 passed. Final tighter same-day/annual-amendment/comparative-restatement guards and affected financial/alert-basis consumers:89 passed on Windows Python3.14.2; the same final89 checks and full-original consumer replay also pass on Linux Python3.12.3. Across both Linux selections,298 distinct tests pass (250 initial,44 final MGA tests including three added guards, and45 additional consumer/basis tests;41 MGA tests overlap). Windows pytest warns about its unwritable cache directory; test results pass. Source transport initially hit sandbox network restrictions, then the explicitly reviewed public-source fetch succeeded.

## Release and remaining gates

Implemented locally; not yet merged or deployed. Exact release/runtime, ordinary production rotation and future filing rollover remain unverified. Public core remains FMP. Broad financial coverage, institutional activation, transcript breadth, full analyst/valuation gaps, all scheduled consumers and sustained capacity, final Massive activation and global zero-FMP/billing remain. Pending production-derived audit/export approvals are unchanged; no rejected export was retried.

The companion SEC filing directly links a Q2 2026 results-webcast transcript. It is a concrete free-source candidate for the transcript work, not yet parsed, integrated or published by this change. No subscription, host change, customer email, video, landing redesign or FMP cancellation.
