# SEC net-debt/EBITDA replacement policy

October 15, 2026. Implemented locally; release and ordinary production preparation remain pending. Core financials still select FMP.

## Whole-universe readiness finding

A bounded production read-only audit at05:15UTC exports aggregate counts only, with no individual records, provider calls or writes. Across1,752 retained scopes,1,107 have usable SEC preparations and1,250 combine with free Finnhub metrics. The comparison still loses359 formerly available financial classifications:150 unsupported current SEC financial coverage,103 absent SEC directory identities,90 missing scored fields,11 missing companyfacts,3 expired caches and2 missing caches. These categories are not all missing U.S. common-stock filings; the universe also contains foreign listings and other securities.

None of the prepared replacements supplies the canonical net-debt/EBITDA metric, compared with1,360 retained FMP rows. Price, market-cap, earnings-yield and free-cash-flow-yield gaps in this audit exclude the deliberately deferred Massive market-data path. Raw presence is not numeric equivalence or actual cutover readiness. Legacy age is neutralized only for this field-effect comparison. Receipt: `artifacts/financial-aggregate-1792041320.json`.

Only272 fresh preparations currently carry leverage evidence v8;1,474 other fresh preparations lack that version. Of the272,67 have complete EBITDA,204 are incomplete and1 has a negative expense component. This version-specific observation must not be described as the loss of every older calculation. Missing consolidated income, interest, D&A and tax overlap. Normal rotation continues; no full-universe cache flush is introduced.

## Consistent definition

The selected replacement definition is **debt including finance leases, minus cash and cash equivalents, divided by unadjusted trailing consolidated EBITDA**. Operating lease liabilities are excluded. EBITDA is consolidated net income plus income tax expense/benefit, interest expense, and depreciation/amortization, with the existing exact-period and filing checks. Issuer-adjusted EBITDA, interest income netting and absent expense observations are not substitutes. Negative net cash is retained; nonpositive EBITDA is unavailable.

Eligible debt representations are a reported finance-inclusive total or a complete current/noncurrent finance-inclusive sum. Borrowing-plus-lease representations require an independent matching inclusive total or maturity sum; generic tags alone do not establish that the inputs are disjoint. Surviving complete alternatives must agree. A previously disproved subtotal does not erase an independent complete maturity sum. Conflicting complete totals are held instead of choosing a convenient value.

A rounded aggregate can corroborate the more precise complete maturity sum only through the existing original-bound precision proof. The selector rebuilds that proof and binds its exact inputs to both surviving candidates and their cash observation. It requires full interval containment under the stated round-to-nearest assumption. This does not authorize other lease additions or constitute a general XBRL calculation validator. Missing original precision or mismatched inputs keep the ratio unavailable.

Banking, securities, insurance and investment-vehicle SIC ranges, unverified industries and non-USD reporting remain outside this industrial policy. This preserves truthful gaps; it does not claim an industrial ratio is meaningful for every security.

## Integration and transition safeguards

The SEC projector emits the canonical metric only when the policy passes and retains its complete source/calculation evidence. Existing source-isolated preparation includes it on ordinary refresh. Previously prepared rows lacking the new optional metric remain usable until their normal expiry; no forced broad invalidation or extra schedule is added.

The financial summary exposes the SEC definition in its existing provenance and source-note fields. Score, classification and outcome methodology identifiers change only when SEC/free fundamentals are selected. The currently selected FMP methodology is unchanged. Custom alerts fingerprint the debt policy's version, formula and lease basis; calculation changes rebaseline rather than generate company-development alerts. Recorded history is not rewritten.

The global core source remains FMP. Candidate availability does not authorize selecting the whole financial source or publishing rebuilt rankings before the other acceptance gates pass.

## Validation checkpoint

Initial affected checks passed204 tests. The expanded selection passed286 tests but one fresh-worker subprocess exceeded its existing45-second timeout. The timeout has not been increased; the exact check is being repeated with the final affected selection. No blanket passing-suite claim is made at this checkpoint.

The original-source replay checks26 companyfacts/submission pairs, all saved SHA256 hashes, exact repeat, unchanged prior financial values and unchanged other evidence. The first pass produced source-backed Goodyear and Campbell's ratios and held Comcast's unresolved rounded-total conflict. Final precision-backed replay and final test results are recorded below when complete.

All broader financial coverage, current institutional activation, transcript coverage, scheduled monitoring/watchlist/daily/weekly/ranking consumers, sustained capacity, final Massive activation and zero-FMP/billing gates remain open. Pending CAT/institutional payload-export approvals and valuation-model choice remain unchanged. This work does not perform those exports or introduce a DCF model.


The final26-source replay now accepts three independently retained source calculations: GT(7.190-0.861)/0.483=13.1035197x, CPB(7.137-0.394)/1.273=5.2969364x, and CMCSA(90.381-7.661)/34.131=2.4236032x (inputs in billions). Other pairs preserve explicit coverage/industry holds. Comcast includes its complete original and same-source precision annotation. All existing canonical fields, other evidence and repeated full outputs remain unchanged. Receipt: artifacts/net-debt-original-replay-oct15.json.

The final rounding selection exposed two test issues: the synthetic income/cash fixture used a different accession than its debt original, and an existing Comcast integration test still asserted the metric was never selected. The fixture now uses one coherent filing and the Comcast test asserts the actual debt/cash/EBITDA ratio, retaining its no-canonical-write/cache-repeat checks. All77 policy/precision tests pass onPython3.12.3. The broader run before those fixture corrections passed286 checks; the previous fresh-worker timeout passed on that repeat without changing its45-second limit. Final complete regression selection remains in progress.


Final regression: all346 selected tests pass in97.89s on Python3.12.3. The recorded process is terminal. This covers policy, leverage/debt/precision, custom-alert basis, provider baseline, financial consumers, confirmation bundle, SEC projection/preparation and financial cache. No timeout change or production source activation. Receipt: artifacts/net-debt-policy-release-oct15.log.

## Deployed release and live verification

PR667 source4b8088c7b8175a7ba9b6f61bf822b0b019c77965 merged8d3b04eda6121d4792da5fe2b2e32ef5e1a2fa64. Backend38688134541/job116267617754 completed successfully. Exact application sources match merged main. Secret scan and Vercel preview pass; unchanged dependency audits report33 Pillow advisory rows and frontend five production groups (four high, one critical). No dependency files changed. The attachment tool reports the existing100-item limit; PR remains at https://github.com/moore11j/congress-tracker/pull/667.

Production Python3.12.15 verifies nine source hashes, exact synthetic repeat, proven finance-inclusive selection, unproved additions/bank holds, summary provenance, custom-alert basis and fresh-worker methodology versions. Process-local SQLite/FMP-disabled verification performs zero production reads/writes or provider calls. It does not change deployed source selection. Receipt: artifacts/net-debt-runtime-1792042993.json.

The initial live Goodyear proof differed because the offline replay lacked the optional full-original precision annotations. Replaying all three retained originals through the same normal acquisition path preserved the26-pair full-field/evidence parity and revealed only a raw-source hash difference. An independent public re-fetch shows identical original filing bytes except for SEC's terminal injected delivery-script URL. The final live verifier checks every other filing byte, binds each prepared precision hash to the exact raw bytes received, and compares every remaining policy field. No production hash normalization or validation relaxation was introduced. Preliminary failed comparisons are preserved as diagnostics, not successful evidence.

A bounded explicit normal preparation makes eight public SEC requests in4.83seconds and verifies GT13.10351966873706x, CPB5.296936370777691x and CMCSA2.4236031759983594x against the independently retained originals. Complete policy evidence, canonical ratio and no-request cached repeat match for all three. Only source-isolated preparation caches are written; no canonical financial rows, events or email are written. The local receipt exports booleans/counts only, not production payloads. Receipt: artifacts/net-debt-warm-1792043299.json. This is explicit preparation, not proof of ordinary scheduled propagation.

At05:48UTC the aggregate-only audit retains1752 scopes,1107 usableSEC and1250 combined preparations, with359 lost classifications. Three canonical replacement ratios now exist versus zero before this release;1360 FMP rows have the field, so1357 remain unmatched in this dated cache snapshot. Fresh v8 leverage now includes84 complete,242 incomplete and one negative-expense preparation;1419 are missing/older version. These counts are not numeric parity or full-source completeness. Receipt: artifacts/financial-aggregate-1792043336.json.

Immediate public reads return200: readiness/database okay, Feed0.234s and Insights0.324s without the server error page. Receipt: artifacts/fusb-public-1792042985.json (filename reused by the existing public probe). Sustained capacity, ordinary policy propagation and all wider cutover gates remain open. Corefinancials still selectFMP; no source, schedule, host, subscription, customer delivery, video, landing-page or cancellation change. All new test/deployment/verifier handles are terminal. Pending CAT/institutional payload approvals and DCF preference remain pending.
