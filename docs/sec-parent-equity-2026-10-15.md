# SEC parent-equity reconciliation

October 15, 2026. Implemented and tested locally; deployment and ordinary propagation are pending. Core financials remain FMP.

## Source evidence and calculation

AMAL and FUSB report their equity total under StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest; FRST changed to that tag for its current balance sheet. It is not a generic alias for parent equity. Six public SEC quarterly originals, two per issuer, verify the complete parent component layout. Source URLs, CIKs, filing dates, accessions and SHA-256 hashes are retained in backend/app/services/sec_parent_equity_reviews.json. Full original artifacts and independent numeric replay remain in local artifacts/equity-gaps-oct15 and parent-equity-replay-oct15.json.

For these three reviewed issuers, calculate common stock plus paid-in capital plus retained earnings plus accumulated other comprehensive income minus treasury stock. Require every component, a matching reported equity total, and independent equality to assets minus liabilities. Use compatible USD, issuer, period and same-filing observations. Preserve reported parent values and conflicts. Missing components, changed minority/preferred structure, negative capital/treasury signs, incompatible filings or restatements stay held. Negative retained earnings and AOCI remain signed. Do not infer missing minority interests as zero or substitute consolidated equity directly. The policy covers December 31, 2024 onward and can reconcile a future period only when all guards pass; synthetic future-period validation does not prove future actual source coverage.

Later filings sometimes repeat an unchanged prior equity total without its old component rows. A later identical comparative is accepted only when an exact same-filing original observation is present and equals the newest reported value. Conflicting duplicates or a changed value block the derivation. Both original filing identity and later confirmation remain in evidence.

| Issuer | June 2025 parent equity | June 2026 parent equity | Replayed trailing ROE |
|---|---:|---:|---:|
| AMAL | $753,984,000 | $834,993,000 | 14.27572583% |
| FRST | $376,415,000 | $433,829,000 | 13.10913749% |
| FUSB | $101,892,000 | $104,285,000 | 7.53042289% |

These are saved-public-source replay values, not a fresh production coverage audit. TFC remains held and unchanged. Recovering ROE does not establish all financial fields or the required classification coverage; no scoring threshold changed.

## Preparation and consumer behavior

Only these three ratio-cache versions change, retaining FUSB's existing bank-revenue revision. Other issuers and public source selection remain unchanged. ROE calculation evidence names the component reconciliation so same-provider alert transitions initialize a baseline instead of sending a false threshold alert.

The full preparation replay found an existing bank acquisition regression: the partial-companyfacts recovery check inspected ordinary revenue tags without the banking revenue basis. A complete bank history could therefore trigger unnecessary original recovery and fail with latest_reported_revenue_unavailable. The check now uses the same existing bank revenue selection as the financial projector. Incomplete histories still attempt recovery; no source guard is disabled. No schedule, batch size or resource changes.

## Validation

- 295 affected Python 3.12.3 tests pass, covering reconciliation, incomplete/complete bank acquisition, fundamentals, preparation, inline parsing, continuity, FUSB, cache/comparison, financial consumers, warming and alert calculation identity.
- Independent reading of six original balance sheets verifies all component values, equity totals and assets less liabilities. Full financial projection adds only ROE, preserves every existing financial value and repeats exactly.
- 24 control issuers retain identical complete results or explicit coverage errors, including TFC's unavailable ROE.
- Real isolated SQLite preparation stores three source caches, with six fixture reads total and no extra reads on repeat. No canonical financial/event rows are created by preparation.
- 21 synthetic watchlist evaluations across the three issuers verify source correction and rollback produce no false alert, repeat produces no duplicate, and a later real threshold change in the synthetic values creates exactly one trigger and one monitoring record per issuer. No emails, network calls or production writes.
- Initial validation caught a dictionary/set integration type error, corrected before final tests. One FUSB version assertion needed updating to check both retained bank and new equity versions. The first full replay exposed the bank acquisition defect described above; it was repaired and the entire replay repeated successfully.

Receipts: artifacts/parent-equity-tests-oct15.log, artifacts/parent-equity-replay-oct15.json and artifacts/parent-equity-replay-oct15.log. No full-suite, production-delivery, full coverage or sustained-capacity claim.

## Remaining gates

Release/runtime verification and ordinary preparation remain pending. Core coverage, institutional publication and current-cohort consumers, broad transcripts/analysts/valuations, all scheduled alert/ranking consumers, sustained capacity, final Massive validation, observed zero-FMP usage and billing cancellation remain open. Existing CAT/institutional verification-export approvals are still pending; rejected commands were not executed or retried. No provider activation, purchase, customer email, video restart or cancellation.
