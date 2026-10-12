# Yahoo financial evidence preparation

October 12, 2026. Local parser implemented and tested; no production collector or consumer is activated. This follows the owner's request to investigate Yahoo/Google, issuer websites and official filings before purchasing another source.

## Live retrieval

At 18:35 UTC, two unauthenticated direct requests to Google Finance for MSFT and NVDA returned HTTP 200 but redirected to /finance/unsupported, with no financial values. Search/browser-visible data is therefore not proof of a working unattended collector from this environment.

Six unauthenticated Yahoo public pages returned statements successfully: income for MSFT, NVDA, ACN and ON, plus MSFT balance sheet and cash flow. The embedded application/json response includes raw values, currency, metric name, provider period type and asOfDate. Display tables use thousands; embedded raw values are already currency amounts. No login, cookies, key, paywall access or paid subscription was used.

At 18:39 UTC, one request to the exact query1.finance.yahoo.com fundamentals-timeseries URL embedded in the MSFT page returned 192 series in 64,918 bytes, compared with 1,144,524 bytes for the page. Its selected 90 observations match the page exactly. This is one successful direct request, not a contractual API, sustained-access, bulk-coverage or rate-limit guarantee. At 18:45 UTC, a second public request for four quarterly metrics returns 20 observations in 3,247 bytes, all explicitly 3M; broader quarterly coverage is not established.

## Implementation and checks

`backend/app/clients/yahoo_financials.py` parses captured public statement HTML or the observed JSON response. It does not fetch, connect to a database, register a schedule or change provider selection. Income, balance-sheet and cash-flow observations keep annual, trailing and quarterly frequency, provider metric names, currency amounts and source URL/hash/time. Balance values retain instant semantics even though Yahoo labels their annual reporting series 12M. No fiscal start or filing date is invented from an asOfDate.

Wrong URL/symbol identity, incompatible periods, future dates, malformed/nonfinite values, conflicting duplicates, ambiguous payloads, duplicate JSON keys and upstream errors are rejected. Missing series and null observations are explicit and separate from zero. Identical points deduplicate; reported EBITDA, NormalizedEBITDA and ReconciledDepreciation never alias. The parser returns evidence_only; issuer CIK/exchange validation and canonical field selection are separate unfinished work.

Python 3.14.2: 75 focused checks pass across the new parser and existing SEC leverage tests, including 42 Yahoo checks. A small fixture preserves four unchanged actual Microsoft series, with the original full-response hash. Offline six-page replay verifies every captured SHA256, all 444 selected observations and identical repeated results. The direct JSON endpoint reproduces the same 90 MSFT observations. Sockets are blocked for replay; no production writes, new provider requests or emails. This is not a full-suite pass.

## Accounting and freshness findings

- Microsoft annual parent net income of $133.749 billion matches the saved, hash-verified SEC annual facts exactly. Yahoo ReconciledDepreciation is $38.534 billion, matching the previously inspected cash-flow extension that includes other adjustments. It is not the $39.0 billion sum of separately reported depreciation and intangible amortization. The parser retains the provider label and does not substitute it into SEC EBITDA components.
- Yahoo ACN latest trailing revenue ends May 31, 2026, and its annual series ends August 31, 2025. Our verified issuer-release parser covers August 31, 2026. Yahoo would not repair that current-period gap. The 18:41 read-only production check still finds no stored ACN issuer receipt and the prior May financial cache; ordinary propagation remains pending.
- Yahoo's ON trailing EBITDA is $1.5672 billion, while the existing source-bound SEC preparation is $1.5657 billion. Definitions must be reconciled; numerical proximity is not equivalence. Normalized EBITDA is a separate value.
- Vendor month-end labels, especially for issuers with week-based fiscal years, require explicit matching to original statements. The parser does not relabel them as SEC period ends or assign historical availability.

## Captured evidence

All artifacts are in primary artifacts/direct-feeds/free-replacements-2026-10-09. Two public-financial-pages folders, 1791830155 and 1791830243, contain HTML/text/manifests. Raw timeseries: yahoo-timeseries-1791830361.json; replay receipt: yahoo-evidence-replay-oct12.json. Source hashes:

| Capture | SHA256 | Selected observations |
|---|---|---:|
| MSFT income | cb1701626e72aa1a94670e139790a52c190a54fad71791c2f841b3f91caf7717 | 90 |
| NVDA income | e0a1b36ea53e9c6ab6e9a500383dca5c1f0270baa27f1f4673b9438e88b8c08a | 75 |
| MSFT balance | 24c7d3096d361232a40c61f56145e63ff96d59fd0621b3712e973f050a395df9 | 64 |
| MSFT cash flow | 1b4c1efef11b274b0187afc526341e6f02e4c0e9f1d00527726afa01b24a7bb9 | 48 |
| ACN income | c63ad3859f7bd91da20ccd430da307039119b13dd020e121b2b863c8a748ccd4 | 71 |
| ON income | f2db6ef4aa92ff8fa4059007c29e9e1cd7c9973dafdbcde8f7544fc1aa7892ce | 96 |
| MSFT direct JSON | 3e6ceb158f3d614ce40c601edad351be92ea359da7216253afe335d7d3960c2f | 90, same observations as page |

Original URLs: https://finance.yahoo.com/quote/MSFT/financials/, https://finance.yahoo.com/quote/NVDA/financials/, https://finance.yahoo.com/quote/ACN/financials/, https://finance.yahoo.com/quote/ON/financials/, https://finance.yahoo.com/quote/MSFT/balance-sheet/, https://finance.yahoo.com/quote/MSFT/cash-flow/. The exact public query URL is retained in the local timeseries manifest; it contains no credential parameter.

## Next acceptance work

Broader original-source reconciliation, identity/share-class coverage, broader quarterly and latest-period freshness, repeated direct transport, bounded retries/shared-budget cache collection, then existing Walnut financial/score/ranking/watchlist/digest contract tests. Preserve SEC/issuer priority where their originals are newer or more specific. No new recurring schedule is authorized by this document.

Production remains PR618/403b0f4a, core FMP and institutional publication false. Ordinary PR618 priority-ticker-prewarm at 18:42:04 UTC exits 0, 2.5448 CPU/4.21 wall seconds (ordinary-idle-cpu-1791830592.json). Older 06:12 sample was 3.4099 CPU/5.01 wall seconds, but work scope differs and other releases intervene, so this does not isolate the saving or prove sustained capacity. Several direct preparation jobs report exit 1; their reasons require the stored coverage/run receipts, not an assumption that all schedules are healthy. No purchase, new source activation, host/schedule change, customer test email, landing/video change or FMP cancellation.

## Quarterly reconciliation and capacity follow-up

Quarterly source SHA256 931e4df028c544272cf44ba21fe84089712ad037d4b8b29843463a26fff22afa is preserved as an exact JSON fixture. Receipt yahoo-quarter-reconciliation-oct12.json compares the four quarters labelled September 2025 through June 2026 with the annual June 2026 record. Revenue sums exactly to $331.839 billion and interest to $3.051 billion. EBITDA sums to $212.433 billion versus the annual $207.519 billion, and ReconciledDepreciation sums to $43.448 billion versus annual $38.534 billion. Both differences are $4.914 billion. The cause is unresolved; no quarter summation replaces a reported value, and these observations are not approved canonical data. This demonstrates a concrete semantic/revision gate rather than merely a hypothetical warning.

Current host receipt role-cpu-1791830783.json: API two-hour mean0.20847 CPU remains above0.125 allowance, while the latest five-minute sample is0.11210 with zero reported throttling and6217.19 credits. Database two-hour mean0.11364 CPU. The short postrelease recovery is encouraging, not sustained-capacity proof. Keep production403b0f4a running while observing it; this unused Yahoo parser does not justify another restart. All new local command handles are terminal; no local PostgreSQL. No new deployment or schedule.

Draft PR #619 (https://github.com/moore11j/congress-tracker/pull/619), source f9aed3822473bd846a60f20ec266d1e5facf3bb6, preserves this local evidence package. Exact-head CI38293307644 passes the secret scan. Backend/frontend dependency audits fail on unchanged Pillow10.4.0 and nanoid/Next/PostCSS/Sharp/source-map-js dependencies; inspected through the required CI tool, with manifests/lockfiles unchanged. This is not a green full-suite claim. PR attachment was attempted but the app rejected its 100-attachment limit. No merge/deployment; production remains403b0f4a. All local command handles terminal. Broader transport/coverage, accounting/revision and scheduled integration work remains active.


## Source-bound bundle diagnostics, October 12 at 18:55 UTC

`backend/app/services/yahoo_financial_evidence.py` prepares at most six captured JSON responses and four MB total, reparsing originals rather than trusting caller-projected numbers. Source hash, URL and observation time remain tied together. Exact repeats deduplicate; different values or currencies for the same statement/metric/frequency/end retain both alternatives as conflicts. No latest-wins rule silently overwrites a revision.

Annual/trailing comparisons use four provider-labelled 3M observations ending on the same label, with adjacent ends 70-110 days apart. This is a diagnostic candidate window, not proof of fiscal starts, non-overlap or the actual issuer year. Currency mismatch, a conflicting quarter, a missing end or an incomplete/nonconsecutive window prevents summation. Decimal arithmetic avoids binary-float artifacts; balance-sheet instant values never sum. Reported and NormalizedEBITDA remain distinct. Agreement still returns canonical_eligible=false and does not establish definitions, identity, freshness, revision compatibility or historical availability.

Python3.14.2: 91 focused tests pass across Yahoo parser, bundle and SEC leverage checks (16 new bundle checks). The first test command used a mistaken doubled backend path and ran no tests; corrected paths pass. Offline replay of both original hash-verified JSON captures yields110 observations, identical repeat and input-order independence, in0.015625 process CPU seconds on this machine. Actual annual June2026 revenue and interest agree with candidate quarter sums; EBITDA and ReconciledDepreciation each differ by4.914billion. Missing quarterly coverage remains explicit. Receipt yahoo-bundle-replay-oct12.json records full sources and diagnostic values. No provider request, database write, email or public metric selection occurs.

The 18:50 read-only ACN check (acn-release-1791831004.json) verifies74runtime hashes, original-source repeat and the ordinary18:47 issuer-financials receipt for period2026-08-31. The separate sec-current-fundamentals cache remains its18:01 May31 result; ordinary financial propagation is still pending. This supersedes the18:41 absent-receipt observation without claiming full handoff.

PR619 remains a local/draft evidence package. Production403b0f4a is unchanged so sustained capacity observation can continue. Durable bounded collection/cache and live integration are still unfinished; no new recurring schedule or provider switch.
