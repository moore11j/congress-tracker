# Alternative financial and transcript source evaluation

October 15, 2026. This is a bounded evaluation for the FMP migration, not a new provider selection. Previous percentage-only turn made no migration progress. This turn obtains new public source evidence that changes candidate selection. The full migration goal remains active.

## EarningsCalls.dev: not a verified evidence replacement

The [official pricing page](https://earningscalls.dev/) currently lists Pro at $24.99/month with 3,000 requests and Ultra at $39.99/month with 20,000 requests. Its free API provides 50 requests/month and 250-character previews, not complete transcript text. Browser reading and automated API access are separate offerings. No account or subscription was created.

The [October 9 terms](https://earningscalls.dev/terms), sections 7 and 8, materially qualify the marketing: some passages or whole transcripts can be accounts based on notes or recollection; records do not identify the production method or original recording. Full raw transcript redistribution is restricted, excerpts are limited, and transcript text has a post-cancellation deletion requirement. This evaluation is not a legal opinion or a claim that the provider fabricates every transcript. It means these records cannot be silently treated as issuer originals or verbatim quotations. The unresolved provenance and completeness are sufficient to keep this candidate unselected for Walnut's evidence pipeline, irrespective of the owner's personal-tier preference.

Walnut's current `issuer_transcripts.py` reader validates retained original bytes, fiscal identity, source metadata and parsed content, with cross-provider document reconciliation. Replacing those proofs with a low-priced provider's summary-like text would change the retained product contract. No adapter or source selector was changed.

## OpenFilings: public feed tested, numerical coverage not established

The [official RSS+ documentation](https://openfilings.org/docs/rss-plus) describes public JSON and Atom feeds containing recent filing KPIs without an account or key. Two documented public URLs were fetched once each at 22:19 UTC, with a 25-second timeout and 4 MB response cap. Initial sandbox socket failures were local permission failures, not provider refusals; the permitted network retry returned HTTP 200 for both sources. Earlier browser-tool timeout/502 results were not treated as provider availability proof.

| Captured response | Actual result |
|---|---|
| JSON, periodic, one day, limit 10 | 10 rows; enriched_count=10; kpi_suppressed=10; all pipeline_status=pending; all has_canonical_kpi=false. No advertised numeric KPI fields found in the complete rows. |
| Atom with the same query | 50 rows despite limit=10; 14 10-Q, one 10-K, one DISCLOSURE, 17 INTERIM and 17 Form 4 entries. No advertised numeric KPI fields found in the complete entry payloads. |

The responses provide filing identities, dates and official source links. They do not supply usable numerical financial statements in this sample. The Atom filter/limit behavior also differs from JSON. This is not proof that the provider never supplies KPIs, nor a test of its authenticated historical API. Do not build a production fundamentals replacement on this sample or equate HTTP 200/enriched_count with usable coverage. No polling schedule was installed.

The [pricing page](https://openfilings.org/pricing) advertises Pro at $15/month with transcripts. The [rate-limit documentation](https://openfilings.org/docs/rate-limits) qualifies request counts: Free includes 100 daily/1,000 monthly credits and Pro 2,500 daily/25,000 monthly credits; financial-series requests cost 10 credits. Thus the included Free allowance supports at most 100 financial-series requests per month if no other requests consume credits. API entitlements, historical financial accuracy, transcript originals, retained-display rights and required-universe coverage remain untested. Its [analyst endpoint](https://openfilings.org/docs/api/analyst-transcripts) describes the provider's own analysis, which must not be substituted for broker consensus estimates.

## Reproducible evidence

Local public-only artifacts in the primary checkout:

- `artifacts/probe-openfilings-oct15.py`: bounded no-account acquisition.
- `artifacts/analyze-openfilings-oct15.py`: full saved-response analysis and SHA-256 verification.
- `artifacts/openfilings-evaluation-oct15/receipt.json`, `analysis.json`, `filings.raw`, `atom.raw`.
- JSON SHA-256: `df53cf505c720fdd33c812b2f549d18382e7592d298d6f2f93c132640e498afd`.
- Atom SHA-256: `816b4f62ca7da1dfa5928becb82cba834c05c2b5dd1c733d115ed0cf1a945794`.

The analyzer verifies both complete response hashes and all returned entry payloads. No production data, credentials, private exports or customer records were read. No production tests or deployment occurred in this evaluation. A local caller review also confirms the visible ticker metadata wrappers call the global FMP guard; that limited review is not a complete zero-egress audit.

## Decision and next work

Neither candidate closes a cutover gate. Continue the existing free/official preparation and consumer acceptance work. OpenFilings may warrant an authenticated sample later if remaining gaps justify it; do not request a purchase based on these advertisements. EarningsCalls.dev is not accepted as a source of verbatim issuer evidence. Preserve the current Unilever daily retry and source hold after the verified production HTTP 403; no bypass or timestamp reset.

Latest recorded application release remains PR689/209c22bb, with core financials/transcripts still FMP and institutional publication gated. Financial coverage and definitions, current institutional validation/activation, Senate freshness, broad transcript coverage, watchlist/monitoring/daily/weekly/ranking consumer checks, sustained performance, paid analyst validation, final Massive activation, global zero-FMP requests and billing retirement remain open. No landing, video, host, schedule, price subscription, publication or customer email change. FMP remains enabled.
