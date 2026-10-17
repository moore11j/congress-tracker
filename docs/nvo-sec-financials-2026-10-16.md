# Novo Nordisk SEC financial preparation, October 16, 2026

## Result and scope

The exact NVO issuer adapter prepares nine ratios from reported DKK IFRS financial statements: gross/operating/net margins, revenue growth, operating margin change, current ratio, ROE, free-cash-flow margin and growth. It does not label Danish-krone amounts as USD, infer ADR EPS, substitute adjusted measures, derive EBITDA from depreciation plus impairment, or claim historical statement-panel completion. Public core fundamentals remain FMP.

Four complete SEC originals are retained as deterministic gzip fixtures (about 837 KiB compressed) with original-byte hashes. These are the June 2026 and June 2025 6-K reports and the December 2025 20-F cover plus its linked financial exhibit. The annual cover supplies exact CIK/form/report-period resources to the explicitly linked exhibit; matching namespaces and the standard inline identity/context/unit validation are required. No general foreign-form policy is widened. Fifteen same/later covers were captured and reviewed: repurchases, officer transactions, two regulatory updates and a same-day adjusted outlook release. Their exact filing identities and hashes are recorded; a new or amended financial filing stops preparation pending review. This bounded issuer implementation requires ongoing source review, not an unrestricted foreign-statement parser.

Primary documents: [current interim](https://www.sec.gov/Archives/edgar/data/353278/000035327826000023/caq22026.htm), [prior interim](https://www.sec.gov/Archives/edgar/data/353278/000162828025038087/caq22025.htm), [annual cover](https://www.sec.gov/Archives/edgar/data/353278/000035327826000012/nvo-20251231.htm), [annual financial exhibit](https://www.sec.gov/Archives/edgar/data/353278/000035327826000012/nvo-20251231_d2.htm).

## Accounting reconciliation

The current reported sales include the DKK 26,760 million 340B provision reversal. Reported figures are preserved, rather than silently replacing them with adjusted sales or constant-currency growth. The evidence names this limitation.

Effective January 2026, interest receipts move from operating cash flow to investing, and interest payments move to financing. The prior H1 CFO of 65,376 million, minus interest received of 824 million, plus interest paid of 1,952 million, equals the independently restated current comparative of 66,504 million. Annual and prior-period CFO are aligned using their own disclosed interest amounts, preserving all three source inputs and the derivation. Current TTM CFO is 133,902 million DKK; prior comparable TTM CFO is 121,713 million. After PPE purchases, current TTM FCF is 77,859 million and prior TTM FCF is 65,410 million. Only dimensionless margin/growth leave native-currency evidence; the canonical USD free_cash_flow field remains absent.

Income, balance and cash identities reconcile within the sum of independently rounded million-unit intervals. Consolidated equity reconciles to share capital, treasury shares, retained earnings and reserves. ROE consistently uses consolidated earnings and average beginning/end consolidated equity. Two 2024 current-balance revisions are not consumed in any published calculation and remain recorded. Conflicts in consumed comparative amounts stop preparation.

The full prior original has two extra unscaled cells beside the million-scale operating-profit/net-profit cells. One fresh SEC download matched every byte of the saved original. The parser accepts only these exact reviewed rows and proves the duplicate number is exactly the scaled adjacent value before reading the remaining columns. It never drops arbitrary extra cells.

## Verification

- 275 distinct Linux Python 3.12.3 checks pass: 239 source/issuer/preparation tests, then 78 final NVO/cache/alert tests with 42 overlapping NVO checks. Initial two test issues (exception assertion and encoded-date mutation) were corrected; no unresolved failing check in this scope.
- Full originals replay through acquisition, projection, prepared SQLite cache and exact repeat with network prohibited. All 32 annual original facts match the independent SEC companyfacts entries for the same accession/period/unit.
- Complete source provenance and nine metrics survive preparation; it creates no canonical fundamentals rows. Actual summary and Confirmation input are exercised. Watchlist rule transition produces zero alerts, a subsequent synthetic crossing produces one, and repeat/rollback produce zero duplicates or false alerts. No customer email or production write occurs.
- Nineteen other saved issuer controls exactly match pre-change source revision 5158db65. The older NVO control remains held under both implementations; the new current bundle is tested separately.
- Eleven-hash installed-runtime verifier is prepared and passes local Python 3.14.2 rehearsal using the four complete gzip originals, an isolated SQLite cache, native-currency and tamper guards, and no network. This is not installed production verification.

Evidence: primary artifacts/foreign-facts-oct16 contains complete originals/manifests, NVO-prior-refetch.json, NVO-annual-parsed.json and full-replay-linux.json. Runtime helper is artifacts/verify-nvo-runtime-oct16.py. Tracked test fixtures are backend/tests/fixtures/sec_nvo; implementation is backend/app/services/sec_nvo_financials.py with sec_nvo_sources.json and exact-symbol acquisition/projection/cache-version hooks.

## Release and outstanding work

Implementation is local on codex/nvo-sec-financials; review/merge, installed verification and ordinary acquisition are pending. The latest aggregate release-guard receipt release-blocker-1792117128.json shows a protected continuing portfolio cycle at 930/1,135 scopes, zero failures and three pending jobs. No live portfolio PID was confirmed in that observation, so it is checkpoint progress, not a verified live-process wait. PR694-703 remain undeployed in the latest evidence. No release bypass or retry occurred.

The preceding percentage-only turn was no migration progress. This turn adds a current, reconciled financial replacement and validates its actual consumers. Goal remains active/FMP enabled. Broad financial coverage, institutions, transcripts, analysts/valuation, all scheduled consumers, sustained performance, final Massive, zero-FMP traffic and billing closure remain. No subscription, source selector, schedule, hardware, customer-send, video or landing-page changes; pending private-export approvals remain untouched.


## Reviewed merge and pending backend release

[PR704](https://github.com/moore11j/congress-tracker/pull/704), tested head `3ecd3a0b7f4d5b4a4ae7f29976b6395cef36c9e1`, merged as `9bf64c51a955512a21cc4810c826f3576f3fcdb9` at 02:26:06 UTC. Backend dependency audit116778408982, frontend audit116778408704 and secret scan116778408988 pass in workflow38840237599; Vercel preview succeeds. Fetched merged application/test content exactly matches the tested head. Native PR attachment hit the existing100-item limit; the ordinary link remains usable.

Final aggregate guard receipt release-blocker-1792117589.json has one continuing protected portfolio cycle at984/1135, zero failed scopes and three queued jobs. A prior983scope observation confirmed a live PID; the final observation does not. This is observed checkpoint advancement, not terminal completion. PR694-704 backend deployment remains deferred without retry/bypass. The other nineteen controls and the full NVO consumer replay are local isolated evidence, not production cutover acceptance. Installed eleven-hash/four-original verification and ordinary source preparation remain next after safe release. Public core staysFMP; no subscription or activation change.


## October17 installed release

This package is deployed in f4f9ff0f through successful guarded workflow38986463595, and its isolated installed-runtime proof passes. See the [central release and ordinary-operation report](migration-release-verification-2026-10-17.md) for exact receipts, verifier corrections, live-source distinctions and remaining gates. Earlier deferred-release notes above are historical. No provider selection changed; FMP retirement remains incomplete.
