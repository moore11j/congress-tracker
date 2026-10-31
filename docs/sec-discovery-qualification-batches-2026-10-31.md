# SEC discovery qualification throughput, October 31, 2026

## Current production evidence

Read-only installed planner receipt `financial-queue-1793479346.json` at20:42UTC reports2,623 attempted scopes and297 due, maximum deadline lateness2.436hours and median1.187hours. All ten planned slots are ordinary scopes. ARQQ/INTR discovery retries are positions288/294. This is a queue observation, not proof that every panel is stale or a reason to increase host cost.

Aggregate retained-product audit `financial-gap-groups-1793479399.json` at20:43UTC covers2,623 scopes:1,941 usable SEC preparations and1,895 combinations.721 previously available financial classifications are unavailable in the pure comparison:374 missing scored fields,133 missing SEC directory identities,118 expired caches,34 unsupported current coverage,15 missing companyfacts,18 held discovery,22 pending discovery and7 missing caches. The universe has grown; this is not a fixed-cohort regression percentage. Cache age is neutralized only for legacy baseline comparison, not the replacement. No financial values or customer records were exported, no source requests or writes occurred.

Freshness receipt `sec-freshness-1793479477.json` at20:44UTC distinguishes the expired preparations:121 pending discovery,3 unsupported-current and6 partial panels across all cached scopes. All130 had attempts within24hours;99 next deadlines were in the future and31 overdue. The last20 ordinary runs completed10/10. Thus most expired preparation rows are failed/pending discovery cases, not successfully prepared ratios simply waiting in the queue. The scope differs from the classification-loss denominator above.

Production remains `b0576bcf` (PR744). Its legacy-cover repair is executing normally: ARQQ/INTR at20:21 and BLSH at20:46 enter `cover_reobserved`; full issuer qualification remains pending. Receipt `discovery-observation-1793479805.json`. No forced retry or deadline bypass. Public response sample `public-render-1793479256758.json`: Feed5.020seconds, Insights0.798,AAPL0.311,NVDA0.279; all200 without server error markers. Feed latency and sustained capacity remain open. Fly machine/status confirms the existing shared2CPU/2GB combined host and passing database readiness; no infrastructure change.

## Local bounded change

The discovery store can qualify at most three filings per existing worker slot, sharing the existing maximum of six original reads. It will not start an additional filing after5 elapsed seconds or2 CPU seconds. Those are continuation thresholds, not a hard timeout for an already running parser. A single filing retains all existing parser/source limits. Complete raw documents are released between filings; the store retains only compact validated receipts.

If the shared read budget ends midway through a later filing, only earlier complete receipts are committed and the unfinished candidate remains pending. Source denial or a different integrity error preserves the entire prior checkpoint. A legacy cover upgrade remains a separate phase. Atomic compare-and-swap, collection-disable checks, exact source integrity, complete-inventory selection, statement reconciliation and the no-publication gate remain. No schedule, configured batch size, subscription, host or public provider selector changes.

More small filings can be checked per slot, so average work per slot may increase even though the previous source-read maximum remains. Actual full-cohort parity and CPU/request measurements are required before release. This does not remove accounting/coverage gaps, authorize discovery publication or prove complete FMP replacement.

## Verification state

Initial seven focused Windows checks pass in146.23seconds, before final source-memory/disable refinements and the added concurrent-writer test. They are preliminary evidence only. Final Windows/Linux discovery checks and complete-source benchmark are running. No new application commit, PR or deployment at this checkpoint. Exact final results and release state must be appended before activation claims.

Core financials, transcripts and prices remain selected from FMP; discovery collection is enabled and public discovery reading disabled. Massive draft PR743 stays separate and unactivated. Institutional export remains unapproved. Goal is active: the prior checkpoint's usageLimited observation has been superseded by the current goal-tool result. Next finish exact-source tests/benchmark and review, then ordinary qualification and all retained consumer acceptance; paid Massive activation and zero-FMP/billing closure remain last.
