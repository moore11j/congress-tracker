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

Final expanded source `76aad3bd1155ec1c7a977261bc5c026c907eef3e` passes11 focused Linux checks and22 Windows checks. The earlier qualification-only variant passes85 broader Linux discovery checks. These counts overlap and are not additive unique coverage. An initial Linux invocation failed during collection because its default `/data` path was not writable; corrected runs explicitly use isolated SQLite. No failed collection is counted as passing verification.

The first benchmark covered newly collected covers. Review identified that the existing legacy-hint backlog also needed direct measurement. The final implementation therefore batches up to three legacy cover observations in an observation-only phase, within the same read and continuation bounds. None of those newly observed hints is qualified in that phase. Known document changes still fail closed, and denial during a later observation preserves the entire old checkpoint.

Four final comparisons on Python3.12.3 preserve the full15-filing checkpoint and complete restored financial output exactly:

| Initial state | One-filing slots | Batched slots | Original reads before/after | CPU seconds before/after |
| --- | ---: | ---: | ---: | ---: |
| New cover inventory | 21 | 12 | 44 / 44 | 52.900 / 48.762 |
| Existing legacy covers | 31 | 12 | 59 / 50 | 41.863 / 33.939 |

The legacy comparison starts after the initial cover inventory already exists; both modes use that same starting checkpoint. The control sets the batch limit to one. This is local replay evidence, not a production throughput or CPU guarantee. Individual slots can do more work: maximum measured slot CPU is20.488 versus20.364seconds for new inventory and9.217 versus16.338seconds for legacy inventory. The continuation thresholds govern whether another filing starts; they do not interrupt an already-running parser. Public latency and ordinary batching must be observed after release.

Receipts: `discovery-batch-v2-linux-oct31.xml`, `discovery-batch-v2-windows-oct31.xml`, `discovery-batch-final-linux-oct31.xml`, `discovery-batch-v2-benchmark-oct31.json`, `discovery-batch-v2-manifest-oct31.json`. Final regressions cover complete fresh/legacy equality, the seventh-read guard, unfinished-candidate preservation, denial rollback, separate legacy observation, collection disable, concurrent-writer CAS rejection, terminal-script integrity and idempotent repeats. No network requests or production data/writes were used by these tests.

PR746: https://github.com/moore11j/congress-tracker/pull/746 . Both dependency audits, secret scan and preview pass on76aad3bd. Attachment was attempted once and hit the existing100-identity limit. Concurrent PR745 changes homepage content/tests and memory only; its backend/workflow trees matchb0576bcf. No new backend deployment is claimed by these checks. Release preflight `release-blocker-api-1793480329.json` finds no protected/active/continuing work and the latest portfolio job succeeded; the deployment guard must still recheck at rollout.

The repeated20:52UTC public-response sample passes: Feed1.000seconds, Insights0.509,AAPL0.372,NVDA0.284 (`public-render-1793479973326.json`). This does not establish sustained recovery or full client-data latency.

Core financials, transcripts and prices remain selected from FMP; discovery collection is enabled and public discovery reading disabled. Massive draft PR743 stays separate and unactivated. Institutional export remains unapproved. Goal is active: the prior checkpoint's usageLimited observation has been superseded by the current goal-tool result. Next finish exact-source tests/benchmark and review, then ordinary qualification and all retained consumer acceptance; paid Massive activation and zero-FMP/billing closure remain last.
