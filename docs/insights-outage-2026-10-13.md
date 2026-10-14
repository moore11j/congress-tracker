# Insights outage, October 13, 2026

Owner reports slow app navigation and Insights server exception, screenshot digest 1486645905. Incident probe October 14 05:33 UTC (October 13 local) reproduces a 30.49-second streamed HTTP 200 carrying an RSC error digest 1582193453. Readiness and a ten-row feed time out at 45.27 seconds. HTTP status alone is not page success.

Initial production revision was db1caf31. LF-normalized hashes of main.py, events.py and request_guards.py match deployed source. Fly health check is critical. API CPU credits are almost depleted, two-hour CPU mean 0.1457 versus 0.125 shared allowance; throttle metric is nonzero. Database has no throttling. Source artifact role-cpu-1791956015.json. A 30-second process sample identifies API process 663 as the largest surviving CPU consumer. Recent logs show simultaneous anonymous SSR insider summaries/trades/alpha requests and checkout log timestamps up to5.6seconds after request start. This establishes a request burst during overload, not the identity of the external caller or a complete CPU attribution.

## Repair

- Optional Insights research preview streams independently and catches an archive fetch failure with a truthful local unavailable message. Published archive behavior still propagates failure instead of presenting an incomplete static-only list. The real public-card fetch aborts after 4.5 seconds.
- Anonymous SSR secondary profile requests share two nonwaiting slots after public cache lookup. Excess gets retryable, noncacheable 503, preserving room for interactive/health work. Signed-in and browser requests retain their existing paths; no records or fields are removed.
- Heavy admission waits cooperatively rather than blocking the event loop on a threading semaphore. Cancellation cannot leave an executor waiter that later consumes a permit.

46 backend checks pass on Python 3.14.2: actual middleware saturation/cancellation/exception/health regressions, request attribution/priority and public cache/ticker semantics. Frontend 22/23 pass; sole failure is an unchanged literal-copy assertion for the research archive introduction. Existing research page is unchanged. TypeScript passes. No dependencies, hosting capacities, schedules, providers or landing-page sections changed.

Initial release was pending at this checkpoint; see subsequent evidence below. No claim that this alone resolves overall capacity or completes FMP migration.


## First deployment and recurrence, October 14 UTC

PR624 merged as c54cfdf9 and backend workflow38518154960 succeeded. Four changed runtime hashes and seven operational hashes match. The isolated admission regression runs on production Python3.12.15 with no production writes. Both hosts remain API shared2/2GB and database performance1/2GB. The browser renders Insights with news, market data and research.

At05:45:49UTC Insights0.438s, feed0.263s, news0.247s, research cards0.505s, AAPL0.463s and NVDA0.568s. This recovery was temporary. At05:48:46 the tickers timed out after20s; at05:58 feed/news/research/tickers also timed out after20s. Insights stayed open in4.831s with a truthful unavailable research section. Public no-cache and direct-origin reads also timed out, so an abandoned response-cache key alone is not proved as the production cause.

At05:49 the portfolio process consumed0.44-0.80 CPU seconds per5seconds, API0.13-0.41. The running portfolio job held the serialized data lane;15-17 jobs were pending. It hit its ordinary1200-second limit at06:05:11 with exit-15, then the next job started after2289 seconds waiting. The reported0.002 child CPU on this terminated shell is not an accurate measure of all descendants. A stack-only diagnostic (no locals) at06:04 found the API main/worker threads idle at that instant; it does not establish the state throughout each timeout. Py-spy0.4.2 was installed only under /tmp/walnut-stack-probe for this diagnostic, not application dependencies.

Correction to the initial log interpretation: db_pool_checkout_slow records elapsed time from request start, not an isolated pool-wait duration. Database snapshots show no blocking sessions; earlier quoted values must not be treated as proven database-pool contention.

## Follow-up repairs

PR625, sourcecd431bd9, merge2561bf9d: a portfolio batch already checks the schema once, but previously repeated it per member. Skip only those redundant checks against the same engine. Standalone calls retain initialization.133 portfolio/strategy tests pass, including exact two-member event/price fixture parity and initialization against an empty database. One guarded production read-only measurement confirms134 catalog SELECTs per redundant check,0.0434CPU/1.837wall seconds. Do not extrapolate that sample as measured whole-job savings.

PR626, source74f718ff, merge8efc1fb8: a streamed response error or cancellation after call_next left the cache in-flight event unset. The new regression reproduces an abandoned ticker key and waiter on the previous implementation. A finally block now releases ownership and wakes waiters after stream consumption/storage; partial bodies are never cached.48 affected cache/admission/priority/feed-epoch checks pass. The first broad invocation used the wrong working directory and produced three fixture-path errors; the corrected backend-directory run passes all48.

Both PRs pass Vercel and secret checks. Existing backend Pillow33-advisory and frontend five-production-group audits remain failing with unchanged dependencies; logs inspected through the required CI tool. PR attachment attempts hit the app's100-attachment limit. Combined deployment38520205907 is in progress as of06:07UTC. Sustained performance, queue recovery and scheduled freshness remain unresolved. Further provider cutovers remain on hold.

Additional measured load: published_cards retrieves43 full stored draft payloads totaling4,589,166 bytes to produce a27KB public card list. No repair to that query/cache has been made yet.


## Follow-up release and latest recovery receipt

Backend workflow38520205907 completed successfully on8efc1fb82de266179968a7187c6b37430198459d. Changed middleware/portfolio source hashes match; operational verification also matches the seven expected runtime files. Exact failure/cancellation regression and admission checks pass in isolated productionPython3.12.15 with no production database access. Both host configurations remain unchanged.

At06:07:51UTC feeds return0.457/0.372s; newly generated AAPL/NVDA contexts return2.228/1.668s. At06:11:28UTC, after ordinary jobs have run, Insights renders in0.565s with research available and no numeric error digest; feed0.409s/10items, news0.246s/12items, research0.433s/43cards, freshAAPL2.404s and NVDA2.317s. The browser also verifies the full feed-to-Insights navigation and rendered headlines/research. The probe's legacy release boolean checks only the earlier c54cfdf9 frontend string; false on this newer release is not a failed backend hash check.

At06:11:39UTC the queue has zero pending/running jobs and no database blockers are visible. This is short-window recovery, not a full-day capacity or data completeness guarantee. Ordinary research operational refresh, SEC preparation and delivery jobs execute; some reference, SEC research and direct publication jobs still return exit1. Their cause/coverage and the portfolio job's1200s timeout remain open. Research-card full-payload loading remains an optimization opportunity, not an applied repair. No new capacity, schedule, provider, subscription, landing page, video or email change.

Evidence under artifacts/direct-feeds/free-replacements-2026-10-09/: insights-release-1791958067.json, cache-release-1791958122.json, two-service-verify-1791958083.json, insights-incident-1791958288.json and incident-load-1791958299.json. All new local processes completed; no local PostgreSQL started. Managed branch codex/public-cache-cancellation retains a generated TypeScript build-info change separately from the committed repair.


## October 14, 14:58 UTC: Throttling recurs after the overnight recovery

Production remains `dd89afda`; no restart or deployment was performed in this observation. API burst balance reaches its floor at 13:49 UTC and remains exhausted at 14:58. Five-minute throttle is 1.1803 in the existing normalized metric, while database CPU remains mostly idle. This contradicts any claim of sustained recovery from the earlier fast samples.

Public results: Feed 4.969 seconds, AAPL context 18.831, NVDA 19.512. Insights returns HTTP 200 in 4.893 seconds but contains the research-unavailable fallback; the separate research API returns 44 cards in 2.864 seconds. News remains 0.517 seconds. A passing readiness response is not acceptable page performance. Read-only database activity shows no blockers; the scheduled data queue is backlogged.

One bounded read-only cProfile run on the current cached institutional reference plan finishes at 15:01:43 UTC. Imports cost 2.7101 CPU seconds; planning costs 0.1410; total 3.5273 CPU seconds and 30.556 wall seconds. The plan has 950 scopes, 514 pending, zero prepared and 436 deferred, and selects two due keys. No provider calls, production writes, new plan scan or schedule change. cProfile adds overhead, so this is attribution evidence, not a controlled savings benchmark. Application model registration is prominent in the profile. Recurring startup overhead is the next optimization target; preserve per-job isolation, database connection/fork safety, timeouts, recovery identities, source coverage and delivery timing in any implementation.

Ordinary receipts independently show repeated CPU costs for reference (3.5733), SEC earnings (7.8253), news (3.3666), financial research (2.2987), metadata (2.6393) and watchlist price alerts (5.4356). Individual samples do not establish their hourly aggregate or justify delaying alerts. The diagnostic import/planner result cannot be subtracted directly from a different ordinary run.

Webull PR630 remains unmerged while performance is addressed. No source cutover or hosting change. Evidence under primary artifacts/direct-feeds/free-replacements-2026-10-09/: cpu-quota-1791989915.json, insights-incident-1791989939.json, scheduled-job-cpu-1791989952.json and incident-load-1791989964.json. Profile: primary artifacts/reference-plan-profile-1791990103.json. All diagnostic handles are terminal; the first queue SSH observation timed out and its process absence was established before retry.
