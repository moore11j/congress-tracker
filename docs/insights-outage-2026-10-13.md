# Insights outage, October 13, 2026

Owner reports slow app navigation and Insights server exception, screenshot digest 1486645905. Incident probe October 14 05:33 UTC (October 13 local) reproduces a 30.49-second streamed HTTP 200 carrying an RSC error digest 1582193453. Readiness and a ten-row feed time out at 45.27 seconds. HTTP status alone is not page success.

Production remains db1caf31. LF-normalized hashes of main.py, events.py and request_guards.py match deployed source. Fly health check is critical. API CPU credits are almost depleted, two-hour CPU mean 0.1457 versus 0.125 shared allowance; throttle metric is nonzero. Database has no throttling. Source artifact role-cpu-1791956015.json. A 30-second process sample identifies API process 663 as the largest surviving CPU consumer. Recent logs show simultaneous anonymous SSR insider summaries/trades/alpha requests and pool waits up to 5.6 seconds. This establishes a request burst during overload, not the identity of the external caller or a complete CPU attribution.

## Repair

- Optional Insights research preview streams independently and catches an archive fetch failure with a truthful local unavailable message. Published archive behavior still propagates failure instead of presenting an incomplete static-only list. The real public-card fetch aborts after 4.5 seconds.
- Anonymous SSR secondary profile requests share two nonwaiting slots after public cache lookup. Excess gets retryable, noncacheable 503, preserving room for interactive/health work. Signed-in and browser requests retain their existing paths; no records or fields are removed.
- Heavy admission waits cooperatively rather than blocking the event loop on a threading semaphore. Cancellation cannot leave an executor waiter that later consumes a permit.

46 backend checks pass on Python 3.14.2: actual middleware saturation/cancellation/exception/health regressions, request attribution/priority and public cache/ticker semantics. Frontend 22/23 pass; sole failure is an unchanged literal-copy assertion for the research archive introduction. Existing research page is unchanged. TypeScript passes. No dependencies, hosting capacities, schedules, providers or landing-page sections changed.

Release/live recovery and sustained-load verification pending. No claim that this alone resolves overall capacity or completes FMP migration.
