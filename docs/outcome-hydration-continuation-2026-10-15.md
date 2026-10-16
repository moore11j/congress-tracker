# Outcomes hydration continuation

Prepared October 15 local time / October 16 UTC, 2026. Implementation is local pending review and guarded release. FMP remains enabled.

## Confirmed defect and production observation

The former hydrator refreshed prices for the entire selected population, calculated score bundles for every symbol even after the time budget, and checked the exhausted budget before saving the first snapshot. The regression reproduces zero saved snapshots with an `ok` result after a slow refresh. This is a verified code defect; the available production log sample contained no completion result, so no particular live run is claimed to have lost captures.

Production observation at October15 23:48 recorded the actual outcome-ledger-hydrator process at599seconds/48.22CPU seconds with19queued jobs. At23:55 the data lane had advanced to profile prewarming; at October16 00:05 the separate outcome-ledger-price-hydrator was actually live at207seconds/9.28CPU seconds with17queued jobs. Those are distinct jobs. The price hydrator is unchanged by this repair. The protected portfolio cycle progressed510 to518 of1135scopes, zero recorded failures, then awaited the data lane. Its last observed continuing state still defers deployment. No queue manipulation, kill, new trigger or deployment retry was performed.

## Change

The existing Outcomes schedule keeps its complete configured symbol plan and priority order. A scheduler-owned checkpoint freezes the plan, settings, market date and scoring version. Each process handles at most eight symbols, checking an eight-second wall/two-second CPU budget between price refreshes. It finishes capture for that bounded refreshed group before yielding. Individual provider calls and that group's score/capture work can exceed the soft budget; this is not a hard eight-second latency promise.

Remaining symbols resume through the existing serialized data lane. Older waiting SEC/news work can run first. Later schedule triggers remain pending for the next logical cycle; intermediate batches do not increment the completed-run counter. Wrong ownership, altered plan, changed scoring version, expired24hour deadline and market-date rollover cannot silently succeed. Errors and stale prices remain explicit. Unscheduled manual runs report partial/nonzero when unfinished, because they have no durable scheduler continuation.

Snapshot capture, score calculation, identity/deduplication and timestamp functions are unchanged. A returned existing snapshot retains the prior meaning of the captured counter; it is not a count of new inserts. No user email is sent by this job. Existing Free ten-row and paid pagination rules remain unchanged.

Only the exact Outcomes command is added to the checkpoint/isolated-preload allowlists. Shared import-template fingerprints ignore its transient continuation flag while still refusing changed configuration. Parent templates have no live DB connections; child pools and process identities remain isolated. The planner imports the broader ingest graph once per logical cycle, while continuing children use the narrow compute facade. Release reservation now also protects unfinished Outcomes cycles; it does not bypass the existing portfolio guard. Active interrupted work remains a visible failure, never blindly replayed.

## Verification

- The new slow-price regression fails before repair: zero captures. It passes afterward.
- Final focused LinuxPython3.12.3 run:207passed/one skipped. The skip is the existing native Supercronic supervision test because Supercronic is absent locally. Existing FastAPI/Starlette deprecation warnings remain. This is not a full-suite claim.
- All1000planned symbols finish across125bounded groups with no replanning or repeat capture calls. Durable real SQLite queue tests prove fairness, retained subsequent triggers, cycle ownership, release deferral and invalid continuation rejection.
- Actual isolated SQLite capture comparison preserves all nine snapshot projections against the original sequence, excluding only DB creation timestamps. A new full logical repeat leaves the exact same snapshots. Zero Event/EmailDelivery rows and zero provider network calls. This is fixture-backed database parity, not a current production coverage audit.
- Existing ledger, outcome integrity, Premium pagination, portfolio continuation/release, scheduler and fork isolation tests pass in the207-check combined run.
- Isolated actual import-template probe succeeds with zero checked-in/out parent connections. Cold current entry import costs2.971-3.305CPU seconds versus0.015-0.017CPU seconds for isolated preloaded disabled-job children; one-time parent template3.706CPU seconds. This measures startup only, not production hydration cost or sustained host capacity. Receipt artifacts/outcome-import-probe-1792109104.json. Initial helper lacked the backend module search path and failed before running a job; corrected probe passed.
- Unrelated frontend/tsconfig.tsbuildinfo remains unchanged at SHA256A185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA. No frontend, crontab, host-size, provider selector or subscription change.

## Outstanding acceptance

Review, exact guarded release, installed hashes/isolated replay and a complete ordinary production hydration cycle remain unverified. Continue the existing SEC collector/publisher observations; the last collector check still had the normal23:47trigger waiting. PR694's Allen repair remains merged but not deployed. Financial/institutional/transcript coverage, all alert/ranking consumers, sustained capacity, final Massive/paid-provider validation, zero-FMP traffic and cancellation remain open. Pending private-export approvals are untouched.
