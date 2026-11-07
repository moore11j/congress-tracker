# Provider telemetry lock contention

The shared FMP budget guard persisted allowed-call and budget-denial telemetry while holding its process-wide bookkeeping lock. A database pool wait or slow commit could therefore block other requests from recording cache hits or reading the in-memory usage summary. A deterministic local reproduction holds a telemetry commit open and confirms that cached Feed bookkeeping waits behind it. This is a reproduced contention mechanism, not proof that it caused the earlier production QueuePool timeout.

The repair reserves the budget, updates counters and captures the exact event under the lock, then persists that event after releasing the lock. Both accepted attempts and budget refusals retain their durable telemetry, original context and exception behavior. Concurrent callers cannot exceed the quota or accidentally persist another caller's event. No pool size, schema, source selector, scheduler, data or hosting change is included.

Three new checks cover a slow allowed-call commit, a slow budget-refusal commit, and forty concurrent reservations with seven allowed attempts and thirty-three refusals. They verify cached Feed bookkeeping completes while the write is still waiting, and all forty distinct events remain correctly associated with their callers despite interleaved events. The focused selection passes 33 tests locally. Wider caller and Linux validation remain release gates.

The complete local caller, cache, price, watchlist, Outcomes and shutdown selection passes all 200 checks. No provider request or production mutation is performed by these tests.
