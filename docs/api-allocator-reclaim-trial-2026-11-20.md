# Bounded API allocator reclamation trial

At 2026-11-20 09:38 MST, the owner-approved institutional window cannot start: all11fresh readiness/database samples pass but available memory falls to805.96484375MiB, below the unchanged1000MiB admission guard. Live20file verification matches9253e258, the parser and source settings are unchanged, and the last job remains215successful/idle with12334of16662prepared and cursor59031. No start receipt, enqueue, retry or capacity request occurred. Approval is recorded; the health prerequisite failed.

The installed API diagnostic records PID625 at16:27:15UTC: RSS791.4453125MiB; allocator-reported free447.66685485839844MiB, allocated113.12220764160156MiB and mmap32.6328125MiB. Accounted response caches total857157bytes, with other inventories partial. These are partial allocator statistics, not a complete RSS census, allocation call-site diagnosis or a guarantee that all free bytes can be returned.

## Local evidence and limits

The earlier simple serialization fixture did not reproduce substantial retention. A new sparse-live-allocation fixture does: the smaller fixture holds roughly141MiB free in the allocator and releases about140MiB RSS while preserving all pinned values. The larger candidate fixture uses real GNU allocation/reclamation with the actual candidate policy, a simulated qualification clock, a bounded cache and concurrent response reads in a separate Python3.12Linux container. Network is disabled, filesystem read-only, memory768MiB and one CPU.

RSS falls539.6to26.0MiB, reducing resident memory513.6MiB. All517live-buffer hashes and383concurrent response checks remain unchanged. The actual measured trial takes77.275ms, and the maximum sampled synthetic read is29.712ms. Those timings are local synthetic evidence, not HTTP production acceptance or a native-call time ceiling. Allocator free-byte counters can remain large after pages are returned using madvise; RSS is measured separately.

GNU `malloc_trim` releases free pages and is documented as thread-safe, but native allocator locks can delay concurrent allocation. Python cannot forcibly impose a safe wall-time limit on this native call. Only the main arena honors the pad argument; it is not a global reserve. [Linux man-pages](https://man7.org/linux/man-pages/man3/malloc_trim.3.html).

## Candidate controls

The existing finite diagnostic can call a new separate trial module, off by default. It requires exact `API_MEMORY_RECLAIM_MODE=once` and an explicit numeric `API_MEMORY_RECLAIM_UNTIL` epoch expiry within one hour. Missing, invalid, infinite, past or excessively distant expiries fail closed. The absolute expiry survives a process restart; there is also a monotonic one-hour process deadline.

Two qualifying observations at least300seconds apart are required, each with API RSS>=512MiB and allocator-reported free space>=128MiB. A low or unavailable measurement resets qualification. The trial consumes its allowance before resolution/invocation and calls GNU malloc_trim with8MiB pad at most once per API process, including failures or a zero-release result. A process restart can reset that per-process allowance only while the original absolute lease remains valid; this is not an exactly-once cross-restart guarantee. No recurring policy, GC, cache eviction, heap dump, provider call, schedule or preparation-guard change is introduced.

Receipts contain only fixed numeric/status fields. Disabled mode does not enter the trial. Shutdown prevents a new call; concurrent callers do not wait or duplicate an attempt. Native latency remains a production trial risk and must be observed. This candidate is a bounded trial, not a proven permanent repair.

## Validation and next gate

Local focused checks:60passed,twoLinux-only skips. Dedicated LinuxCI and the real native fixture are prepared, results pending. Existing ticker/cache/context/chart/outcome and frontend regressions are included in the workflow. No candidate deployment or production lease is authorized by window10approval, which explicitly required9253e258and fresh health. Prepare the exact release and trial observation for separate review before any production allocator action. Preparation remains stopped until a fresh full health observation passes its existing threshold.

Evidence: artifacts/prewindow10-readiness-observation-nov19.json; artifacts/window10-preflight-state-1795191850.json (timestamped preflight); artifacts/native-memory-window10-next-sample-nov20.json; artifacts/allocator-retention-pinned-nov20.json; artifacts/allocator-reclaim-candidate-fixture-nov20.json. Production sources, canonical data and customer sends are unchanged.
