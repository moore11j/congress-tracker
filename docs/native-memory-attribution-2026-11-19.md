# Native allocator attribution for the remaining memory hold

PR805 is deployed as280037c5. Its15installed price-file checks and20institutional source/parser checks pass. Institutional preparation remains12,334of16,662originals,cursor59031,run215successful/idle, with no pending request. Neither the expired window nor the newly prepared window10 has been restarted.

Two independent five-minute observations have11healthy readiness samples each, but fail the unchanged1,000MiB preparation-admission threshold. The first reaches986.4MiB, the second796.3MiB. A concurrent worker exits between measurements, restoring headroom, but API RSS also grows from296.8to623.0MiB across the observation period. A momentary recovery is not sustained capacity. The finite Python-cache inventory is partial and does not establish exact allocation ownership.

This candidate adds allocator-reported aggregate counters to the existing finite API diagnostic. GNU's `mallinfo2` reports allocator arena, allocated/free and mapped-block counters using ten `size_t` fields. It can help distinguish allocator-retained free space from allocated space; it is not a complete heap census or an RSS total. [GNU libc statistics documentation](https://www.sourceware.org/glibc/manual/2.35/html_node/Statistics-of-Malloc.html).

The diagnostic resolves `mallinfo2` only on Linux with the GNU libc symbol present. Unsupported systems report unsupported; failures omit exception text. It logs fixed integer counters and elapsed time, with no pointers, allocation contents, customer values or heap dumps. It does not collect garbage, trim memory, change allocator settings, evict caches or alter scheduling. The existing60-second initial delay, five-minute interval and24-hour maximum remain.

Native allocator locking has no Python-enforceable time bound. Its duration is reported separately; the existing cache-graph traversal budget must not be described as a hard wall-time ceiling. These counters will not identify a specific Python call site or establish a repair on their own. The next decision depends on installed API-process observations, not a counter read from a separate helper process.

Tests cover64-bit values above4GiB, ABI structure width, private error/object omission, unsupported platforms and real GNU allocator invocation in Linux CI with a retained buffer. Existing cache privacy/budget/finite-observer tests remain. No memory guard is relaxed and no preparation request is added by this candidate.

Release verification will check20installed files with only the diagnostic hash changed, preserved parser/schema/source settings, actual allocator API availability, public readiness and a filtered finite API log capture. A new preparation window and price/source activation remain separate gates.

Evidence: `artifacts/postprice-runtime-observation-nov19.json`, `postprice-recovery-observation-nov19.json`, `postprice-diagnostics-1795150360.json`, `institutional-preparation-observed-1795150102.json`. No claim that the memory cause is already proven or fixed.
