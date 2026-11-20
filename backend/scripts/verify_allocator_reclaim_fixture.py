"""Isolated GNU/Linux synthetic retention fixture, never production evidence.

Run in a separate process with >=768 MiB. The qualification clock is simulated;
allocator calls, RSS measurements, live values and concurrent reads are real.
"""
import gc
import hashlib
import json
import threading
import time

from app.services.api_memory_inventory import process_summary
from app.services.api_memory_reclaim import ReclaimTrial
from app.services.response_memory_cache import ResponseMemoryCache


def main():
    baseline = process_summary()
    blocks = [(str(i) + 'x' * 8192).encode() for i in range(66000)]
    pins = blocks[::128] + blocks[-1:]
    hashes = [hashlib.sha256(value).hexdigest() for value in pins]
    del blocks
    gc.collect()
    cache = ResponseMemoryCache(max_bytes=256*1024, clock=lambda: 0)
    cache['fixture'] = (100, {'values': list(range(1000)), 'source': 'synthetic'})
    encoded = json.dumps(cache['fixture'], sort_keys=True)
    expected = hashlib.sha256(encoded.encode()).hexdigest()
    stop, entered = threading.Event(), threading.Event()
    reads, latencies, failures = [0], [], []

    def reader():
        try:
            while not stop.is_set():
                start = time.monotonic()
                body = json.dumps(cache['fixture'], sort_keys=True)
                assert hashlib.sha256(body.encode()).hexdigest() == expected
                elapsed = (time.monotonic() - start)*1000
                if len(latencies) < 10000:
                    latencies.append(elapsed)
                reads[0] += 1
                entered.set()
        except BaseException as exc:
            failures.append(type(exc).__name__)

    worker = threading.Thread(target=reader)
    worker.start()
    try:
        assert entered.wait(2)
        before = process_summary()
        now = [0]
        trial = ReclaimTrial(clock=lambda: now[0])
        assert trial.observe(before) is None
        now[0] = 300
        started = time.monotonic()
        result = trial.observe(process_summary())
        elapsed_ms = (time.monotonic() - started)*1000
        assert result and result['state'] == 'attempted' and result['released_pages']
        assert trial.observe(process_summary()) is None
        after = process_summary()
    finally:
        stop.set()
        worker.join(5)
    assert not worker.is_alive() and not failures and reads[0] > 0
    assert hashes == [hashlib.sha256(value).hexdigest() for value in pins]
    assert json.dumps(cache['fixture'], sort_keys=True) == encoded
    rss_reduction = before['process_status']['VmRSS'] - after['process_status']['VmRSS']
    assert rss_reduction >= 64 * 1024, 'Synthetic free-page release not demonstrated'
    print(json.dumps(dict(scope='synthetic_separate_process', qualification_clock='simulated',
                          baseline=baseline, before=before, after=after, result=result,
                          actual_trial_ms=round(elapsed_ms, 3), rss_reduction_kib=rss_reduction,
                          live_pin_count=len(pins), live_hashes_preserved=True,
                          concurrent_read_count=reads[0], response_hash=expected,
                          max_sampled_read_ms=round(max(latencies), 3),
                          production_calls=0, provider_calls=0)))


if __name__ == '__main__':
    main()
