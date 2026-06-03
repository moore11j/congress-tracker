"use client";

const MAX_HEAVY_TICKER_REQUESTS = 2;
let activeHeavyTickerRequests = 0;
const heavyTickerQueue: Array<() => void> = [];

function pumpHeavyTickerQueue() {
  while (activeHeavyTickerRequests < MAX_HEAVY_TICKER_REQUESTS) {
    const next = heavyTickerQueue.shift();
    if (!next) return;
    next();
  }
}

export function runHeavyTickerRequest<T>(task: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let started = false;

    const run = () => {
      if (signal?.aborted) {
        reject(new DOMException("Request aborted", "AbortError"));
        return;
      }
      started = true;
      activeHeavyTickerRequests += 1;
      task()
        .then(resolve)
        .catch(reject)
        .finally(() => {
          activeHeavyTickerRequests = Math.max(activeHeavyTickerRequests - 1, 0);
          pumpHeavyTickerQueue();
        });
    };

    const onAbort = () => {
      if (started) return;
      const index = heavyTickerQueue.indexOf(run);
      if (index >= 0) heavyTickerQueue.splice(index, 1);
      reject(new DOMException("Request aborted", "AbortError"));
    };

    signal?.addEventListener("abort", onAbort, { once: true });
    heavyTickerQueue.push(run);
    pumpHeavyTickerQueue();
  });
}
