import type { CalculatorOptionChain, CalculatorOptionContract } from "./api";

/** Abortable wait: navigating away or changing the market cancels queued requests. */
export function waitForOptions(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(new DOMException("Cancelled", "AbortError")); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, ms);
    if (signal.aborted) abort(); else signal.addEventListener("abort", abort, { once: true });
  });
}

export async function loadOptionsData<T>(action: () => Promise<T>, signal: AbortSignal, onWait: () => void): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    signal.throwIfAborted();
    try { return await action(); } catch (error) {
      if (!(error instanceof Error) || !("status" in error) || error.status !== 429 || attempt >= 2) throw error;
      onWait();
      await waitForOptions(61000, signal);
    }
  }
}
/** Selected modeled entries first, then nearby strikes; never refetch completed contracts. */
export function optionPriceBatch(chain: CalculatorOptionChain, selectedTickers: string[], spot: number, wholeChain: boolean): CalculatorOptionContract[] {
  const selected = new Set(selectedTickers);
  return chain.contracts.filter(c => !c.close && !c.no_trade && (wholeChain || selected.has(c.ticker)))
    .sort((a, b) => Number(selected.has(b.ticker)) - Number(selected.has(a.ticker)) || Math.abs(a.strike - spot) - Math.abs(b.strike - spot))
    .slice(0, chain.price_provider === "alpaca" ? 100 : 1);
}
