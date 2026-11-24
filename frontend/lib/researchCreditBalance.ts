export type CreditBalance = { monthly: number; purchased: number; available: number; reserved: number; billing_hold: boolean; sandbox: true };
export type CreditSnapshot = { balance: CreditBalance | null; status: string; pending: boolean };
export const emptyCreditSnapshot: CreditSnapshot = { balance: null, status: "Balance not connected", pending: false };

export function validCreditBalance(data: CreditBalance): boolean {
  return data?.sandbox === true && typeof data.billing_hold === "boolean"
    && [data.monthly, data.purchased, data.available, data.reserved].every(n => Number.isSafeInteger(n) && n >= 0)
    && data.available === data.monthly + data.purchased;
}

/** One request loop shared by the account badge, quote and billing panel. */
export function createCreditBalanceStore(runtime: {
  fetch: typeof fetch; visible: () => boolean;
  schedule: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  cancel: (timer: ReturnType<typeof setTimeout>) => void;
}) {
  let snapshot = emptyCreditSnapshot;
  const listeners = new Set<() => void>();
  let abort: AbortController | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let generation = 0;
  const publish = (next: CreditSnapshot) => { snapshot = next; listeners.forEach(listener => listener()); };
  const stop = () => { generation++; abort?.abort(); abort = null; if (timer !== undefined) runtime.cancel(timer); timer = undefined; };
  async function refresh(clear = false) {
    stop();
    if (clear) publish(emptyCreditSnapshot);
    if (!listeners.size || !runtime.visible()) return;
    const version = generation;
    abort = new AbortController();
    let retry = 2000;
    try {
      const response = await runtime.fetch("/api/ask/credits/balance", { signal: abort.signal, cache: "no-store" });
      if (version !== generation) return;
      if (!response.ok) {
        retry = 30000;
        publish({ balance: null, pending: false, status: response.status === 401 || response.status === 403
          ? "Sign in with a verified sandbox account to view credits."
          : "Billing sandbox is not connected yet. Purchases are unavailable." });
      } else {
        const data = await response.json();
        if (version !== generation) return;
        if (!validCreditBalance(data)) throw new Error("Invalid balance");
        publish({ balance: data, pending: false, status: data.billing_hold
          ? "Billing review required. New purchases are paused." : "Sandbox balance. These credits are for testing only." });
      }
    } catch {
      if (version !== generation) return;
      retry = 10000;
      publish({ balance: null, pending: false, status: "Balance unavailable. Reconnecting…" });
    } finally {
      if (version === generation && listeners.size) timer = runtime.schedule(() => void refresh(), retry);
    }
  }
  return {
    getSnapshot: () => snapshot,
    subscribe(listener: () => void) {
      listeners.add(listener);
      if (listeners.size === 1) void refresh();
      return () => { listeners.delete(listener); if (!listeners.size) { stop(); snapshot = emptyCreditSnapshot; } };
    },
    refresh,
  };
}
