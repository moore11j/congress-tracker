import { hasResolvedTickerProfile, usablePublicTickerSnapshot } from "./tickerSeo";

// A public request may only read persisted data. It must never rebuild a ticker
// or call market-data providers just because a crawler visits a cold URL.
export async function publicTickerReady(apiBase: string, symbol: string, search: URLSearchParams, fetcher: typeof fetch = fetch): Promise<boolean> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  const lookback = search.get("lookback") ?? "365";
  const side = search.get("side") ?? "all";
  const query = new URLSearchParams({
    cached_only: "true", context_version: "12", limit: "3",
    lookback_days: ["1", "5", "30", "90", "180", "365"].includes(lookback) ? lookback : "365",
    side: ["buy", "sell", "all"].includes(side) ? side : "all",
  });
  const base = apiBase.replace(/\/$/, "");
  const read = async (path: string) => {
    const response = await fetcher(`${base}${path}`, {
      cache: "no-store", signal: controller.signal,
      headers: { "x-walnut-request-source": "ssr" },
    });
    if (!response.ok) throw new Error("Public ticker data unavailable");
    return response.json();
  };
  try {
    // Either source is enough; a slow context read cannot hide a saved snapshot.
    await Promise.any([
      read(`/api/seo-snapshots/ticker/${encodeURIComponent(symbol)}`).then(value => {
        if (!usablePublicTickerSnapshot(value.snapshot, symbol)) throw new Error("No public snapshot");
      }),
      read(`/api/tickers/${encodeURIComponent(symbol)}/context-bundle?${query}`).then(value => {
        if (!hasResolvedTickerProfile(value) || value.ticker.symbol !== symbol) throw new Error("No public context");
      }),
    ]);
    return true;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
    controller.abort();
  }
}

export function unavailableTickerResponse(): Response {
  return new Response('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Stock research temporarily unavailable | Walnut</title></head><body><main><h1>Stock research is temporarily unavailable</h1><p>Please try again shortly.</p><p><a href="/screener">Open Screener</a> · <a href="https://walnutmarkets.com/research">Read research briefs</a></p></main></body></html>', {
    status: 503,
    headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store", "Retry-After": "60" },
  });
}
