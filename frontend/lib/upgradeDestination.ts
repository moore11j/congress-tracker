import { safeAppReturnPath } from "./returnPaths";

const researchParameters = new Set([
  "tab", "view", "mode", "category", "period", "sort", "selected", "ticker", "timeframe",
  "window", "direction", "side", "status", "page", "limit", "filter", "institutional_lookback_days",
  "positions", "holdings_page", "history_page", "reported_page",
]);

/** Keep research state without copying authentication or campaign parameters. */
export function upgradeReturnPath(pathname: string, search = "", hash = "") {
  const params = new URLSearchParams(search);
  for (const key of [...params.keys()]) if (!researchParameters.has(key)) params.delete(key);
  const query = params.toString();
  return safeAppReturnPath(`${pathname}${query ? `?${query}` : ""}${hash}`);
}

export function upgradePricingHref(tier: "Premium" | "Pro", returnTo: string, compare = false) {
  const params = new URLSearchParams({ plan: tier.toLowerCase(), returnTo: safeAppReturnPath(returnTo) });
  return `/pricing?${params.toString()}${compare ? "#compare" : ""}`;
}
