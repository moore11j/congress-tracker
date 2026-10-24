import type { Entitlements } from "@/lib/entitlements";

export const feedPageSizes = [25, 50, 100, 250, 500, 1000] as const;
export type FeedPageSize = (typeof feedPageSizes)[number];
export const feedSortOptions = [
  ["filed_after", "Date"], ["ticker", "Ticker"], ["entity", "Person / Entity"],
  ["source", "Source"], ["action", "Action"], ["amount", "Amount"],
  ["pnl", "% Delta"], ["signal", "Signal"],
] as const;
export type FeedSortKey = (typeof feedSortOptions)[number][0];

export function feedAccess(entitlements: Entitlements) {
  const tier = entitlements.effective_tier ?? entitlements.tier;
  return { canExpand: ["premium", "pro", "admin"].includes(tier), canSortExport: ["pro", "admin"].includes(tier) };
}

export function feedPageSize(value: number, canExpand = true): FeedPageSize {
  return canExpand && feedPageSizes.includes(value as FeedPageSize) ? value as FeedPageSize : 25;
}
