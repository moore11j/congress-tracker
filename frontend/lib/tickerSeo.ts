import type { SeoEntitySnapshot } from "@/lib/api";
import type { TickerProfile } from "@/lib/types";

export function hasResolvedTickerProfile(profile: TickerProfile | null | undefined): boolean {
  return Boolean(profile?.ticker?.symbol)
    && !["loading", "unknown", "unresolved"].includes(String(profile?.ticker?.identity_status ?? "").toLowerCase());
}

// Only the existing public snapshot endpoint is used here. Never serialize an
// authenticated context bundle or infer values from empty activity arrays.
export function usablePublicTickerSnapshot(snapshot: SeoEntitySnapshot | null, symbol: string): boolean {
  return Boolean(snapshot?.indexable
    && snapshot.entity_type === "ticker"
    && snapshot.payload.symbol === symbol
    && typeof snapshot.payload.company_name === "string"
    && snapshot.payload.company_name.trim()
    && snapshot.data_as_of
    && Number.isFinite(Date.parse(snapshot.data_as_of))
    && snapshot.payload.sections?.some((section) => section.heading && section.body));
}

export function tickerSeoTitle(symbol: string, snapshotOnly = false): string {
  return snapshotOnly
    ? `${symbol} Public Stock Research Snapshot | Walnut`
    : `${symbol} Stock Analysis, Insider Trades & Congress Data | Walnut`;
}

export function tickerSeoDescription(symbol: string, companyName?: string | null, snapshotOnly = false): string {
  const name = companyName?.trim();
  const identity = name && name.toUpperCase() !== symbol.toUpperCase() ? `${name} (${symbol})` : symbol;
  const description = snapshotOnly
    ? `Review ${identity} with dated public market data and disclosure context. Current stock research is temporarily unavailable.`
    : `Analyze ${identity} with fundamentals, technicals, insider activity, Congress trades, analyst data and institutional holdings on Walnut.`;
  return description.length <= 165 || identity === symbol
    ? description
    : tickerSeoDescription(symbol, null, snapshotOnly);
}
