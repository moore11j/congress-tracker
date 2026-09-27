import type { ResearchMemoryClaim } from "./api";
import type { InsightsQuoteItem } from "./types";

const assets = [
  { symbol: "ETHUSD", name: "Ethereum", group: "crypto", aliases: /\b(?:eth(?:[\s/-]?usd)?|ethereum|etheruem|ether)\b/i },
  { symbol: "BTCUSD", name: "Bitcoin", group: "crypto", aliases: /\b(?:btc(?:[\s/-]?usd)?|bitcoin)\b/i },
  { symbol: "SOLUSD", name: "Solana", group: "crypto", aliases: /\b(?:sol(?:[\s/-]?usd)?|solana)\b/i },
  { symbol: "XRPUSD", name: "XRP", group: "crypto", aliases: /\b(?:xrp(?:[\s/-]?usd)?|ripple)\b/i },
  { symbol: "BNBUSD", name: "BNB", group: "crypto", aliases: /\b(?:bnb(?:[\s/-]?usd)?|binance coin)\b/i },
  { symbol: "GCUSD", name: "Gold", group: "commodities", aliases: /\b(?:gcusd|xau(?:[\s/-]?usd)?|gold)\b/i },
  { symbol: "SILUSD", name: "Silver", group: "commodities", aliases: /\b(?:silusd|xag(?:[\s/-]?usd)?|silver)\b/i },
  { symbol: "HGUSD", name: "Copper", group: "commodities", aliases: /\b(?:hgusd|copper)\b/i },
] as const;

export type ResearchMarketAsset = { symbol: string; name: string; group: "crypto" | "commodities"; expectations: string[] };

export function researchMarketAssets(claims: ResearchMemoryClaim[]): ResearchMarketAsset[] {
  return assets.flatMap((asset) => {
    const matches = claims.filter((claim) => asset.aliases.test(`${claim.subject} ${claim.metric ?? ""}`.replaceAll("_", " ")));
    if (!matches.length) return [];
    const expectations = [...new Set(matches.map((claim) => [claim.expected_direction, claim.expected_magnitude, claim.expected_timeframe].filter(Boolean).join(" · ")).filter(Boolean))];
    return [{ symbol: asset.symbol, name: asset.name, group: asset.group, expectations }];
  });
}

export function researchQuoteStatus(quote: InsightsQuoteItem | undefined, now = Date.now()): "available" | "stale" | "unavailable" {
  if (!quote || quote.status !== "ok" || typeof quote.price !== "number" || !Number.isFinite(quote.price) || quote.price <= 0) return "unavailable";
  const timestamp = Date.parse(quote.as_of ?? "");
  if (!Number.isFinite(timestamp) || timestamp > now) return "unavailable";
  return now - timestamp > 24 * 60 * 60 * 1000 ? "stale" : "available";
}
