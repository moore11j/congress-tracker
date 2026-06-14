export function normalizeTickerSymbol(symbol?: string | null): string | null {
  if (!symbol) return null;
  let cleaned = symbol.trim();
  if (cleaned.includes(":")) cleaned = cleaned.split(":").slice(1).join(":").trim();
  cleaned = cleaned.replace(/\s+/g, "").toUpperCase();
  if (!cleaned || cleaned === "---") return null;
  if (["[SYMBOL]", "SYMBOL", "UNKNOWN", "NULL", "NONE"].includes(cleaned)) return null;
  if (cleaned.includes("[") || cleaned.includes("]")) return null;
  return cleaned;
}

export function tickerHref(symbol?: string | null): string | null {
  const normalized = normalizeTickerSymbol(symbol);
  if (!normalized) return null;
  return `/ticker/${encodeURIComponent(normalized)}`;
}
