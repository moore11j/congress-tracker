export function normalizeTickerSymbol(symbol?: string | null): string | null {
  if (!symbol) return null;
  const cleaned = symbol.trim().toUpperCase();
  if (!cleaned || cleaned === "—") return null;
  return cleaned;
}

export function tickerHref(symbol?: string | null): string | null {
  const normalized = normalizeTickerSymbol(symbol);
  if (!normalized) return null;
  return `/ticker/${encodeURIComponent(normalized)}`;
}
