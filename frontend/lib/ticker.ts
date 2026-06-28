const invalidTickerSymbols = new Set(["-", "--", "---", "N/A", "NA", "[SYMBOL]", "SYMBOL", "UNKNOWN", "NULL", "NONE"]);
const validTickerSymbolPattern = /^[A-Z^][A-Z0-9./-]{0,14}$/;

export function normalizeTickerSymbol(symbol?: string | null): string | null {
  if (!symbol) return null;
  let cleaned = symbol.trim();
  if (cleaned.includes(":")) cleaned = cleaned.split(":").slice(1).join(":").trim();
  if (/\s/.test(cleaned)) return null;
  cleaned = cleaned.toUpperCase();
  while (cleaned.startsWith("$")) cleaned = cleaned.slice(1).trim();
  if (!cleaned || invalidTickerSymbols.has(cleaned)) return null;
  if (cleaned.includes("[") || cleaned.includes("]")) return null;
  return cleaned;
}

export function tickerRouteSymbol(symbol?: string | null): string | null {
  const normalized = normalizeTickerSymbol(symbol);
  if (!normalized || !validTickerSymbolPattern.test(normalized)) return null;
  const classSeparatorCount = (normalized.match(/[./]/g) ?? []).length;
  if (classSeparatorCount > 1 || /[./-]$/.test(normalized) || /[./-]{2,}/.test(normalized)) return null;
  return normalized.replace(/[./]/g, "-");
}

export function tickerHref(symbol?: string | null): string | null {
  const routeSymbol = tickerRouteSymbol(symbol);
  if (!routeSymbol) return null;
  return `/ticker/${encodeURIComponent(routeSymbol)}`;
}
