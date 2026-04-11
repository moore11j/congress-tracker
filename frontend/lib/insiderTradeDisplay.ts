export type InsiderTradeDisplayInput = {
  amount_range_min?: number | string | null;
  amount_range_max?: number | string | null;
  amount_min?: number | string | null;
  amount_max?: number | string | null;
  estimated_price?: number | string | null;
  payload?: any;
  insider?: {
    price?: number | string | null;
    display_price?: number | string | null;
    reported_price?: number | string | null;
    reported_price_currency?: string | null;
  } | null;
};

export function parseInsiderNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,% ,]/g, "").trim();
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

export function resolveInsiderDisplayPrice(item: InsiderTradeDisplayInput): number | null {
  return (
    parseInsiderNumber(item.insider?.display_price) ??
    parseInsiderNumber(item.payload?.display_price) ??
    parseInsiderNumber(item.payload?.displayPrice) ??
    parseInsiderNumber(item.estimated_price) ??
    parseInsiderNumber(item.insider?.price) ??
    parseInsiderNumber(item.payload?.price) ??
    parseInsiderNumber(item.payload?.raw?.price) ??
    parseInsiderNumber(item.payload?.raw?.transactionPrice) ??
    parseInsiderNumber(item.payload?.raw?.transactionPricePerShare) ??
    null
  );
}

export function resolveInsiderShares(item: InsiderTradeDisplayInput): number | null {
  const shares =
    parseInsiderNumber(item.payload?.shares) ??
    parseInsiderNumber(item.payload?.raw?.securitiesTransacted) ??
    parseInsiderNumber(item.payload?.raw?.transactionShares) ??
    null;
  return shares !== null && shares > 0 ? shares : null;
}

export function resolveInsiderDisplayValue(item: InsiderTradeDisplayInput): number | null {
  const explicit =
    parseInsiderNumber(item.payload?.display_trade_value) ??
    parseInsiderNumber(item.payload?.displayTradeValue) ??
    parseInsiderNumber(item.amount_range_min) ??
    parseInsiderNumber(item.amount_min) ??
    parseInsiderNumber(item.amount_range_max) ??
    parseInsiderNumber(item.amount_max);
  if (explicit !== null) return explicit;

  const shares = resolveInsiderShares(item);
  const price = resolveInsiderDisplayPrice(item);
  return shares !== null && price !== null && price > 0 ? shares * price : null;
}

export function resolveInsiderReportedPrice(item: InsiderTradeDisplayInput): {
  price: number | null;
  currency: string | null;
} {
  return {
    price:
      parseInsiderNumber(item.insider?.reported_price) ??
      parseInsiderNumber(item.payload?.reported_price) ??
      parseInsiderNumber(item.payload?.reportedPrice) ??
      parseInsiderNumber(item.payload?.price) ??
      parseInsiderNumber(item.payload?.raw?.price) ??
      null,
    currency:
      text(item.insider?.reported_price_currency) ??
      text(item.payload?.reported_price_currency) ??
      text(item.payload?.reportedPriceCurrency) ??
      null,
  };
}

export function formatReportedInsiderPrice(price: number | null, currency?: string | null): string | null {
  if (price === null) return null;
  const label = currency?.trim().toUpperCase() || "USD";
  return `Reported: ${label} ${price.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}
