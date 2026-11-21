export type MarketQuote = {
  symbol: string;
  current_price?: number | null;
  day_change_pct?: number | null;
  as_of?: string | null;
  is_delayed?: boolean | null;
  delay_minutes?: number | null;
  price_kind?: string | null;
  is_stale?: boolean | null;
};

export type DailyHeaderPrice = {
  price?: number | null;
  date?: string | null;
  changePercent?: number | null;
  relativeVolume?: number | null;
};

function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

export function formatPriceSession(date: string | null | undefined): string | null {
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  const parsed = new Date(`${date}T12:00:00Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== date) return null;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(parsed);
}

export function tickerHeaderPrice(symbol: string, quote: MarketQuote | null, daily: DailyHeaderPrice, failed = false, now = Date.now()) {
  const stamp = quote?.as_of;
  const dated = typeof stamp === "string" && /T.*(?:Z|[+-]\d{2}:\d{2})$/.test(stamp)
    && formatPriceSession(stamp.slice(0, 10)) !== null && Number.isFinite(Date.parse(stamp))
    && Date.parse(stamp) <= now + 60_000;
  const dailyDate = formatPriceSession(daily.date);
  const quoteDate = dated ? new Intl.DateTimeFormat("en-CA", {
    year: "numeric", month: "2-digit", day: "2-digit", timeZone: "America/New_York",
  }).format(new Date(stamp!)) : null;
  const dailyQuote = quote?.price_kind === "daily_close";
  // Do not let an older retained quote replace a later completed daily session.
  const quoteSession = dailyQuote && dated ? stamp!.slice(0, 10) : quoteDate;
  const usable = quote?.symbol === symbol && finite(quote.current_price) && quote.current_price > 0
    && dated && (!dailyDate || !quoteSession || quoteSession >= daily.date!);
  if (usable) {
    const stale = quote.is_stale === true || failed;
    const delay = quote.is_delayed === true
      ? finite(quote.delay_minutes) && quote.delay_minutes > 0 ? `Delayed ${quote.delay_minutes} minutes` : "Delayed quote"
      : null;
    const observed = dailyQuote ? `Daily close as of ${formatPriceSession(stamp!.slice(0, 10))}`
      : `As of ${new Intl.DateTimeFormat("en-US", {
        month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit",
        timeZone: "America/New_York", timeZoneName: "short",
      }).format(new Date(stamp!))}`;
    return {
      price: quote.current_price!, changePercent: stale || !finite(quote.day_change_pct) ? null : quote.day_change_pct,
      label: dailyQuote ? "Daily close" : stale ? "Last available quote" : delay ? "Delayed quote" : "Latest quote",
      observed, delay, stale, fallback: false,
    };
  }
  const price = finite(daily.price) && daily.price > 0 && dailyDate ? daily.price : null;
  return {
    price, changePercent: price !== null && finite(daily.changePercent) ? daily.changePercent : null,
    label: price === null ? "Price unavailable" : "Daily close",
    observed: price === null ? "Waiting for a dated quote" : `Daily close as of ${dailyDate}`,
    delay: null, stale: failed, fallback: true,
  };
}
