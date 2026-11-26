import type { TickerChartQuote } from "./api";

export function chartQuoteLabels(quote?: TickerChartQuote | null) {
  const extended = quote?.market_session === "after_hours" ? "After-hours"
    : quote?.market_session === "pre_market" ? "Pre-market" : null;
  return {
    price: quote?.price_kind === "daily_close" ? "Daily Close"
      : extended ? `${extended} Price${quote?.is_delayed ? " (delayed)" : ""}`
      : quote?.is_delayed ? "Delayed Price" : "Current Price",
    extended,
    extendedComparison: quote?.market_session === "after_hours"
      ? "After-hours return compares this price with that session's regular close."
      : quote?.market_session === "pre_market"
      ? "Pre-market return compares this price with the previous regular-session close."
      : null,
    comparison: quote?.regular_session_date && quote?.previous_close_date
      ? `Day return: ${quote.regular_session_date} regular session versus the ${quote.previous_close_date} regular-session close.`
      : null,
  };
}
