/**
 * Canonical public-homepage positioning. The marketing route is rendered on
 * the server and hydrated in the browser, so its shared copy must originate
 * here rather than from a client-side replacement or crawler fallback.
 */
export const homepageContent = {
  hero: {
    eyebrow: "Stock Analysis · Alternative Data · Market Intelligence",
    title: "Find Top-Ranked Stocks. See Who Actually Outperformed.",
    description:
      "See which stocks rank highest now, how Congress members, insiders and institutions performed historically, and which backtested strategies beat their benchmarks. Then see the data behind every result.",
  },
  metadata: {
    title: "Stock Analysis, Congress Trades & Insider Data | Walnut Markets",
    description:
      "Research ranked stocks with fundamentals, technicals, Congress trades, insider data, institutional holdings, contracts and analysts. Compare backtested strategies.",
    socialDescription:
      "Find top-ranked stocks, compare historical performers and backtested strategies, then review fundamentals, technicals, Congress trades, insiders and institutions.",
  },
  differentiation: {
    description:
      "Most platforms give you one slice of the market. Walnut connects the data, ranks the opportunities, tracks the participants and strategies, and measures the outcome afterward—so every conclusion remains inspectable.",
  },
  confirmationScore: {
    description:
      "Walnut ranks stocks with a proprietary Confirmation Score built from fundamentals, technical analysis, disclosures and other available data. Inspect which sources support the ranking, which conflict, and when they last changed.",
    disclaimer:
      "It is not a probability of future returns, a guaranteed prediction, or a recommendation. Its purpose is to summarize the strength and alignment of available evidence.",
  },
  monitoring: {
    title: "Follow the stocks you care about.",
    description:
      "Save tickers to watchlists and get alerted when meaningful disclosures, news, press releases, institutional activity, or other monitored evidence changes.",
  },
  strategies: {
    title: "See how data-driven strategies performed historically.",
    description:
      "Explore strategies built from Walnut datasets, including Congress activity, insider activity, fundamentals, technical conditions, and multi-source confirmation. Historical and backtested results are research context, not forecasts.",
  },
  pricing: {
    title: "Start free. Unlock deeper research when you need it.",
  },
} as const;
