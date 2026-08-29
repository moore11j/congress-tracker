/**
 * Canonical public-homepage positioning. The marketing route is rendered on
 * the server and hydrated in the browser, so its shared copy must originate
 * here rather than from a client-side replacement or crawler fallback.
 */
export const homepageContent = {
  hero: {
    eyebrow: "Stock Analysis · Alternative Data · Market Intelligence",
    title: "Build Your Next Winning Portfolio.",
    description:
      "See the stocks, insiders, and backtested strategies with a history of market outperformance—then use Walnut’s Confirmation Score, fundamentals, technicals, Congress activity, institutional holdings, government contracts, analyst ratings, and more to find what could outperform next.",
  },
  metadata: {
    title: "Walnut Markets | Stock Analysis, Alternative Data & Strategies",
    description:
      "Find top-ranked stocks, track insiders and Congress trades, explore backtested strategies, and analyze market data with Walnut Markets.",
    socialDescription:
      "Research stocks across multiple data sources, follow the evidence, and get alerted when it changes. Built for research. Not investment advice.",
  },
  differentiation: {
    description:
      "Most platforms give you one slice of the market. Walnut connects the data, ranks the opportunities, tracks the participants and strategies, and measures the outcome afterward—so every conclusion remains inspectable.",
  },
  confirmationScore: {
    description:
      "Walnut combines multiple independent data sources into a proprietary Confirmation Score to identify stocks where the current setup is strongest. The score summarizes current evidence alignment and strength, not a probability of future return.",
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
