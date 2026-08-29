/**
 * Canonical public-homepage positioning. The marketing route is rendered on
 * the server and hydrated in the browser, so its shared copy must originate
 * here rather than from a client-side replacement or crawler fallback.
 */
export const homepageContent = {
  hero: {
    eyebrow: "For self-directed stock investors",
    title: "Stock Research & Market Intelligence in One Platform.",
    description:
      "Research stocks across fundamentals, technicals, insider trades, Congress trades, institutional holdings, government contracts, analyst expectations, and macro data — all in one place.",
  },
  metadata: {
    title: "Alternative Data Stock Research | Walnut Markets",
    description:
      "Research stocks across fundamentals, technicals, Congress trades, insiders, institutions, contracts, and more. See where the evidence agrees, conflicts, and changes.",
    socialDescription:
      "Research stocks across multiple data sources, follow the evidence, and get alerted when it changes. Built for research. Not investment advice.",
  },
  differentiation: {
    description:
      "Other research tools often specialize in individual data categories or leave investors to connect everything manually. Walnut combines evidence into context and ongoing research so you can see where the evidence agrees, where it conflicts, what changed, the catalysts and risks, and what to watch next.",
  },
  confirmationScore: {
    description:
      "The Walnut Confirmation Score is a proprietary, evidence-based, and explainable 0-100 measure of how strongly Walnut's available evidence supports a directional view at a specific point in time.",
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
