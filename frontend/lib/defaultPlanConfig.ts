import type { PlanConfig, PlanConfigFeature, PlanLimit, PlanPrice } from "@/lib/api";

type PlanTier = "free" | "premium" | "pro";
type BillingInterval = "monthly" | "annual";
type FeatureKey =
  | "signals"
  | "leaderboards"
  | "backtesting"
  | "screener"
  | "screener_intelligence"
  | "screener_presets"
  | "screener_saved_screens"
  | "screener_monitoring"
  | "screener_csv_export"
  | "screener_results"
  | "watchlists"
  | "watchlist_tickers"
  | "saved_views"
  | "notification_digests"
  | "monitoring_sources"
  | "inbox_alerts"
  | "inbox_alert_retention"
  | "government_contracts_feed"
  | "government_contracts_filters"
  | "insider_feed"
  | "congress_feed"
  | "options_flow_feed"
  | "options_flow_filters"
  | "institutional_feed"
  | "institutional_filters"
  | "api_webhooks";

type FeatureDefinition = Omit<PlanConfigFeature, "limits"> & {
  feature_key: FeatureKey;
  required_tier: PlanTier;
};

const planTiers: PlanTier[] = ["free", "premium", "pro"];

const defaultLimits: Record<PlanTier, Record<FeatureKey, number>> = {
  free: {
    signals: 0,
    leaderboards: 0,
    backtesting: 0,
    screener: 0,
    screener_intelligence: 0,
    screener_presets: 0,
    screener_saved_screens: 3,
    screener_monitoring: 0,
    screener_csv_export: 0,
    screener_results: 25,
    watchlists: 1,
    watchlist_tickers: 10,
    saved_views: 3,
    notification_digests: 0,
    monitoring_sources: 2,
    inbox_alerts: 1,
    inbox_alert_retention: 14,
    government_contracts_feed: 1,
    government_contracts_filters: 0,
    insider_feed: 1,
    congress_feed: 1,
    options_flow_feed: 0,
    options_flow_filters: 0,
    institutional_feed: 0,
    institutional_filters: 0,
    api_webhooks: 0,
  },
  premium: {
    signals: 1,
    leaderboards: 1,
    backtesting: 1,
    screener: 1,
    screener_intelligence: 1,
    screener_presets: 1,
    screener_saved_screens: 10,
    screener_monitoring: 1,
    screener_csv_export: 1,
    screener_results: 250,
    watchlists: 10,
    watchlist_tickers: 30,
    saved_views: 50,
    notification_digests: 25,
    monitoring_sources: 5,
    inbox_alerts: 1,
    inbox_alert_retention: 90,
    government_contracts_feed: 1,
    government_contracts_filters: 1,
    insider_feed: 1,
    congress_feed: 1,
    options_flow_feed: 0,
    options_flow_filters: 0,
    institutional_feed: 0,
    institutional_filters: 0,
    api_webhooks: 0,
  },
  pro: {
    signals: 1,
    leaderboards: 1,
    backtesting: 1,
    screener: 1,
    screener_intelligence: 1,
    screener_presets: 1,
    screener_saved_screens: 50,
    screener_monitoring: 1,
    screener_csv_export: 1,
    screener_results: 1000,
    watchlists: 25,
    watchlist_tickers: 100,
    saved_views: 50,
    notification_digests: 100,
    monitoring_sources: 15,
    inbox_alerts: 1,
    inbox_alert_retention: 365,
    government_contracts_feed: 1,
    government_contracts_filters: 1,
    insider_feed: 1,
    congress_feed: 1,
    options_flow_feed: 1,
    options_flow_filters: 1,
    institutional_feed: 1,
    institutional_filters: 1,
    api_webhooks: 1,
  },
};

const featureDefinitions: FeatureDefinition[] = [
  {
    feature_key: "signals",
    label: "Signals",
    kind: "feature",
    description: "Premium signal screens for unusual Congress and insider activity.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 10,
  },
  {
    feature_key: "leaderboards",
    label: "Leaderboards",
    kind: "feature",
    description: "Performance leaderboards for deeper political and insider intelligence.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 20,
  },
  {
    feature_key: "backtesting",
    label: "Portfolio backtesting",
    kind: "feature",
    description: "Historical backtests across watchlists, saved screens, Congress disclosures, and insider filings.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 22,
  },
  {
    feature_key: "screener",
    label: "Stock screener",
    kind: "feature",
    description: "Core market and company filters across the stock screener.",
    required_tier: "free",
    unit_singular: "",
    unit_plural: "",
    sort_order: 25,
  },
  {
    feature_key: "screener_intelligence",
    label: "Screener intelligence filters",
    kind: "feature",
    description: "Congress, insider, confirmation, Why Now, and freshness filters inside the screener.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 26,
  },
  {
    feature_key: "screener_presets",
    label: "Screener starter presets",
    kind: "feature",
    description: "One-click starter screens for higher-conviction discovery workflows.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 27,
  },
  {
    feature_key: "screener_saved_screens",
    label: "Saved screens",
    kind: "limit",
    description: "Saved screener setups you can revisit from the discovery workflow.",
    required_tier: "free",
    unit_singular: "screen",
    unit_plural: "screens",
    sort_order: 28,
  },
  {
    feature_key: "screener_monitoring",
    label: "Saved screen monitoring",
    kind: "feature",
    description: "Monitoring events when names enter, exit, or upgrade inside a saved screen.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 29,
  },
  {
    feature_key: "screener_csv_export",
    label: "Screener CSV export",
    kind: "feature",
    description: "Download screener results as CSV for offline workflow and triage.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 30,
  },
  {
    feature_key: "screener_results",
    label: "Screener results",
    kind: "limit",
    description: "Maximum screener results returned per query.",
    required_tier: "free",
    unit_singular: "result",
    unit_plural: "results",
    sort_order: 31,
  },
  {
    feature_key: "watchlists",
    label: "Watchlists",
    kind: "limit",
    description: "Saved research lists for monitoring symbols, filings, and alerts.",
    required_tier: "free",
    unit_singular: "watchlist",
    unit_plural: "watchlists",
    sort_order: 40,
  },
  {
    feature_key: "watchlist_tickers",
    label: "Tickers per watchlist",
    kind: "limit",
    description: "Ticker capacity inside each watchlist.",
    required_tier: "free",
    unit_singular: "ticker",
    unit_plural: "tickers",
    sort_order: 50,
  },
  {
    feature_key: "notification_digests",
    label: "Alerts and digests",
    kind: "feature",
    description: "Email digests and alert-trigger subscriptions for monitored research.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 60,
  },
  {
    feature_key: "saved_views",
    label: "Saved views",
    kind: "limit",
    description: "Reusable feed, signal, and watchlist filters.",
    required_tier: "free",
    unit_singular: "view",
    unit_plural: "views",
    sort_order: 70,
  },
  {
    feature_key: "monitoring_sources",
    label: "Monitoring sources",
    kind: "limit",
    description: "Watchlists and saved screens monitored in the inbox.",
    required_tier: "free",
    unit_singular: "source",
    unit_plural: "sources",
    sort_order: 80,
  },
  {
    feature_key: "inbox_alerts",
    label: "Inbox and alerts",
    kind: "feature",
    description: "Inbox alerts for monitored watchlists and saved screens.",
    required_tier: "free",
    unit_singular: "",
    unit_plural: "",
    sort_order: 82,
  },
  {
    feature_key: "inbox_alert_retention",
    label: "Alert retention",
    kind: "limit",
    description: "How long inbox alert history remains available.",
    required_tier: "free",
    unit_singular: "day",
    unit_plural: "days",
    sort_order: 84,
  },
  {
    feature_key: "congress_feed",
    label: "Congress feed",
    kind: "feature",
    description: "Congress trading disclosures in the main feed.",
    required_tier: "free",
    unit_singular: "",
    unit_plural: "",
    sort_order: 90,
  },
  {
    feature_key: "insider_feed",
    label: "Insider feed",
    kind: "feature",
    description: "Insider filings and trading activity in the main feed.",
    required_tier: "free",
    unit_singular: "",
    unit_plural: "",
    sort_order: 92,
  },
  {
    feature_key: "government_contracts_feed",
    label: "Government contracts feed",
    kind: "feature",
    description: "Government contract awards and modifications in market context.",
    required_tier: "free",
    unit_singular: "",
    unit_plural: "",
    sort_order: 94,
  },
  {
    feature_key: "government_contracts_filters",
    label: "Government contracts filters",
    kind: "feature",
    description: "Filter and triage contract activity by richer contract attributes.",
    required_tier: "premium",
    unit_singular: "",
    unit_plural: "",
    sort_order: 96,
  },
  {
    feature_key: "options_flow_feed",
    label: "Options Flow Feed",
    kind: "feature",
    description: "Options flow overlay and feed access.",
    required_tier: "pro",
    unit_singular: "",
    unit_plural: "",
    sort_order: 100,
  },
  {
    feature_key: "options_flow_filters",
    label: "Options Flow Filters",
    kind: "feature",
    description: "Options flow filters for screeners and intelligence workflows.",
    required_tier: "pro",
    unit_singular: "",
    unit_plural: "",
    sort_order: 102,
  },
  {
    feature_key: "institutional_feed",
    label: "Institutional Feed",
    kind: "feature",
    description: "Institutional Activity and 13F filing access.",
    required_tier: "pro",
    unit_singular: "",
    unit_plural: "",
    sort_order: 110,
  },
  {
    feature_key: "institutional_filters",
    label: "Institutional Filters",
    kind: "feature",
    description: "Institutional activity filters for screeners and intelligence workflows.",
    required_tier: "pro",
    unit_singular: "",
    unit_plural: "",
    sort_order: 112,
  },
  {
    feature_key: "api_webhooks",
    label: "API and webhooks",
    kind: "feature",
    description: "API and webhook workflow automation placeholder.",
    required_tier: "pro",
    unit_singular: "",
    unit_plural: "",
    sort_order: 120,
  },
];

const defaultPlanPrices: PlanPrice[] = [
  { tier: "free", billing_interval: "monthly", amount_cents: 0, currency: "USD" },
  { tier: "free", billing_interval: "annual", amount_cents: 0, currency: "USD" },
  { tier: "premium", billing_interval: "monthly", amount_cents: 1995, currency: "USD" },
  { tier: "premium", billing_interval: "annual", amount_cents: 19995, currency: "USD" },
  { tier: "pro", billing_interval: "monthly", amount_cents: 4995, currency: "USD" },
  { tier: "pro", billing_interval: "annual", amount_cents: 49995, currency: "USD" },
];

function priceFor(tier: PlanTier, billingInterval: BillingInterval) {
  return defaultPlanPrices.find((price) => price.tier === tier && price.billing_interval === billingInterval);
}

const features: PlanConfigFeature[] = featureDefinitions.map((feature) => ({
  ...feature,
  limits: {
    free: defaultLimits.free[feature.feature_key],
    premium: defaultLimits.premium[feature.feature_key],
    pro: defaultLimits.pro[feature.feature_key],
  },
}));

const planLimits: PlanLimit[] = planTiers.flatMap((tier) =>
  featureDefinitions.map((feature) => ({
    feature_key: feature.feature_key,
    tier,
    limit_value: defaultLimits[tier][feature.feature_key],
    label: feature.label,
    unit_singular: feature.unit_singular,
    unit_plural: feature.unit_plural,
    sort_order: feature.sort_order,
  })),
);

export const defaultPlanConfig: PlanConfig = {
  tiers: [
    {
      tier: "free",
      name: "Free",
      description: "For casual research and a focused starter watchlist.",
      limits: defaultLimits.free,
      prices: {
        monthly: priceFor("free", "monthly"),
        annual: priceFor("free", "annual"),
      },
    },
    {
      tier: "premium",
      name: "Premium",
      description: "For daily monitoring, premium research signals, alerts, and deeper market-political intelligence.",
      limits: defaultLimits.premium,
      prices: {
        monthly: priceFor("premium", "monthly"),
        annual: priceFor("premium", "annual"),
      },
    },
    {
      tier: "pro",
      name: "Pro",
      description: "For serious investors and operators who need higher limits, Pro-only data sets, and workflow automation readiness.",
      limits: defaultLimits.pro,
      prices: {
        monthly: priceFor("pro", "monthly"),
        annual: priceFor("pro", "annual"),
      },
    },
  ],
  features,
  feature_gates: featureDefinitions.map((feature) => ({
    feature_key: feature.feature_key,
    required_tier: feature.required_tier,
    description: feature.description,
  })),
  plan_limits: planLimits,
  plan_prices: defaultPlanPrices,
};
