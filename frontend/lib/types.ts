export type MemberSummary = {
  bioguide_id: string;
  member_id?: number;
  name: string;
  chamber: string;
  party?: string | null;
  state?: string | null;
  district?: string | null;
};

export type TopMemberSummary = {
  member_id: string;
  bioguide_id?: string;
  name: string;
  chamber: string;
  party?: string | null;
  state?: string | null;
  district?: string | null;
};

export type SecuritySummary = {
  symbol?: string | null;
  name: string;
  asset_class: string;
  sector?: string | null;
};

export type FeedItem = {
  id: number;
  member: MemberSummary;
  security: SecuritySummary;
  transaction_type: string;
  owner_type: string;
  contract_description?: string | null;
  payload?: Record<string, unknown> | null;
  url?: string | null;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
  estimated_trade_value?: number | null;
  estimated_price?: number | null;
  estimated_shares?: number | null;
  current_price?: number | null;
  display_price?: number | null;
  reported_price?: number | null;
  reported_price_currency?: string | null;
  smart_score?: number | null;
  smart_band?: string | null;
  pnl_pct?: number | null;
  pnl_source?: "filing" | "normalized_filing" | "eod" | "trade_outcome" | "normalization_unavailable" | "none" | null;
  outcome_status?: string | null;
  outcome_skip_reason?: string | null;
  outcome_methodology?: string | null;
  outcome_error?: string | null;
  price_basis?: string | null;
  quote_asof_ts?: string | null;
  quote_is_stale?: boolean | null;
  member_net_30d?: number | null;
  symbol_net_30d?: number | null;
  confirmation_30d?: {
    congress_active_30d: boolean;
    insider_active_30d: boolean;
    congress_trade_count_30d: number;
    insider_trade_count_30d: number;
    insider_buy_count_30d: number;
    insider_sell_count_30d: number;
    cross_source_confirmed_30d: boolean;
    repeat_congress_30d: boolean;
    repeat_insider_30d: boolean;
  } | null;
  kind?:
    | "congress_trade"
    | "congress_treasury_trade"
    | "congress_crypto_trade"
    | "insider_trade"
    | "institutional_buy"
    | "institutional_accumulation"
    | "institutional_distribution"
    | "new_institutional_position"
    | "major_holder_reduction"
    | "major_holder_exit"
    | "cluster_accumulation"
    | "cluster_distribution"
    | "smart_money_confirmation"
    | "crowded_long"
    | "contrarian_accumulation"
    | "government_contract"
    | "event";
  insider?: {
    name: string;
    ownership?: string | null;
    filing_date?: string | null;
    transaction_date?: string | null;
    price?: number | null;
    display_price?: number | null;
    reported_price?: number | null;
    reported_price_currency?: string | null;
    role?: string | null;
    reporting_cik?: string | null;
  };
};

export type FeedResponse = {
  items: FeedItem[];
  next_cursor: string | null;
};

export type MemberTrade = {
  id: number;
  event_id?: number | null;
  symbol: string | null;
  security_name: string;
  asset_class?: string | null;
  instrument_type?: string | null;
  maturity_date?: string | null;
  duration_days?: number | null;
  duration_label?: string | null;
  coupon_rate?: number | null;
  cusip?: string | null;
  transaction_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
  estimated_trade_value?: number | null;
  estimated_price?: number | null;
  estimated_shares?: number | null;
  current_price?: number | null;
  pnl_pct?: number | null;
  return_pct?: number | null;
  alpha_pct?: number | null;
  benchmark_return_pct?: number | null;
  holding_period_days?: number | null;
  outcome_horizon?: string | null;
  return_label?: string | null;
  pnl_source?: string | null;
  outcome_status?: string | null;
  outcome_skip_reason?: string | null;
  outcome_methodology?: string | null;
  outcome_error?: string | null;
  price_basis?: string | null;
  smart_score?: number | null;
  smart_band?: string | null;
};

export type MemberProfile = {
  member: MemberSummary;
  top_tickers: { symbol: string; trades: number }[];
  trades: MemberTrade[];
};

export type TickerTrade = {
  id: number;
  member: MemberSummary;
  transaction_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
};

export type ConfirmationDirection = "bullish" | "bearish" | "neutral" | "mixed";
export type ConfirmationBand = "inactive" | "weak" | "moderate" | "strong" | "exceptional";

export type ConfirmationScoreSource = {
  present: boolean;
  direction: ConfirmationDirection;
  strength: number;
  quality: number;
  freshness_days: number | null;
  label: string;
  status?: string | null;
  title?: string | null;
  score_contribution?: number;
  detail?: string | null;
  summary?: string | null;
  lines?: string[];
};

export type ConfirmationScoreBundle = {
  ticker: string;
  lookback_days: number;
  score: number;
  band: ConfirmationBand;
  direction: ConfirmationDirection;
  status: string;
  explanation: string;
  sources: {
    congress: ConfirmationScoreSource;
    insiders: ConfirmationScoreSource;
    signals: ConfirmationScoreSource;
    price_volume: ConfirmationScoreSource;
    options_flow: ConfirmationScoreSource;
    government_contracts: ConfirmationScoreSource;
    institutional_activity: ConfirmationScoreSource;
  };
  drivers: string[];
  active_sources?: string[];
  source_details?: Record<string, string>;
};

export type OptionsFlowState = "bullish" | "bearish" | "mixed" | "inactive" | "unavailable";
export type OptionsFlowConfidence = "low" | "moderate" | "high";

export type OptionsFlowSummary = {
  ticker: string;
  lookback_days: number;
  state: OptionsFlowState;
  label: string;
  is_active: boolean;
  confidence: OptionsFlowConfidence;
  freshness_days: number | null;
  summary: string;
  signals: string[];
  metrics: {
    put_call_premium_ratio: number | null;
    net_premium_skew: number;
    recent_contract_volume?: number;
    observed_contracts?: number;
    freshness_days: number | null;
  };
  can_confirm: boolean;
  provider: string;
  reason?: string | null;
};

export type WhyNowState = "early" | "strengthening" | "strong" | "mixed" | "fading" | "inactive";

export type WhyNowBundle = {
  ticker: string;
  lookback_days: number;
  state: WhyNowState;
  headline: string;
  evidence: string[];
  caveat?: string | null;
};

export type SignalFreshnessState = "fresh" | "early" | "active" | "maturing" | "stale" | "inactive";

export type SignalFreshnessBundle = {
  ticker: string;
  lookback_days: number;
  freshness_score: number;
  freshness_state: SignalFreshnessState;
  freshness_label: string;
  explanation: string;
  timing: {
    freshest_source_days: number | null;
    stalest_active_source_days: number | null;
    active_source_count: number;
    overlap_window_days: number | null;
  };
};

export type TickerProfile = {
  status?: "ok" | "partial" | "loading" | string;
  ticker: {
    symbol: string;
    name: string;
    asset_class: string;
    sector?: string | null;
    industry?: string | null;
    country?: string | null;
    exchange?: string | null;
    exchange_short_name?: string | null;
    display_market_chain?: string | null;
    identity_status?: "ok" | "partial" | "loading" | "unknown" | string | null;
    price_history_points?: number | null;
    price_history_start?: string | null;
    price_history_end?: string | null;
    limited_data_state?: "newly_listed" | "limited_history" | string | null;
    limited_data_message?: string | null;
  };
  top_members: (TopMemberSummary & { trade_count: number })[];
  trades: TickerTrade[];
  confirmation_score_bundle?: ConfirmationScoreBundle | null;
  options_flow_summary?: OptionsFlowSummary | null;
  why_now?: WhyNowBundle | null;
  signal_freshness?: SignalFreshnessBundle | null;
  technical_indicators?: TickerTechnicalIndicators | null;
};

export type NewsItem = {
  symbol?: string | null;
  title: string;
  site?: string | null;
  published_at?: string | null;
  url: string;
  image_url?: string | null;
  summary?: string | null;
  market_read?: "bullish" | "bearish" | "neutral" | string;
  source: "fmp_general_news" | "fmp_stock_news" | string;
};

export type InsightsNewsResponse = {
  items: NewsItem[];
  status?: "ok" | "loading" | "no_data" | "unavailable" | string;
  item_count?: number;
  updated_at?: string | null;
  message?: string | null;
  page: number;
  limit: number;
  has_next: boolean;
};

export type PressReleaseItem = {
  symbol: string;
  title: string;
  site?: string | null;
  published_at: string | null;
  url?: string | null;
  image_url?: string | null;
  summary?: string | null;
  market_read?: "bullish" | "bearish" | "neutral" | string;
  source: "fmp_press_release" | string;
};

export type PressReleasesResponse = {
  items: PressReleaseItem[];
  status?: "ok" | "loading" | "no_data" | "unavailable" | string;
  item_count?: number;
  updated_at?: string | null;
  message?: string | null;
  page: number;
  limit: number;
  has_next: boolean;
};

export type SecFilingItem = {
  symbol: string;
  filing_date: string | null;
  accepted_date?: string | null;
  form_type: string;
  title?: string | null;
  url?: string | null;
  source: "fmp_sec_filings" | string;
};

export type SecFilingsResponse = {
  items: SecFilingItem[];
  status?: "ok" | "loading" | "no_data" | "unavailable" | string;
  item_count?: number;
  window_days?: number;
  updated_at?: string | null;
  message?: string | null;
  page: number;
  limit: number;
  has_next: boolean;
};

export type TechnicalIndicatorReading = {
  status: "ok" | "unavailable" | string;
  signal: "bullish" | "bearish" | "neutral" | "unavailable" | string;
  message: string;
  reason?: string | null;
  value?: number | null;
  period?: number | null;
  macd?: number | null;
  signal_line?: number | null;
  histogram?: number | null;
  short_period?: number | null;
  medium_period?: number | null;
  short_ema?: number | null;
  medium_ema?: number | null;
};

export type TickerTechnicalIndicators = {
  source: string;
  asof?: string | null;
  price_points: number;
  rsi: TechnicalIndicatorReading;
  macd: TechnicalIndicatorReading;
  ema_trend: TechnicalIndicatorReading;
};

export type MacroSnapshotIndex = {
  label: string;
  symbol: string;
  value?: number | null;
  change_pct?: number | null;
  timeframe_label?: string | null;
  is_proxy?: boolean;
  source?: string | null;
  date?: string | null;
  status?: "ok" | "unavailable" | string;
};

export type MacroSnapshotPoint = {
  label: string;
  value?: number | null;
  change?: number | null;
  change_unit?: string | null;
  change_value?: number | null;
  change_format?: "percent" | "percentage_points" | "bps" | "currency" | "number" | string | null;
  change_label?: "MoM" | "YoY" | "QoQ" | "prior release" | string | null;
  timeframe_label?: string | null;
  context_label?: string | null;
  unit_label?: string | null;
  value_format?: "percent" | "currency" | "number" | "bps" | string | null;
  date?: string | null;
};

export type SnapshotInstrument = {
  label: string;
  symbol?: string | null;
  value?: number | string | null;
  change?: number | null;
  change_pct?: number | null;
  timeframe_label: string;
  unit_label?: string | null;
  status?: "ok" | "unavailable" | string;
  date?: string | null;
};

export type InsightsQuoteGroup = "global_markets" | "commodities" | "currencies" | "crypto";

export type InsightsQuoteItem = {
  group: InsightsQuoteGroup;
  label: string;
  symbol: string;
  display_symbol: string;
  price: number | null;
  change: number | null;
  change_percent: number | null;
  volume: number | null;
  as_of: string | null;
  status: "ok" | "unavailable";
};

export type InsightsOverviewResponse = {
  global_markets: InsightsQuoteItem[];
  commodities: InsightsQuoteItem[];
  currencies: InsightsQuoteItem[];
  crypto: InsightsQuoteItem[];
  updated_at: string | null;
};

export type SectorPerformancePoint = {
  sector: string;
  change_pct: number;
};

export type MacroSnapshotResponse = {
  world_indexes?: MacroSnapshotIndex[];
  indexes: MacroSnapshotIndex[];
  treasury: MacroSnapshotPoint[];
  economics: MacroSnapshotPoint[];
  commodities?: SnapshotInstrument[];
  currencies?: SnapshotInstrument[];
  crypto?: SnapshotInstrument[];
  sector_performance: SectorPerformancePoint[];
  status: "ok" | "partial" | "unavailable" | string;
  generated_at: string;
  updated_at?: string | null;
  as_of?: string | null;
  stale?: boolean;
  source?: string | null;
  category?: string | null;
  cache_hit?: boolean;
};

export type WatchlistSummary = {
  id: number;
  name: string;
  symbols?: string[];
  unseen_count?: number;
  unread_count?: number;
  new_count?: number;
  last_seen_at?: string | null;
  unseen_since?: string | null;
};

export type ConfirmationMonitoringEvent = {
  id: number;
  watchlist_id: number;
  ticker: string;
  event_type: string;
  event_label: string;
  title: string;
  body?: string | null;
  score_before?: number | null;
  score_after: number;
  band_before?: ConfirmationBand | string | null;
  band_after: ConfirmationBand | string;
  direction_before?: ConfirmationDirection | string | null;
  direction_after: ConfirmationDirection | string;
  source_count_before?: number | null;
  source_count_after: number;
  payload?: Record<string, unknown> | null;
  created_at: string;
};

export type ConfirmationMonitoringEventsResponse = {
  items: ConfirmationMonitoringEvent[];
};

export type ConfirmationMonitoringClearResponse = {
  cleared: number;
};

export type ConfirmationMonitoringRefreshResponse = {
  updated: number;
  initialized: number;
  generated: number;
  deduped: number;
  items: ConfirmationMonitoringEvent[];
};

export type SavedScreen = {
  id: number;
  name: string;
  params: Record<string, string>;
  last_viewed_at?: string | null;
  last_refreshed_at?: string | null;
  created_at: string;
  updated_at: string;
  monitoring?: {
    initialized: number;
    generated: number;
    deduped: number;
    membership_changes_allowed: boolean;
  } | null;
};

export type SavedScreenEventSnapshot = {
  ticker: string;
  confirmation_score: number;
  confirmation_band: ConfirmationBand | string;
  direction: ConfirmationDirection | string;
  source_count: number;
  why_now_state: string;
  observed_at: string;
};

export type SavedScreenEvent = {
  id: number;
  saved_screen_id: number;
  screen_name?: string | null;
  ticker: string;
  event_type: string;
  title: string;
  description: string;
  before_snapshot?: SavedScreenEventSnapshot | null;
  after_snapshot?: SavedScreenEventSnapshot | null;
  created_at: string;
};

export type SavedScreensResponse = {
  items: SavedScreen[];
};

export type SavedScreenEventsResponse = {
  items: SavedScreenEvent[];
};

export type MonitoringAlert = {
  id: number;
  item_key?: string;
  source_type: "watchlist" | "saved-screen" | string;
  source_id: string;
  source_name: string;
  event_id: number;
  alert_type: string;
  symbol?: string | null;
  title: string;
  description?: string | null;
  body?: string | null;
  payload?: Record<string, unknown> | null;
  timestamp?: string;
  event_created_at: string;
  created_at: string;
  read_at?: string | null;
  dismissed_at?: string | null;
  is_read?: boolean;
  is_unread?: boolean;
  is_dismissed?: boolean;
  score?: number | null;
};

export type MonitoringInboxSource = {
  id: string;
  type: "watchlist" | "saved-screen" | string;
  name: string;
  unread_count: number;
  new_count: number;
};

export type MonitoringCounts = {
  total_unread: number;
  watchlist_unread: number;
  saved_screen_unread: number;
  unread_sources_count: number;
  sources: MonitoringInboxSource[];
};

export type MonitoringInboxResponse = {
  unread_total: number;
  sources: MonitoringInboxSource[];
  counts?: MonitoringCounts;
  screen_changes: SavedScreenEvent[];
  latest_important: MonitoringAlert[];
  alerts?: MonitoringAlert[];
  items?: MonitoringAlert[];
};

export type WatchlistDetail = {
  watchlist_id: number;
  name?: string;
  tickers: { symbol: string; name: string }[];
  unseen_count?: number;
  unread_count?: number;
  new_count?: number;
  last_seen_at?: string | null;
  unseen_since?: string | null;
};


export type TickerProfilesMap = Record<string, TickerProfile>;
