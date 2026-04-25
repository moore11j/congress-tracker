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
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
  estimated_price?: number | null;
  current_price?: number | null;
  display_price?: number | null;
  reported_price?: number | null;
  reported_price_currency?: string | null;
  smart_score?: number | null;
  smart_band?: string | null;
  pnl_pct?: number | null;
  pnl_source?: "filing" | "normalized_filing" | "eod" | "normalization_unavailable" | "none" | null;
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
  kind?: "congress_trade" | "insider_trade" | "institutional_buy" | "event";
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
  transaction_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
  pnl_pct?: number | null;
  alpha_pct?: number | null;
  pnl_source?: string | null;
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
  };
  drivers: string[];
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
  ticker: {
    symbol: string;
    name: string;
    asset_class: string;
    sector?: string | null;
    industry?: string | null;
    country?: string | null;
    exchange?: string | null;
  };
  top_members: (TopMemberSummary & { trade_count: number })[];
  trades: TickerTrade[];
  confirmation_score_bundle?: ConfirmationScoreBundle | null;
  options_flow_summary?: OptionsFlowSummary | null;
  why_now?: WhyNowBundle | null;
  signal_freshness?: SignalFreshnessBundle | null;
};

export type NewsItem = {
  symbol?: string | null;
  title: string;
  site?: string | null;
  published_at?: string | null;
  url: string;
  image_url?: string | null;
  summary?: string | null;
  source: "fmp_general_news" | "fmp_stock_news" | string;
};

export type InsightsNewsResponse = {
  items: NewsItem[];
  status?: "ok" | "empty" | "unavailable" | string;
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
  summary?: string | null;
  source: "fmp_press_release" | string;
};

export type PressReleasesResponse = {
  items: PressReleaseItem[];
  status?: "ok" | "empty" | "unavailable" | string;
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
  status?: "ok" | "empty" | "unavailable" | string;
  message?: string | null;
  page: number;
  limit: number;
  has_next: boolean;
};

export type WatchlistSummary = {
  id: number;
  name: string;
  unseen_count?: number;
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

export type WatchlistDetail = {
  watchlist_id: number;
  name?: string;
  tickers: { symbol: string; name: string }[];
  unseen_count?: number;
  last_seen_at?: string | null;
  unseen_since?: string | null;
};


export type TickerProfilesMap = Record<string, TickerProfile>;
