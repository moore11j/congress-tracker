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

export type TickerProfile = {
  ticker: {
    symbol: string;
    name: string;
    asset_class: string;
    sector?: string | null;
  };
  top_members: (TopMemberSummary & { trade_count: number })[];
  trades: TickerTrade[];
};

export type WatchlistSummary = {
  id: number;
  name: string;
  unseen_count?: number;
  last_seen_at?: string | null;
  unseen_since?: string | null;
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
