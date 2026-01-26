export type MemberSummary = {
  bioguide_id: string;
  name: string;
  chamber: string;
  party?: string | null;
  state?: string | null;
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
};

export type FeedResponse = {
  items: FeedItem[];
  next_cursor: string | null;
};

export type MemberTrade = {
  id: number;
  symbol: string | null;
  security_name: string;
  transaction_type: string;
  trade_date: string | null;
  report_date: string | null;
  amount_range_min: number | null;
  amount_range_max: number | null;
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
  top_members: { bioguide_id: string; trades: number }[];
  trades: TickerTrade[];
};

export type WatchlistSummary = {
  id: number;
  name: string;
};

export type WatchlistDetail = {
  watchlist_id: number;
  tickers: { symbol: string; name: string }[];
};
