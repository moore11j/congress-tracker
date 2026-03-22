import type { FeedResponse, MemberProfile, TickerProfile, TickerProfilesMap, WatchlistDetail, WatchlistSummary } from "@/lib/types";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

export const EVENTS_API_MAX_LIMIT = 100;

export type NormalizedEventType = "congress_trade" | "insider_trade";

export function normalizeEventType(uiValue: string | null | undefined): NormalizedEventType | undefined {
  const normalized = (uiValue ?? "").trim().toLowerCase();
  if (!normalized || normalized === "all") return undefined;
  if (normalized === "congress" || normalized === "congress_trade") return "congress_trade";
  if (normalized === "insider" || normalized === "insider_trade") return "insider_trade";
  return undefined;
}

function buildApiUrl(path: string, params?: QueryParams) {
  const url = new URL(path, API_BASE);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === null || value === undefined) return;
      const stringValue = String(value).trim();
      if (!stringValue) return;
      url.searchParams.set(key, stringValue);
    });
  }
  return url.toString();
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;

  try {
    response = await fetch(url, { cache: "no-store", ...init });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Fetch failed for ${url}: ${message}`);
  }

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    const snippet = text.length > 2000 ? `${text.slice(0, 2000)}…` : text;
    throw new Error(
      `HTTP ${response.status} ${response.statusText}
URL: ${url}${snippet ? `
Body: ${snippet}` : ""}`
    );
  }

  return (await response.json()) as T;
}

async function fetchNoContent(url: string, init?: RequestInit): Promise<void> {
  let response: Response;

  try {
    response = await fetch(url, { cache: "no-store", ...init });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Fetch failed for ${url}: ${message}`);
  }

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    const snippet = text.length > 2000 ? `${text.slice(0, 2000)}…` : text;
    throw new Error(
      `HTTP ${response.status} ${response.statusText}
URL: ${url}${snippet ? `
Body: ${snippet}` : ""}`
    );
  }
}


export type EventItem = {
  id: number;
  event_type: string;
  ts: string;
  symbol?: string | null;
  member_name?: string | null;
  member_bioguide_id?: string | null;
  chamber?: string | null;
  party?: string | null;
  trade_type?: string | null;
  ticker?: string | null;
  source?: string | null;
  headline?: string | null;
  summary?: string | null;
  url?: string | null;
  impact_score?: number | null;
  estimated_price?: number | null;
  current_price?: number | null;
  pnl_pct?: number | null;
  member_net_30d?: number | null;
  symbol_net_30d?: number | null;
  amount_min?: number | null;
  amount_max?: number | null;
  payload?: any;
};

export type EventsResponse = {
  items: EventItem[];
  limit?: number | null;
  offset?: number | null;
  total?: number | null;
};


export type SuggestResponse = {
  items: string[];
};

export type TickerPriceHistoryPoint = {
  date: string;
  close: number;
};

export type TickerPriceHistoryResponse = {
  symbol: string;
  days: number;
  start_date: string;
  end_date: string;
  points: TickerPriceHistoryPoint[];
};

export type InsiderSummary = {
  reporting_cik: string;
  insider_name: string | null;
  primary_company_name: string | null;
  primary_role: string | null;
  primary_symbol: string | null;
  lookback_days: number;
  total_trades: number;
  buy_count: number;
  sell_count: number;
  unique_tickers: number;
  gross_buy_value: number;
  gross_sell_value: number;
  net_flow: number;
  latest_filing_date: string | null;
  latest_transaction_date: string | null;
};

export type InsiderTrade = {
  event_id: number;
  symbol: string | null;
  company_name: string | null;
  companyName?: string | null;
  transaction_date: string | null;
  trade_date?: string | null;
  filing_date: string | null;
  trade_type: string | null;
  tradeType?: string | null;
  amount_min: number | null;
  amount_max: number | null;
  trade_value?: number | null;
  tradeValue?: number | null;
  shares: number | null;
  price: number | null;
  insider_name: string | null;
  reporting_cik: string | null;
  role: string | null;
  external_id: string | null;
  url: string | null;
  pnl_pct?: number | null;
  pnlPct?: number | null;
  pnl?: number | null;
  pnl_source?: string | null;
  pnlSource?: string | null;
  smart_score?: number | null;
  smartScore?: number | null;
  smart_band?: string | null;
  smartBand?: string | null;
};

export type InsiderTopTicker = {
  symbol: string;
  company_name: string | null;
  trades: number;
  buy_count: number;
  sell_count: number;
  net_flow: number;
};

export type InsiderAlphaTrade = {
  event_id: number;
  symbol: string;
  trade_type?: string | null;
  asof_date: string | null;
  return_pct: number | null;
  alpha_pct: number | null;
  holding_days?: number | null;
};

export type InsiderAlphaSummary = {
  reporting_cik: string;
  lookback_days: number;
  benchmark_symbol: string | null;
  trades_analyzed: number;
  avg_return_pct: number | null;
  avg_alpha_pct: number | null;
  win_rate: number | null;
  avg_holding_days: number | null;
  best_trades: InsiderAlphaTrade[];
  worst_trades: InsiderAlphaTrade[];
  member_series?: MemberPerformancePoint[];
  benchmark_series?: BenchmarkPerformancePoint[];
  performance_series?: MemberPerformancePoint[];
};


export type SignalMode = "all" | "congress" | "insider";
export type SignalPreset = "discovery" | "balanced" | "strict";
export type SignalSort = "smart" | "multiple" | "recent" | "amount";

export type SignalItem = {
  kind?: SignalMode | string;
  event_id: number;
  ts: string;
  symbol: string;
  who?: string;
  position?: string;
  reporting_cik?: string | null;
  member_bioguide_id?: string;
  party?: string;
  chamber?: string;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  smart_score?: number;
  smart_band?: string;
  source?: string;
};

type SignalsAllResponse = SignalItem[] | { items?: SignalItem[]; debug?: unknown };

export async function getSignalsAll(params: {
  mode?: SignalMode;
  side?: string;
  preset?: SignalPreset;
  sort?: SignalSort;
  limit?: number;
  debug?: boolean;
  symbol?: string;
}): Promise<{ items: SignalItem[]; debug?: unknown }> {
  const url = buildApiUrl("/api/signals/all", {
    mode: params.mode ?? "all",
    side: params.side,
    preset: params.preset ?? "balanced",
    sort: params.sort ?? "smart",
    limit: params.limit,
    debug: params.debug ? "1" : undefined,
    symbol: params.symbol,
  });

  const data = await fetchJson<SignalsAllResponse>(url, {
    cache: "no-store",
    next: { revalidate: 0 },
  });

  if (Array.isArray(data)) {
    return { items: data };
  }

  return {
    items: Array.isArray(data.items) ? data.items : [],
    debug: data.debug,
  };
}

export async function suggestSymbols(q: string, tape: string, limit = 10): Promise<SuggestResponse> {
  return fetchJson<SuggestResponse>(buildApiUrl("/api/suggest/symbol", { q, tape, limit }), {
    cache: "no-store",
  });
}

export async function suggestMembers(q: string, limit = 10): Promise<SuggestResponse> {
  return fetchJson<SuggestResponse>(buildApiUrl("/api/suggest/member", { q, limit }), {
    cache: "no-store",
  });
}

export async function suggestRoles(q: string, limit = 10): Promise<SuggestResponse> {
  return fetchJson<SuggestResponse>(buildApiUrl("/api/suggest/role", { q, limit }), {
    cache: "no-store",
  });
}

export async function getEvents(params: QueryParams & { tape?: string }): Promise<EventsResponse> {
  const nextParams: QueryParams = { ...params };
  const tape = typeof nextParams.tape === "string" ? nextParams.tape.trim().toLowerCase() : "";
  const parsedLimit = Number(nextParams.limit);

  if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
    nextParams.limit = Math.min(Math.floor(parsedLimit), EVENTS_API_MAX_LIMIT);
  }

  if (tape === "congress") {
    nextParams.event_type = "congress_trade";
  } else if (tape === "insider") {
    nextParams.event_type = "insider_trade";
  } else {
    delete nextParams.event_type;
  }

  delete nextParams.tape;

  const url = buildApiUrl("/api/events", nextParams);
  if (process.env.NODE_ENV === "development") {
    console.info(`[feed] GET ${url}`);
  }
  return fetchJson<EventsResponse>(url, {
    cache: "no-store",
    next: { revalidate: 0 },
  });
}

export async function getMemberProfile(bioguideId: string): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(buildApiUrl(`/api/members/${bioguideId}`));
}

export async function getInsiderSummary(reportingCik: string, lookbackDays: number): Promise<InsiderSummary> {
  return fetchJson<InsiderSummary>(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/summary`, {
      lookback_days: lookbackDays,
    }),
  );
}

export async function getInsiderTrades(
  reportingCik: string,
  lookbackDays: number,
  limit = 50,
): Promise<{ reporting_cik: string; lookback_days: number; items: InsiderTrade[] }> {
  return fetchJson(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/trades`, {
      lookback_days: lookbackDays,
      limit,
    }),
  );
}

export async function getInsiderTopTickers(
  reportingCik: string,
  lookbackDays: number,
  limit = 10,
): Promise<{ reporting_cik: string; lookback_days: number; items: InsiderTopTicker[] }> {
  return fetchJson(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/top-tickers`, {
      lookback_days: lookbackDays,
      limit,
    }),
  );
}

export async function getInsiderAlphaSummary(
  reportingCik: string,
  params?: { lookback_days?: number },
): Promise<InsiderAlphaSummary> {
  return fetchJson<InsiderAlphaSummary>(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/alpha-summary`, {
      lookback_days: params?.lookback_days,
    }),
  );
}


export async function getMemberProfileBySlug(slug: string): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(buildApiUrl(`/api/members/by-slug/${slug}`));
}

export type MemberPerformance = {
  member_id: string;
  lookback_days: number;
  trade_count_total: number;
  trade_count_scored: number;
  pnl_status: string | null;
  avg_return: number | null;
  median_return: number | null;
  win_rate: number | null;
  avg_alpha: number | null;
  median_alpha: number | null;
  benchmark_symbol: string | null;
};

export type MemberAlphaTrade = {
  event_id: number;
  symbol: string;
  trade_type?: string | null;
  asof_date: string;
  return_pct: number | null;
  alpha_pct: number | null;
  holding_days?: number | null;
};

export type MemberPerformancePoint = {
  event_id: number;
  symbol: string | null;
  trade_type?: string | null;
  asof_date: string | null;
  return_pct: number | null;
  alpha_pct: number | null;
  benchmark_return_pct: number | null;
  holding_days?: number | null;
  cumulative_return_pct: number | null;
  running_benchmark_return_pct: number | null;
  cumulative_alpha_pct: number | null;
};

export type BenchmarkPerformancePoint = {
  asof_date: string | null;
  cumulative_return_pct: number | null;
};

export type MemberAlphaSummary = {
  member_id: string;
  lookback_days: number;
  benchmark_symbol: string | null;
  trades_analyzed: number;
  avg_return_pct: number | null;
  avg_alpha_pct: number | null;
  win_rate: number | null;
  avg_holding_days: number | null;
  best_trades: MemberAlphaTrade[];
  worst_trades: MemberAlphaTrade[];
  member_series?: MemberPerformancePoint[];
  benchmark_series?: BenchmarkPerformancePoint[];
  performance_series?: MemberPerformancePoint[];
};

export type MemberTradesResponse = {
  member_id: string;
  lookback_days: number;
  limit: number;
  items: MemberProfile["trades"];
};

type MemberAnalyticsParams = {
  lookback_days?: number;
};

export type CongressTraderLeaderboardSort = "avg_alpha" | "avg_return" | "win_rate" | "trade_count";
export type CongressTraderLeaderboardChamber = "all" | "house" | "senate";
export type CongressTraderLeaderboardSourceMode = "congress" | "insiders";
export type CongressTraderLeaderboardApiSourceMode = CongressTraderLeaderboardSourceMode | "all";

export type CongressTraderLeaderboardRow = {
  rank: number;
  member_id: string;
  member_name: string;
  reporting_cik?: string | null;
  chamber: string | null;
  party: string | null;
  symbol?: string | null;
  ticker?: string | null;
  company_name?: string | null;
  role?: string | null;
  trade_count_total: number;
  trade_count_scored: number;
  avg_return: number | null;
  median_return: number | null;
  win_rate: number | null;
  avg_alpha: number | null;
  median_alpha: number | null;
  benchmark_symbol: string | null;
  pnl_status: string | null;
};

export type CongressTraderLeaderboardResponse = {
  lookback_days: number;
  chamber: CongressTraderLeaderboardChamber;
  source_mode: CongressTraderLeaderboardApiSourceMode;
  sort: CongressTraderLeaderboardSort;
  min_trades: number;
  limit: number;
  benchmark_symbol: string;
  rows: CongressTraderLeaderboardRow[];
};

export async function getMemberPerformance(
  bioguideId: string,
  params?: MemberAnalyticsParams,
): Promise<MemberPerformance> {
  return fetchJson<MemberPerformance>(
    buildApiUrl(`/api/members/${bioguideId}/performance`, {
      lookback_days: params?.lookback_days,
    }),
  );
}

export async function getMemberAlphaSummary(
  bioguideId: string,
  params?: MemberAnalyticsParams,
): Promise<MemberAlphaSummary> {
  return fetchJson<MemberAlphaSummary>(
    buildApiUrl(`/api/members/${bioguideId}/alpha-summary`, {
      lookback_days: params?.lookback_days,
    }),
  );
}

export async function getMemberTrades(
  bioguideId: string,
  params?: { lookback_days?: number; limit?: number },
): Promise<MemberTradesResponse> {
  return fetchJson<MemberTradesResponse>(
    buildApiUrl(`/api/members/${bioguideId}/trades`, {
      lookback_days: params?.lookback_days,
      limit: params?.limit,
    }),
  );
}

export async function getCongressTraderLeaderboard(params?: {
  lookback_days?: number;
  chamber?: CongressTraderLeaderboardChamber;
  source_mode?: CongressTraderLeaderboardSourceMode;
  sort?: CongressTraderLeaderboardSort;
  min_trades?: number;
  limit?: number;
}): Promise<CongressTraderLeaderboardResponse> {
  return fetchJson<CongressTraderLeaderboardResponse>(
    buildApiUrl("/api/leaderboards/congress-traders", {
      lookback_days: params?.lookback_days,
      chamber: params?.chamber,
      source_mode: params?.source_mode,
      sort: params?.sort,
      min_trades: params?.min_trades,
      limit: params?.limit,
    }),
  );
}

export async function getTickerProfile(symbol: string): Promise<TickerProfile> {
  return fetchJson<TickerProfile>(buildApiUrl(`/api/tickers/${symbol}`));
}

export async function getTickerPriceHistory(symbol: string, days: number): Promise<TickerPriceHistoryResponse> {
  return fetchJson<TickerPriceHistoryResponse>(buildApiUrl(`/api/tickers/${symbol}/price-history`, { days }));
}


export async function getTickerProfiles(symbols: string[]): Promise<TickerProfilesMap> {
  const normalized = Array.from(
    new Set(
      symbols
        .map((symbol) => symbol.trim().toUpperCase())
        .filter(Boolean)
    )
  );

  if (normalized.length === 0) return {};

  return fetchJson<TickerProfilesMap>(buildApiUrl("/api/tickers", { symbols: normalized.join(",") }));
}

export async function listWatchlists(): Promise<WatchlistSummary[]> {
  return fetchJson<WatchlistSummary[]>(buildApiUrl("/api/watchlists"));
}

export async function createWatchlist(name: string): Promise<WatchlistSummary> {
  return fetchJson<WatchlistSummary>(buildApiUrl("/api/watchlists"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
}

export async function renameWatchlist(id: number, name: string): Promise<WatchlistSummary> {
  return fetchJson<WatchlistSummary>(buildApiUrl(`/api/watchlists/${id}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
}

export async function getWatchlist(id: number): Promise<WatchlistDetail> {
  return fetchJson<WatchlistDetail>(buildApiUrl(`/api/watchlists/${id}`));
}

export async function addToWatchlist(id: number, symbol: string) {
  return fetchJson<{ status: string; symbol: string }>(buildApiUrl(`/api/watchlists/${id}/add`, { symbol }), {
    method: "POST",
  });
}

export async function removeFromWatchlist(id: number, symbol: string) {
  return fetchJson<{ status: string; symbol: string }>(buildApiUrl(`/api/watchlists/${id}/remove`, { symbol }), {
    method: "DELETE",
  });
}

export async function deleteWatchlist(id: number) {
  return fetchNoContent(buildApiUrl(`/api/watchlists/${id}`), {
    method: "DELETE",
  });
}

export async function getWatchlistFeed(id: number, params: QueryParams): Promise<FeedResponse> {
  return fetchJson<FeedResponse>(buildApiUrl(`/api/watchlists/${id}/feed`, params));
}
