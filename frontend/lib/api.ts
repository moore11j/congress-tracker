import type { FeedResponse, MemberProfile, TickerProfile, TickerProfilesMap, WatchlistDetail, WatchlistSummary } from "@/lib/types";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

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
  ticker?: string | null;
  source?: string | null;
  headline?: string | null;
  summary?: string | null;
  url?: string | null;
  impact_score?: number | null;
  payload?: any;
};

export type EventsResponse = {
  items: EventItem[];
  next_cursor: string | null;
  total?: number | null;
};


export type SuggestResponse = {
  items: string[];
};

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

export async function getFeed(params: QueryParams): Promise<EventsResponse> {
  const nextParams: QueryParams = { ...params };
  const tape = typeof nextParams.tape === "string" ? nextParams.tape.trim().toLowerCase() : "";

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

export async function getEvents(params: Record<string, string | undefined>): Promise<EventsResponse> {
  const normalizedEventType = normalizeEventType(params.event_type);
  const query = {
    ...params,
    event_type: normalizedEventType,
  };
  return fetchJson<EventsResponse>(buildApiUrl("/api/events", query), {
    cache: "no-store",
    next: { revalidate: 0 },
  });
}

export async function getMemberProfile(bioguideId: string): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(buildApiUrl(`/api/members/${bioguideId}`));
}

export async function getTickerProfile(symbol: string): Promise<TickerProfile> {
  return fetchJson<TickerProfile>(buildApiUrl(`/api/tickers/${symbol}`));
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
