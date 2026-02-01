import type { FeedResponse, MemberProfile, TickerProfile, WatchlistDetail, WatchlistSummary } from "@/lib/types";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

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
    const contentType = response.headers.get("content-type") || "";
    let detail = "";

    try {
      if (contentType.includes("application/json")) {
        const data = await response.json();
        detail = data?.detail ? String(data.detail) : JSON.stringify(data);
      } else {
        detail = await response.text();
      }
    } catch {
      detail = await response.text().catch(() => "");
    }

    throw new Error(detail || `Request failed: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

async function fetchNoContent(url: string, init?: RequestInit): Promise<void> {
  const response = await fetch(url, { cache: "no-store", ...init });

  if (!response.ok) {
    const contentType = response.headers.get("content-type") || "";
    let detail = "";

    try {
      if (contentType.includes("application/json")) {
        const data = await response.json();
        detail = data?.detail ? String(data.detail) : JSON.stringify(data);
      } else {
        detail = await response.text();
      }
    } catch {
      detail = await response.text().catch(() => "");
    }

    throw new Error(detail || `Request failed: ${response.status} ${response.statusText}`);
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
};

export async function getFeed(params: QueryParams): Promise<EventsResponse> {
  const url = buildApiUrl("/api/events", params);
  if (process.env.NODE_ENV === "development") {
    console.info(`[feed] GET ${url}`);
  }
  return fetchJson<EventsResponse>(url);
}

export async function getEvents(params: Record<string, string | undefined>): Promise<EventsResponse> {
  return fetchJson<EventsResponse>(buildApiUrl("/api/events", params));
}

export async function getMemberProfile(bioguideId: string): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(buildApiUrl(`/api/members/${bioguideId}`));
}

export async function getTickerProfile(symbol: string): Promise<TickerProfile> {
  return fetchJson<TickerProfile>(buildApiUrl(`/api/tickers/${symbol}`));
}

export async function listWatchlists(): Promise<WatchlistSummary[]> {
  return fetchJson<WatchlistSummary[]>(buildApiUrl("/api/watchlists"));
}

export async function createWatchlist(name: string): Promise<WatchlistSummary> {
  return fetchJson<WatchlistSummary>(buildApiUrl("/api/watchlists", { name }), {
    method: "POST",
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
