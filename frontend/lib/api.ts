import type { FeedResponse, MemberProfile, TickerProfile, WatchlistDetail, WatchlistSummary } from "@/lib/types";

const DEFAULT_API_BASE_URL = "https://congress-tracker-api.fly.dev";

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  (process.env.NODE_ENV === "development" ? DEFAULT_API_BASE_URL : "");

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

export function buildApiUrl(path: string, params?: QueryParams) {
  if (!API_BASE_URL) {
    throw new Error("API base URL is not configured. Set NEXT_PUBLIC_API_BASE_URL.");
  }
  const url = new URL(path, API_BASE_URL);
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

export function getResolvedApiBaseUrl() {
  return API_BASE_URL || "";
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
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


export async function getFeed(params: QueryParams): Promise<FeedResponse> {
  return fetchJson<FeedResponse>(buildApiUrl("/api/feed", params));
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

export type EventsWithMeta = {
  data: EventsResponse;
  requestUrl: string;
};

function normalizeEventsResponse(response: Partial<EventsResponse> | null | undefined): EventsResponse {
  return {
    items: Array.isArray(response?.items) ? response?.items : [],
    next_cursor: response?.next_cursor ?? null,
  };
}

export async function getEventsWithMeta(params: Record<string, string | undefined>): Promise<EventsWithMeta> {
  const requestUrl = buildApiUrl("/api/events", params);
  const response = await fetchJson<EventsResponse>(requestUrl);
  return { data: normalizeEventsResponse(response), requestUrl };
}

export async function getEvents(params: Record<string, string | undefined>): Promise<EventsResponse> {
  const { data } = await getEventsWithMeta(params);
  return data;
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
