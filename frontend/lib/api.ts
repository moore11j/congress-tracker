import type { FeedResponse, MemberProfile, TickerProfile, WatchlistDetail, WatchlistSummary } from "@/lib/types";

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  process.env.API_BASE_URL ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

function buildApiUrl(path: string, params?: QueryParams) {
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

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    cache: "no-store",
    ...init,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Request failed: ${response.status} ${response.statusText}. ${text}`);
  }

  return (await response.json()) as T;
}

export async function getFeed(params: QueryParams): Promise<FeedResponse> {
  return fetchJson<FeedResponse>(buildApiUrl("/api/feed", params));
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
  return fetchJson<{ status?: string }>(buildApiUrl(`/api/watchlists/${id}`), {
    method: "DELETE",
  });
}

export async function getWatchlistFeed(id: number, params: QueryParams): Promise<FeedResponse> {
  return fetchJson<FeedResponse>(buildApiUrl(`/api/watchlists/${id}/feed`, params));
}
