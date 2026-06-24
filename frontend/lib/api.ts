import type {
  ConfirmationMonitoringEventsResponse,
  ConfirmationMonitoringClearResponse,
  ConfirmationMonitoringRefreshResponse,
  ConfirmationScoreBundle,
  FeedResponse,
  InsightsNewsResponse,
  MacroSnapshotResponse,
  MemberProfile,
  MonitoringCounts,
  MonitoringInboxResponse,
  NewsItem,
  PressReleasesResponse,
  SavedScreen,
  SavedScreenEventsResponse,
  SavedScreensResponse,
  SecFilingsResponse,
  TickerProfile,
  TickerProfilesMap,
  WatchlistDetail,
  WatchlistSummary,
} from "@/lib/types";
import { defaultEntitlements, entitlementTierStorageKey, storedEntitlementTier, type Entitlements } from "@/lib/entitlements";
import { normalizeTickerSymbol } from "@/lib/ticker";

const legacyAuthTokenStorageKey = "ct:authToken";
const legacyServerSessionSyncStorageKey = "ct:serverSessionToken";
export const backendSessionCookieName = "ct_session";
export const authHintCookieName = "ct_auth_hint";
export const entitlementHintCookieName = "ct_entitlement_hint";

export function clearLegacyAuthStorage() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(legacyAuthTokenStorageKey);
  window.sessionStorage.removeItem(legacyServerSessionSyncStorageKey);
}

export function hasClientAuthHint() {
  if (typeof window === "undefined") return false;
  clearLegacyAuthStorage();
  const hasCookieHint = document.cookie
    .split(";")
    .map((part) => part.trim())
    .includes(`${authHintCookieName}=1`);
  return hasCookieHint;
}

function notifyAuthChanged() {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new Event("ct:auth-updated"));
}

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;
type QueryParamsWithRequestOptions = Record<string, QueryValue | AbortSignal | undefined> & {
  authToken?: string;
  mode?: string;
  signal?: AbortSignal;
  source?: string;
};
type ApiRequestInit = RequestInit & {
  source?: string;
  component?: string;
  route?: string;
};

export const EVENTS_API_MAX_LIMIT = 100;
const CLIENT_CACHE_TTL_MS = 30_000;
const SEARCH_CACHE_TTL_MS = 20 * 60_000;
const EVENTS_CACHE_TTL_MS = 5_000;

export class ApiError extends Error {
  status: number;
  statusText: string;
  url: string;
  body: string;
  detail: unknown;

  constructor({
    status,
    statusText,
    url,
    body,
  }: {
    status: number;
    statusText: string;
    url: string;
    body: string;
  }) {
    const detail = parseApiErrorDetail(body);
    super(apiErrorMessage(status, statusText, detail, body));
    this.name = "ApiError";
    this.status = status;
    this.statusText = statusText;
    this.url = url;
    this.body = body;
    this.detail = detail;
  }
}

function parseApiErrorDetail(body: string): unknown {
  if (!body) return null;
  try {
    const parsed = JSON.parse(body) as { detail?: unknown; message?: unknown };
    return parsed.detail ?? parsed.message ?? parsed;
  } catch {
    return body;
  }
}

function structuredDetailMessage(item: unknown) {
  if (!item || typeof item !== "object") return "";
  const detail = item as { msg?: unknown; message?: unknown; loc?: unknown };
  const message = typeof detail.msg === "string" ? detail.msg : typeof detail.message === "string" ? detail.message : "";
  if (!message.trim()) return "";
  if (!Array.isArray(detail.loc)) return message.trim();
  const field = detail.loc
    .filter((part) => typeof part === "string" || typeof part === "number")
    .map(String)
    .filter((part) => part !== "body")
    .join(".");
  return field ? `${field}: ${message.trim()}` : message.trim();
}

function apiErrorMessage(status: number, statusText: string, detail: unknown, body: string) {
  if (typeof detail === "string" && detail.trim()) return detail.trim();
  if (Array.isArray(detail)) {
    const messages = detail.map(structuredDetailMessage).filter(Boolean);
    if (messages.length) return messages.join(" ");
  }
  if (detail && typeof detail === "object" && "message" in detail) {
    const message = (detail as { message?: unknown }).message;
    if (typeof message === "string" && message.trim()) return message.trim();
  }
  if (body && !body.trim().startsWith("{")) return body.trim().slice(0, 300);
  return `Request failed (${status} ${statusText}).`;
}

export type NormalizedEventType =
  | "congress_trade"
  | "congress_treasury_trade"
  | "congress_crypto_trade"
  | "insider_trade"
  | "institutional_buy"
  | "government_contract";

export function normalizeEventType(uiValue: string | null | undefined): NormalizedEventType | undefined {
  const normalized = (uiValue ?? "").trim().toLowerCase();
  if (!normalized || normalized === "all") return undefined;
  if (normalized === "congress" || normalized === "congress_trade") return "congress_trade";
  if (normalized === "congress_treasury_trade") return "congress_treasury_trade";
  if (normalized === "congress_crypto_trade") return "congress_crypto_trade";
  if (normalized === "insider" || normalized === "insider_trade") return "insider_trade";
  if (normalized === "institutional" || normalized === "institutional_buy") return "institutional_buy";
  if (normalized === "government_contracts" || normalized === "government_contract") return "government_contract";
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

function tickerPathSymbol(symbol: string) {
  return encodeURIComponent(normalizeTickerSymbol(symbol) ?? symbol.trim());
}

function apiDebugEnabled() {
  if (typeof window === "undefined") return false;
  if (process.env.NEXT_PUBLIC_API_DEBUG !== "1" && process.env.NEXT_PUBLIC_CT_DEBUG_FETCH !== "1") return false;
  return process.env.NODE_ENV !== "production" || process.env.NEXT_PUBLIC_APP_ENV === "staging";
}

function requestAttribution(init?: ApiRequestInit) {
  const component = (init?.component ?? init?.source ?? "unknown").trim() || "unknown";
  const route =
    init?.route ??
    (typeof window !== "undefined" ? window.location.pathname : "server");
  return { component, route };
}

function traceApiFetch(url: string, init?: ApiRequestInit) {
  if (!apiDebugEnabled()) return;
  let route = url;
  try {
    const parsed = new URL(url);
    route = `${parsed.pathname}${parsed.search}`;
  } catch {
    // Leave route as the original URL if it is not parseable.
  }
  const stackLine = new Error().stack?.split("\n").slice(3).find((line) => line.includes("frontend"))?.trim() ?? "unknown";
  const attribution = requestAttribution(init);
  console.info("[ct-api]", { method: init?.method ?? "GET", route, url, component: attribution.component, page: attribution.route, source: stackLine });
}

function withRequestAttribution(init?: ApiRequestInit): RequestInit {
  const { source: _source, component: _component, route: _route, ...fetchInit } = init ?? {};
  const headers = new Headers(fetchInit.headers);
  const attribution = requestAttribution(init);
  headers.set("X-Walnut-Route", attribution.route);
  headers.set("X-Walnut-Component", attribution.component);
  return { ...fetchInit, headers };
}

function requestInitWithEntitlements(init?: ApiRequestInit): RequestInit {
  const fetchInit = withRequestAttribution(init);
  const headers = new Headers(fetchInit.headers);
  if (typeof window !== "undefined") {
    clearLegacyAuthStorage();
    const tier = storedEntitlementTier();
    if (tier) headers.set("X-CT-Entitlement-Tier", tier);
  }
  return { ...fetchInit, credentials: fetchInit.credentials ?? "include", headers };
}

function rememberEntitlements(entitlements: Entitlements | null | undefined) {
  if (typeof window === "undefined" || !entitlements) return;
  const tier = entitlements.tier === "admin" || entitlements.tier === "pro" || entitlements.tier === "premium" ? entitlements.tier : "free";
  window.localStorage.setItem(entitlementTierStorageKey, tier);
  document.cookie = `${entitlementHintCookieName}=${tier}; Path=/; SameSite=Lax; Max-Age=${60 * 60 * 24 * 30}`;
}

function rememberAuthenticatedSession() {
  if (typeof window === "undefined") return;
  clearLegacyAuthStorage();
  document.cookie = `${authHintCookieName}=1; Path=/; SameSite=Lax; Max-Age=${60 * 60 * 24 * 30}`;
  resetClientApiCaches();
  notifyAuthChanged();
}

function forgetAuthenticatedSession() {
  if (typeof window === "undefined") return;
  clearLegacyAuthStorage();
  window.localStorage.removeItem(entitlementTierStorageKey);
  void clearFrontendSessionCookie();
  document.cookie = `${backendSessionCookieName}=; Path=/; SameSite=Lax; Max-Age=0`;
  document.cookie = `${authHintCookieName}=; Path=/; SameSite=Lax; Max-Age=0`;
  document.cookie = `${entitlementHintCookieName}=; Path=/; SameSite=Lax; Max-Age=0`;
  resetClientApiCaches();
  notifyAuthChanged();
}

function authHeaders(sessionToken?: string | null): Record<string, string> {
  if (!sessionToken || typeof window !== "undefined") return {};
  return { Cookie: `${backendSessionCookieName}=${sessionToken}` };
}

async function clearFrontendSessionCookie(): Promise<void> {
  if (typeof window === "undefined") return;
  clearLegacyAuthStorage();
  try {
    await fetch("/api/auth/session", { method: "DELETE", cache: "no-store" });
  } catch {
    // Best-effort cleanup; local auth state is still cleared below.
  }
}

let meCache: { value: MeResponse; expiresAt: number } | null = null;
let mePromise: Promise<MeResponse> | null = null;
const entitlementCache = new Map<string, { value: Entitlements; expiresAt: number }>();
const entitlementPromises = new Map<string, Promise<Entitlements>>();
let unreadCache: { value: MonitoringUnreadCountResponse; expiresAt: number } | null = null;
let unreadPromise: Promise<MonitoringUnreadCountResponse> | null = null;
const globalSearchCache = new Map<string, { value: GlobalSearchResponse; expiresAt: number }>();
const searchSuggestCache = new Map<string, { value: SearchSuggestResponse; expiresAt: number }>();
const searchSuggestPromises = new Map<string, Promise<SearchSuggestResponse>>();
const eventsCache = new Map<string, { value: EventsResponse; expiresAt: number }>();
const eventsPromises = new Map<string, Promise<EventsResponse>>();
const tickerDataCache = new Map<string, { value: unknown; expiresAt: number }>();
const tickerDataPromises = new Map<string, Promise<unknown>>();

function resetClientApiCaches() {
  if (typeof window === "undefined") return;
  meCache = null;
  mePromise = null;
  entitlementCache.clear();
  entitlementPromises.clear();
  searchSuggestCache.clear();
  searchSuggestPromises.clear();
  eventsCache.clear();
  eventsPromises.clear();
  tickerDataCache.clear();
  tickerDataPromises.clear();
  unreadCache = null;
  unreadPromise = null;
}

function createAbortError(): Error {
  if (typeof DOMException !== "undefined") {
    return new DOMException("The operation was aborted.", "AbortError");
  }
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

function raceWithAbort<T>(request: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return request;
  if (signal.aborted) return Promise.reject(createAbortError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(createAbortError());
    signal.addEventListener("abort", onAbort, { once: true });
    request.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}

async function clientCachedJson<T>(
  cacheKey: string,
  signal: AbortSignal | undefined,
  request: (signal?: AbortSignal) => Promise<T>,
  ttlMs = CLIENT_CACHE_TTL_MS,
): Promise<T> {
  if (typeof window === "undefined") return request(signal);
  const now = Date.now();
  const cached = tickerDataCache.get(cacheKey);
  if (cached && cached.expiresAt > now) return cached.value as T;
  const pending = tickerDataPromises.get(cacheKey) as Promise<T> | undefined;
  if (pending) return raceWithAbort(pending, signal);

  const next = request()
    .then((response) => {
      tickerDataCache.set(cacheKey, { value: response, expiresAt: Date.now() + ttlMs });
      return response;
    })
    .finally(() => {
      tickerDataPromises.delete(cacheKey);
    });
  tickerDataPromises.set(cacheKey, next);
  return raceWithAbort(next, signal);
}

async function fetchJson<T>(url: string, init?: ApiRequestInit): Promise<T> {
  let response: Response;
  traceApiFetch(url, init);

  try {
    response = await fetch(url, requestInitWithEntitlements({ cache: "no-store", ...init }));
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Fetch failed for ${url}: ${message}`);
  }

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new ApiError({ status: response.status, statusText: response.statusText, url, body: text });
  }

  return (await response.json()) as T;
}

async function fetchPublicJson<T>(url: string, init?: ApiRequestInit): Promise<T> {
  let response: Response;
  traceApiFetch(url, init);

  try {
    response = await fetch(url, withRequestAttribution({ cache: "no-store", ...init }));
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Fetch failed for ${url}: ${message}`);
  }

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new ApiError({ status: response.status, statusText: response.statusText, url, body: text });
  }

  return (await response.json()) as T;
}

async function fetchNoContent(url: string, init?: ApiRequestInit): Promise<void> {
  let response: Response;
  traceApiFetch(url, init);

  try {
    response = await fetch(url, requestInitWithEntitlements({ cache: "no-store", ...init }));
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Fetch failed for ${url}: ${message}`);
  }

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new ApiError({ status: response.status, statusText: response.statusText, url, body: text });
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
  estimated_trade_value?: number | null;
  estimated_price?: number | null;
  estimated_shares?: number | null;
  current_price?: number | null;
  display_price?: number | null;
  reported_price?: number | null;
  reported_price_currency?: string | null;
  pnl_pct?: number | null;
  return_pct?: number | null;
  pnl_source?: string | null;
  outcome_status?: string | null;
  outcome_skip_reason?: string | null;
  outcome_methodology?: string | null;
  outcome_error?: string | null;
  price_basis?: string | null;
  smart_score?: number | null;
  smart_band?: string | null;
  symbol_net_30d?: number | null;
  member_net_30d?: number | null;
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
  amount_min?: number | null;
  amount_max?: number | null;
  payload?: any;
};

export type EventsResponse = {
  items: EventItem[];
  next_cursor?: string | null;
  limit?: number | null;
  offset?: number | null;
  total?: number | null;
  status?: "ok" | "loading" | "no_data" | "unavailable" | string;
  item_count?: number;
  window_days?: number | null;
  updated_at?: string | null;
};

export type { InsightsNewsResponse, MacroSnapshotResponse, NewsItem, PressReleasesResponse, SecFilingsResponse };

type TickerContentStatus = "ok" | "loading" | "no_data" | "unavailable" | string;

function recordFromPayload(payload: unknown): Record<string, unknown> {
  return payload && typeof payload === "object" && !Array.isArray(payload) ? payload as Record<string, unknown> : {};
}

function firstArrayValue(record: Record<string, unknown>, keys: string[]): unknown[] {
  for (const key of keys) {
    const value = record[key];
    if (Array.isArray(value)) return value;
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const nested = value as Record<string, unknown>;
      if (Array.isArray(nested.items)) return nested.items;
      if (Array.isArray(nested.results)) return nested.results;
    }
  }
  return [];
}

function normalizeTickerContentStatus(rawStatus: unknown, itemCount: number): TickerContentStatus {
  const status = typeof rawStatus === "string" ? rawStatus.trim().toLowerCase() : "";
  if (status === "warming" || status === "pending") return "loading";
  if (status === "empty" || status === "no-data") return "no_data";
  if (status === "ok" || status === "loading" || status === "no_data" || status === "unavailable") return status;
  return itemCount > 0 ? "ok" : "no_data";
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function numberValue(value: unknown, fallback: number): number {
  return isFiniteNumber(value) ? value : fallback;
}

function normalizeTickerItemsResponse<TItem>(
  payload: unknown,
  options: {
    arrayKeys: string[];
    page: number;
    limit: number;
    windowDays?: number | null;
    emptyMessage: string;
  },
): {
  items: TItem[];
  status: TickerContentStatus;
  item_count: number;
  updated_at: string | null;
  message?: string | null;
  page: number;
  limit: number;
  has_next: boolean;
  window_days?: number | null;
} {
  const record = recordFromPayload(payload);
  const rawItems = Array.isArray(payload) ? payload : firstArrayValue(record, options.arrayKeys);
  const items = rawItems as TItem[];
  const itemCount = isFiniteNumber(record.item_count) ? record.item_count : items.length;
  const status = normalizeTickerContentStatus(record.status, itemCount);
  const message = typeof record.message === "string"
    ? record.message
    : status === "no_data"
      ? options.emptyMessage
      : undefined;
  return {
    ...record,
    items,
    status,
    item_count: itemCount,
    updated_at: typeof record.updated_at === "string" ? record.updated_at : typeof record.updatedAt === "string" ? record.updatedAt : null,
    message,
    page: numberValue(record.page, options.page),
    limit: numberValue(record.limit, options.limit),
    has_next: Boolean(record.has_next ?? record.hasNext),
    ...(options.windowDays !== undefined
      ? { window_days: numberValue(record.window_days, options.windowDays ?? 0) }
      : {}),
  };
}

function normalizeEventsResponse(payload: unknown, windowDays?: number | null): EventsResponse {
  const record = recordFromPayload(payload);
  const items = (Array.isArray(payload) ? payload : firstArrayValue(record, ["items", "events", "results", "data"])) as EventItem[];
  const itemCount = isFiniteNumber(record.item_count) ? record.item_count : items.length;
  return {
    ...record,
    items,
    status: normalizeTickerContentStatus(record.status, itemCount),
    item_count: itemCount,
    window_days: isFiniteNumber(record.window_days) ? record.window_days : windowDays ?? null,
    next_cursor: typeof record.next_cursor === "string" ? record.next_cursor : null,
    limit: isFiniteNumber(record.limit) ? record.limit : null,
    offset: isFiniteNumber(record.offset) ? record.offset : null,
    total: isFiniteNumber(record.total) ? record.total : null,
    updated_at: typeof record.updated_at === "string" ? record.updated_at : null,
  };
}

export type AlertTriggerType =
  | "cross_source_confirmation"
  | "smart_score_threshold"
  | "large_trade_threshold"
  | "congress_activity"
  | "insider_activity";

export type NotificationSubscription = {
  id: number;
  email: string;
  source_type: "watchlist" | "saved_view";
  source_id: string;
  source_name: string;
  source_payload?: Record<string, unknown> | null;
  frequency: "daily";
  only_if_new: boolean;
  active: boolean;
  alert_triggers: AlertTriggerType[];
  min_smart_score?: number | null;
  large_trade_amount?: number | null;
  last_delivered_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type NotificationSubscriptionPayload = {
  email?: string;
  source_type: "watchlist" | "saved_view";
  source_id: string;
  source_name: string;
  source_payload?: Record<string, unknown>;
  frequency?: "daily";
  only_if_new: boolean;
  active: boolean;
  alert_triggers: AlertTriggerType[];
  min_smart_score?: number | null;
  large_trade_amount?: number | null;
};

export type AccountUser = {
  id: number;
  user_display_id?: string | null;
  user_id_display?: string | null;
  email: string;
  original_email?: string | null;
  name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  country?: string | null;
  state_province?: string | null;
  postal_code?: string | null;
  city?: string | null;
  address_line1?: string | null;
  address_line2?: string | null;
  billing_location?: BillingLocation | null;
  billing_profile_complete?: boolean;
  billing_profile_missing_fields?: string[];
  auth_provider?: string | null;
  avatar_url?: string | null;
  role: "user" | "admin" | string;
  is_admin?: boolean;
  is_super_admin?: boolean;
  super_admin?: boolean;
  plan?: "free" | "premium" | string;
  status?: string;
  admin_flag?: string;
  entitlement_tier?: "free" | "premium" | "pro" | "admin";
  manual_tier_override?: "free" | "premium" | "pro" | null;
  monthly_price_override?: number | null;
  annual_price_override?: number | null;
  override_currency?: string | null;
  override_note?: string | null;
  subscription_price_amount?: number | null;
  billing_price_amount?: number | null;
  subscription_currency?: string | null;
  subscription_interval?: "monthly" | "annual" | null;
  billing_frequency?: "monthly" | "annual" | null;
  current_plan?: "free" | "premium" | "pro" | string | null;
  billing_interval?: "monthly" | "annual" | null;
  current_plan_amount_cents?: number | null;
  current_plan_currency?: string | null;
  current_plan_display?: string | null;
  total_paid_cents?: number | null;
  total_paid_currency?: string | null;
  total_paid_display?: string | null;
  last_payment_amount_cents?: number | null;
  last_payment_currency?: string | null;
  last_payment_display?: string | null;
  billing_price_source?: "stripe" | "billing" | "override" | "plan_default" | string | null;
  billing_price_display?: string | null;
  billing_interval_display?: string | null;
  billing_frequency_display?: string | null;
  subscription_status?: string | null;
  subscription_plan?: string | null;
  subscription_cancel_at_period_end?: boolean;
  access_expires_at?: string | null;
  is_suspended?: boolean;
  deleted_at?: string | null;
  deleted_by_user?: boolean;
  deletion_reason?: string | null;
  deletion_plan?: string | null;
  reactivation_expires_at?: string | null;
  reactivation_expired?: boolean;
  current_period_end?: string | null;
  subscription_state_label?: string | null;
  is_deleted?: boolean;
  email_verified_at?: string | null;
  email_verified?: boolean;
  email_verification_required?: boolean;
  created_at?: string | null;
  last_seen_at?: string | null;
  notifications?: AccountNotificationSettings;
};

export type BillingLocation = {
  first_name?: string | null;
  last_name?: string | null;
  country?: string | null;
  state_province?: string | null;
  postal_code?: string | null;
  city?: string | null;
  address_line1?: string | null;
  address_line2?: string | null;
};

export type AccountNotificationSettings = {
  alerts_enabled: boolean;
  email_notifications_enabled: boolean;
  watchlist_activity_notifications: boolean;
  signals_notifications: boolean;
};

export type AuthResponse = {
  authenticated: boolean;
  user: AccountUser;
  entitlements: Entitlements;
  return_to?: string;
  email_verification_required?: boolean;
  dev_verification_url?: string;
};

export type PasswordResetConfirmResponse = {
  ok: boolean;
  authenticated: false;
  redirect_to?: string;
};

export type MeResponse = {
  user: AccountUser | null;
  entitlements: Entitlements;
};

export type AccountSettingsResponse = {
  user: AccountUser;
  notifications: AccountNotificationSettings;
};

export type StripeConfigStatus = {
  configured: boolean;
  billing_enabled?: boolean;
  overall?: {
    ready: boolean;
    status: "ready" | "not_ready" | string;
    missing_env_vars: string[];
  };
  checkout?: {
    ready: boolean;
    status: "ready" | "not_ready" | string;
    missing_env_vars: string[];
    selected_plan?: string | null;
    selected_interval?: string | null;
    selected_price_id?: string | null;
  };
  webhooks?: {
    ready: boolean;
    status: "ready" | "not_ready" | string;
    missing_env_vars: string[];
  };
  missing_env_vars?: string[];
  missing_price_env_vars?: string[];
  missing_price_ids?: string[];
  secret_key: "configured" | "missing";
  price_id: string;
  monthly_price_id?: string;
  annual_price_id?: string;
  premium_monthly_price_id?: string;
  premium_annual_price_id?: string;
  pro_monthly_price_id?: string;
  pro_annual_price_id?: string;
  price_ids?: {
    premium_monthly: string;
    premium_annual: string;
    pro_monthly: string;
    pro_annual: string;
  };
  price_env_vars?: {
    premium_monthly: string;
    premium_annual: string;
    pro_monthly: string;
    pro_annual: string;
  };
  prices?: Record<
    string,
    {
      label: string;
      tier: string;
      billing_interval: string;
      env_name: string;
      price_id: string;
      configured: boolean;
    }
  >;
  webhook_secret: "configured" | "missing";
  portal_return_url: string;
  success_url: string;
  cancel_url: string;
  webhook_url: string;
  notes: string;
};

export type FeatureGate = {
  feature_key: string;
  required_tier: "free" | "premium" | "pro";
  description?: string | null;
};

export type PlanLimit = {
  feature_key: string;
  tier: "free" | "premium" | "pro";
  limit_value: number;
  label?: string;
  unit_singular?: string;
  unit_plural?: string;
  sort_order?: number;
};

export type PlanPrice = {
  tier: "free" | "premium" | "pro";
  billing_interval: "monthly" | "annual";
  amount_cents: number;
  currency: string;
};

export type StripeTaxReadinessCheck = {
  key: string;
  label: string;
  status: "ready" | "missing" | "optional" | string;
  detail: string;
  required: boolean;
};

export type StripeTaxSettingsPayload = {
  automatic_tax_enabled: boolean;
  require_billing_address: boolean;
  product_tax_code?: string | null;
  price_tax_behavior: "unspecified" | "exclusive" | "inclusive";
};

export type StripeTaxConfig = StripeTaxSettingsPayload & {
  configured: boolean;
  stripe_tax_status: "ready" | "not_ready" | string;
  stripe_dashboard_status: string;
  price_id: string;
  price_configured: boolean;
  secret_key: "configured" | "missing";
  webhook_secret: "configured" | "missing";
  business_support: {
    configured: boolean;
    fields: Record<string, boolean>;
  };
  readiness: {
    automatic_tax_enabled: boolean;
    requires_customer_location: boolean;
    has_required_customer_location: boolean;
    missing_fields: string[];
    should_prompt_for_location: boolean;
    can_start_checkout: boolean;
    note: string;
  };
  checks: StripeTaxReadinessCheck[];
  notes: string;
};

export type PlanConfigFeature = {
  feature_key: string;
  label: string;
  kind: "feature" | "limit" | string;
  description: string;
  required_tier: "free" | "premium" | "pro";
  unit_singular?: string;
  unit_plural?: string;
  sort_order: number;
  limits: {
    free: number;
    premium: number;
    pro: number;
  };
};

export type PlanConfigTier = {
  tier: "free" | "premium" | "pro";
  name: string;
  description: string;
  limits: Record<string, number>;
  prices: Record<"monthly" | "annual", PlanPrice | undefined>;
};

export type PlanConfig = {
  tiers: PlanConfigTier[];
  features: PlanConfigFeature[];
  feature_gates: FeatureGate[];
  plan_limits: PlanLimit[];
  plan_prices: PlanPrice[];
};

export type AdminSettings = {
  stripe: StripeConfigStatus;
  stripe_tax: StripeTaxConfig;
  oauth: {
    google_client_id: string;
  };
  users: AccountUser[];
  users_limit?: number;
  users_truncated?: boolean;
  feature_gates: FeatureGate[];
  features: Record<string, { required_tier: "free" | "premium" | "pro"; description: string }>;
  plan_config: PlanConfig;
};

export type BacktestStrategyType = "watchlist" | "saved_screen" | "congress" | "insider" | "custom_tickers";
export type BacktestSourceScope = "all_congress" | "house" | "senate" | "member" | "member_list" | "all_insiders" | "insider";
export type BacktestContributionFrequency = "none" | "monthly" | "quarterly" | "annually";
export type BacktestRebalancingFrequency = "monthly" | "quarterly" | "semi_annually" | "annually";

export type BacktestTickerInput = {
  symbol: string;
  allocation_pct?: number;
};

export type BacktestRunRequest = {
  strategy_type: BacktestStrategyType;
  watchlist_id?: number;
  saved_screen_id?: number;
  tickers?: string[] | BacktestTickerInput[];
  source_label?: string;
  source_scope?: BacktestSourceScope;
  member_id?: string;
  member_ids?: string[];
  insider_cik?: string;
  start_date: string;
  end_date: string;
  hold_days: 30 | 60 | 90 | 180 | 365;
  start_balance: number;
  contribution_amount: number;
  contribution_frequency: BacktestContributionFrequency;
  rebalancing_frequency: BacktestRebalancingFrequency;
  max_position_weight?: number;
  weighting: "equal";
  benchmark: "^GSPC";
};

export type BacktestSummary = {
  start_balance: number;
  ending_balance: number;
  benchmark_ending_balance: number;
  total_contributions: number;
  net_profit: number;
  strategy_return_pct: number;
  time_weighted_return_pct: number;
  benchmark_return_pct: number;
  alpha_pct: number;
  cagr_pct: number;
  sharpe_ratio: number | null;
  win_rate: number;
  max_drawdown_pct: number;
  volatility_pct: number;
  trade_count: number;
  positions_count: number;
  skipped_positions_count: number;
  skipped_reasons: string[];
  price_fallback_positions_count: number;
};

export type BacktestTimelinePoint = {
  date: string;
  strategy_value: number;
  benchmark_value: number;
  strategy_return_pct: number;
  benchmark_return_pct: number;
  active_positions: number;
  invested_pct: number;
  cash: number;
  daily_return_pct: number;
};

export type BacktestDiagnostics = {
  average_active_positions: number;
  max_active_positions: number;
  average_invested_pct: number;
  max_invested_pct: number;
  max_position_weight_observed: number;
  skipped_positions_count: number;
  skipped_reasons: string[];
  price_fallback_positions_count: number;
};

export type BacktestPosition = {
  symbol: string;
  entry_date: string;
  exit_date: string;
  entry_price: number;
  exit_price: number;
  return_pct: number;
  source_event_id?: number | null;
  source_label?: string | null;
  price_fallback_used?: boolean;
};

export type BacktestRunResponse = {
  summary: BacktestSummary;
  timeline: BacktestTimelinePoint[];
  positions: BacktestPosition[];
  assumptions: string[];
  diagnostics?: BacktestDiagnostics | null;
};

export type BacktestPresetsResponse = {
  today: string;
  defaults: {
    benchmark: "^GSPC";
    weighting: "equal";
    hold_days: 90;
    lookback_days: number;
    start_balance: number;
    contribution_amount: number;
    contribution_frequency: BacktestContributionFrequency;
    rebalancing_frequency: BacktestRebalancingFrequency;
    max_position_weight: number;
  };
  access: {
    tier: "free" | "premium" | "pro" | "admin";
    can_run: boolean;
    signed_in: boolean;
  };
  strategy_types: { key: BacktestStrategyType; label: string }[];
  lookback_options: { days: number; label: string }[];
  hold_day_options: { days: 30 | 60 | 90 | 180 | 365; label: string }[];
  benchmark_options: { symbol: "^GSPC"; label: string }[];
  contribution_frequency_options: { key: BacktestContributionFrequency; label: string }[];
  rebalancing_frequency_options: { key: BacktestRebalancingFrequency; label: string }[];
  source_scopes: {
    congress: { key: "all_congress" | "house" | "senate" | "member"; label: string }[];
    insider: { key: "all_insiders" | "insider"; label: string }[];
  };
  watchlists: { id: number; name: string; ticker_count: number }[];
  saved_screens: { id: number; name: string; last_refreshed_at?: string | null; updated_at?: string | null }[];
};

export type SalesLedgerPeriod =
  | "last_7_days"
  | "last_30_days"
  | "month_to_date"
  | "year_to_date"
  | "all_dates"
  | "current_month"
  | "current_quarter"
  | "current_year"
  | "last_month"
  | "last_quarter"
  | "last_year"
  | "custom";

export type BillingDocumentLinks = {
  invoice_number?: string | null;
  hosted_invoice_url?: string | null;
  invoice_pdf_url?: string | null;
  receipt_url?: string | null;
  has_stripe_document: boolean;
  fallback_message?: string | null;
};

export type BillingHistoryItem = {
  id: number;
  transaction_id: string;
  date_charged?: string | null;
  description: string;
  billing_period_type?: string | null;
  service_period_start?: string | null;
  service_period_end?: string | null;
  subtotal_amount?: number | null;
  tax_amount?: number | null;
  total_amount?: number | null;
  total_display: string;
  currency: string;
  status: string;
  refund_state: string;
  status_refund_state: string;
  documents: BillingDocumentLinks;
};

export type BillingHistoryResponse = {
  items: BillingHistoryItem[];
};

export type SalesLedgerSortBy = "date_charged" | "customer_name" | "gross_amount" | "country";
export type SalesLedgerSortDir = "asc" | "desc";

export type SalesLedgerParams = {
  period?: SalesLedgerPeriod;
  start_date?: string;
  end_date?: string;
  country?: string;
  sort_by?: SalesLedgerSortBy;
  sort_dir?: SalesLedgerSortDir;
  page?: number;
  page_size?: number;
};

export type SalesLedgerRow = {
  id: number;
  transaction_id: string;
  customer_name: string;
  date_charged?: string | null;
  description: string;
  country: string;
  state_province: string;
  net_revenue_amount: number;
  net_revenue_display: string;
  vat1_label: string;
  vat1_collected: number;
  vat1_collected_display: string;
  vat2_label: string;
  vat2_collected: number;
  vat2_collected_display: string;
  gross_amount: number;
  gross_amount_display: string;
  currency: string;
  status: string;
  refund_state: string;
  status_refund_state: string;
};

export type SalesLedgerResponse = {
  items: SalesLedgerRow[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  has_previous: boolean;
  has_next: boolean;
  filters: {
    period: SalesLedgerPeriod;
    start_date?: string | null;
    end_date?: string | null;
    country?: string | null;
  };
  sort: {
    sort_by: SalesLedgerSortBy;
    sort_dir: SalesLedgerSortDir;
  };
  summary: {
    net_revenue_amount: number;
    vat_collected: number;
    gross_amount: number;
  };
};

export type AdminReportsSummary = {
  active_free_users: number;
  active_premium_users: number;
  active_pro_users?: number;
  monthly_recurring_revenue: number;
  revenue_ytd: number;
  new_users_last_30_days: number;
  total_users: number;
  currency: string;
  generated_at: string;
  notes?: string[];
};

export type AdminUserPlanFilter = "all" | "free" | "premium" | "pro" | "admin";
export type AdminUserAdminFilter = "all" | "admin" | "non_admin";
export type AdminUserSortBy = "created_at" | "last_seen_at" | "email" | "name" | "country" | "plan" | "status";
export type AdminUserSortDir = "asc" | "desc";

export type AdminUsersParams = {
  search?: string;
  q?: string;
  plan?: AdminUserPlanFilter;
  status?: string;
  country?: string;
  admin?: AdminUserAdminFilter;
  sort_by?: AdminUserSortBy;
  sort_dir?: AdminUserSortDir;
  page?: number;
  page_size?: number;
};

export type AdminUsersResponse = {
  items: AccountUser[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  has_previous: boolean;
  has_next: boolean;
  filters: {
    plan: AdminUserPlanFilter;
    status?: string | null;
    country?: string | null;
    admin: AdminUserAdminFilter;
    search?: string | null;
  };
  sort: {
    sort_by: AdminUserSortBy;
    sort_dir: AdminUserSortDir;
  };
};

export type AdminPageAnalyticsPeriod = "24h" | "7d" | "30d";

export type AdminPageAnalyticsRow = {
  page: string;
  route_group: string;
  views: number;
  unique_users: number;
  authenticated_views: number;
  anonymous_views: number;
  auth_percent: number;
  paid_percent: number;
  pro_percent: number;
  mobile_percent: number;
  last_viewed_at?: string | null;
};

export type AdminPageAnalyticsResponse = {
  period: AdminPageAnalyticsPeriod;
  generated_at: string;
  top_pages: AdminPageAnalyticsRow[];
  low_usage_pages: AdminPageAnalyticsRow[];
  trend_by_day: Array<{ day: string; views: number }>;
};

export type AdminProviderUsageItem = {
  name: string;
  kind: string;
  count: number;
};

export type AdminProviderUsageEvent = {
  id?: number;
  provider: string;
  kind?: string;
  category?: string | null;
  endpoint?: string | null;
  symbol?: string | null;
  source?: string | null;
  route?: string | null;
  cache_status?: string | null;
  status_code?: string | number | null;
  success?: boolean;
  throttled?: boolean;
  error?: string | null;
  reason?: string | null;
  item_count?: number | null;
  budget_tier?: string | null;
  created_at?: string | null;
  ts?: string | null;
};

export type AdminEnrichmentQueueRow = {
  job_type: string;
  status?: string | null;
  reason?: string | null;
  error?: string | null;
  count?: number;
};

export type AdminProviderBudget = {
  plan_calls_per_minute?: number;
  soft_limit_per_minute?: number;
  hard_limit_per_minute?: number;
  throttle_limit_per_minute?: number;
  used_last_minute?: number;
  remaining_last_minute?: number;
  usage_pct?: number | null;
  soft_exceeded?: boolean;
  hard_exceeded?: boolean;
};

export type AdminProviderContentWrite = {
  category: string;
  symbol?: string | null;
  writes: number;
  items_written: number;
  latest_at?: string | null;
};

export type AdminProviderContentDiagnostic = {
  content_type: string;
  category: string;
  cache_hits: number;
  cache_misses: number;
  jobs_done: number;
  jobs_queued: number;
  jobs_failed: number;
  items_written: number;
  oldest_pending_at?: string | null;
};

export type AdminFredMacroCacheDiagnostic = {
  source: string;
  status: string;
  last_refresh_at?: string | null;
  missing_series: string[];
  stale_series: string[];
  error_series?: string[];
  error?: string | null;
  series: Array<{
    series_id: string;
    label?: string | null;
    block?: string | null;
    status?: string | null;
    cache_status?: string | null;
    last_refreshed_at?: string | null;
    latest_observation_date?: string | null;
    observation_count?: number;
    error?: string | null;
  }>;
};

export type AdminProviderUsageResponse = {
  provider: "fmp" | string;
  enabled: boolean;
  cache_mode: string;
  live_page_fetch_enabled: boolean;
  status: "ok" | "warning" | "critical";
  configured_calls_per_minute: number;
  calls_last_minute: number;
  calls_today: number;
  call_windows?: {
    last_1_min?: number;
    last_5_min?: number;
    last_1_hour?: number;
    last_24_hours?: number;
  };
  cache_hit_rate?: number | null;
  budget?: AdminProviderBudget;
  totals: {
    provider_calls: number;
    cache_hits: number;
    cache_misses: number;
    fallbacks: number;
    throttles: number;
    provider_errors: number;
  };
  top_routes: AdminProviderUsageItem[];
  top_categories: AdminProviderUsageItem[];
  reasons?: Array<{ reason: string; count: number }>;
  fallback_reasons?: Array<{ reason: string; count: number }>;
  content_writes?: AdminProviderContentWrite[];
  content_diagnostics?: AdminProviderContentDiagnostic[];
  fred_macro_cache?: AdminFredMacroCacheDiagnostic;
  warnings: string[];
  recommendation: string;
  recent_throttles: AdminProviderUsageEvent[];
  recent_errors: AdminProviderUsageEvent[];
  enrichment_queue?: {
    by_type_status: AdminEnrichmentQueueRow[];
    failed_by_reason: AdminEnrichmentQueueRow[];
    oldest_pending_job?: {
      id: number;
      job_type: string;
      symbol?: string | null;
      status: string;
      source?: string | null;
      reason?: string | null;
      created_at?: string | null;
      updated_at?: string | null;
    } | null;
    oldest_pending_content_job?: {
      id: number;
      job_type: string;
      symbol?: string | null;
      status: string;
      source?: string | null;
      reason?: string | null;
      created_at?: string | null;
      updated_at?: string | null;
    } | null;
    recent_successes_by_type?: Array<{ job_type: string; count: number }>;
    recent: Array<{
      id: number;
      job_type: string;
      symbol?: string | null;
      date_key?: string | null;
      window_key?: string | null;
      status: string;
      attempts: number;
      max_attempts: number;
      source?: string | null;
      reason?: string | null;
      error?: string | null;
      next_run_at?: string | null;
      updated_at?: string | null;
    }>;
  };
  cache_coverage?: {
    fundamentals_rows?: number;
    fundamentals_ok_rows?: number;
    fundamentals_avg_volume_rows?: number;
    technical_price_history_symbols?: number;
  };
};

export type AdminProviderSetting = {
  id: number;
  domain_key: string;
  active_provider: string;
  fallback_provider?: string | null;
  mode: string;
  is_enabled: boolean;
  allow_external_live_fetch: boolean;
  allow_user_route_sync_fetch: boolean;
  builder_safe_required: boolean;
  notes?: string | null;
  updated_by?: string | null;
  updated_at?: string | null;
};

export type AdminDataSourceDomain = {
  domain_key: string;
  data_domain: string;
  active_provider: string;
  fallback_provider?: string | null;
  source_type: string;
  mode: string;
  builder_safe_status: "safe" | "warning" | "unsafe" | string;
  endpoint_names: string[];
  last_successful_refresh?: string | null;
  last_attempted_refresh?: string | null;
  stale_status: "fresh" | "stale" | "missing" | string;
  cache_table?: string | null;
  row_count?: number | null;
  coverage?: string | null;
  last_error?: string | null;
  call_count_24h?: number | null;
  queue_depth?: number | null;
  settings: AdminProviderSetting;
  badges: string[];
  allowed_providers?: string[];
  allowed_fallbacks?: string[];
  allowed_modes?: string[];
  default_provider?: string;
  default_fallback?: string | null;
  default_mode?: string;
  provider_labels?: Record<string, string>;
  provider_help_text?: Record<string, string>;
  domain_help_text?: string | null;
  validation_warnings?: string[];
  can_save?: boolean;
  admin_actions: {
    can_run_dry_run: boolean;
    can_refresh_cache: boolean;
    can_view_diagnostics: boolean;
  };
  notes?: string | null;
};

export type AdminDataSourcesStatusResponse = {
  generated_at: string;
  provider_options: string[];
  mode_options: string[];
  provider_labels?: Record<string, string>;
  filters: string[];
  status_badges: string[];
  domains: AdminDataSourceDomain[];
  current_data_source_map: Record<string, string>;
  endpoint_map: Record<string, string[]>;
  tables: {
    existing_core: string[];
    official_shadow: string[];
  };
  diagnostics: {
    congress: Record<string, unknown>;
    insider: Record<string, unknown>;
    production_source_counts: Record<string, number>;
  };
  dry_run_commands: Record<string, string>;
  risks: string[];
};

export type AdminDataSourceSettingPatch = Partial<{
  active_provider: string | null;
  fallback_provider: string | null;
  mode: string;
  is_enabled: boolean;
  allow_external_live_fetch: boolean;
  allow_user_route_sync_fetch: boolean;
  builder_safe_required: boolean;
  notes: string | null;
  reason: string | null;
}>;

export type AdminDataSourceRunResponse = {
  status: string;
  domain_key: string;
  mode: string;
  dry_run: boolean;
  job?: {
    id?: number;
    job_type?: string;
    dedupe_key?: string;
    status?: string;
  };
};

export type AdminAiMarketingMode =
  | "ticker_thread_assist"
  | "congress_trade_angle"
  | "insider_buying_angle"
  | "unusual_signal_angle"
  | "pain_point_tool_alternative"
  | "manual_url_review";

export type AdminAiMarketingPlatform = "reddit" | "web_search_reddit" | "x_stub" | "facebook_manual";
export type AdminAiMarketingRecency = "any" | "day" | "week" | "month" | string;
export type AdminAiMarketingStatus = "new" | "emailed" | "dismissed" | "copied" | "archived";

export type AdminAiMarketingConfig = {
  openai_configured: boolean;
  openai_model: string;
  reddit_configured: boolean;
  reddit_status?: "pending" | "missing" | "configured" | string;
  reddit_missing: string[];
  web_search_reddit_configured?: boolean;
  web_search_reddit_status?: "missing" | "configured" | string;
  web_search_reddit_provider?: string | null;
  web_search_reddit_missing?: string[];
  manual_text_status?: "available" | string;
  x_status: string;
  facebook_status: string;
  warnings: string[];
  recipient: string;
  settings?: Record<string, AdminAiMarketingSetting>;
};

export type AdminAiMarketingSetting = {
  key: string;
  label: string;
  is_secret: boolean;
  configured: boolean;
  source: "admin_settings" | "server_env" | "default" | "missing" | string;
  source_label: string;
  required_for: string;
  masked_value?: string | null;
  value?: string | null;
  updated_at?: string | null;
  deprecated_admin_setting?: boolean;
};

export type AdminAiMarketingSettingsResponse = {
  items: AdminAiMarketingSetting[];
  config: AdminAiMarketingConfig;
};

export type AdminAiMarketingSettingsTestResponse = {
  ok: boolean;
  message: string;
  model?: string;
  status_code?: number;
};

export type AdminAiMarketingCampaign = {
  id: number;
  name: string;
  enabled: boolean;
  mode: AdminAiMarketingMode;
  platforms: AdminAiMarketingPlatform[];
  keywords: string[];
  tickers: string[];
  subreddits: string[];
  query_templates: string[];
  minimum_relevance_score: number;
  max_items_per_run: number;
  recency: AdminAiMarketingRecency;
  default_destination_page: string;
  include_disclosure: boolean;
  scheduled_digest_enabled: boolean;
  created_at?: string | null;
  updated_at?: string | null;
};

export type AdminAiMarketingCampaignPayload = Omit<AdminAiMarketingCampaign, "id" | "created_at" | "updated_at">;

export type AdminAiMarketingSuggestion = {
  id: number;
  opportunity_id: number;
  campaign_id?: number | null;
  model: string;
  relevance_score: number;
  spam_risk_score: number;
  detected_tickers: string[];
  intent: "question" | "complaint" | "trade_idea" | "tool_search" | "news_reaction" | "other";
  recommended_action: "reply" | "skip" | "monitor";
  reply_angle:
    | "margin_analysis"
    | "ticker_context"
    | "congress_activity"
    | "insider_activity"
    | "government_contracts"
    | "screener_tool"
    | "general_market_context"
    | "other";
  value_added_insight: string;
  walnut_feature_to_mention: string;
  suggested_destination_url: string;
  suggested_reply: string;
  alternate_reply_more_direct: string;
  short_reason: string;
  compliance_notes: string;
  prompt_version: string;
  created_at?: string | null;
};

export type AdminAiMarketingOpportunity = {
  id: number;
  campaign_id?: number | null;
  platform: AdminAiMarketingPlatform | string;
  source_provider?: string | null;
  source_id?: string | null;
  source_url: string;
  title: string;
  excerpt?: string | null;
  author?: string | null;
  community?: string | null;
  source_score?: number | null;
  comment_count?: number | null;
  source_created_at?: string | null;
  status: AdminAiMarketingStatus;
  matched_keywords: string[];
  matched_tickers: string[];
  relevance_score?: number | null;
  spam_risk_score?: number | null;
  intent?: string | null;
  suggested_destination_url?: string | null;
  short_reason?: string | null;
  compliance_notes?: string | null;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  last_seen_at?: string | null;
  suggestion?: AdminAiMarketingSuggestion | null;
};

export type AdminAiMarketingCampaignsResponse = {
  items: AdminAiMarketingCampaign[];
  config: AdminAiMarketingConfig;
};

export type AdminAiMarketingOpportunitiesResponse = {
  items: AdminAiMarketingOpportunity[];
  config: AdminAiMarketingConfig;
};

export type AdminAiMarketingRunResponse = {
  created: number;
  deduped: number;
  suggested: number;
  warnings: string[];
  opportunities: AdminAiMarketingOpportunity[];
};

export type AdminAiMarketingManualResponse = {
  opportunity: AdminAiMarketingOpportunity;
  warning?: string | null;
};

export type AdminAiMarketingEmailDigestResponse = {
  to_email?: string;
  subject?: string;
  body_text?: string;
  body_html?: string;
  count: number;
  items: AdminAiMarketingOpportunity[];
  delivery?: AdminEmailDelivery;
  email_log?: {
    id: number;
    delivery_id?: number | null;
    to_email: string;
    subject: string;
    opportunity_ids: string[];
    status: string;
    created_at?: string | null;
    sent_at?: string | null;
  };
};

export type AdminEmailTemplate = {
  id: number;
  template_key: string;
  name: string;
  category: string;
  from_name: string;
  from_email: string;
  reply_to?: string | null;
  subject: string;
  preheader?: string | null;
  body_text: string;
  body_html?: string | null;
  variables: string[];
  variables_json: string;
  enabled: boolean;
  created_at?: string | null;
  updated_at?: string | null;
};

export type AdminEmailTemplateUpdatePayload = Partial<{
  name: string;
  category: string;
  from_name: string;
  from_email: string;
  reply_to: string | null;
  subject: string;
  preheader: string | null;
  body_text: string;
  body_html: string | null;
  variables_json: string;
  enabled: boolean;
}>;

export type AdminEmailRendered = {
  subject: string;
  body_text: string;
  body_html?: string | null;
};

export type AdminEmailTemplatePreviewResponse = {
  template: AdminEmailTemplate;
  rendered: AdminEmailRendered;
};

export type AdminEmailDelivery = {
  id: number;
  user_id?: number | null;
  to_email: string;
  from_email?: string | null;
  template_key?: string | null;
  category?: string | null;
  subject?: string | null;
  provider?: string | null;
  provider_message_id?: string | null;
  status: string;
  idempotency_key?: string | null;
  error?: string | null;
  payload?: Record<string, unknown> | null;
  created_at?: string | null;
  sent_at?: string | null;
};

export type AdminDigestSendTestPayload = {
  user_id?: number | null;
  email?: string | null;
  watchlist_id?: number | null;
  since?: string | null;
  lookback_days?: number;
  force?: boolean;
};

export type AdminBillingStatementSendTestPayload = {
  user_id?: number | null;
  email?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  force?: boolean;
};

export type AdminDigestSendResult = AdminEmailDelivery & {
  item_count?: number;
  items_count?: number;
  candidate_count?: number;
  qualified_count?: number;
  excluded_count?: number;
  excluded_reasons?: Record<string, number>;
  skip_reason?: string | null;
  window_start?: string | null;
  window_end?: string | null;
  rendered_preview?: {
    summary?: string;
    items_count?: number;
    sample_items?: Record<string, unknown>[];
    diagnostics?: {
      candidate_count?: number;
      qualified_count?: number;
      excluded_count?: number;
      excluded_reasons?: Record<string, number>;
    };
  };
};

export type AdminDigestRunNowPayload = {
  kind: "watchlist_activity" | "monitoring" | "signals";
  lookback_days?: number;
  limit?: number;
  force?: boolean;
  dry_run?: boolean;
};

export type AdminDigestRunNowResponse = {
  kind: "watchlist_activity" | "monitoring" | "signals";
  dry_run: boolean;
  force: boolean;
  lookback_days: number;
  limit: number;
  summary: {
    total: number;
    sent: number;
    log_only: number;
    queued: number;
    failed: number;
    skipped: number;
    would_send: number;
    item_count: number;
    candidate_count?: number;
    qualified_count?: number;
    excluded_count?: number;
    excluded_reasons?: Record<string, number>;
  };
  items: AdminDigestSendResult[];
};

export type AdminIntradayRunNowPayload = {
  lookback_minutes?: number;
  limit?: number;
  dry_run?: boolean;
  market_hours_only?: boolean;
};

export type AdminIntradayRunNowResponse = {
  dry_run: boolean;
  lookback_minutes: number;
  limit: number;
  market_hours_only: boolean;
  summary: {
    candidate_count: number;
    sent_count: number;
    skipped_count: number;
    would_send_count: number;
    failed_count: number;
    skip_reasons: Record<string, number>;
  };
  items: AdminDigestSendResult[];
};

export type AdminEmailDeliveriesResponse = {
  items: AdminEmailDelivery[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  filters?: {
    recipient?: string;
    status?: string;
    template_key?: string;
    date_window?: string;
  };
};

export async function getEntitlements(authToken?: string, options?: { source?: string }): Promise<Entitlements> {
  const cacheKey = authToken ? `token:${authToken}` : "cookie";
  if (typeof window !== "undefined") {
    const cached = entitlementCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.value;
    const pending = entitlementPromises.get(cacheKey);
    if (pending) return pending;
  }

  const request = (async () => {
    try {
      const entitlements = await fetchJson<Entitlements>(buildApiUrl("/api/entitlements"), {
        headers: authHeaders(authToken),
        source: options?.source ?? "unknown",
      });
      rememberEntitlements(entitlements);
      if (typeof window !== "undefined") {
        entitlementCache.set(cacheKey, { value: entitlements, expiresAt: Date.now() + CLIENT_CACHE_TTL_MS });
      }
      return entitlements;
    } catch (error) {
      if (error instanceof ApiError && (error.status === 500 || error.status === 503)) {
        return { ...defaultEntitlements, status: "temporarily_unavailable" };
      }
      throw error;
    } finally {
      if (typeof window !== "undefined") entitlementPromises.delete(cacheKey);
    }
  })();

  if (typeof window !== "undefined") entitlementPromises.set(cacheKey, request);
  return request;
}

export async function getBacktestPresets(authToken?: string): Promise<BacktestPresetsResponse> {
  return fetchJson<BacktestPresetsResponse>(buildApiUrl("/api/backtests/presets"), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
  });
}

export async function runBacktest(payload: BacktestRunRequest, authToken?: string): Promise<BacktestRunResponse> {
  return fetchJson<BacktestRunResponse>(buildApiUrl("/api/backtests/run"), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
    body: JSON.stringify(payload),
  });
}

export async function login(payload: { email: string; password?: string; name?: string; admin_token?: string }): Promise<AuthResponse> {
  const response = await fetchJson<AuthResponse>(buildApiUrl("/api/auth/login"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  rememberAuthenticatedSession();
  rememberEntitlements(response.entitlements);
  return response;
}

export async function register(payload: {
  first_name: string;
  last_name: string;
  email: string;
  password: string;
  country: string;
  state_province?: string;
  postal_code: string;
  city: string;
  address_line1: string;
  address_line2?: string;
}): Promise<AuthResponse> {
  const response = await fetchJson<AuthResponse>(buildApiUrl("/api/auth/register"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  rememberAuthenticatedSession();
  rememberEntitlements(response.entitlements);
  return response;
}

export async function getGoogleAuthUrl(returnTo = "/?mode=all"): Promise<{ authorization_url: string; state: string }> {
  return fetchJson<{ authorization_url: string; state: string }>(
    buildApiUrl("/api/auth/google/start", { return_to: returnTo }),
  );
}

export async function completeGoogleSignIn(payload: {
  code: string;
  state: string;
  redirect_uri?: string;
}): Promise<AuthResponse> {
  const response = await fetchJson<AuthResponse>(buildApiUrl("/api/auth/google/callback"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  rememberAuthenticatedSession();
  rememberEntitlements(response.entitlements);
  return response;
}

export async function getMe(options?: { force?: boolean; source?: string }): Promise<MeResponse> {
  if (typeof window !== "undefined") {
    if (mePromise) return mePromise;
    if (!options?.force && meCache && meCache.expiresAt > Date.now()) return meCache.value;
  }

  const request = fetchJson<MeResponse>(buildApiUrl("/api/auth/me"), { source: options?.source ?? "unknown" })
    .then((response) => {
      rememberEntitlements(response.entitlements);
      if (typeof window !== "undefined") {
        meCache = { value: response, expiresAt: Date.now() + CLIENT_CACHE_TTL_MS };
        entitlementCache.set("cookie", { value: response.entitlements, expiresAt: Date.now() + CLIENT_CACHE_TTL_MS });
      }
      return response;
    })
    .finally(() => {
      if (typeof window !== "undefined") mePromise = null;
    });

  if (typeof window !== "undefined") mePromise = request;
  return request;
}

export async function logout(): Promise<void> {
  try {
    await fetchJson<{ status: string }>(buildApiUrl("/api/auth/logout"), { method: "POST" });
  } finally {
    forgetAuthenticatedSession();
  }
}

export async function getAccountSettings(): Promise<AccountSettingsResponse> {
  return fetchJson<AccountSettingsResponse>(buildApiUrl("/api/account/settings"));
}

export async function updateAccountProfile(payload: {
  first_name: string;
  last_name: string;
  country: string;
  state_province?: string;
  postal_code: string;
  city: string;
  address_line1: string;
  address_line2?: string;
}): Promise<AccountUser> {
  return fetchJson<AccountUser>(buildApiUrl("/api/account/profile"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function updateAccountPassword(payload: {
  current_password: string;
  new_password: string;
  confirm_password: string;
}): Promise<{ status: string }> {
  return fetchJson<{ status: string }>(buildApiUrl("/api/account/password"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function updateAccountNotifications(
  payload: AccountNotificationSettings,
): Promise<AccountNotificationSettings> {
  return fetchJson<AccountNotificationSettings>(buildApiUrl("/api/account/notifications"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function deleteAccount(confirmation: string): Promise<{
  status: string;
  deleted_at?: string | null;
  reactivation_expires_at?: string | null;
  current_period_end?: string | null;
  is_paid?: boolean;
}> {
  const response = await fetchJson<{
    status: string;
    deleted_at?: string | null;
    reactivation_expires_at?: string | null;
    current_period_end?: string | null;
    is_paid?: boolean;
  }>(buildApiUrl("/api/account/delete"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ confirmation }),
  });
  forgetAuthenticatedSession();
  return response;
}

export async function reactivateAccount(token: string): Promise<{
  status: "reactivated" | "already_active";
  email?: string | null;
  subscription_plan?: string | null;
  subscription_cancel_at_period_end?: boolean;
  current_period_end?: string | null;
  entitlement_tier?: string | null;
}> {
  const response = await fetchPublicJson<{
    status: "reactivated" | "already_active";
    email?: string | null;
    subscription_plan?: string | null;
    subscription_cancel_at_period_end?: boolean;
    current_period_end?: string | null;
    entitlement_tier?: string | null;
  }>(
    buildApiUrl("/api/account/reactivate"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    },
  );
  resetClientApiCaches();
  notifyAuthChanged();
  return response;
}

export async function requestPasswordReset(email: string): Promise<{ status: string; message: string }> {
  return fetchJson(buildApiUrl("/api/auth/password-reset/request"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
}

export async function confirmPasswordReset(payload: { token: string; password: string; confirm_password: string }): Promise<PasswordResetConfirmResponse> {
  const response = await fetchJson<PasswordResetConfirmResponse>(buildApiUrl("/api/auth/password-reset/confirm"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  forgetAuthenticatedSession();
  return response;
}

export async function createCheckoutSession(
  billingInterval: "monthly" | "annual" = "monthly",
  plan: "premium" | "pro" = "premium",
): Promise<{ id?: string | null; url?: string | null }> {
  return fetchJson(buildApiUrl("/api/billing/checkout-session"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ interval: billingInterval, plan }),
  });
}

export async function createCustomerPortalSession(): Promise<{ url?: string | null }> {
  return fetchJson(buildApiUrl("/api/billing/customer-portal"), { method: "POST" });
}

export type BillingRefreshResponse = {
  status: string;
  message?: string;
  user?: AccountUser;
};

export async function refreshBillingSubscription(): Promise<BillingRefreshResponse> {
  const response = await fetchJson<BillingRefreshResponse>(
    buildApiUrl("/api/billing/refresh-subscription"),
    { method: "POST" },
  );
  resetClientApiCaches();
  notifyAuthChanged();
  return response;
}

export async function verifyEmail(token: string): Promise<{ status: string; email: string; email_verified_at?: string | null }> {
  const response = await fetchJson<{ status: string; email: string; email_verified_at?: string | null }>(
    buildApiUrl("/api/account/verify-email", { token }),
    { method: "POST" },
  );
  resetClientApiCaches();
  notifyAuthChanged();
  return response;
}

export async function resendVerificationEmail(email?: string): Promise<{ status: string; message: string; email_verification_required?: boolean }> {
  return fetchJson(buildApiUrl("/api/account/resend-verification"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(email ? { email } : {}),
  });
}

export async function getAccountBillingHistory(limit = 25): Promise<BillingHistoryResponse> {
  return fetchJson<BillingHistoryResponse>(buildApiUrl("/api/account/billing/history", { limit }));
}

export async function getAdminSettings(): Promise<AdminSettings> {
  return fetchJson<AdminSettings>(buildApiUrl("/api/admin/settings", { include_users: 0 }), { source: "AdminSettings" });
}

export async function getAdminSalesLedger(params: SalesLedgerParams): Promise<SalesLedgerResponse> {
  return fetchJson<SalesLedgerResponse>(buildApiUrl("/api/admin/reports/sales-ledger", params));
}

export async function getAdminReportsSummary(): Promise<AdminReportsSummary> {
  return fetchJson<AdminReportsSummary>(buildApiUrl("/api/admin/reports/summary"));
}

export async function getAdminPageAnalytics(params: { period?: AdminPageAnalyticsPeriod; limit?: number }): Promise<AdminPageAnalyticsResponse> {
  return fetchJson<AdminPageAnalyticsResponse>(buildApiUrl("/api/admin/reports/page-analytics", params));
}

export async function getAdminProviderUsageFmp(): Promise<AdminProviderUsageResponse> {
  return fetchJson<AdminProviderUsageResponse>(buildApiUrl("/api/admin/provider-usage/fmp"));
}

export async function getAdminDataSourcesStatus(): Promise<AdminDataSourcesStatusResponse> {
  return fetchJson<AdminDataSourcesStatusResponse>(buildApiUrl("/api/admin/data-sources/status"), {
    cache: "no-store",
    next: { revalidate: 0 },
    source: "AdminDataSources",
  });
}

export async function updateAdminDataSourceSetting(
  domainKey: string,
  payload: AdminDataSourceSettingPatch,
): Promise<AdminProviderSetting> {
  return fetchJson<AdminProviderSetting>(
    buildApiUrl(`/api/admin/data-sources/settings/${encodeURIComponent(domainKey)}`),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      source: "AdminDataSources",
    },
  );
}

export async function runAdminDataSource(
  domainKey: string,
  payload: { mode?: string; reason?: string | null } = {},
): Promise<AdminDataSourceRunResponse> {
  return fetchJson<AdminDataSourceRunResponse>(
    buildApiUrl(`/api/admin/data-sources/run/${encodeURIComponent(domainKey)}`),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      source: "AdminDataSources",
    },
  );
}

export async function getAdminAiMarketingCampaigns(): Promise<AdminAiMarketingCampaignsResponse> {
  return fetchJson<AdminAiMarketingCampaignsResponse>(buildApiUrl("/api/admin/ai-marketing/campaigns"), {
    cache: "no-store",
    next: { revalidate: 0 },
    source: "AdminAiMarketing",
  });
}

export async function getAdminAiMarketingSettings(): Promise<AdminAiMarketingSettingsResponse> {
  return fetchJson<AdminAiMarketingSettingsResponse>(buildApiUrl("/api/admin/ai-marketing/settings"), {
    cache: "no-store",
    next: { revalidate: 0 },
    source: "AdminAiMarketing",
  });
}

export async function updateAdminAiMarketingSettings(payload: {
  updates?: Record<string, string | null>;
  clear?: string[];
}): Promise<AdminAiMarketingSettingsResponse> {
  return fetchJson<AdminAiMarketingSettingsResponse>(buildApiUrl("/api/admin/ai-marketing/settings"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ updates: {}, clear: [], ...payload }),
    source: "AdminAiMarketing",
  });
}

export async function testAdminAiMarketingOpenAI(): Promise<AdminAiMarketingSettingsTestResponse> {
  return fetchJson<AdminAiMarketingSettingsTestResponse>(
    buildApiUrl("/api/admin/ai-marketing/settings/test-openai"),
    { method: "POST", source: "AdminAiMarketing" },
  );
}

export async function testAdminAiMarketingReddit(): Promise<AdminAiMarketingSettingsTestResponse> {
  return fetchJson<AdminAiMarketingSettingsTestResponse>(
    buildApiUrl("/api/admin/ai-marketing/settings/test-reddit"),
    { method: "POST", source: "AdminAiMarketing" },
  );
}

export async function createAdminAiMarketingCampaign(
  payload: AdminAiMarketingCampaignPayload,
): Promise<AdminAiMarketingCampaign> {
  return fetchJson<AdminAiMarketingCampaign>(buildApiUrl("/api/admin/ai-marketing/campaigns"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    source: "AdminAiMarketing",
  });
}

export async function updateAdminAiMarketingCampaign(
  campaignId: number,
  payload: Partial<AdminAiMarketingCampaignPayload>,
): Promise<AdminAiMarketingCampaign> {
  return fetchJson<AdminAiMarketingCampaign>(
    buildApiUrl(`/api/admin/ai-marketing/campaigns/${campaignId}`),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      source: "AdminAiMarketing",
    },
  );
}

export async function runAdminAiMarketingCampaign(campaignId: number): Promise<AdminAiMarketingRunResponse> {
  return fetchJson<AdminAiMarketingRunResponse>(
    buildApiUrl(`/api/admin/ai-marketing/campaigns/${campaignId}/run`),
    { method: "POST", source: "AdminAiMarketing" },
  );
}

export async function getAdminAiMarketingOpportunities(params: {
  status?: string;
  campaign_id?: number;
  limit?: number;
} = {}): Promise<AdminAiMarketingOpportunitiesResponse> {
  return fetchJson<AdminAiMarketingOpportunitiesResponse>(
    buildApiUrl("/api/admin/ai-marketing/opportunities", params),
    {
      cache: "no-store",
      next: { revalidate: 0 },
      source: "AdminAiMarketing",
    },
  );
}

export async function updateAdminAiMarketingOpportunity(
  opportunityId: number,
  payload: { status?: AdminAiMarketingStatus },
): Promise<AdminAiMarketingOpportunity> {
  return fetchJson<AdminAiMarketingOpportunity>(
    buildApiUrl(`/api/admin/ai-marketing/opportunities/${opportunityId}`),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      source: "AdminAiMarketing",
    },
  );
}

export async function analyzeAdminAiMarketingManualUrl(payload: {
  url?: string | null;
  text?: string | null;
  title?: string | null;
  campaign_id?: number | null;
  generate?: boolean;
}): Promise<AdminAiMarketingManualResponse> {
  return fetchJson<AdminAiMarketingManualResponse>(buildApiUrl("/api/admin/ai-marketing/manual-url"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    source: "AdminAiMarketing",
  });
}

export async function regenerateAdminAiMarketingSuggestion(opportunityId: number): Promise<AdminAiMarketingSuggestion> {
  return fetchJson<AdminAiMarketingSuggestion>(
    buildApiUrl(`/api/admin/ai-marketing/suggestions/${opportunityId}/regenerate`),
    { method: "POST", source: "AdminAiMarketing" },
  );
}

export async function sendAdminAiMarketingEmailDigest(payload: {
  send?: boolean;
  opportunity_ids?: number[] | null;
  statuses?: string[] | null;
  limit?: number;
}): Promise<AdminAiMarketingEmailDigestResponse> {
  return fetchJson<AdminAiMarketingEmailDigestResponse>(
    buildApiUrl("/api/admin/ai-marketing/email-digest"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      source: "AdminAiMarketing",
    },
  );
}

export async function getAdminUsers(params: AdminUsersParams): Promise<AdminUsersResponse> {
  return fetchJson<AdminUsersResponse>(buildApiUrl("/api/admin/users", params));
}

export function recordPageView(payload: { path: string; referrer_path?: string | null; title?: string | null }): void {
  if (typeof window === "undefined") return;
  const url = buildApiUrl("/api/analytics/page-view");
  const sessionKey = "ct:analyticsSession";
  let sessionId = window.sessionStorage.getItem(sessionKey);
  if (!sessionId) {
    sessionId = window.crypto?.randomUUID ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    window.sessionStorage.setItem(sessionKey, sessionId);
  }
  const body = JSON.stringify({ ...payload, session_id: sessionId });
  const headers = { type: "application/json" } as const;
  if (navigator.sendBeacon) {
    const blob = new Blob([body], headers);
    if (navigator.sendBeacon(url, blob)) return;
  }
  clearLegacyAuthStorage();
  void fetch(url, {
    method: "POST",
    body,
    keepalive: true,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      "X-Walnut-Analytics-Session": sessionId,
    },
  }).catch(() => undefined);
}

export async function getAdminEmailTemplates(): Promise<{ items: AdminEmailTemplate[] }> {
  return fetchJson<{ items: AdminEmailTemplate[] }>(buildApiUrl("/api/admin/email/templates"));
}

export async function getAdminEmailTemplate(templateKey: string): Promise<AdminEmailTemplate> {
  return fetchJson<AdminEmailTemplate>(buildApiUrl(`/api/admin/email/templates/${encodeURIComponent(templateKey)}`));
}

export async function adminUpdateEmailTemplate(
  templateKey: string,
  payload: AdminEmailTemplateUpdatePayload,
): Promise<AdminEmailTemplate> {
  return fetchJson<AdminEmailTemplate>(buildApiUrl(`/api/admin/email/templates/${encodeURIComponent(templateKey)}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function adminResetEmailTemplateDefault(templateKey: string): Promise<AdminEmailTemplate> {
  return fetchJson<AdminEmailTemplate>(
    buildApiUrl(`/api/admin/email/templates/${encodeURIComponent(templateKey)}/reset-default`),
    {
      method: "POST",
    },
  );
}

export async function adminResetEmailTemplateDefaults(templateKeys?: string[]): Promise<{ items: AdminEmailTemplate[] }> {
  return fetchJson<{ items: AdminEmailTemplate[] }>(buildApiUrl("/api/admin/email/templates/reset-defaults"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ template_keys: templateKeys ?? null }),
  });
}

export async function adminPreviewEmailTemplate(
  templateKey: string,
  context: Record<string, unknown>,
): Promise<AdminEmailTemplatePreviewResponse> {
  return fetchJson<AdminEmailTemplatePreviewResponse>(
    buildApiUrl(`/api/admin/email/templates/${encodeURIComponent(templateKey)}/preview`),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ context }),
    },
  );
}

export async function adminSendTestEmailTemplate(
  templateKey: string,
  payload: { to_email?: string | null; context?: Record<string, unknown> },
): Promise<AdminEmailDelivery> {
  return fetchJson<AdminEmailDelivery>(
    buildApiUrl(`/api/admin/email/templates/${encodeURIComponent(templateKey)}/send-test`),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ context: {}, ...payload }),
    },
  );
}

export async function adminSendDigestTest(
  kind: "watchlist_activity" | "monitoring" | "signals",
  payload: AdminDigestSendTestPayload,
): Promise<AdminDigestSendResult> {
  const pathByKind = {
    watchlist_activity: "/api/admin/email/digests/watchlist-activity/send-test",
    monitoring: "/api/admin/email/digests/monitoring/send-test",
    signals: "/api/admin/email/digests/signals/send-test",
  };
  return fetchJson<AdminDigestSendResult>(buildApiUrl(pathByKind[kind]), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function adminRunEmailDigestsNow(payload: AdminDigestRunNowPayload): Promise<AdminDigestRunNowResponse> {
  return fetchJson<AdminDigestRunNowResponse>(buildApiUrl("/api/admin/email/digests/run-now"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function adminRunIntradayEmailAlertsNow(
  payload: AdminIntradayRunNowPayload,
): Promise<AdminIntradayRunNowResponse> {
  return fetchJson<AdminIntradayRunNowResponse>(buildApiUrl("/api/admin/email/intraday/run-now"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function adminSendMonthlyStatementTest(
  payload: AdminBillingStatementSendTestPayload,
): Promise<AdminDigestSendResult> {
  return fetchJson<AdminDigestSendResult>(buildApiUrl("/api/admin/email/billing/monthly-statement/send-test"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function getAdminEmailDeliveries(params: {
  status?: string;
  template_key?: string;
  recipient?: string;
  date_window?: string;
  page?: number;
  page_size?: number;
} = {}): Promise<AdminEmailDeliveriesResponse> {
  return fetchJson<AdminEmailDeliveriesResponse>(buildApiUrl("/api/admin/email/deliveries", params));
}

export async function downloadAdminSalesLedger(
  format: "xlsx" | "pdf",
  params: SalesLedgerParams,
): Promise<{ blob: Blob; filename: string }> {
  const response = await fetch(
    buildApiUrl(`/api/admin/reports/sales-ledger/export.${format}`, params),
    requestInitWithEntitlements({ cache: "no-store" }),
  );
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Export failed with HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
  }
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const match = disposition.match(/filename="?([^";]+)"?/i);
  return {
    blob: await response.blob(),
    filename: match?.[1] ?? `sales-ledger.${format}`,
  };
}

export async function downloadAdminUsers(
  format: "xlsx" | "pdf",
  params: AdminUsersParams,
): Promise<{ blob: Blob; filename: string }> {
  const response = await fetch(
    buildApiUrl(`/api/admin/users/export.${format}`, params),
    requestInitWithEntitlements({ cache: "no-store" }),
  );
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Export failed with HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
  }
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const match = disposition.match(/filename="?([^";]+)"?/i);
  return {
    blob: await response.blob(),
    filename: match?.[1] ?? `admin-users.${format}`,
  };
}

export async function downloadScreenerCsv(
  params: Record<string, string | number | null | undefined>,
  filenamePrefix = "screener",
): Promise<{ blob: Blob; filename: string; rowCap: number | null; exportedRows: number | null }> {
  const exportParams: QueryParams = { filename_prefix: filenamePrefix };
  Object.entries(params).forEach(([key, value]) => {
    if (key === "page" || key === "page_size") return;
    exportParams[key] = value;
  });
  const response = await fetch(buildApiUrl("/api/screener/export.csv", exportParams), requestInitWithEntitlements({ cache: "no-store" }));
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const detail = body?.detail;
    if (typeof detail?.message === "string") {
      throw new Error(detail.message);
    }
    if (typeof detail === "string") {
      throw new Error(detail);
    }
    const text = await response.text().catch(() => "");
    throw new Error(`Export failed with HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
  }
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const match = disposition.match(/filename="?([^";]+)"?/i);
  const rowCapHeader = response.headers.get("X-Screener-Export-Row-Cap");
  const exportedRowsHeader = response.headers.get("X-Screener-Exported-Rows");
  return {
    blob: await response.blob(),
    filename: match?.[1] ?? "screener.csv",
    rowCap: rowCapHeader ? Number(rowCapHeader) : null,
    exportedRows: exportedRowsHeader ? Number(exportedRowsHeader) : null,
  };
}

export async function adminUpdateOAuthSettings(googleClientId: string): Promise<{ google_client_id: string }> {
  return fetchJson<{ google_client_id: string }>(buildApiUrl("/api/admin/settings/oauth"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ google_client_id: googleClientId }),
  });
}

export async function adminUpdateStripeTaxSettings(payload: StripeTaxSettingsPayload): Promise<StripeTaxConfig> {
  return fetchJson<StripeTaxConfig>(buildApiUrl("/api/admin/settings/stripe-tax"), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function getPlanConfig(): Promise<PlanConfig> {
  return fetchPublicJson<PlanConfig>(buildApiUrl("/api/plan-config"), {
    cache: "force-cache",
    next: { revalidate: 3600 },
  });
}

export async function adminSetPremium(userId: number, tier: "free" | "premium" | "pro" | null): Promise<AccountUser> {
  return fetchJson<AccountUser>(buildApiUrl(`/api/admin/users/${userId}/premium`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tier }),
  });
}

export async function adminSuspendUser(userId: number, suspended: boolean): Promise<AccountUser> {
  return fetchJson<AccountUser>(buildApiUrl(`/api/admin/users/${userId}/suspend`), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ suspended }),
  });
}

export async function adminDeleteUser(userId: number): Promise<void> {
  return fetchNoContent(buildApiUrl(`/api/admin/users/${userId}`), { method: "DELETE" });
}

export async function adminUpdateFeatureGate(featureKey: string, requiredTier: "free" | "premium" | "pro"): Promise<FeatureGate> {
  return fetchJson<FeatureGate>(buildApiUrl(`/api/admin/feature-gates/${featureKey}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ required_tier: requiredTier }),
  });
}

export async function adminUpdatePlanLimit(featureKey: string, tier: "free" | "premium" | "pro", limitValue: number): Promise<PlanLimit> {
  return fetchJson<PlanLimit>(buildApiUrl(`/api/admin/plan-limits/${featureKey}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tier, limit_value: limitValue }),
  });
}

export async function adminUpdatePlanPrice(
  tier: "free" | "premium" | "pro",
  billingInterval: "monthly" | "annual",
  amountCents: number,
  currency = "USD",
): Promise<PlanPrice> {
  return fetchJson<PlanPrice>(buildApiUrl(`/api/admin/plan-prices/${tier}/${billingInterval}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ amount_cents: amountCents, currency }),
  });
}

export type PriceOverridePayload = {
  monthly_price_override?: number | null;
  annual_price_override?: number | null;
  override_currency?: string | null;
  override_note?: string | null;
};

export async function adminSetUserPriceOverride(userId: number, payload: PriceOverridePayload): Promise<AccountUser> {
  return fetchJson<AccountUser>(buildApiUrl(`/api/admin/users/${userId}/price-override`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function adminClearUserPriceOverride(userId: number): Promise<AccountUser> {
  return fetchJson<AccountUser>(buildApiUrl(`/api/admin/users/${userId}/price-override`), {
    method: "DELETE",
  });
}

export async function adminBatchUpdateUsers(payload: {
  user_ids: number[];
  tier?: "free" | "premium" | "pro" | null;
  suspended?: boolean | null;
  price_override?: PriceOverridePayload | null;
  clear_price_override?: boolean;
}): Promise<{ status: string; updated: number; items: AccountUser[] }> {
  return fetchJson(buildApiUrl("/api/admin/users/batch"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}


export type SymbolSuggestion = {
  symbol: string;
  name?: string | null;
  type?: "ticker" | "government_agency";
  id?: string | null;
  label?: string | null;
  subtitle?: string | null;
  route?: string | null;
};

export type SymbolSuggestResponse = {
  items: SymbolSuggestion[];
};

export type SearchSuggestKind = "ticker" | "member" | "insider" | "agency" | "event";

export type SearchSuggestResult = {
  kind: SearchSuggestKind;
  id: string;
  symbol?: string | null;
  label: string;
  subtitle?: string | null;
  href: string;
};

export type SearchSuggestResponse = {
  items: SearchSuggestResult[];
  results?: SearchSuggestResult[];
  query?: string;
};

export type SuggestResponse = {
  items: string[];
};

export type MemberInsiderSuggestion = {
  label: string;
  value: string;
  category: "congress" | "insider";
  bioguide_id?: string | null;
  party?: string | null;
  state?: string | null;
  chamber?: string | null;
  reporting_cik?: string | null;
  symbol?: string | null;
  company_name?: string | null;
  role?: string | null;
};

export type MemberInsiderSuggestResponse = {
  items: MemberInsiderSuggestion[];
};

export type GlobalSearchResult = {
  type: "ticker" | "member" | "insider" | "government_agency" | "event";
  id: string;
  label: string;
  subtitle?: string | null;
  symbol?: string | null;
  route: string;
  score?: number | null;
};

export type GlobalSearchResponse = {
  results: GlobalSearchResult[];
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

export type TickerChartMarkerKind = "congress" | "insider" | "signals" | "government_contract";

export type TickerChartMarkerMeta = {
  agency?: string | null;
  amount?: number | null;
  description?: string | null;
  event_subtype?: string | null;
  report_date?: string | null;
  modification_number?: string | null;
  action_type?: string | null;
  transaction_date?: string | null;
  filing_date?: string | null;
  shares?: number | null;
  value?: number | null;
  price?: number | null;
  signal_score?: number | null;
  signal_label?: string | null;
  source_event_id?: number | null;
};

export type TickerChartMarker = {
  id: string;
  event_id?: number | null;
  kind: TickerChartMarkerKind;
  date: string;
  actor: string;
  action: string;
  side?: "buy" | "sell" | string | null;
  amount_min?: number | null;
  amount_max?: number | null;
  detail?: string | null;
  score?: number | null;
  band?: string | null;
  label?: string | null;
  meta?: TickerChartMarkerMeta | null;
};

export type TickerChartQuote = {
  current_price: number | null;
  day_change: number | null;
  day_change_pct: number | null;
  market_cap: number | null;
  day_volume: number | null;
  average_volume: number | null;
  trailing_pe: number | null;
  beta: number | null;
  asof?: string | null;
};

export type TickerChartBundle = {
  symbol: string | null;
  company_name?: string | null;
  resolution: "daily";
  days: number;
  status?: "ok" | "loading" | "no_data" | "unavailable" | string;
  start_date: string | null;
  end_date: string | null;
  points?: TickerPriceHistoryPoint[];
  point_count?: number;
  requested_days?: number;
  updated_at?: string | null;
  prices: TickerPriceHistoryPoint[];
  benchmark: {
    symbol: string;
    label: string;
    points: TickerPriceHistoryPoint[];
  };
  markers: TickerChartMarker[];
  quote: TickerChartQuote;
  available_symbols?: string[];
};

export type TickerHydrationState = "ok" | "missing" | "loading" | "unavailable" | string;

export type TickerHydrationStatus = {
  symbol: string;
  critical: {
    profile: TickerHydrationState;
    quote: TickerHydrationState;
    chart_30d: TickerHydrationState;
    chart_365d: TickerHydrationState;
    fundamentals: TickerHydrationState;
    technicals: TickerHydrationState;
  };
  optional: {
    news: TickerHydrationState;
    financials: TickerHydrationState;
    press_releases: TickerHydrationState;
    sec_filings: TickerHydrationState;
  };
  missing_sections?: string[];
  should_request_hydration?: boolean;
  queued_jobs_count?: number;
  queued_jobs: Array<{
    id?: number;
    job_type: string;
    symbol?: string | null;
    status?: string;
    window_key?: string | null;
    priority?: number;
    reason?: string | null;
    updated_at?: string | null;
  }>;
  updated_at: string;
  enqueued_jobs?: Array<{ job_type: string; symbol: string; window_key?: string | null }>;
  jobs_enqueued_by_type?: Record<string, number>;
  already_pending_count?: number;
  skipped_invalid_count?: number;
  status?: string;
  refreshed?: {
    attempted: boolean;
    reason: string;
    calls: number;
    refreshed: string[];
  };
};

export type TickerFinancialsPoint = {
  period: string;
  date?: string | null;
  revenue?: number | null;
  netIncome?: number | null;
  eps?: number | null;
  grossMargin?: number | null;
  operatingMargin?: number | null;
  freeCashFlow?: number | null;
  operatingCashFlow?: number | null;
  capex?: number | null;
};

export type TickerEarningsPoint = {
  date: string;
  period: string;
  epsActual?: number | null;
  epsEstimate?: number | null;
  surprise?: number | null;
  surprisePct?: number | null;
  result: "beat" | "miss" | "inline" | "unknown" | string;
};

export type TickerFinancialForecast = {
  period?: string | null;
  date?: string | null;
  revenueEstimate?: number | null;
  revenueLow?: number | null;
  revenueHigh?: number | null;
  epsEstimate?: number | null;
  epsLow?: number | null;
  epsHigh?: number | null;
  earningsEstimate?: number | null;
  earningsLow?: number | null;
  earningsHigh?: number | null;
};

export type TickerFinancialForecasts = {
  nextQuarter?: TickerFinancialForecast | null;
  nextFiscalYear?: TickerFinancialForecast | null;
};

export type TickerFinancialSubsection<T = unknown> = {
  status: "ok" | "unavailable" | "limited" | "loading" | string;
  reason_code?: string | null;
  data?: T;
};

export type TickerFinancialsResponse = {
  symbol: string;
  companyName?: string | null;
  status: "ok" | "partial" | "loading" | "no_data" | "unavailable" | string;
  message?: string | null;
  sections_present?: string[];
  updated_at?: string | null;
  summary: {
    revenueTtm?: number | null;
    netIncomeTtm?: number | null;
    epsTtm?: number | null;
    trailingPE?: number | null;
    forwardPE?: number | null;
    grossMargin?: number | null;
    operatingMargin?: number | null;
    nextEarningsDate?: string | null;
    latestQuarter?: string | null;
    freeCashFlowTtm?: number | null;
    operatingCashFlowTtm?: number | null;
    debtToEquity?: number | null;
    currentRatio?: number | null;
    assetRatio?: number | null;
  };
  annual: TickerFinancialsPoint[];
  quarterly: TickerFinancialsPoint[];
  earnings: TickerEarningsPoint[];
  forecasts?: TickerFinancialForecasts | null;
  health?: Record<string, unknown>;
  section_statuses?: {
    income?: "ok" | "partial" | "unavailable" | string;
    earnings?: "ok" | "partial" | "unavailable" | string;
    cashFlow?: "ok" | "partial" | "unavailable" | string;
    cash_flow?: "ok" | "partial" | "unavailable" | string;
    forecasts?: "ok" | "partial" | "unavailable" | string;
    analyst_estimates?: "ok" | "partial" | "unavailable" | string;
    valuation?: "ok" | "partial" | "unavailable" | string;
    health?: "ok" | "partial" | "unavailable" | string;
  };
  sections?: Partial<Record<"income" | "cash_flow" | "earnings" | "analyst_estimates" | "valuation" | "health", unknown>>;
  subsections?: {
    income?: TickerFinancialSubsection<{ annual?: TickerFinancialsPoint[]; quarterly?: TickerFinancialsPoint[] }>;
    cash_flow?: TickerFinancialSubsection;
    earnings?: TickerFinancialSubsection<TickerEarningsPoint[]>;
    analyst_estimates?: TickerFinancialSubsection<TickerFinancialForecasts>;
    valuation?: TickerFinancialSubsection<{ trailingPE?: number | null; forwardPE?: number | null }>;
    health?: TickerFinancialSubsection<Record<string, unknown>>;
  };
  updatedAt: string;
};

export type TickerGovernmentContractItem = {
  symbol?: string | null;
  contract_id?: string | null;
  award_id?: string | null;
  award_date?: string | null;
  award_amount?: number | null;
  recipient_name?: string | null;
  raw_recipient_name?: string | null;
  awarding_agency?: string | null;
  awarding_sub_agency?: string | null;
  funding_agency?: string | null;
  funding_sub_agency?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  description?: string | null;
  contract_type?: string | null;
  source_url?: string | null;
  source?: string | null;
  mapping_method?: string | null;
  mapping_confidence?: number | null;
};

export type TickerGovernmentContractsResponse = {
  symbol: string | null;
  status: string;
  source_status?: string | null;
  lookback_days?: number;
  cutoff_date?: string | null;
  min_amount?: number;
  page?: number;
  limit?: number;
  total?: number;
  has_next?: boolean;
  contract_count?: number;
  total_award_amount?: number;
  largest_award_amount?: number | null;
  latest_award_date?: string | null;
  top_agency?: string | null;
  items: TickerGovernmentContractItem[];
};

export type DepartmentContractItem = {
  id: string;
  symbol?: string | null;
  companyName?: string | null;
  recipientName: string;
  amount: number | null;
  date: string | null;
  department: string;
  agency?: string | null;
  description?: string | null;
  awardId?: string | null;
};

export type DepartmentTickerItem = {
  symbol: string;
  companyName: string;
  totalAwarded: number;
  contractCount: number;
  latestAwardDate: string | null;
  topDescription: string | null;
};

export type DepartmentTrendPoint = {
  period: string;
  totalAwarded: number;
  contractCount: number;
};

export type DepartmentProfileResponse = {
  slug: string;
  name: string;
  aliases: string[];
  summary: {
    totalAwarded: number | null;
    contractCount: number;
    linkedTickerCount: number;
    latestAwardDate: string | null;
    topTicker: string | null;
    topCompany: string | null;
  };
  tickers: DepartmentTickerItem[];
  recentContracts: DepartmentContractItem[];
  largestContracts: DepartmentContractItem[];
  trend?: DepartmentTrendPoint[];
};

export type DepartmentListResponse = {
  items: Array<{
    slug: string;
    name: string;
    aliases: string[];
    totalAwarded: number;
    contractCount: number;
    linkedTickerCount: number;
    latestAwardDate: string | null;
  }>;
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
  security_name?: string | null;
  securityName?: string | null;
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
  display_price?: number | null;
  displayPrice?: number | null;
  display_price_currency?: string | null;
  displayPriceCurrency?: string | null;
  display_share_basis?: string | null;
  displayShareBasis?: string | null;
  reported_price?: number | null;
  reportedPrice?: number | null;
  reported_price_currency?: string | null;
  reportedPriceCurrency?: string | null;
  reported_share_basis?: string | null;
  reportedShareBasis?: string | null;
  price_normalization?: Record<string, unknown> | null;
  priceNormalization?: Record<string, unknown> | null;
  insider_name: string | null;
  reporting_cik: string | null;
  role: string | null;
  external_id: string | null;
  url: string | null;
  pnl_pct?: number | null;
  pnlPct?: number | null;
  pnl?: number | null;
  alpha_pct?: number | null;
  alphaPct?: number | null;
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
  metric_definitions?: Record<string, string>;
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
export type SignalSort = "smart" | "multiple" | "recent" | "amount" | "confirmation" | "freshness";
export type SignalConfirmationBand = "inactive" | "weak" | "moderate" | "strong" | "exceptional";
export type SignalConfirmationDirection = "bullish" | "bearish" | "neutral" | "mixed";
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
  state?: string | null;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  smart_score?: number;
  smart_band?: string;
  source?: string;
  price?: number | null;
  estimated_price?: number | null;
  current_price?: number | null;
  pnl_pct?: number | null;
  return_pct?: number | null;
  pnl_source?: string | null;
  outcome_status?: string | null;
  outcome_skip_reason?: string | null;
  outcome_methodology?: string | null;
  outcome_error?: string | null;
  price_basis?: string | null;
  pnlPct?: number | null;
  confirmation_30d?: EventItem["confirmation_30d"];
  confirmation_score?: number | null;
  confirmation_band?: SignalConfirmationBand | null;
  confirmation_direction?: SignalConfirmationDirection | null;
  confirmation_status?: string | null;
  confirmation_source_count?: number | null;
  confirmation_explanation?: string | null;
  is_multi_source?: boolean | null;
  why_now?: WhyNowBundle | null;
  signal_freshness?: SignalFreshnessBundle | null;
};

type SignalsAllResponse = SignalItem[] | { items?: SignalItem[]; debug?: unknown };

export type TickerSourceEntitlement = {
  source: string;
  required_plan?: "free" | "premium" | "pro" | null;
  lock_state?: "available" | "requires_login" | "premium_locked" | "pro_locked" | null;
  locked: boolean;
  available: boolean;
};

export type TickerSourceEntitlements = Record<string, TickerSourceEntitlement>;

export type TickerSignalsSummaryResponse = {
  symbol: string;
  status: "ok" | "empty" | "loading" | "unavailable" | string;
  lookback_days?: number;
  effective_window_days?: number;
  updated_at?: string | null;
  latest_signal_score: number | null;
  recent_signal_count: number;
  recent_count?: number;
  items: SignalItem[];
  price_volume?: {
    status?: string | null;
    direction?: string | null;
    title?: string | null;
    summary?: string | null;
    score?: number | null;
    lines?: string[];
    price_points?: number | null;
    latest_close?: number | null;
    previous_close?: number | null;
    change_pct_1d?: number | null;
    latest_volume?: number | null;
    avg_volume_20d?: number | null;
    volume_vs_avg?: number | null;
    latest_date?: string | null;
    inputs?: {
      has_price_series?: boolean;
      has_volume?: boolean;
      has_technicals?: boolean;
      point_count?: number | null;
    } | null;
  } | null;
  insiders?: {
    status?: string | null;
    direction?: string | null;
    title?: string | null;
    subtitle?: string | null;
    buy_count?: number;
    sell_count?: number;
    net_flow?: number | null;
  } | null;
  congress?: {
    status?: string | null;
    direction?: string | null;
    title?: string | null;
    subtitle?: string | null;
    buy_count?: number;
    sell_count?: number;
    net_flow?: number | null;
  } | null;
  signals?: {
    status?: string | null;
    direction?: string | null;
    title?: string | null;
    subtitle?: string | null;
    recent_count?: number;
    latest_score?: number | null;
  } | null;
  government_contracts?: {
    status?: string | null;
    direction?: string | null;
    title?: string | null;
    subtitle?: string | null;
    contract_count?: number;
    contract_value?: number | null;
    latest_date?: string | null;
    freshness_days?: number | null;
  } | null;
  source_entitlements?: TickerSourceEntitlements | null;
  confirmation_score_bundle?: ConfirmationScoreBundle | null;
  signal_freshness?: SignalFreshnessBundle | null;
};

export async function getSignalsAll(params: {
  mode?: SignalMode;
  side?: string;
  sort?: SignalSort;
  limit?: number;
  debug?: boolean;
  symbol?: string;
  confirmation_band?: "all" | "active" | "weak" | "moderate" | "strong" | "exceptional" | "strong_plus";
  confirmation_direction?: "all" | SignalConfirmationDirection;
  min_confirmation_sources?: number;
  multi_source_only?: boolean;
  authToken?: string;
  signal?: AbortSignal;
}): Promise<{ items: SignalItem[]; debug?: unknown }> {
  const url = buildApiUrl("/api/signals/all", {
    mode: params.mode ?? "all",
    side: params.side,
    sort: params.sort ?? "smart",
    limit: params.limit,
    debug: params.debug ? "1" : undefined,
    symbol: params.symbol,
    confirmation_band: params.confirmation_band,
    confirmation_direction: params.confirmation_direction,
    min_confirmation_sources: params.min_confirmation_sources,
    multi_source_only: params.multi_source_only ? "1" : undefined,
  });

  const data = await fetchJson<SignalsAllResponse>(url, {
    headers: authHeaders(params.authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    signal: params.signal,
  });

  if (Array.isArray(data)) {
    return { items: data };
  }

  return {
    items: Array.isArray(data.items) ? data.items : [],
    debug: data.debug,
  };
}

export async function getTickerSignalsSummary(
  symbol: string,
  params?: {
    side?: string;
    limit?: number;
    lookback_days?: number;
    authToken?: string;
    signal?: AbortSignal;
    source?: string;
  },
): Promise<TickerSignalsSummaryResponse> {
  const url = buildApiUrl(`/api/tickers/${symbol}/signals-summary`, {
    side: params?.side ?? "all",
    limit: params?.limit ?? 3,
    lookback_days: params?.lookback_days,
  });
  const request = (signal?: AbortSignal) => fetchJson<TickerSignalsSummaryResponse>(url, {
    headers: authHeaders(params?.authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    signal,
    source: params?.source ?? "TickerSignalsSummary",
  });
  const data = await clientCachedJson<TickerSignalsSummaryResponse>(
    `ticker-signals-summary:${url}`,
    params?.signal,
    request,
  );
  return {
    ...data,
    items: Array.isArray(data.items) ? data.items : [],
    recent_signal_count: Number.isFinite(data.recent_signal_count) ? data.recent_signal_count : 0,
    latest_signal_score: typeof data.latest_signal_score === "number" ? data.latest_signal_score : null,
  };
}

function suggestKindToGlobalType(kind: SearchSuggestKind): GlobalSearchResult["type"] {
  return kind === "agency" ? "government_agency" : kind;
}

function searchSuggestToGlobalResult(result: SearchSuggestResult): GlobalSearchResult {
  return {
    type: suggestKindToGlobalType(result.kind),
    id: result.id,
    label: result.kind === "ticker" && result.symbol ? result.symbol : result.label,
    subtitle: result.subtitle,
    symbol: result.symbol,
    route: result.href,
  };
}

export async function searchSuggest(q: string, limit = 8, options?: { signal?: AbortSignal; source?: string }): Promise<SearchSuggestResponse> {
  const normalized = q.trim().toLowerCase();
  const cacheKey = `${normalized}:${limit}`;
  if (typeof window !== "undefined") {
    const cached = searchSuggestCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.value;
    const pending = searchSuggestPromises.get(cacheKey);
    if (pending) return raceWithAbort(pending, options?.signal);
  }

  const request = fetchJson<SearchSuggestResponse>(buildApiUrl("/api/search/suggest", { q: normalized || q, limit }), {
    cache: "no-store",
    signal: options?.signal,
    source: options?.source ?? "FastSearchSuggest",
  }).then((response) => {
    const items = Array.isArray(response.items) ? response.items : Array.isArray(response.results) ? response.results : [];
    const normalizedResponse = { ...response, items };
    if (typeof window !== "undefined") {
      searchSuggestCache.set(cacheKey, { value: normalizedResponse, expiresAt: Date.now() + SEARCH_CACHE_TTL_MS });
    }
    return normalizedResponse;
  }).finally(() => {
    if (typeof window !== "undefined") searchSuggestPromises.delete(cacheKey);
  });

  if (typeof window !== "undefined") searchSuggestPromises.set(cacheKey, request);
  return raceWithAbort(request, options?.signal);
}

export function cachedSearchSuggest(q: string, limit = 8): SearchSuggestResponse | null {
  if (typeof window === "undefined") return null;
  const normalized = q.trim().toLowerCase();
  const cached = searchSuggestCache.get(`${normalized}:${limit}`);
  if (!cached || cached.expiresAt <= Date.now()) return null;
  return cached.value;
}

export async function suggestSymbols(
  q: string,
  tape: string,
  limit = 10,
  options?: { includeDepartments?: boolean; signal?: AbortSignal; source?: string },
): Promise<SymbolSuggestResponse> {
  const tapeValue = (tape || "").trim().toLowerCase();
  if (tapeValue === "all" || tapeValue === "") {
    const response = await searchSuggest(q, limit, { signal: options?.signal, source: options?.source ?? "SymbolSuggest" });
    const items = response.items
      .filter((item) => item.kind === "ticker" || (options?.includeDepartments && item.kind === "agency"))
      .map((item) => ({
        symbol: item.symbol || item.id,
        name: item.kind === "ticker" ? item.label : null,
        type: item.kind === "agency" ? "government_agency" as const : "ticker" as const,
        id: item.id,
        label: item.label,
        subtitle: item.subtitle,
        route: item.href,
      }));
    return { items };
  }

  return fetchJson<SymbolSuggestResponse>(buildApiUrl("/api/suggest/symbol", { q, tape, limit, include_departments: options?.includeDepartments ? 1 : undefined }), {
    cache: "no-store",
    signal: options?.signal,
    source: options?.source,
  });
}

export async function suggestMembers(q: string, limit = 10, options?: { signal?: AbortSignal; source?: string }): Promise<SuggestResponse> {
  return fetchJson<SuggestResponse>(buildApiUrl("/api/suggest/member", { q, limit }), {
    cache: "no-store",
    signal: options?.signal,
    source: options?.source,
  });
}

export async function suggestMemberInsiders(q: string, limit = 10, options?: { signal?: AbortSignal; source?: string }): Promise<MemberInsiderSuggestResponse> {
  return fetchJson<MemberInsiderSuggestResponse>(buildApiUrl("/api/suggest/member-insider", { q, limit }), {
    cache: "no-store",
    signal: options?.signal,
    source: options?.source,
  });
}

export async function suggestRoles(q: string, limit = 10): Promise<SuggestResponse> {
  return fetchJson<SuggestResponse>(buildApiUrl("/api/suggest/role", { q, limit }), {
    cache: "no-store",
  });
}

export async function globalSearch(q: string, limit = 8, options?: { signal?: AbortSignal; source?: string }): Promise<GlobalSearchResponse> {
  const normalized = q.trim().toLowerCase();
  const cacheKey = `${normalized}:${limit}`;
  if (typeof window !== "undefined") {
    const cached = globalSearchCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.value;
  }

  const suggestResponse = await searchSuggest(normalized || q, limit, { signal: options?.signal, source: options?.source ?? "GlobalSearch" });
  const response = { results: suggestResponse.items.map(searchSuggestToGlobalResult) };
  if (typeof window !== "undefined" && !options?.signal?.aborted) {
    globalSearchCache.set(cacheKey, { value: response, expiresAt: Date.now() + SEARCH_CACHE_TTL_MS });
  }
  return response;
}

export async function getEvents(params: QueryParamsWithRequestOptions & { tape?: string }): Promise<EventsResponse> {
  const { tape: rawTape, signal, source: sourceLabel, ...queryParams } = params;
  const nextParams: QueryParams = {};
  Object.entries(queryParams).forEach(([key, value]) => {
    if (value === null || value === undefined || typeof value === "string" || typeof value === "number") {
      nextParams[key] = value;
    }
  });
  const tape = typeof rawTape === "string" ? rawTape.trim().toLowerCase() : "";
  const parsedLimit = Number(nextParams.limit);
  const requestSignal = signal instanceof AbortSignal ? signal : undefined;
  const source = typeof sourceLabel === "string" ? sourceLabel : "Feed";

  if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
    nextParams.limit = Math.min(Math.floor(parsedLimit), EVENTS_API_MAX_LIMIT);
  }

  if (tape === "congress") {
    nextParams.event_type = "congress_trade,congress_treasury_trade,congress_crypto_trade";
  } else if (tape === "insider") {
    nextParams.event_type = "insider_trade";
  } else if (tape === "government_contracts" || tape === "government_contract") {
    nextParams.event_type = "government_contract";
  } else {
    delete nextParams.event_type;
  }

  const url = buildApiUrl("/api/events", nextParams);
  if (process.env.NODE_ENV === "development") {
    console.info(`[feed] GET ${url}`);
  }
  const windowDays = isFiniteNumber(nextParams.recent_days) ? nextParams.recent_days : null;
  const cacheKey = `events:${url}`;
  const canShortCache = nextParams.debug === undefined && !requestSignal?.aborted;
  if (canShortCache) {
    const now = Date.now();
    const cached = eventsCache.get(cacheKey);
    if (cached && cached.expiresAt > now) return cached.value;
    const pending = eventsPromises.get(cacheKey);
    if (pending) return raceWithAbort(pending, requestSignal);
  }

  const request = fetchJson<unknown>(url, {
    cache: "force-cache",
    next: { revalidate: 30 },
    signal: canShortCache ? undefined : requestSignal,
    source,
  }).then((response) => {
    const normalized = normalizeEventsResponse(response, windowDays);
    if (canShortCache) {
      eventsCache.set(cacheKey, { value: normalized, expiresAt: Date.now() + EVENTS_CACHE_TTL_MS });
      if (eventsCache.size > 80) {
        const staleKeys = [...eventsCache.entries()]
          .sort((left, right) => left[1].expiresAt - right[1].expiresAt)
          .slice(0, 20)
          .map(([key]) => key);
        staleKeys.forEach((key) => eventsCache.delete(key));
      }
    }
    return normalized;
  }).finally(() => {
    if (canShortCache) eventsPromises.delete(cacheKey);
  });

  if (canShortCache) eventsPromises.set(cacheKey, request);
  return raceWithAbort(request, requestSignal);
}

export async function getWatchlistEvents(id: number, params: QueryParamsWithRequestOptions): Promise<EventsResponse> {
  const { mode: rawMode, source: sourceLabel, signal, authToken: rawAuthToken, ...queryParams } = params;
  const nextParams: QueryParams = {};
  Object.entries(queryParams).forEach(([key, value]) => {
    if (value === null || value === undefined || typeof value === "string" || typeof value === "number") {
      nextParams[key] = value;
    }
  });
  const mode = typeof rawMode === "string" ? rawMode.trim().toLowerCase() : "";

  if (mode === "congress") {
    nextParams.types = "congress_trade";
  } else if (mode === "insider") {
    nextParams.types = "insider_trade";
  } else if (mode === "government_contracts" || mode === "government_contract") {
    nextParams.types = "government_contract";
  } else {
    delete nextParams.types;
  }

  const authToken = typeof rawAuthToken === "string" ? rawAuthToken : undefined;
  const source = typeof sourceLabel === "string" ? sourceLabel : "WatchlistPage";

  return fetchJson<EventsResponse>(buildApiUrl(`/api/watchlists/${id}/events`, nextParams), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    signal,
    source,
  });
}

export async function getWatchlistSignals(id: number, params: {
  mode?: SignalMode;
  side?: string;
  sort?: SignalSort;
  limit?: number;
  offset?: number;
  min_smart_score?: number;
  confirmation_band?: "all" | "active" | "weak" | "moderate" | "strong" | "exceptional" | "strong_plus";
  confirmation_direction?: "all" | SignalConfirmationDirection;
  min_confirmation_sources?: number;
  multi_source_only?: boolean;
  authToken?: string;
  signal?: AbortSignal;
  source?: string;
}): Promise<{ items: SignalItem[] }> {
  const data = await fetchJson<SignalItem[]>(buildApiUrl(`/api/watchlists/${id}/signals`, {
    mode: params.mode ?? "all",
    side: params.side,
    sort: params.sort ?? "smart",
    limit: params.limit,
    offset: params.offset,
    min_smart_score: params.min_smart_score,
    confirmation_band: params.confirmation_band,
    confirmation_direction: params.confirmation_direction,
    min_confirmation_sources: params.min_confirmation_sources,
    multi_source_only: params.multi_source_only ? "1" : undefined,
  }), {
    headers: authHeaders(params.authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    signal: params.signal,
    source: params.source ?? "WatchlistEvents",
  });

  return { items: Array.isArray(data) ? data : [] };
}

export async function getMemberProfile(bioguideId: string, options?: { source?: string; signal?: AbortSignal }): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(buildApiUrl(`/api/members/${bioguideId}`), {
    signal: options?.signal,
    source: options?.source ?? "MemberProfile",
  });
}

export async function getInsiderSummary(
  reportingCik: string,
  lookbackDays: number,
  issuer?: string,
  options?: { source?: string; signal?: AbortSignal },
): Promise<InsiderSummary> {
  return fetchJson<InsiderSummary>(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/summary`, {
      lookback_days: lookbackDays,
      issuer,
    }),
    {
      signal: options?.signal,
      source: options?.source ?? "InsiderSummary",
    },
  );
}

export async function getInsiderTrades(
  reportingCik: string,
  lookbackDays: number,
  limit = 50,
  issuer?: string,
  options?: { page?: number; source?: string; signal?: AbortSignal },
): Promise<{
  reporting_cik: string;
  lookback_days: number;
  total?: number;
  page?: number;
  limit?: number;
  has_next?: boolean;
  items: InsiderTrade[];
}> {
  return fetchJson(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/trades`, {
      lookback_days: lookbackDays,
      limit,
      page: options?.page,
      issuer,
    }),
    {
      signal: options?.signal,
      source: options?.source ?? "InsiderTrades",
    },
  );
}

export async function getInsiderTopTickers(
  reportingCik: string,
  lookbackDays: number,
  limit = 10,
  issuer?: string,
  options?: { source?: string; signal?: AbortSignal },
): Promise<{ reporting_cik: string; lookback_days: number; items: InsiderTopTicker[] }> {
  return fetchJson(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/top-tickers`, {
      lookback_days: lookbackDays,
      limit,
      issuer,
    }),
    {
      signal: options?.signal,
      source: options?.source ?? "InsiderTopTickers",
    },
  );
}

export async function getInsiderAlphaSummary(
  reportingCik: string,
  params?: { lookback_days?: number; issuer?: string; source?: string; signal?: AbortSignal },
): Promise<InsiderAlphaSummary> {
  return fetchJson<InsiderAlphaSummary>(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/alpha-summary`, {
      lookback_days: params?.lookback_days,
      issuer: params?.issuer,
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "InsiderAlphaSummary",
    },
  );
}


export async function getMemberProfileBySlug(
  slug: string,
  params?: { include_trades?: boolean; source?: string; signal?: AbortSignal },
): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(
    buildApiUrl(`/api/members/by-slug/${encodeURIComponent(slug)}`, {
      include_trades:
        params?.include_trades === undefined ? undefined : (params.include_trades ? 1 : 0),
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "MemberProfile",
    },
  );
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
  date?: string | null;
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
  strategy_return_pct?: number | null;
  strategy_value?: number | null;
  benchmark_value?: number | null;
  benchmark_running_return_pct?: number | null;
  alpha?: number | null;
  active_positions?: number | null;
};

export type BenchmarkPerformancePoint = {
  asof_date: string | null;
  cumulative_return_pct: number | null;
};

export type MemberPortfolioPoint = {
  asof_date: string | null;
  strategy_value: number | null;
  benchmark_value: number | null;
  strategy_return_pct: number | null;
  benchmark_return_pct: number | null;
  alpha_pct: number | null;
  daily_return_pct: number | null;
  active_positions: number | null;
  exposure_pct: number | null;
  cash_pct: number | null;
};

export type MemberPortfolioPosition = {
  source_event_id: number | null;
  symbol: string | null;
  side: string | null;
  entry_date: string | null;
  exit_date: string | null;
  trade_date?: string | null;
  report_date?: string | null;
  entry_price: number | null;
  exit_price: number | null;
  shares: number | null;
  market_value: number | null;
  return_pct: number | null;
  amount_min?: number | null;
  amount_max?: number | null;
  status: string | null;
  skip_reason: string | null;
  skip_category?: string | null;
  source_type?: "disclosed_trade" | "estimated_opening_position" | string | null;
  source_reason?: string | null;
  confidence?: string | null;
  estimated_opening_value?: number | null;
  raw_estimated_opening_value?: number | null;
};

export type MemberPortfolioSummary = {
  starting_value: number | null;
  ending_value: number | null;
  benchmark_ending_value: number | null;
  total_return_pct: number | null;
  benchmark_return_pct: number | null;
  alpha_pct: number | null;
  cagr_pct: number | null;
  max_drawdown_pct: number | null;
  volatility_pct: number | null;
  sharpe_ratio: number | null;
  win_rate_pct: number | null;
  average_exposure_pct: number | null;
  ending_cash_pct: number | null;
  points_count: number;
  positions_count: number;
  skipped_events_count: number;
  skip_reason_summary?: Record<string, number>;
  skip_diagnostics?: Record<string, number>;
};

export type MemberPortfolioPerformance = {
  status: string;
  persisted_only: boolean;
  run_id?: number | null;
  entity_type: string;
  entity_id: string;
  lookback_days: number;
  mode: string;
  benchmark_symbol: string | null;
  start_date?: string | null;
  end_date?: string | null;
  requested_start_date?: string | null;
  effective_start_date?: string | null;
  effective_end_date?: string | null;
  effective_window_days?: number | null;
  effective_window_reason?: string | null;
  no_active_holdings?: boolean | null;
  warmup_diagnostics?: {
    warmup_start_date?: string | null;
    visible_start_date?: string | null;
    warmup_days?: number | null;
    opening_positions_count?: number | null;
    sale_without_position_before_warmup?: number | null;
    sale_without_position_after_warmup?: number | null;
    opening_position_estimated?: boolean | null;
    estimated_opening_positions_count?: number | null;
    estimated_opening_positions_symbols?: string[];
    estimated_opening_positions_value?: number | null;
    raw_estimated_opening_value?: number | null;
    scaled_estimated_opening_value?: number | null;
    estimated_opening_scale_factor?: number | null;
    estimated_opening_exposure_pct?: number | null;
    estimated_opening_method?: string | null;
    estimated_opening_cap?: number | null;
    opening_holdings_from_warmup?: number | null;
    opening_holdings_from_annual_disclosure?: number | null;
    annual_disclosure_opening_positions_symbols?: string[];
    annual_disclosure_opening_positions_value?: number | null;
    sale_without_position_before_estimation?: number | null;
    sale_without_position_after_estimation?: number | null;
  } | null;
  warmup_start_date?: string | null;
  visible_start_date?: string | null;
  opening_positions_count?: number | null;
  sale_without_position_before_warmup?: number | null;
  sale_without_position_after_warmup?: number | null;
  opening_position_estimated?: boolean | null;
  estimated_opening_positions_count?: number | null;
  estimated_opening_positions_symbols?: string[];
  estimated_opening_positions_value?: number | null;
  raw_estimated_opening_value?: number | null;
  scaled_estimated_opening_value?: number | null;
  estimated_opening_scale_factor?: number | null;
  estimated_opening_exposure_pct?: number | null;
  estimated_opening_method?: string | null;
  estimated_opening_cap?: number | null;
  opening_holdings_from_warmup?: number | null;
  opening_holdings_from_annual_disclosure?: number | null;
  annual_disclosure_opening_positions_symbols?: string[];
  annual_disclosure_opening_positions_value?: number | null;
  sale_without_position_before_estimation?: number | null;
  sale_without_position_after_estimation?: number | null;
  curve_quality_status?: "good" | "warning" | "poor" | string | null;
  longest_flat_segment_days?: number | null;
  max_exposure_pct?: number | null;
  pct_days_with_price_gaps?: number | null;
  data_coverage_notes?: string[];
  public_safety_flags?: string[];
  summary: MemberPortfolioSummary | null;
  points: MemberPortfolioPoint[];
  positions?: MemberPortfolioPosition[];
  skip_reason_summary?: Record<string, number>;
  skip_diagnostics?: Record<string, number>;
};

export type MemberAlphaSummary = {
  member_id: string;
  lookback_days: number;
  benchmark_symbol: string | null;
  metric_definitions?: Record<string, string>;
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

export type CongressTraderLeaderboardTradeSort = "avg_alpha" | "avg_return" | "win_rate" | "trade_count";
export type CongressTraderLeaderboardPortfolioSort =
  | "alpha_pct"
  | "total_return_pct"
  | "cagr_pct"
  | "sharpe_ratio"
  | "max_drawdown_pct"
  | "win_rate_pct";
export type CongressTraderLeaderboardSort = CongressTraderLeaderboardTradeSort | CongressTraderLeaderboardPortfolioSort;
export type CongressTraderLeaderboardChamber = "all" | "house" | "senate";
export type CongressTraderLeaderboardSourceMode = "congress" | "insiders";
export type CongressTraderLeaderboardApiSourceMode = CongressTraderLeaderboardSourceMode | "all";
export type CongressTraderLeaderboardPerformanceModel = "outcomes" | "portfolio";
export type CongressTraderLeaderboardApiPerformanceModel = "trade_outcomes" | "portfolio";

export type CongressTraderLeaderboardRow = {
  rank: number;
  member_id: string;
  member_name: string;
  member_slug?: string | null;
  reporting_cik?: string | null;
  chamber: string | null;
  party: string | null;
  state?: string | null;
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
  bioguide_id?: string | null;
  portfolio_entity_id?: string | null;
  portfolio_run_id?: number | null;
  lookback_days?: number | null;
  mode?: string | null;
  starting_value?: number | null;
  ending_value?: number | null;
  benchmark_ending_value?: number | null;
  total_return_pct?: number | null;
  benchmark_return_pct?: number | null;
  alpha_pct?: number | null;
  cagr_pct?: number | null;
  max_drawdown_pct?: number | null;
  volatility_pct?: number | null;
  sharpe_ratio?: number | null;
  win_rate_pct?: number | null;
  average_exposure_pct?: number | null;
  avg_priced_invested_value_pct?: number | null;
  min_priced_invested_value_pct?: number | null;
  positions_count?: number | null;
  skipped_events_count?: number | null;
  points_count?: number | null;
  status?: string | null;
  status_message?: string | null;
  curve_quality_status?: "good" | "warning" | "poor" | string | null;
  public_safety_flags?: string[];
  data_coverage?: {
    status?: string | null;
    curve_quality_status?: "good" | "warning" | "poor" | string | null;
    public_safety_flags?: string[];
    avg_priced_invested_value_pct?: number | null;
    min_priced_invested_value_pct?: number | null;
    points_count?: number | null;
    positions_count?: number | null;
    skipped_events_count?: number | null;
  } | null;
};

export type CongressTraderLeaderboardResponse = {
  lookback_days: number;
  chamber: CongressTraderLeaderboardChamber;
  source_mode: CongressTraderLeaderboardApiSourceMode;
  performance_model?: CongressTraderLeaderboardApiPerformanceModel;
  persisted_only?: boolean;
  mode?: string;
  sort: CongressTraderLeaderboardSort;
  min_trades?: number;
  limit: number;
  benchmark_symbol: string;
  quality_filter_applied?: boolean;
  excluded_poor_quality_count?: number;
  included_quality_statuses?: string[];
  metadata?: {
    performance_model?: CongressTraderLeaderboardApiPerformanceModel;
    persisted_only?: boolean;
    lookback_days?: number;
    mode?: string;
    sort?: string;
    rows_returned?: number;
    missing_portfolio_runs_count?: number;
    quality_filter_applied?: boolean;
    excluded_poor_quality_count?: number;
    included_quality_statuses?: string[];
    generated_at?: string;
  };
  rows: CongressTraderLeaderboardRow[];
};

export type ScreenerApiActivityOverlay = {
  present: boolean;
  label: string;
  direction?: string | null;
  freshness_days?: number | null;
  locked?: boolean;
};

export type ScreenerApiRow = {
  symbol: string;
  company_name: string;
  sector?: string | null;
  industry?: string | null;
  market_cap?: number | null;
  price?: number | null;
  volume?: number | null;
  avg_volume?: number | null;
  rel_volume?: number | null;
  price_move_pct?: number | null;
  rsi?: number | null;
  macd_state?: string | null;
  trend_state?: string | null;
  beta?: number | null;
  country?: string | null;
  exchange?: string | null;
  trailing_pe?: number | null;
  forward_pe?: number | null;
  price_sales?: number | null;
  ev_ebitda?: number | null;
  gross_margin?: number | null;
  operating_margin?: number | null;
  net_margin?: number | null;
  roe?: number | null;
  roic?: number | null;
  revenue_growth?: number | null;
  eps_growth?: number | null;
  ebitda_growth?: number | null;
  fcf_growth?: number | null;
  debt_equity?: number | null;
  current_ratio?: number | null;
  net_debt_ebitda?: number | null;
  eps_ttm?: number | null;
  fcf?: number | null;
  fcf_margin?: number | null;
  earnings_yield?: number | null;
  congress_activity: ScreenerApiActivityOverlay;
  insider_activity: ScreenerApiActivityOverlay;
  confirmation: {
    score: number | null;
    band: string;
    direction: string;
    status: string;
    locked?: boolean;
  };
  why_now: {
    state: string;
    headline: string;
    locked?: boolean;
  };
  signal_freshness: {
    freshness_score: number | null;
    freshness_state: string;
    freshness_label: string;
    locked?: boolean;
  };
  ticker_url?: string;
  government_contracts_status?: string | null;
  government_contracts_active?: boolean | null;
  government_contracts_count?: number | null;
  government_contracts_total_amount?: number | null;
  government_contracts_latest_date?: string | null;
  government_contracts_top_agency?: string | null;
  options_flow_active?: boolean | null;
  options_flow_score?: number | null;
  options_flow_direction?: string | null;
  options_flow_intensity?: string | null;
  options_flow_total_premium?: number | null;
  options_flow_status?: string | null;
  options_flow_locked?: boolean | null;
  institutional_activity_active?: boolean | null;
  institutional_activity_direction?: string | null;
  institutional_activity_net_activity?: number | null;
  institutional_activity_institution_count?: number | null;
  institutional_activity_status?: string | null;
  institutional_activity_locked?: boolean | null;
};

export type ScreenerApiResponse = {
  items: ScreenerApiRow[];
  page: number;
  page_size: number;
  returned: number;
  total_available: number;
  has_next: boolean;
  lookback_days: number;
  result_cap?: number;
  overlay_availability?: {
    government_contracts?: { enabled: boolean; status: string; filterable: boolean; locked?: boolean; required_plan?: string | null };
    options_flow?: { enabled: boolean; status: string; filterable: boolean; locked?: boolean; required_plan?: string | null };
    institutional_activity?: { enabled: boolean; status: string; filterable: boolean; locked?: boolean; required_plan?: string | null };
  };
  access?: {
    tier?: string;
    intelligence_locked?: boolean;
    options_flow_locked?: boolean;
    institutional_activity_locked?: boolean;
    presets_locked?: boolean;
    monitoring_locked?: boolean;
    csv_export_locked?: boolean;
    saved_screens_limit?: number;
  };
};

export async function getScreener(params?: QueryParams & { authToken?: string }): Promise<ScreenerApiResponse> {
  const nextParams: QueryParams = { ...(params ?? {}) };
  const authToken = typeof params?.authToken === "string" ? params.authToken : undefined;
  delete nextParams.authToken;
  return fetchJson<ScreenerApiResponse>(buildApiUrl("/api/screener", nextParams), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
  });
}

export async function getMemberPerformance(
  bioguideId: string,
  params?: MemberAnalyticsParams & { source?: string; signal?: AbortSignal },
): Promise<MemberPerformance> {
  return fetchJson<MemberPerformance>(
    buildApiUrl(`/api/members/${bioguideId}/performance`, {
      lookback_days: params?.lookback_days,
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "MemberAnalytics",
    },
  );
}

export async function getMemberAlphaSummary(
  bioguideId: string,
  params?: MemberAnalyticsParams & { source?: string; signal?: AbortSignal },
): Promise<MemberAlphaSummary> {
  return fetchJson<MemberAlphaSummary>(
    buildApiUrl(`/api/members/${bioguideId}/alpha-summary`, {
      lookback_days: params?.lookback_days,
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "MemberAnalytics",
    },
  );
}

export async function getMemberPortfolioPerformance(
  bioguideId: string,
  params?: MemberAnalyticsParams & { mode?: string; source?: string; signal?: AbortSignal },
): Promise<MemberPortfolioPerformance> {
  return fetchJson<MemberPortfolioPerformance>(
    buildApiUrl(`/api/members/${bioguideId}/portfolio-performance`, {
      lookback_days: params?.lookback_days,
      mode: params?.mode,
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "MemberAnalytics",
    },
  );
}

export async function getMemberTrades(
  bioguideId: string,
  params?: { lookback_days?: number; limit?: number; source?: string; signal?: AbortSignal },
): Promise<MemberTradesResponse> {
  return fetchJson<MemberTradesResponse>(
    buildApiUrl(`/api/members/${bioguideId}/trades`, {
      lookback_days: params?.lookback_days,
      limit: params?.limit,
    }),
    {
      signal: params?.signal,
      source: params?.source ?? "MemberAnalytics",
    },
  );
}

export async function getCongressTraderLeaderboard(params?: {
  lookback_days?: number;
  chamber?: CongressTraderLeaderboardChamber;
  source_mode?: CongressTraderLeaderboardSourceMode;
  performance_model?: "portfolio" | "trade_outcomes" | "outcomes";
  mode?: string;
  sort?: CongressTraderLeaderboardSort;
  min_trades?: number;
  limit?: number;
  authToken?: string;
  signal?: AbortSignal;
  source?: string;
}): Promise<CongressTraderLeaderboardResponse> {
  const url = buildApiUrl("/api/leaderboards/congress-traders", {
    lookback_days: params?.lookback_days,
    chamber: params?.chamber,
    source_mode: params?.source_mode,
    performance_model: params?.performance_model === "outcomes" ? "trade_outcomes" : params?.performance_model,
    mode: params?.mode,
    sort: params?.sort,
    min_trades: params?.min_trades,
    limit: params?.limit,
  });
  const init: ApiRequestInit = {
    headers: authHeaders(params?.authToken),
    signal: params?.signal,
    source: params?.source ?? "Leaderboards",
  };
  try {
    return await fetchJson<CongressTraderLeaderboardResponse>(url, init);
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 503 || params?.signal?.aborted) throw error;
    await new Promise((resolve) => setTimeout(resolve, 1000));
    if (params?.signal?.aborted) throw error;
    return fetchJson<CongressTraderLeaderboardResponse>(url, init);
  }
}

export async function getTickerProfile(symbol: string, options?: { source?: string; signal?: AbortSignal }): Promise<TickerProfile> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}`);
  return clientCachedJson<TickerProfile>(
    `ticker-profile:${url}`,
    options?.signal,
    (signal) => fetchJson<TickerProfile>(url, {
      signal,
      source: options?.source ?? "TickerProfile",
    }),
  );
}

export async function getTickerGovernmentContracts(symbol: string, params?: { lookback_days?: number; min_amount?: number; limit?: number; page?: number; signal?: AbortSignal; source?: string }): Promise<TickerGovernmentContractsResponse> {
  return fetchJson<TickerGovernmentContractsResponse>(
    buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/government-contracts`, {
      lookback_days: params?.lookback_days,
      min_amount: params?.min_amount,
      limit: params?.limit,
      page: params?.page,
    }),
    { cache: "no-store", next: { revalidate: 0 }, signal: params?.signal, source: params?.source ?? "TickerGovernmentContracts" },
  );
}

export async function getTickerHydrationStatus(symbol: string, params?: { signal?: AbortSignal; source?: string }): Promise<TickerHydrationStatus> {
  return fetchPublicJson<TickerHydrationStatus>(
    buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/hydration-status`),
    { cache: "no-store", next: { revalidate: 0 }, signal: params?.signal, source: params?.source ?? "TickerHydrationStatus" },
  );
}

export async function requestTickerHydration(symbol: string, params?: { reason?: string; priority?: number; signal?: AbortSignal; source?: string }): Promise<TickerHydrationStatus> {
  return fetchPublicJson<TickerHydrationStatus>(
    buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/hydration-request`, {
      reason: params?.reason,
      priority: params?.priority,
    }),
    { method: "POST", cache: "no-store", next: { revalidate: 0 }, signal: params?.signal, source: params?.source ?? "TickerHydrationRequest" },
  );
}

export async function getDepartmentProfile(slug: string, params?: { limit?: number }): Promise<DepartmentProfileResponse> {
  return fetchJson<DepartmentProfileResponse>(
    buildApiUrl(`/api/departments/${slug}`, { limit: params?.limit }),
    { cache: "no-store", next: { revalidate: 0 } },
  );
}

export async function getDepartments(): Promise<DepartmentListResponse> {
  return fetchJson<DepartmentListResponse>(
    buildApiUrl("/api/departments"),
    { cache: "no-store", next: { revalidate: 0 } },
  );
}

export async function getInsightsNews(params?: {
  limit?: number;
  page?: number;
  authToken?: string | null;
  signal?: AbortSignal;
}): Promise<InsightsNewsResponse> {
  return fetchJson<InsightsNewsResponse>(
    buildApiUrl("/api/insights/news", {
      limit: params?.limit,
      page: params?.page,
    }),
    {
      headers: authHeaders(params?.authToken ?? undefined),
      cache: "no-store",
      next: { revalidate: 0 },
      signal: params?.signal,
    },
  );
}

export async function getInsightsMacroSnapshot(params?: {
  authToken?: string | null;
  signal?: AbortSignal;
}): Promise<MacroSnapshotResponse> {
  const url = buildApiUrl("/api/insights/snapshot");
  return clientCachedJson<MacroSnapshotResponse>(
    `insights-snapshot:${url}:${params?.authToken ? "auth" : "anon"}`,
    params?.signal,
    (signal) => fetchJson<MacroSnapshotResponse>(url, {
      headers: authHeaders(params?.authToken ?? undefined),
      cache: "no-store",
      next: { revalidate: 0 },
      signal,
      source: "InsightsSnapshot",
    }),
  );
}

export async function getTickerNews(
  symbol: string,
  params?: { page?: number; limit?: number; authToken?: string | null; signal?: AbortSignal; source?: string },
): Promise<InsightsNewsResponse> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/news`, { page: params?.page, limit: params?.limit });
  return clientCachedJson<InsightsNewsResponse>(
    `ticker-news:${url}`,
    params?.signal,
    async (signal) => normalizeTickerItemsResponse<NewsItem>(
      await fetchPublicJson<unknown>(url, {
        cache: "no-store",
        next: { revalidate: 0 },
        signal,
        source: params?.source ?? "TickerPage",
      }),
      {
        arrayKeys: ["items", "news", "articles", "results", "data"],
        page: params?.page ?? 0,
        limit: params?.limit ?? 20,
        emptyMessage: "No recent news found.",
      },
    ) as InsightsNewsResponse,
  );
}

export async function getTickerPressReleases(
  symbol: string,
  params?: { page?: number; limit?: number; authToken?: string | null; signal?: AbortSignal; source?: string },
): Promise<PressReleasesResponse> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/press-releases`, { page: params?.page, limit: params?.limit });
  return clientCachedJson<PressReleasesResponse>(
    `ticker-press:${url}`,
    params?.signal,
    async (signal) => normalizeTickerItemsResponse(
      await fetchPublicJson<unknown>(url, {
        cache: "no-store",
        next: { revalidate: 0 },
        signal,
        source: params?.source ?? "TickerPage",
      }),
      {
        arrayKeys: ["items", "press_releases", "pressReleases", "releases", "results", "data"],
        page: params?.page ?? 0,
        limit: params?.limit ?? 20,
        emptyMessage: "No press releases found.",
      },
    ) as PressReleasesResponse,
  );
}

export async function getTickerSecFilings(
  symbol: string,
  params?: { from?: string; to?: string; page?: number; limit?: number; authToken?: string | null; signal?: AbortSignal; source?: string },
): Promise<SecFilingsResponse> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/sec-filings`, {
    from: params?.from,
    to: params?.to,
    page: params?.page,
    limit: params?.limit,
  });
  return clientCachedJson<SecFilingsResponse>(
    `ticker-filings:${url}`,
    params?.signal,
    async (signal) => normalizeTickerItemsResponse(
      await fetchPublicJson<unknown>(url, {
        cache: "no-store",
        next: { revalidate: 0 },
        signal,
        source: params?.source ?? "TickerPage",
      }),
      {
        arrayKeys: ["items", "filings", "sec_filings", "secFilings", "results", "data"],
        page: params?.page ?? 0,
        limit: params?.limit ?? 100,
        windowDays: 365,
        emptyMessage: "No recent filings found.",
      },
    ) as SecFilingsResponse,
  );
}

export async function getTickerFinancials(
  symbol: string,
  params?: { authToken?: string | null; signal?: AbortSignal; source?: string },
): Promise<TickerFinancialsResponse> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/financials`);
  return clientCachedJson<TickerFinancialsResponse>(
    `ticker-financials:${url}`,
    params?.signal,
    (signal) => fetchPublicJson<TickerFinancialsResponse>(url, {
      cache: "no-store",
      next: { revalidate: 0 },
      signal,
      source: params?.source ?? "TickerPage",
    }),
  );
}

export async function getTickerPriceHistory(symbol: string, days: number): Promise<TickerPriceHistoryResponse> {
  return fetchJson<TickerPriceHistoryResponse>(buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/price-history`, { days }));
}

export async function getTickerChartBundle(symbol: string, days: number, options?: { signal?: AbortSignal; source?: string }): Promise<TickerChartBundle> {
  const url = buildApiUrl(`/api/tickers/${tickerPathSymbol(symbol)}/chart-bundle`, { days });
  return clientCachedJson<TickerChartBundle>(
    `ticker-chart:${url}`,
    options?.signal,
    (signal) => fetchJson<TickerChartBundle>(url, {
      cache: "no-store",
      next: { revalidate: 0 },
      signal,
      source: options?.source ?? "TickerChart",
    }),
  );
}

export async function getInsiderStockChart(
  reportingCik: string,
  params: { lookback_days: number; symbol?: string; signal?: AbortSignal; source?: string },
): Promise<TickerChartBundle> {
  return fetchJson<TickerChartBundle>(
    buildApiUrl(`/api/insiders/${encodeURIComponent(reportingCik)}/stock-chart`, {
      lookback_days: params.lookback_days,
      symbol: params.symbol,
    }),
    {
      cache: "no-store",
      next: { revalidate: 0 },
      signal: params.signal,
      source: params.source ?? "InsiderStockChart",
    },
  );
}


export async function getTickerProfiles(symbols: string[], options?: { source?: string }): Promise<TickerProfilesMap> {
  const normalized = Array.from(
    new Set(
      symbols
        .map((symbol) => normalizeTickerSymbol(symbol))
        .filter((symbol): symbol is string => Boolean(symbol))
    )
  );

  if (normalized.length === 0) return {};

  const data = await fetchJson<Record<string, unknown> | { tickers?: unknown }>(
    buildApiUrl("/api/tickers", { symbols: normalized.join(",") }),
    { source: options?.source ?? "unknown" },
  );
  const wrapped = data && typeof data === "object" && "tickers" in data ? data.tickers : null;
  if (wrapped && typeof wrapped === "object" && !Array.isArray(wrapped)) {
    return wrapped as TickerProfilesMap;
  }
  return data as TickerProfilesMap;
}

export async function listWatchlists(authToken?: string): Promise<WatchlistSummary[]> {
  return fetchJson<WatchlistSummary[]>(buildApiUrl("/api/watchlists"), { headers: authHeaders(authToken) });
}

export async function createWatchlist(name: string, authToken?: string): Promise<WatchlistSummary> {
  return fetchJson<WatchlistSummary>(buildApiUrl("/api/watchlists"), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
    body: JSON.stringify({ name }),
  });
}

export async function renameWatchlist(id: number, name: string, authToken?: string): Promise<WatchlistSummary> {
  return fetchJson<WatchlistSummary>(buildApiUrl(`/api/watchlists/${id}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
    body: JSON.stringify({ name }),
  });
}

export async function getWatchlist(id: number, authToken?: string, options?: { signal?: AbortSignal; source?: string }): Promise<WatchlistDetail> {
  return fetchJson<WatchlistDetail>(buildApiUrl(`/api/watchlists/${id}`), {
    headers: authHeaders(authToken),
    signal: options?.signal,
    source: options?.source ?? "WatchlistEvents",
  });
}

export async function getWatchlistConfirmationEvents(
  id: number,
  params: QueryParamsWithRequestOptions = {},
): Promise<ConfirmationMonitoringEventsResponse> {
  const { signal, source, authToken: rawAuthToken, ...queryParams } = params;
  const nextParams: QueryParams = {};
  Object.entries(queryParams).forEach(([key, value]) => {
    if (value === null || value === undefined || typeof value === "string" || typeof value === "number") {
      nextParams[key] = value;
    }
  });
  const authToken = typeof rawAuthToken === "string" ? rawAuthToken : undefined;

  return fetchJson<ConfirmationMonitoringEventsResponse>(
    buildApiUrl(`/api/watchlists/${id}/confirmation-events`, nextParams),
    {
      headers: authHeaders(authToken),
      cache: "no-store",
      next: { revalidate: 0 },
      signal,
      source: source ?? "WatchlistEvents",
    },
  );
}

export async function clearWatchlistConfirmationEvents(
  id: number,
  authToken?: string,
): Promise<ConfirmationMonitoringClearResponse> {
  return fetchJson<ConfirmationMonitoringClearResponse>(
    buildApiUrl(`/api/watchlists/${id}/confirmation-events`),
    { method: "DELETE", headers: authHeaders(authToken) },
  );
}

export async function clearWatchlistConfirmationEvent(
  id: number,
  eventId: number,
  authToken?: string,
): Promise<ConfirmationMonitoringClearResponse> {
  return fetchJson<ConfirmationMonitoringClearResponse>(
    buildApiUrl(`/api/watchlists/${id}/confirmation-events/${eventId}`),
    { method: "DELETE", headers: authHeaders(authToken) },
  );
}

export async function refreshWatchlistConfirmationMonitoring(
  id: number,
  authToken?: string,
): Promise<ConfirmationMonitoringRefreshResponse> {
  return fetchJson<ConfirmationMonitoringRefreshResponse>(
    buildApiUrl(`/api/watchlists/${id}/confirmation-monitoring/refresh`),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function markWatchlistSeen(id: number, authToken?: string) {
  return fetchJson<{ watchlist_id: number; last_seen_at: string; unseen_count: number }>(
    buildApiUrl(`/api/watchlists/${id}/seen`),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function addToWatchlist(id: number, symbol: string, authToken?: string) {
  return fetchJson<{ status: string; symbol: string }>(buildApiUrl(`/api/watchlists/${id}/add`, { symbol }), {
    method: "POST",
    headers: authHeaders(authToken),
  });
}

export async function removeFromWatchlist(id: number, symbol: string, authToken?: string) {
  return fetchJson<{ status: string; symbol: string }>(buildApiUrl(`/api/watchlists/${id}/remove`, { symbol }), {
    method: "DELETE",
    headers: authHeaders(authToken),
  });
}

export async function deleteWatchlist(id: number, authToken?: string) {
  return fetchNoContent(buildApiUrl(`/api/watchlists/${id}`), {
    method: "DELETE",
    headers: authHeaders(authToken),
  });
}

export async function listSavedScreens(authToken?: string): Promise<SavedScreensResponse> {
  return fetchJson<SavedScreensResponse>(buildApiUrl("/api/saved-screens"), {
    headers: authHeaders(authToken),
  });
}

export async function createSavedScreen(
  payload: { name: string; params: Record<string, string>; last_viewed_at?: string | null },
  authToken?: string,
): Promise<SavedScreen> {
  return fetchJson<SavedScreen>(buildApiUrl("/api/saved-screens"), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
    body: JSON.stringify(payload),
  });
}

export async function updateSavedScreen(
  id: number,
  payload: { name?: string; params?: Record<string, string>; last_viewed_at?: string | null },
  authToken?: string,
): Promise<SavedScreen> {
  return fetchJson<SavedScreen>(buildApiUrl(`/api/saved-screens/${id}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
    body: JSON.stringify(payload),
  });
}

export async function deleteSavedScreen(id: number, authToken?: string) {
  return fetchNoContent(buildApiUrl(`/api/saved-screens/${id}`), {
    method: "DELETE",
    headers: authHeaders(authToken),
  });
}

export async function listSavedScreenEvents(
  params: QueryParams & { authToken?: string } = {},
): Promise<SavedScreenEventsResponse> {
  const nextParams: QueryParams = { ...params };
  const authToken = typeof params.authToken === "string" ? params.authToken : undefined;
  delete nextParams.authToken;
  return fetchJson<SavedScreenEventsResponse>(buildApiUrl("/api/saved-screens/events", nextParams), {
    headers: authHeaders(authToken),
  });
}

export async function getMonitoringInbox(authToken?: string, options?: { source?: string }): Promise<MonitoringInboxResponse> {
  return fetchJson<MonitoringInboxResponse>(buildApiUrl("/api/monitoring/inbox"), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    source: options?.source ?? "MonitoringInbox",
  });
}

export type MonitoringUnreadCountResponse = {
  unread_count: number;
  status?: string;
};

export async function getMonitoringUnreadCount(authToken?: string, options?: { force?: boolean; source?: string }): Promise<MonitoringUnreadCountResponse> {
  if (typeof window !== "undefined") {
    if (unreadPromise) return unreadPromise;
    if (!options?.force && unreadCache && unreadCache.expiresAt > Date.now()) return unreadCache.value;
  }

  const request = fetchJson<MonitoringUnreadCountResponse>(buildApiUrl("/api/monitoring/unread-count"), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
    source: options?.source ?? "AccountNav",
  })
    .then((response) => {
      if (typeof window !== "undefined" && response.status !== "temporarily_unavailable") {
        unreadCache = { value: response, expiresAt: Date.now() + CLIENT_CACHE_TTL_MS };
      }
      return response;
    })
    .finally(() => {
      if (typeof window !== "undefined") unreadPromise = null;
    });

  if (typeof window !== "undefined") unreadPromise = request;
  return request;
}

export type MonitoringReadMutationResponse = {
  id?: number;
  item_ids?: number[];
  read?: boolean;
  source_id?: string;
  source_type?: string;
  marked_read?: number;
  marked_unread?: number;
  dismissed?: number;
  source_unread_count?: number;
  unread_count: number;
  counts?: MonitoringCounts;
};

export async function markMonitoringItemsRead(itemIds: number[], authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl("/api/monitoring/items/mark-read"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
      body: JSON.stringify({ item_ids: itemIds }),
    },
  );
}

export async function markMonitoringItemsUnread(itemIds: number[], authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl("/api/monitoring/items/mark-unread"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
      body: JSON.stringify({ item_ids: itemIds }),
    },
  );
}

export async function dismissMonitoringItems(itemIds: number[], authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl("/api/monitoring/items/dismiss"),
    {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders(authToken) },
      body: JSON.stringify({ item_ids: itemIds }),
    },
  );
}

export async function markMonitoringAlertRead(alertId: number, authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl(`/api/monitoring/alerts/${encodeURIComponent(String(alertId))}/read`),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function markMonitoringAlertUnread(alertId: number, authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl(`/api/monitoring/alerts/${encodeURIComponent(String(alertId))}/unread`),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function markMonitoringSourceRead(sourceId: string, sourceType = "watchlist", authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl(`/api/monitoring/sources/${encodeURIComponent(sourceId)}/mark-read`, { source_type: sourceType }),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function markMonitoringSourceUnread(sourceId: string, sourceType = "watchlist", authToken?: string): Promise<MonitoringReadMutationResponse> {
  return fetchJson<MonitoringReadMutationResponse>(
    buildApiUrl(`/api/monitoring/sources/${encodeURIComponent(sourceId)}/mark-unread`, { source_type: sourceType }),
    { method: "POST", headers: authHeaders(authToken) },
  );
}

export async function getWatchlistFeed(id: number, params: QueryParams): Promise<FeedResponse> {
  return fetchJson<FeedResponse>(buildApiUrl(`/api/watchlists/${id}/feed`, params));
}

export async function listNotificationSubscriptions(params: {
  source_type?: "watchlist" | "saved_view";
  source_id?: string;
  email?: string;
}): Promise<{ items: NotificationSubscription[] }> {
  return fetchJson<{ items: NotificationSubscription[] }>(
    buildApiUrl("/api/notification-subscriptions", {
      source_type: params.source_type,
      source_id: params.source_id,
      email: params.email,
    }),
  );
}

export async function saveNotificationSubscription(
  payload: NotificationSubscriptionPayload,
): Promise<NotificationSubscription> {
  return fetchJson<NotificationSubscription>(buildApiUrl("/api/notification-subscriptions"), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ frequency: "daily", ...payload }),
  });
}

export async function deleteNotificationSubscription(id: number): Promise<void> {
  return fetchNoContent(buildApiUrl(`/api/notification-subscriptions/${id}`), {
    method: "DELETE",
  });
}

