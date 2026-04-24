import type {
  ConfirmationMonitoringEventsResponse,
  ConfirmationMonitoringRefreshResponse,
  FeedResponse,
  MemberProfile,
  SavedScreen,
  SavedScreenEventsResponse,
  SavedScreensResponse,
  TickerProfile,
  TickerProfilesMap,
  WatchlistDetail,
  WatchlistSummary,
} from "@/lib/types";
import { storedEntitlementTier, type Entitlements } from "@/lib/entitlements";

export const authTokenStorageKey = "ct:authToken";
export const authSessionCookieName = "ct_session";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

type QueryValue = string | number | null | undefined;

type QueryParams = Record<string, QueryValue>;

export const EVENTS_API_MAX_LIMIT = 100;

export type NormalizedEventType = "congress_trade" | "insider_trade" | "institutional_buy";

export function normalizeEventType(uiValue: string | null | undefined): NormalizedEventType | undefined {
  const normalized = (uiValue ?? "").trim().toLowerCase();
  if (!normalized || normalized === "all") return undefined;
  if (normalized === "congress" || normalized === "congress_trade") return "congress_trade";
  if (normalized === "insider" || normalized === "insider_trade") return "insider_trade";
  if (normalized === "institutional" || normalized === "institutional_buy") return "institutional_buy";
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

function requestInitWithEntitlements(init?: RequestInit): RequestInit {
  const headers = new Headers(init?.headers);
  if (typeof window !== "undefined") {
    const tier = storedEntitlementTier();
    if (tier) headers.set("X-CT-Entitlement-Tier", tier);
    const token = window.localStorage.getItem(authTokenStorageKey);
    if (token) headers.set("Authorization", `Bearer ${token}`);
  }
  return { ...init, headers };
}

function rememberAuthToken(token: string) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(authTokenStorageKey, token);
  document.cookie = `${authSessionCookieName}=${encodeURIComponent(token)}; Path=/; SameSite=Lax; Max-Age=${60 * 60 * 24 * 30}`;
}

function forgetAuthToken() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(authTokenStorageKey);
  document.cookie = `${authSessionCookieName}=; Path=/; SameSite=Lax; Max-Age=0`;
}

function authHeaders(authToken?: string): Record<string, string> {
  return authToken ? { Authorization: `Bearer ${authToken}` } : {};
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;
  const debugFetch = process.env.CT_DEBUG_FETCH === "1" || process.env.NEXT_PUBLIC_CT_DEBUG_FETCH === "1";
  if (debugFetch) {
    const stackLine = new Error().stack?.split("\n").slice(2).find((line) => line.includes("/frontend/"))?.trim() ?? "unknown";
    console.info(`[ct-fetch] GET ${url} :: ${stackLine}`);
  }

  try {
    response = await fetch(url, requestInitWithEntitlements({ cache: "no-store", ...init }));
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
  const debugFetch = process.env.CT_DEBUG_FETCH === "1" || process.env.NEXT_PUBLIC_CT_DEBUG_FETCH === "1";
  if (debugFetch) {
    const stackLine = new Error().stack?.split("\n").slice(2).find((line) => line.includes("/frontend/"))?.trim() ?? "unknown";
    console.info(`[ct-fetch] ${init?.method ?? "GET"} ${url} :: ${stackLine}`);
  }

  try {
    response = await fetch(url, requestInitWithEntitlements({ cache: "no-store", ...init }));
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
  display_price?: number | null;
  reported_price?: number | null;
  reported_price_currency?: string | null;
  pnl_pct?: number | null;
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
};

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
  email: string;
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
  plan?: "free" | "premium" | string;
  status?: string;
  admin_flag?: string;
  entitlement_tier?: "free" | "premium";
  manual_tier_override?: "free" | "premium" | null;
  subscription_status?: string | null;
  subscription_plan?: string | null;
  subscription_cancel_at_period_end?: boolean;
  access_expires_at?: string | null;
  stripe_customer_id?: string | null;
  stripe_subscription_id?: string | null;
  is_suspended?: boolean;
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
  token: string;
  user: AccountUser;
  entitlements: Entitlements;
  return_to?: string;
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
  secret_key: "configured" | "missing";
  price_id: string;
  monthly_price_id?: string;
  annual_price_id?: string;
  webhook_secret: "configured" | "missing";
  success_url: string;
  cancel_url: string;
  webhook_url: string;
  notes: string;
};

export type FeatureGate = {
  feature_key: string;
  required_tier: "free" | "premium";
  description?: string | null;
};

export type PlanLimit = {
  feature_key: string;
  tier: "free" | "premium";
  limit_value: number;
  label?: string;
  unit_singular?: string;
  unit_plural?: string;
  sort_order?: number;
};

export type PlanPrice = {
  tier: "free" | "premium";
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
  stripe_tax_status: "ready_in_app" | "not_ready" | string;
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
  required_tier: "free" | "premium";
  unit_singular?: string;
  unit_plural?: string;
  sort_order: number;
  limits: {
    free: number;
    premium: number;
  };
};

export type PlanConfigTier = {
  tier: "free" | "premium";
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
  feature_gates: FeatureGate[];
  features: Record<string, { required_tier: "free" | "premium"; description: string }>;
  plan_config: PlanConfig;
};

export type SalesLedgerPeriod =
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
  stripe_invoice_id?: string | null;
  stripe_payment_intent_id?: string | null;
  stripe_charge_id?: string | null;
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

export type AdminUserPlanFilter = "all" | "free" | "premium";
export type AdminUserAdminFilter = "all" | "admin" | "non_admin";
export type AdminUserSortBy = "created_at" | "last_seen_at" | "email" | "name" | "country" | "plan" | "status";
export type AdminUserSortDir = "asc" | "desc";

export type AdminUsersParams = {
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
  };
  sort: {
    sort_by: AdminUserSortBy;
    sort_dir: AdminUserSortDir;
  };
};

export async function getEntitlements(): Promise<Entitlements> {
  return fetchJson<Entitlements>(buildApiUrl("/api/entitlements"));
}

export async function login(payload: { email: string; password?: string; name?: string; admin_token?: string }): Promise<AuthResponse> {
  const response = await fetchJson<AuthResponse>(buildApiUrl("/api/auth/login"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  rememberAuthToken(response.token);
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
  rememberAuthToken(response.token);
  return response;
}

export async function getGoogleAuthUrl(returnTo = "/account/billing"): Promise<{ authorization_url: string; state: string }> {
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
  rememberAuthToken(response.token);
  return response;
}

export async function getMe(): Promise<MeResponse> {
  return fetchJson<MeResponse>(buildApiUrl("/api/auth/me"));
}

export async function logout(): Promise<void> {
  forgetAuthToken();
  await fetchJson<{ status: string }>(buildApiUrl("/api/auth/logout"), { method: "POST" });
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

export async function requestPasswordReset(email: string): Promise<{ status: string; message: string; reset_path?: string }> {
  return fetchJson(buildApiUrl("/api/auth/password-reset/request"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
}

export async function confirmPasswordReset(payload: { token: string; password: string }): Promise<AuthResponse> {
  const response = await fetchJson<AuthResponse>(buildApiUrl("/api/auth/password-reset/confirm"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  rememberAuthToken(response.token);
  return response;
}

export async function createCheckoutSession(billingInterval: "monthly" | "annual" = "monthly"): Promise<{ id?: string | null; url?: string | null }> {
  return fetchJson(buildApiUrl("/api/billing/checkout-session"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ billing_interval: billingInterval }),
  });
}

export async function createCustomerPortalSession(): Promise<{ url?: string | null }> {
  return fetchJson(buildApiUrl("/api/billing/customer-portal"), { method: "POST" });
}

export async function getAccountBillingHistory(limit = 25): Promise<BillingHistoryResponse> {
  return fetchJson<BillingHistoryResponse>(buildApiUrl("/api/account/billing/history", { limit }));
}

export async function getAdminSettings(): Promise<AdminSettings> {
  return fetchJson<AdminSettings>(buildApiUrl("/api/admin/settings", { include_users: 0 }));
}

export async function getAdminSalesLedger(params: SalesLedgerParams): Promise<SalesLedgerResponse> {
  return fetchJson<SalesLedgerResponse>(buildApiUrl("/api/admin/reports/sales-ledger", params));
}

export async function getAdminUsers(params: AdminUsersParams): Promise<AdminUsersResponse> {
  return fetchJson<AdminUsersResponse>(buildApiUrl("/api/admin/users", params));
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
  return fetchJson<PlanConfig>(buildApiUrl("/api/plan-config"), {
    cache: "no-store",
    next: { revalidate: 0 },
  });
}

export async function adminSetPremium(userId: number, tier: "free" | "premium" | null): Promise<AccountUser> {
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

export async function adminUpdateFeatureGate(featureKey: string, requiredTier: "free" | "premium"): Promise<FeatureGate> {
  return fetchJson<FeatureGate>(buildApiUrl(`/api/admin/feature-gates/${featureKey}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ required_tier: requiredTier }),
  });
}

export async function adminUpdatePlanLimit(featureKey: string, tier: "free" | "premium", limitValue: number): Promise<PlanLimit> {
  return fetchJson<PlanLimit>(buildApiUrl(`/api/admin/plan-limits/${featureKey}`), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tier, limit_value: limitValue }),
  });
}

export async function adminUpdatePlanPrice(
  tier: "free" | "premium",
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


export type SuggestResponse = {
  items: string[];
};

export type MemberInsiderSuggestion = {
  label: string;
  value: string;
  category: "congress" | "insider";
};

export type MemberInsiderSuggestResponse = {
  items: MemberInsiderSuggestion[];
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

export type TickerChartMarkerKind = "congress" | "insider" | "signals";

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
  symbol: string;
  resolution: "daily";
  days: number;
  start_date: string;
  end_date: string;
  prices: TickerPriceHistoryPoint[];
  benchmark: {
    symbol: string;
    label: string;
    points: TickerPriceHistoryPoint[];
  };
  markers: TickerChartMarker[];
  quote: TickerChartQuote;
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
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  smart_score?: number;
  smart_band?: string;
  source?: string;
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

export async function suggestMemberInsiders(q: string, limit = 10): Promise<MemberInsiderSuggestResponse> {
  return fetchJson<MemberInsiderSuggestResponse>(buildApiUrl("/api/suggest/member-insider", { q, limit }), {
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

export async function getWatchlistEvents(id: number, params: QueryParams & { mode?: string }): Promise<EventsResponse> {
  const nextParams: QueryParams = { ...params };
  const mode = typeof nextParams.mode === "string" ? nextParams.mode.trim().toLowerCase() : "";

  if (mode === "congress") {
    nextParams.types = "congress_trade";
  } else if (mode === "insider") {
    nextParams.types = "insider_trade";
  } else {
    delete nextParams.types;
  }

  delete nextParams.mode;

  const authToken = typeof params.authToken === "string" ? params.authToken : undefined;
  delete nextParams.authToken;

  return fetchJson<EventsResponse>(buildApiUrl(`/api/watchlists/${id}/events`, nextParams), {
    headers: authHeaders(authToken),
    cache: "no-store",
    next: { revalidate: 0 },
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
  });

  return { items: Array.isArray(data) ? data : [] };
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


export async function getMemberProfileBySlug(
  slug: string,
  params?: { include_trades?: boolean },
): Promise<MemberProfile> {
  return fetchJson<MemberProfile>(
    buildApiUrl(`/api/members/by-slug/${slug}`, {
      include_trades:
        params?.include_trades === undefined ? undefined : (params.include_trades ? 1 : 0),
    }),
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
  authToken?: string;
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
    { headers: authHeaders(params?.authToken) },
  );
}

export async function getTickerProfile(symbol: string): Promise<TickerProfile> {
  return fetchJson<TickerProfile>(buildApiUrl(`/api/tickers/${symbol}`));
}

export async function getTickerPriceHistory(symbol: string, days: number): Promise<TickerPriceHistoryResponse> {
  return fetchJson<TickerPriceHistoryResponse>(buildApiUrl(`/api/tickers/${symbol}/price-history`, { days }));
}

export async function getTickerChartBundle(symbol: string, days: number): Promise<TickerChartBundle> {
  return fetchJson<TickerChartBundle>(buildApiUrl(`/api/tickers/${symbol}/chart-bundle`, { days }), {
    cache: "no-store",
    next: { revalidate: 0 },
  });
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

export async function getWatchlist(id: number, authToken?: string): Promise<WatchlistDetail> {
  return fetchJson<WatchlistDetail>(buildApiUrl(`/api/watchlists/${id}`), { headers: authHeaders(authToken) });
}

export async function getWatchlistConfirmationEvents(
  id: number,
  params: QueryParams & { authToken?: string } = {},
): Promise<ConfirmationMonitoringEventsResponse> {
  const nextParams: QueryParams = { ...params };
  const authToken = typeof params.authToken === "string" ? params.authToken : undefined;
  delete nextParams.authToken;

  return fetchJson<ConfirmationMonitoringEventsResponse>(
    buildApiUrl(`/api/watchlists/${id}/confirmation-events`, nextParams),
    {
      headers: authHeaders(authToken),
      cache: "no-store",
      next: { revalidate: 0 },
    },
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
