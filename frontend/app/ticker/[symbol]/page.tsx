import Link from "next/link";
import { headers } from "next/headers";
import type { ReactNode } from "react";
import { Suspense } from "react";
import type { Metadata } from "next";
import { Badge } from "@/components/Badge";
import { ApiError, getEntitlements, getEvents, getTickerContextBundle, getTickerGovernmentContracts, getTickerProfile, getTickerSignalsSummary, INSTITUTIONAL_ACTIVITY_EVENT_TYPES, type SignalItem, type TickerContextBundleResponse, type TickerDecisionItem, type TickerDecisionLayer, type TickerFundamentalsSummary, type TickerGovernmentContractItem, type TickerSignalsSummaryResponse, type TickerSourceEntitlement, type TickerSourceEntitlements } from "@/lib/api";
import { TickerChartLoader } from "@/components/ticker/TickerChartLoader";
import { TickerActivityDetailClient } from "@/components/ticker/TickerActivityDetailClient";
import { TickerContextCard } from "@/components/ticker/TickerContextCard";
import { TickerDeferredActivityRefresh } from "@/components/ticker/TickerDeferredActivityRefresh";
import { EntitlementHintRefresh } from "@/components/auth/EntitlementHintRefresh";
import { ExpandableTickerSection } from "@/components/ticker/ExpandableTickerSection";
import { TickerActivityPaginationFooter } from "@/components/ticker/TickerActivityPaginationFooter";
import { TickerInstitutionalSourceCardClient } from "@/components/ticker/TickerInstitutionalSourceCardClient";
import { TickerSignalActivityClient } from "@/components/ticker/TickerSignalActivityClient";
import { TickerSignalsSourceCardClient } from "@/components/ticker/TickerSignalsSourceCardClient";
import { ShareLinks } from "@/components/member/ShareLinks";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { entitlementsFromTierHint, hasEntitlement, type Entitlements } from "@/lib/entitlements";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  compactInteractiveTitleClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import {
  chamberBadge,
  formatCongressAffiliationText,
  formatCurrency,
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  partyBadge,
  transactionTone,
} from "@/lib/format";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";
import { formatCompanyName } from "@/lib/companyName";
import { departmentHref } from "@/lib/departments";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { LockedSmartSignalPill, SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  resolveInsiderDisplayPrice,
} from "@/lib/insiderTradeDisplay";
import { resolveCongressActivityPrice, resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { gainLossLabel, tickerGainLossTooltip } from "@/lib/gainLossCopy";
import { WALNUT_MARKETING_URL, WALNUT_SOCIAL_IMAGE_ALT, WALNUT_SOCIAL_IMAGE_URL } from "@/lib/marketingMetadata";

type Props = {
  params: Promise<{ symbol: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365";
type SourceFilter = "all" | "congress" | "insider" | "signals" | "institutional" | "government_contract";
type SideFilter = "all" | "buy" | "sell";
const SIGNAL_WINDOW_DAYS = 30;
const ACTIVITY_PAGE_SIZE = 20;
const ACTIVITY_FETCH_SIZE = ACTIVITY_PAGE_SIZE + 1;
const GOVERNMENT_CONTRACTS_PAGE_SIZE = ACTIVITY_PAGE_SIZE;
const TICKER_METADATA_DESCRIPTION =
  "Research public ticker context, Congress trades, insider activity, government contracts, technicals, fundamentals, and confirmation-stack signals in Walnut Markets.";

function contextWindowLabel(days: number): string {
  return `${days} Day`;
}

function contextWindowNoun(days: number): string {
  return `${contextWindowLabel(days)} context window`;
}

function lastContextWindowLabel(days: number): string {
  return `last ${days} ${days === 1 ? "Day" : "Days"}`;
}

function normalizeUpperCardWindowCopy(value: string | null | undefined, days: number): string | null {
  const text = value?.trim();
  if (!text) return null;
  return text
    .replace(/\bin the selected window\b/gi, `in the ${contextWindowNoun(days)}`)
    .replace(/\bin selected window\b/gi, `in the ${contextWindowNoun(days)}`)
    .replace(/\bselected window\b/gi, `the ${contextWindowNoun(days)}`)
    .replace(/\b(\d+)D\b/g, "$1 Day");
}

type TickerProfileResponse = Awaited<ReturnType<typeof getTickerProfile>>;
type TickerContextBundle = TickerContextBundleResponse;
type EventsResponse = Awaited<ReturnType<typeof getEvents>>;
type ActivityPageMeta = {
  page: number;
  limit: number;
  total: number | null;
  hasNext: boolean;
};
type ParticipantStats = {
  name: string;
  memberId?: string | null;
  trades: number;
  buys: number;
  sells: number;
  netFlow: number;
  href?: string;
  reportingCik?: string;
  chamber?: string | null;
  party?: string | null;
  state?: string | null;
  role?: string | null;
};
type SignalGateReason = "auth" | "upgrade" | "unavailable";
type SignalGateState = {
  reason: SignalGateReason;
  message: string;
};
type TickerConfirmationGate = {
  locked: boolean;
  href: string;
  label: string;
  message: string;
};

type ConfirmationSummary = {
  congress_active_30d: boolean;
  insider_active_30d: boolean;
  congress_trade_count_30d: number;
  insider_trade_count_30d: number;
  insider_buy_count_30d: number;
  insider_sell_count_30d: number;
  cross_source_confirmed_30d: boolean;
  repeat_congress_30d: boolean;
  repeat_insider_30d: boolean;
};
type ConfirmationScoreBundle = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["confirmation_score_bundle"]>;
type SignalFreshnessBundle = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["signal_freshness"]>;
type OptionsFlowSummary = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["options_flow_summary"]>;
type TechnicalIndicators = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["technical_indicators"]>;
type ConfirmationSourceKey = keyof ConfirmationScoreBundle["sources"];

type TickerActivityData = {
  events: Awaited<ReturnType<typeof getEvents>>["items"];
  signals: SignalItem[];
  signalsTotal: number | null;
  priceVolumeContext: TickerSignalsSummaryResponse["price_volume"] | null;
  fundamentalsContext: TickerFundamentalsSummary;
  sourceEntitlements: TickerSourceEntitlements | null;
  confirmationScoreBundle: TickerSignalsSummaryResponse["confirmation_score_bundle"] | null;
  signalFreshness: TickerSignalsSummaryResponse["signal_freshness"] | null;
  signalSummaryResolved: boolean;
  effectiveWindowDays: number | null;
  summaryInsiders: TickerSignalsSummaryResponse["insiders"] | null;
  summaryCongress: TickerSignalsSummaryResponse["congress"] | null;
  signalsUnavailable: SignalGateState | null;
  congressEvents: EventsResponse["items"];
  congressEventsTotal: number | null;
  congressEventsPage: number;
  congressEventsLimit: number;
  congressEventsHasNext: boolean;
  insiderEvents: EventsResponse["items"];
  insiderEventsTotal: number | null;
  insiderEventsPage: number;
  insiderEventsLimit: number;
  insiderEventsHasNext: boolean;
  institutionalEvents: EventsResponse["items"];
  institutionalEventsTotal: number | null;
  institutionalEventsPage: number;
  institutionalEventsLimit: number;
  institutionalEventsHasNext: boolean;
  institutionalEventsStatus: string;
  governmentContracts: TickerGovernmentContractItem[];
  governmentContractsTotal: number;
  governmentContractsPage: number;
  governmentContractsLimit: number;
  governmentContractsHasNext: boolean;
  governmentContractsStatus: string;
  congressBuys: number;
  congressSells: number;
  insiderBuys: number;
  insiderSells: number;
  topSignal: SignalItem | undefined;
  topCongressParticipants: ParticipantStats[];
  topInsiderParticipants: ParticipantStats[];
};

function MissingTickerSearchFallback({ symbol }: { symbol: string }) {
  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto flex min-h-screen w-full max-w-4xl items-center px-4 py-12 sm:px-6 lg:px-8">
        <section className="w-full rounded-lg border border-white/10 bg-slate-900/70 p-6">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Ticker not found</p>
          <h1 className="mt-3 text-2xl font-semibold text-white">We could not find a ticker for {symbol}.</h1>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">
            Search Walnut instead to find likely company, member, insider, or department matches.
          </p>
          <div className="mt-6">
            <Link href={`/search?q=${encodeURIComponent(symbol)}`} className={ghostButtonClassName}>
              Search for {symbol}
            </Link>
          </div>
        </section>
      </div>
    </main>
  );
}

function fallbackTickerProfile(symbol: string): TickerProfileResponse {
  return {
    status: "partial",
    ticker: {
      symbol,
      name: "Company details loading",
      asset_class: "Equity",
      sector: null,
      industry: null,
      country: null,
      exchange: null,
      identity_status: "loading",
      limited_data_state: null,
      limited_data_message: null,
    },
    top_members: [],
    trades: [],
    confirmation_score_bundle: null,
    options_flow_summary: null,
    why_now: null,
    signal_freshness: null,
    technical_indicators: null,
  };
}

function normalizedTickerSymbolForRoute(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function canonicalTickerPathForSymbol(symbol: string): string {
  return tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
}

function canonicalTickerUrlForSymbol(symbol: string): string {
  return new URL(canonicalTickerPathForSymbol(symbol), WALNUT_MARKETING_URL).toString();
}

function publicTickerMetadataTitle(symbol: string, companyName?: string | null): string {
  const cleanedCompanyName = formatCompanyName(companyName);
  const hasDistinctName = cleanedCompanyName && cleanedCompanyName.toUpperCase() !== symbol.toUpperCase();
  return hasDistinctName
    ? `${symbol} ${cleanedCompanyName} | Walnut Markets Ticker Intelligence`
    : `${symbol} | Walnut Markets Ticker Intelligence`;
}

function publicTickerMetadataDescription(symbol: string, companyName?: string | null): string {
  const cleanedCompanyName = formatCompanyName(companyName);
  const identity = cleanedCompanyName && cleanedCompanyName.toUpperCase() !== symbol.toUpperCase()
    ? `${cleanedCompanyName} (${symbol})`
    : symbol;
  return `${TICKER_METADATA_DESCRIPTION} Current page: ${identity}. Built for research, not investment advice.`;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { symbol } = await params;
  const normalizedSymbol = normalizedTickerSymbolForRoute(symbol);
  const canonicalUrl = canonicalTickerUrlForSymbol(normalizedSymbol);
  const companyName: string | null = null;

  const title = publicTickerMetadataTitle(normalizedSymbol, companyName);
  const description = publicTickerMetadataDescription(normalizedSymbol, companyName);

  return {
    metadataBase: new URL(WALNUT_MARKETING_URL),
    title,
    description,
    alternates: {
      canonical: canonicalUrl,
    },
    openGraph: {
      type: "website",
      title,
      description,
      url: canonicalUrl,
      siteName: "Walnut Markets",
      images: [
        {
          url: WALNUT_SOCIAL_IMAGE_URL,
          width: 1200,
          height: 630,
          alt: WALNUT_SOCIAL_IMAGE_ALT,
        },
      ],
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
      images: [
        {
          url: WALNUT_SOCIAL_IMAGE_URL,
          alt: WALNUT_SOCIAL_IMAGE_ALT,
        },
      ],
    },
  };
}

function isRecoverableTickerProfileError(error: unknown): boolean {
  if (error instanceof ApiError) return error.status === 503 || error.status >= 500;
  return error instanceof Error;
}

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

type HeaderReader = Pick<Headers, "get">;

function headerIncludes(headersList: HeaderReader, name: string, expected: string): boolean {
  return (headersList.get(name) ?? "").toLowerCase().includes(expected);
}

function userAgentLooksInteractiveBrowser(userAgent: string | null): boolean {
  const ua = (userAgent ?? "").toLowerCase();
  if (!ua) return false;
  if (/bot|crawler|spider|headless|preview|prerender|curl|wget|python|go-http|uptime|monitor/.test(ua)) return false;
  return /mozilla|chrome|safari|firefox|edg\//.test(ua);
}

function shouldDeferAnonymousTickerActivityDetails({
  requestHeaders,
  authToken,
  hasAuthHint,
  activityDetailsRequested,
}: {
  requestHeaders: HeaderReader;
  authToken: string | null | undefined;
  hasAuthHint: boolean;
  activityDetailsRequested: boolean;
}): boolean {
  if (authToken || hasAuthHint || activityDetailsRequested) return false;
  if (requestHeaders.get("next-router-prefetch") === "1") return true;
  if (requestHeaders.get("x-middleware-prefetch") === "1") return true;
  if (headerIncludes(requestHeaders, "purpose", "prefetch")) return true;
  if (headerIncludes(requestHeaders, "sec-purpose", "prefetch")) return true;
  return !userAgentLooksInteractiveBrowser(requestHeaders.get("user-agent"));
}

function shouldUseAnonymousTickerSsrShell({
  requestHeaders,
  authToken,
  hasAuthHint,
  activityDetailsRequested,
}: {
  requestHeaders: HeaderReader;
  authToken: string | null | undefined;
  hasAuthHint: boolean;
  activityDetailsRequested: boolean;
}): boolean {
  if (authToken || hasAuthHint || activityDetailsRequested) return false;
  if (requestHeaders.get("next-router-prefetch") === "1") return true;
  if (requestHeaders.get("x-middleware-prefetch") === "1") return true;
  if (headerIncludes(requestHeaders, "purpose", "prefetch")) return true;
  if (headerIncludes(requestHeaders, "sec-purpose", "prefetch")) return true;
  return false;
}

function clampLookback(v: string): Lookback {
  return v === "30" || v === "90" || v === "180" || v === "365" ? v : "365";
}

function clampSource(v: string): SourceFilter {
  return v === "congress" || v === "insider" || v === "signals" || v === "institutional" || v === "government_contract" || v === "all" ? v : "all";
}

function clampSide(v: string): SideFilter {
  return v === "buy" || v === "sell" || v === "all" ? v : "all";
}

function clampPage(v: string): number {
  const parsed = Number(v);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function sideToTradeType(side: SideFilter): "purchase" | "sale" | null {
  if (side === "buy") return "purchase";
  if (side === "sell") return "sale";
  return null;
}

function emptyEventsResponse(page = 0, limit = ACTIVITY_PAGE_SIZE): EventsResponse {
  return {
    items: [],
    total: null,
    has_more: false,
    limit,
    offset: Math.max(page, 0) * Math.max(limit, 1),
    item_count: 0,
    status: "ok",
  };
}

function activityPageMeta(response: EventsResponse, fallbackPage = 0, fallbackLimit = ACTIVITY_PAGE_SIZE): ActivityPageMeta {
  const rawLimit = typeof response.limit === "number" && response.limit > 0 ? response.limit : fallbackLimit;
  const limit = Math.min(rawLimit, fallbackLimit);
  const offset = typeof response.offset === "number" && response.offset >= 0 ? response.offset : fallbackPage * limit;
  const page = Math.max(Math.floor(offset / Math.max(limit, 1)), 0);
  const visibleCount = Math.min(response.items.length, limit);
  const inferredHasNext = response.items.length > limit;
  const total: number | null = typeof response.total === "number" && response.total >= 0 ? response.total : null;
  const hasExactTotal = total !== null;
  const hasMore = typeof response.has_more === "boolean" ? response.has_more : inferredHasNext;
  return {
    page,
    limit,
    total,
    hasNext: hasExactTotal
      ? offset + visibleCount < total
      : hasMore,
  };
}

function visibleActivityItems(response: EventsResponse, limit = ACTIVITY_PAGE_SIZE) {
  return (response.items ?? []).slice(0, limit);
}

function formatActivityPrice(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  const hasDecimals = !Number.isInteger(value);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: hasDecimals ? 2 : 0,
    maximumFractionDigits: hasDecimals ? 2 : 0,
  }).format(value);
}

function titleCase(value: string | null | undefined): string {
  return (value ?? "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function isTickerSignalKind(value: string | null | undefined): boolean {
  const kind = canonicalize(value);
  return kind === "congress" || kind === "insider";
}

function isInstitutionalActivityEventType(value?: string | null): boolean {
  const normalized = canonicalize(value);
  return normalized === "institutional_buy" || (INSTITUTIONAL_ACTIVITY_EVENT_TYPES as readonly string[]).includes(normalized);
}

function eventPayload(event: EventsResponse["items"][number]): Record<string, unknown> {
  return event.payload && typeof event.payload === "object" ? event.payload as Record<string, unknown> : {};
}

function payloadString(payload: Record<string, unknown>, key: string): string | null {
  const value = payload[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function payloadNumber(payload: Record<string, unknown>, key: string): number | null {
  const value = payload[key];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function institutionalHolderName(event: EventsResponse["items"][number]): string {
  const payload = eventPayload(event);
  return event.member_name?.trim()
    || payloadString(payload, "holder_name")
    || payloadString(payload, "institution_name")
    || "Institution";
}

function institutionalActionLabel(event: EventsResponse["items"][number]): string {
  const payload = eventPayload(event);
  const eventType = canonicalize(event.event_type);
  const action = payloadString(payload, "action") || event.trade_type || payloadString(payload, "direction");
  if (eventType === "new_institutional_position") return "New Position";
  if (eventType === "major_holder_exit") return "Exit";
  if (eventType === "major_holder_reduction" || eventType === "institutional_distribution" || eventType === "cluster_distribution") return "Reduced";
  if (eventType === "institutional_accumulation" || eventType === "institutional_buy" || eventType === "cluster_accumulation" || eventType === "contrarian_accumulation") return "Increased";
  return titleCase(action) || "Reported";
}

function institutionalTone(event: EventsResponse["items"][number]): "pos" | "neg" | "neutral" {
  const payload = eventPayload(event);
  const direction = canonicalize(payloadString(payload, "direction") || event.trade_type || event.event_type);
  if (direction.includes("bearish") || direction.includes("distribution") || direction.includes("reduction") || direction.includes("exit") || direction.includes("reduced")) return "neg";
  if (direction.includes("bullish") || direction.includes("accumulation") || direction.includes("buy") || direction.includes("new") || direction.includes("increased")) return "pos";
  return "neutral";
}

function institutionalValue(event: EventsResponse["items"][number]): number | null {
  const payload = eventPayload(event);
  const candidates = [
    event.amount_max,
    event.amount_min,
    payloadNumber(payload, "reported_value_usd"),
    payloadNumber(payload, "value_delta_usd"),
    payloadNumber(payload, "current_value_usd"),
  ];
  return candidates.find((value) => typeof value === "number" && Number.isFinite(value) && value > 0) ?? null;
}

function institutionalDate(event: EventsResponse["items"][number]): string | null {
  const payload = eventPayload(event);
  return payloadString(payload, "filing_date") || event.ts || null;
}

function institutionalReportPeriod(event: EventsResponse["items"][number]): string | null {
  const payload = eventPayload(event);
  const reportPeriod = payloadString(payload, "report_period");
  if (reportPeriod) return reportPeriod;
  const year = payloadNumber(payload, "report_year");
  const quarter = payloadNumber(payload, "report_quarter");
  if (year && quarter) return `Q${quarter} ${year}`;
  return null;
}

function activityCountLabel(total: number | null | undefined, itemCount: number, noun: string, unavailable = false): string {
  if (unavailable) return "unavailable";
  const count = total ?? itemCount;
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function normalizeTradeSide(value?: string | null): "buy" | "sell" | null {
  const t = (value ?? "").trim().toLowerCase();
  if (!t) return null;
  if (t.includes("buy") || t.includes("purchase") || t.startsWith("p-")) return "buy";
  if (t.includes("sell") || t.includes("sale") || t.startsWith("s-")) return "sell";
  return null;
}

function isGovernmentContractEventType(value?: string | null): boolean {
  const normalized = canonicalize(value);
  return normalized === "government_contract"
    || normalized === "government_contract_award"
    || normalized === "contract_award"
    || normalized === "government_exposure";
}

function toDateKey(value?: string | null): string | null {
  const raw = (value ?? "").trim();
  if (!raw) return null;
  const day = raw.slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : null;
}

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned ? cleaned : null;
}

function canonicalize(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function cleanTickerHeaderMetadata(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  if (!cleaned) return null;
  if (["n/a", "na", "none", "null", "unknown", "-", "--"].includes(cleaned.toLowerCase())) return null;
  return cleaned;
}

function normalizedAmountLabel(min?: number | null, max?: number | null): string {
  const minValue = Number.isFinite(min) ? Number(min) : null;
  const maxValue = Number.isFinite(max) ? Number(max) : null;
  return `${minValue ?? ""}-${maxValue ?? ""}`;
}

function tickerHeaderMetadata(ticker: Awaited<ReturnType<typeof getTickerProfile>>["ticker"]): string[] {
  return [ticker.sector, ticker.industry]
    .map(cleanTickerHeaderMetadata)
    .filter((value): value is string => Boolean(value));
}

function payloadDateKey(payload: any): string {
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  return (
    toDateKey(asTrimmedString(payload?.transaction_date)) ??
    toDateKey(asTrimmedString(payload?.trade_date)) ??
    toDateKey(asTrimmedString(raw?.transactionDate)) ??
    toDateKey(asTrimmedString(raw?.tradeDate)) ??
    ""
  );
}

function dedupeByKey<T>(items: T[], keyFor: (item: T) => string): T[] {
  const seen = new Set<string>();
  return items.filter((item) => {
    const key = keyFor(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function stableEventIdentity(event: { event_type?: string | null; source?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const transaction = payload?.transaction && typeof payload.transaction === "object" ? payload.transaction : null;

  const stableId =
    asTrimmedString(payload?.event_id) ??
    asTrimmedString(payload?.external_id) ??
    asTrimmedString(payload?.transaction_id) ??
    asTrimmedString(payload?.transactionId) ??
    asTrimmedString(transaction?.id) ??
    asTrimmedString(payload?.filing_id) ??
    asTrimmedString(payload?.filingId) ??
    asTrimmedString(payload?.disclosure_id) ??
    asTrimmedString(payload?.disclosureId) ??
    asTrimmedString(raw?.id) ??
    asTrimmedString(raw?.transaction_id) ??
    asTrimmedString(raw?.transactionId) ??
    asTrimmedString(raw?.filing_id) ??
    asTrimmedString(raw?.filingId) ??
    asTrimmedString(raw?.disclosure_id) ??
    asTrimmedString(raw?.disclosureId);

  if (!stableId) return null;
  return [canonicalize(event.event_type), canonicalize(event.source), canonicalize(stableId)].join("|");
}

function readNumeric(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,]/g, "").trim();
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function resolveInsiderName(event: { member_name?: string | null; payload?: any }): string {
  const payload = event.payload;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const insider = payload?.insider && typeof payload.insider === "object" ? payload.insider : null;

  return (
    getInsiderDisplayName(
      asTrimmedString(payload?.insider_name),
      asTrimmedString(insider?.name),
      asTrimmedString(raw?.reportingName),
      asTrimmedString(raw?.reportingOwnerName),
      asTrimmedString(raw?.ownerName),
      asTrimmedString(raw?.insiderName),
      asTrimmedString(event.member_name),
    ) ?? "Unknown Insider"
  );
}



function resolveInsiderReportingCik(event: { payload?: any }): string | null {
  const payload = event.payload;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  return (
    asTrimmedString(payload?.reporting_cik) ??
    asTrimmedString(payload?.reportingCik) ??
    asTrimmedString(raw?.reportingCik) ??
    asTrimmedString(raw?.reportingCIK) ??
    null
  );
}

function resolveInsiderRole(event: { payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const insider = payload?.insider && typeof payload.insider === "object" ? payload.insider : null;

  return (
    asTrimmedString(payload?.role) ??
    asTrimmedString(payload?.typeOfOwner) ??
    asTrimmedString(payload?.position) ??
    asTrimmedString(payload?.officer_title) ??
    asTrimmedString(payload?.officerTitle) ??
    asTrimmedString(insider?.role) ??
    asTrimmedString(insider?.position) ??
    asTrimmedString(raw?.typeOfOwner) ??
    asTrimmedString(raw?.officerTitle) ??
    asTrimmedString(raw?.insiderRole) ??
    asTrimmedString(raw?.position) ??
    null
  );
}

function resolveCongressPayloadMember(event: { payload?: any }) {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return payload?.member && typeof payload.member === "object" ? payload.member : null;
}

function resolveCongressChamber(event: { chamber?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.chamber) ??
    asTrimmedString(payload?.chamber) ??
    asTrimmedString(payload?.raw?.chamber) ??
    asTrimmedString(event.chamber) ??
    null
  );
}

function resolveCongressParty(event: { party?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.party) ??
    asTrimmedString(payload?.party) ??
    asTrimmedString(payload?.raw?.party) ??
    asTrimmedString(event.party) ??
    null
  );
}

function resolveCongressState(event: { state?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.state) ??
    asTrimmedString(payload?.state) ??
    asTrimmedString(payload?.raw?.state) ??
    asTrimmedString(event.state) ??
    null
  );
}

function formatCongressIdentity(event: { member_name?: string | null; party?: string | null; payload?: any }): string {
  const memberName = event.member_name?.trim() || "An unknown Congress member";
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = payload?.member && typeof payload.member === "object" ? payload.member : null;
  const party = partyBadge(
    asTrimmedString(memberPayload?.party) ??
    asTrimmedString(payload?.party) ??
    event.party ??
    null,
  ).label;
  const state =
    asTrimmedString(memberPayload?.state) ??
    asTrimmedString(payload?.state) ??
    asTrimmedString(payload?.raw?.state) ??
    null;
  const suffix = [party !== "Ã¢â‚¬â€" ? party : null, state ? state.toUpperCase() : null].filter(Boolean).join("-");
  return suffix ? `${memberName} (${suffix})` : memberName;
}

function formatInsiderIdentity(event: { member_name?: string | null; payload?: any }): string {
  const insiderName = resolveInsiderName(event);
  const role = resolveInsiderRole(event);
  return role ? `${insiderName} (${role})` : insiderName;
}

function resolveCongressTradeDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.trade_date) ??
    asTrimmedString(payload?.transaction_date) ??
    asTrimmedString(payload?.raw?.tradeDate) ??
    asTrimmedString(payload?.raw?.transactionDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveCongressReportDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.report_date) ??
    asTrimmedString(payload?.filing_date) ??
    asTrimmedString(payload?.raw?.reportDate) ??
    asTrimmedString(payload?.raw?.filingDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveCongressTradePrice(event: { estimated_price?: number | null; payload?: any }): number | null {
  return resolveCongressActivityPrice(event as Record<string, unknown>);
}

function resolveInsiderTradeDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.transaction_date) ??
    asTrimmedString(payload?.trade_date) ??
    asTrimmedString(payload?.raw?.transactionDate) ??
    asTrimmedString(payload?.raw?.tradeDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveInsiderFilingDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.filing_date) ??
    asTrimmedString(payload?.raw?.filingDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveInsiderTradePrice(event: { estimated_price?: number | null; payload?: any }): number | null {
  return resolveInsiderDisplayPrice(event);
}

function resolveGovernmentContractAgency(event: { source?: string | null; payload?: any }): string {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    asTrimmedString(payload?.awarding_agency) ??
    asTrimmedString(payload?.awardingAgency) ??
    asTrimmedString(nestedPayload?.awarding_agency) ??
    asTrimmedString(nestedPayload?.awardingAgency) ??
    asTrimmedString(payload?.agency) ??
    asTrimmedString(nestedPayload?.agency) ??
    asTrimmedString(raw?.awarding_agency) ??
    asTrimmedString(raw?.awardingAgency) ??
    asTrimmedString(raw?.agency) ??
    asTrimmedString(event.source) ??
    "Government Contract"
  );
}

function resolveGovernmentContractDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    asTrimmedString(payload?.award_date) ??
    asTrimmedString(payload?.awardDate) ??
    asTrimmedString(nestedPayload?.award_date) ??
    asTrimmedString(nestedPayload?.awardDate) ??
    asTrimmedString(raw?.award_date) ??
    asTrimmedString(raw?.awardDate) ??
    asTrimmedString(payload?.period_start) ??
    asTrimmedString(payload?.periodStart) ??
    asTrimmedString(nestedPayload?.period_start) ??
    asTrimmedString(nestedPayload?.periodStart) ??
    asTrimmedString(raw?.period_start) ??
    asTrimmedString(raw?.periodStart) ??
    asTrimmedString(payload?.report_date) ??
    asTrimmedString(payload?.reportDate) ??
    asTrimmedString(nestedPayload?.report_date) ??
    asTrimmedString(nestedPayload?.reportDate) ??
    asTrimmedString(raw?.report_date) ??
    asTrimmedString(raw?.reportDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveGovernmentContractAmount(event: { amount_max?: number | null; amount_min?: number | null; payload?: any }): number | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    readNumeric(payload?.award_amount) ??
    readNumeric(payload?.awardAmount) ??
    readNumeric(nestedPayload?.award_amount) ??
    readNumeric(nestedPayload?.awardAmount) ??
    readNumeric(payload?.amount) ??
    readNumeric(nestedPayload?.amount) ??
    readNumeric(raw?.award_amount) ??
    readNumeric(raw?.awardAmount) ??
    readNumeric(raw?.amount) ??
    readNumeric(event.amount_max) ??
    readNumeric(event.amount_min) ??
    null
  );
}

function resolveGovernmentContractDescription(event: { payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    asTrimmedString(payload?.description) ??
    asTrimmedString(nestedPayload?.description) ??
    asTrimmedString(payload?.summary) ??
    asTrimmedString(nestedPayload?.summary) ??
    asTrimmedString(payload?.title) ??
    asTrimmedString(nestedPayload?.title) ??
    asTrimmedString(raw?.description) ??
    asTrimmedString(raw?.summary) ??
    asTrimmedString(raw?.title) ??
    null
  );
}

function resolveGovernmentContractRecipient(event: { member_name?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    asTrimmedString(payload?.recipient_name) ??
    asTrimmedString(payload?.recipientName) ??
    asTrimmedString(payload?.raw_recipient_name) ??
    asTrimmedString(payload?.rawRecipientName) ??
    asTrimmedString(nestedPayload?.recipient_name) ??
    asTrimmedString(nestedPayload?.recipientName) ??
    asTrimmedString(nestedPayload?.raw_recipient_name) ??
    asTrimmedString(nestedPayload?.rawRecipientName) ??
    asTrimmedString(raw?.recipient_name) ??
    asTrimmedString(raw?.recipientName) ??
    asTrimmedString(raw?.["Recipient Name"]) ??
    asTrimmedString(event.member_name) ??
    null
  );
}

function resolveGovernmentContractSourceUrl(event: { payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const nestedPayload = payload?.payload && typeof payload.payload === "object" ? payload.payload : null;
  return (
    asTrimmedString(payload?.source_url) ??
    asTrimmedString(payload?.sourceUrl) ??
    asTrimmedString(nestedPayload?.source_url) ??
    asTrimmedString(nestedPayload?.sourceUrl) ??
    asTrimmedString(raw?.source_url) ??
    asTrimmedString(raw?.sourceUrl) ??
    null
  );
}

function latestEvent<T extends { ts?: string | null }>(events: T[]): T | null {
  if (events.length === 0) return null;
  return [...events].sort((a, b) => {
    const aTime = Date.parse(a.ts ?? "");
    const bTime = Date.parse(b.ts ?? "");
    if (!Number.isNaN(aTime) && !Number.isNaN(bTime)) return bTime - aTime;
    if (!Number.isNaN(aTime)) return -1;
    if (!Number.isNaN(bTime)) return 1;
    return 0;
  })[0] ?? null;
}

function buildCrossSourceSummary({
  confirmation,
  congressEvents,
  insiderEvents,
}: {
  confirmation: ConfirmationSummary | null;
  congressEvents: Awaited<ReturnType<typeof getEvents>>["items"];
  insiderEvents: Awaited<ReturnType<typeof getEvents>>["items"];
}): string {
  const congressEvent = latestEvent(congressEvents);
  const insiderEvent = latestEvent(insiderEvents);

  if (!congressEvent && !insiderEvent) {
    return confirmation?.cross_source_confirmed_30d
      ? "Congress and insider activity are both flagged in the recent activity window, but detailed trade records are not available in the current filter."
      : "No matching Congress or insider trade details are available in the current filter.";
  }

  const parts: string[] = [];

  if (congressEvent) {
    const memberName = formatCongressIdentity(congressEvent);
    const side = formatTransactionLabel(congressEvent.trade_type).toLowerCase();
    const price = resolveCongressTradePrice(congressEvent);
    const tradeDate = formatDateShort(resolveCongressTradeDate(congressEvent));
    const reportDate = formatDateShort(resolveCongressReportDate(congressEvent));
    parts.push(
      `${memberName} ${side} at ${price !== null ? formatCurrency(price) : "an undisclosed price"} on ${tradeDate} (reported ${reportDate}).`,
    );
  }

  if (insiderEvent) {
    const insiderName = formatInsiderIdentity(insiderEvent);
    const side = formatTransactionLabel(insiderEvent.trade_type).toLowerCase();
    const price = resolveInsiderTradePrice(insiderEvent);
    const tradeDate = formatDateShort(resolveInsiderTradeDate(insiderEvent));
    const filingDate = formatDateShort(resolveInsiderFilingDate(insiderEvent));
    parts.push(
      `${insiderName} ${side} at ${price !== null ? formatCurrency(price) : "an undisclosed price"} on ${tradeDate} (filed ${filingDate}).`,
    );
  }

  return parts.join(" ");
}

function formatCompactUsd(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatPnl(value: number): string {
  const marker = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${marker} ${Math.abs(value).toFixed(1)}%`;
}

function pnlClass(value: number): string {
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-300";
}

function biasLabel(buys: number, sells: number): { label: string; tone: "pos" | "neg" | "neutral" } {
  if (buys === 0 && sells === 0) return { label: "No side data", tone: "neutral" };
  if (buys > sells) return { label: "BUY LEANING", tone: "pos" };
  if (sells > buys) return { label: "SELL LEANING", tone: "neg" };
  return { label: "Balanced", tone: "neutral" };
}

function biasTextClass(tone: "pos" | "neg" | "neutral"): string {
  if (tone === "pos") return "text-emerald-300";
  if (tone === "neg") return "text-rose-300";
  return "text-slate-400";
}

function formatSignalStrengthText(band?: string | null): string {
  const cleaned = (band ?? "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "Signal";
  const label = cleaned.replace(/\b\w/g, (letter) => letter.toUpperCase());
  return `${label} signal`;
}

function signalGateForAuthenticatedFreeUser(): SignalGateState {
  return { reason: "upgrade", message: "Subscribe to premium to unlock signal activity." };
}

function signalGateForUnauthenticatedUser(): SignalGateState {
  return { reason: "auth", message: "Create an account and subscribe to premium to unlock signal activity." };
}

function canUseSignalActivity(entitlements: Entitlements | null): boolean {
  if (entitlements?.status === "temporarily_unavailable") return true;
  return entitlements ? hasEntitlement(entitlements, "signals") : true;
}

function canUseTickerConfirmation(entitlements: Entitlements | null): boolean {
  if (entitlements?.status === "temporarily_unavailable") return true;
  return entitlements ? hasEntitlement(entitlements, "ticker_confirmation") : true;
}

function canUseProTickerContext(entitlements: Entitlements | null): boolean {
  if (entitlements?.status === "temporarily_unavailable") return true;
  if (!entitlements) return true;
  return (
    entitlements.tier === "pro"
    || entitlements.tier === "admin"
    || entitlements.effective_tier === "pro"
    || entitlements.effective_tier === "admin"
    || Boolean(entitlements.is_admin || entitlements.user?.is_admin)
  );
}

function tickerContextSourceEntitlements(entitlements: Entitlements | null, authenticated: boolean): TickerSourceEntitlements {
  const signalsLocked = !canUseSignalActivity(entitlements);
  const proLocked = !canUseProTickerContext(entitlements);
  const meta = (
    source: ConfirmationSourceKey,
    requiredPlan: TickerSourceEntitlement["required_plan"],
    locked: boolean,
    lockState?: TickerSourceEntitlement["lock_state"],
  ): TickerSourceEntitlement => ({
    source,
    required_plan: requiredPlan,
    lock_state: locked ? lockState ?? (requiredPlan === "pro" ? "pro_locked" : requiredPlan === "premium" ? "premium_locked" : "requires_login") : "available",
    locked,
    available: !locked,
  });

  if (!authenticated) {
    return {
      price_volume: meta("price_volume", null, false),
      fundamentals: meta("fundamentals", null, false),
      insiders: meta("insiders", null, false),
      congress: meta("congress", null, false),
      government_contracts: meta("government_contracts", null, false),
      signals: meta("signals", "premium", true, "premium_locked"),
      institutional_activity: meta("institutional_activity", "pro", true, "pro_locked"),
      options_flow: meta("options_flow", "pro", true, "pro_locked"),
      macro_positioning: meta("macro_positioning", "pro", true, "pro_locked"),
    };
  }

  return {
    price_volume: meta("price_volume", null, false),
    fundamentals: meta("fundamentals", null, false),
    insiders: meta("insiders", null, false),
    congress: meta("congress", null, false),
    government_contracts: meta("government_contracts", null, false),
    signals: meta("signals", "premium", signalsLocked, "premium_locked"),
    institutional_activity: meta("institutional_activity", "pro", proLocked, "pro_locked"),
    options_flow: meta("options_flow", "pro", proLocked, "pro_locked"),
    macro_positioning: meta("macro_positioning", "pro", proLocked, "pro_locked"),
  };
}

function insiderBiasLabel(confirmation: ConfirmationSummary | null): { label: string; tone: "pos" | "neg" | "neutral" } {
  if (!confirmation || !confirmation.insider_active_30d) return { label: "No insider side signal", tone: "neutral" };
  if (confirmation.insider_buy_count_30d > confirmation.insider_sell_count_30d) return { label: "Insider buy-skewed", tone: "pos" };
  if (confirmation.insider_sell_count_30d > confirmation.insider_buy_count_30d) return { label: "Insider sell-skewed", tone: "neg" };
  return { label: "Insider mixed", tone: "neutral" };
}

function inactiveConfirmationBundle(ticker: string, lookbackDays = 30): ConfirmationScoreBundle {
  return {
    ticker,
    lookback_days: lookbackDays,
    score: 0,
    band: "inactive",
    direction: "neutral",
    status: "Inactive",
    explanation: "Congress, insider, signal conviction, and price confirmation sources are inactive for this lookback.",
    sources: {
      congress: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Inactive" },
      insiders: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Inactive" },
      signals: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No current signal conviction" },
      price_volume: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No price confirmation" },
      fundamentals: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Fundamentals unavailable", status: "unavailable" },
      options_flow: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Options flow not confirming" },
      government_contracts: {
        present: false,
        direction: "neutral",
        strength: 0,
        quality: 0,
        freshness_days: null,
        label: "Government Contracts",
        score_contribution: 0,
        detail: `No qualifying contracts found in the ${lastContextWindowLabel(lookbackDays)}.`,
        summary: `No qualifying contracts found in the ${lastContextWindowLabel(lookbackDays)}.`,
      },
      institutional_activity: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No recent institutional activity" },
      macro_positioning: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No macro positioning signal" },
    },
    drivers: ["Congress inactive", "Insiders inactive", "No current signal conviction"],
    active_sources: [],
    source_details: {},
  };
}

function fallbackDecisionLayer(bundle: ConfirmationScoreBundle): TickerDecisionLayer {
  const directionLabel = bundle.direction === "mixed" ? "Conflicted" : capitalizeWord(bundle.direction);
  const label = bundle.band === "inactive" && bundle.direction === "neutral"
    ? "Inactive"
    : `${capitalizeWord(bundle.band)} ${directionLabel}`;
  const activeItems = confirmationSourceOrder
    .filter((key) => bundle.sources[key].present)
    .map((key) => ({
      category: key,
      title: confirmationSourceLabels[key],
      description: bundle.sources[key].detail ?? bundle.sources[key].summary ?? bundle.sources[key].label,
      freshness: typeof bundle.sources[key].freshness_days === "number" ? `${bundle.sources[key].freshness_days}d ago` : null,
    }));
  return {
    symbol: bundle.ticker,
    freshness_window: `${bundle.lookback_days}d`,
    confirmation: {
      score: bundle.score,
      label,
      direction: bundle.direction,
      band: bundle.band,
      history: [],
    },
    summary: bundle.explanation,
    what_changed: [],
    catalysts: activeItems.filter((item) => bundle.sources[item.category as ConfirmationSourceKey]?.direction === "bullish").slice(0, 4),
    risks: activeItems.filter((item) => bundle.sources[item.category as ConfirmationSourceKey]?.direction === "bearish").slice(0, 4),
    watch_items: [
      { category: "price_volume", title: "Tape confirmation", description: "Watch whether price and volume confirm or fade." },
      { category: "fundamentals", title: "Fundamental update", description: "Watch the next reported fundamental refresh." },
    ],
    missing_data_notes: [],
  };
}

function decisionToneClass(direction?: string | null): string {
  if (direction === "bullish") return "text-emerald-300";
  if (direction === "bearish") return "text-rose-300";
  if (direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function decisionDotClass(category: string): string {
  if (category === "fundamentals" || category === "government_contracts") return "bg-emerald-300";
  if (category === "price_volume" || category === "signals") return "bg-sky-300";
  if (category === "insiders" || category === "congress") return "bg-violet-300";
  if (category === "institutional_activity" || category === "options_flow") return "bg-indigo-300";
  if (category === "macro_positioning") return "bg-amber-300";
  return "bg-slate-400";
}

function decisionDateLabel(item: TickerDecisionItem): string | null {
  if (item.date) return formatDateShort(item.date);
  return item.freshness ?? null;
}

function DecisionTrendChart({ history, direction }: { history?: { date: string; score: number }[]; direction?: string | null }) {
  const points = Array.isArray(history) ? history.filter((point) => Number.isFinite(point.score)).slice(-30) : [];
  if (points.length < 2) {
    return (
      <div className="flex h-24 items-center justify-center rounded-md border border-white/10 bg-slate-950/35 px-3 text-xs font-medium text-slate-500">
        Score history unavailable
      </div>
    );
  }
  const width = 220;
  const height = 72;
  const step = width / Math.max(points.length - 1, 1);
  const path = points.map((point, index) => {
    const x = Math.round(index * step);
    const y = Math.round(height - (Math.max(0, Math.min(100, point.score)) / 100) * height);
    return `${x},${y}`;
  }).join(" ");
  const stroke = direction === "bearish" ? "stroke-rose-400" : direction === "mixed" ? "stroke-amber-300" : "stroke-emerald-300";
  return (
    <svg className="h-24 w-full overflow-visible" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Confirmation score history">
      <line x1="0" y1={height} x2={width} y2={height} className="stroke-white/10" />
      <line x1="0" y1={Math.round(height / 2)} x2={width} y2={Math.round(height / 2)} className="stroke-white/10" />
      <polyline points={path} fill="none" className={stroke} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function DecisionItemList({ items, empty }: { items?: TickerDecisionItem[]; empty: string }) {
  const visible = (items ?? []).slice(0, 5);
  if (visible.length === 0) return <p className="text-sm leading-6 text-slate-500">{empty}</p>;
  return (
    <div className="space-y-4">
      {visible.map((item, index) => {
        const date = decisionDateLabel(item);
        return (
          <div key={`${item.category}-${item.title}-${index}`} className="grid grid-cols-[2.25rem_minmax(0,1fr)_auto] gap-3">
            <span className="flex h-9 w-9 items-center justify-center rounded-md border border-white/10 bg-slate-900/70">
              <span className={`h-2 w-2 rounded-full ${decisionDotClass(item.category)}`} />
            </span>
            <div className="min-w-0">
              <p className="text-sm font-semibold leading-5 text-slate-100">{item.title}</p>
              <p className="mt-1 text-xs leading-5 text-slate-400">{item.description}</p>
            </div>
            {date ? <span className="text-xs font-medium text-slate-500">{date}</span> : null}
          </div>
        );
      })}
    </div>
  );
}

function DecisionPanel({ title, items, empty }: { title: string; items?: TickerDecisionItem[]; empty: string }) {
  const hasMore = (items?.length ?? 0) > 5;
  return (
    <section className="min-h-[21.25rem] rounded-lg border border-white/10 bg-slate-950/40 p-5">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-200">{title}</h3>
        {hasMore ? <button type="button" className="text-sm font-medium text-sky-300">View all</button> : null}
      </div>
      <DecisionItemList items={items} empty={empty} />
      {hasMore ? <button type="button" className="mt-5 text-sm font-medium text-sky-300">View all {title.toLowerCase().replace(" (30d)", "")}</button> : null}
    </section>
  );
}

function TickerOverviewPanel({
  confirmationBundle,
  sourceDisplayBundle = confirmationBundle,
  decisionLayer,
  confirmationGate,
}: {
  confirmationBundle: ConfirmationScoreBundle;
  sourceDisplayBundle?: ConfirmationScoreBundle;
  decisionLayer?: TickerDecisionLayer | null;
  confirmationGate?: TickerConfirmationGate | null;
}) {
  const displayBundle = sourceDisplayBundle;
  const confirmationLocked = Boolean(confirmationGate?.locked);
  const layer = decisionLayer ?? fallbackDecisionLayer(displayBundle);
  const confirmation = layer.confirmation ?? {};
  const score = typeof confirmation.score === "number" ? Math.round(confirmation.score) : null;
  const direction = confirmation.direction ?? displayBundle.direction;
  const label = confirmation.label ?? overviewScoreLine(displayBundle).split(" / 100 · ").pop() ?? "Unavailable";
  const updated = confirmation.updated_at ? `Last updated ${formatDateShort(confirmation.updated_at)}` : "Last updated unavailable";

  return (
    <div className="relative">
      <div className={confirmationLocked ? "pointer-events-none select-none opacity-70 blur-[2.5px]" : ""} aria-hidden={confirmationLocked ? "true" : undefined}>
        <section className="grid gap-5 rounded-lg border border-white/10 bg-slate-950/30 px-6 py-5 lg:grid-cols-[minmax(10rem,1fr)_minmax(10rem,0.9fr)_minmax(14rem,1.05fr)_minmax(16rem,1fr)] lg:items-center">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">30-DAY CONFIRMATION</p>
            <p className="mt-3 text-xs text-slate-500">{updated}</p>
          </div>
          <div>
            <p className={`text-5xl font-semibold tabular-nums ${decisionToneClass(direction)}`}>
              {score === null ? "--" : score} <span className="text-2xl text-slate-500">/ 100</span>
            </p>
            <p className={`mt-2 text-2xl font-semibold ${decisionToneClass(direction)}`}>{label}</p>
          </div>
          <div>
            <p className="text-base leading-7 text-slate-100">{layer.summary ?? displayBundle.explanation}</p>
          </div>
          <div>
            <DecisionTrendChart history={confirmation.history} direction={direction} />
          </div>
        </section>

        <div className="mt-5 grid gap-4 lg:grid-cols-3">
          <DecisionPanel title="WHAT CHANGED (30D)" items={layer.what_changed} empty="No meaningful dated changes are available for this window." />
          <DecisionPanel title="CATALYSTS" items={layer.catalysts} empty="No positive catalyst is active in the available data." />
          <DecisionPanel title="RISKS" items={layer.risks} empty="No decision-relevant risk is active in the available data." />
        </div>

        <section className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 px-5 py-4">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h3 className="flex items-center gap-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-200">
              <span className="text-slate-400"><IntelligenceIcon kind="eye" /></span>
              WHAT TO WATCH NEXT
            </h3>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {(layer.watch_items ?? []).slice(0, 5).map((item, index) => (
              <div key={`${item.category}-${item.title}-${index}`} className="border-l border-white/10 pl-3">
                <p className="text-sm font-semibold leading-5 text-slate-100">{item.title}</p>
                <p className="mt-1 text-xs leading-5 text-slate-400">{item.description}</p>
              </div>
            ))}
          </div>
          {(!layer.watch_items || layer.watch_items.length === 0) ? (
            <p className="text-sm leading-6 text-slate-500">No specific watch items are available yet.</p>
          ) : null}
        </section>
      </div>

      {confirmationLocked && confirmationGate ? (
        <div className="absolute inset-0 z-10 flex items-start justify-center rounded-lg bg-slate-950/35 p-4 pt-16 backdrop-blur-[1px]">
          <div className="max-w-sm rounded-lg border border-emerald-300/20 bg-slate-950/90 p-4 text-center shadow-2xl shadow-black/40">
            <p className="text-sm font-semibold text-white">Premium confirmation</p>
            <p className="mt-2 text-xs leading-5 text-slate-400">{confirmationGate.message}</p>
            <Link
              href={confirmationGate.href}
              className="mt-4 inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
            >
              {confirmationGate.label}
            </Link>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function inactiveTechnicalIndicators(): TechnicalIndicators {
  return {
    source: "daily_close_history",
    asof: null,
    price_points: 0,
    rsi: {
      status: "unavailable",
      signal: "unavailable",
      message: "RSI temporarily unavailable",
      reason: "provider_error",
      value: null,
      period: 14,
    },
    macd: {
      status: "unavailable",
      signal: "unavailable",
      message: "MACD temporarily unavailable",
      reason: "provider_error",
      value: null,
    },
    ema_trend: {
      status: "unavailable",
      signal: "unavailable",
      message: "EMA trend temporarily unavailable",
      reason: "provider_error",
      value: null,
    },
  };
}

function inactiveOptionsFlowSummary(ticker: string): OptionsFlowSummary {
  return {
    ticker,
    lookback_days: 30,
    state: "unavailable",
    label: "Options flow unavailable",
    is_active: false,
    confidence: "low",
    freshness_days: null,
    summary: "Options flow unavailable.",
    signals: ["Options flow unavailable"],
    metrics: {
      put_call_premium_ratio: null,
      net_premium_skew: 0,
      recent_contract_volume: 0,
      observed_contracts: 0,
      freshness_days: null,
    },
    can_confirm: false,
    provider: "options",
  };
}

function inactiveSignalFreshnessBundle(ticker: string, lookbackDays = 30): SignalFreshnessBundle {
  return {
    ticker,
    lookback_days: lookbackDays,
    freshness_score: 0,
    freshness_state: "inactive",
    freshness_label: "No active setup",
    explanation: "No active directional confirmation sources are present in this lookback.",
    timing: {
      freshest_source_days: null,
      stalest_active_source_days: null,
      active_source_count: 0,
      overlap_window_days: null,
    },
  };
}

function normalizeConfirmationBundle(bundle: ConfirmationScoreBundle | null | undefined, ticker: string, lookbackDays = 30): ConfirmationScoreBundle {
  const fallback = inactiveConfirmationBundle(ticker, lookbackDays);
  if (!bundle) return fallback;
  const effectiveLookback = Number.isFinite(bundle.lookback_days) ? bundle.lookback_days : lookbackDays;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    lookback_days: effectiveLookback,
    sources: {
      congress: { ...fallback.sources.congress, ...(bundle.sources?.congress ?? {}) },
      insiders: { ...fallback.sources.insiders, ...(bundle.sources?.insiders ?? {}) },
      signals: { ...fallback.sources.signals, ...(bundle.sources?.signals ?? {}) },
      price_volume: { ...fallback.sources.price_volume, ...(bundle.sources?.price_volume ?? {}) },
      fundamentals: { ...fallback.sources.fundamentals, ...(bundle.sources?.fundamentals ?? {}) },
      options_flow: { ...fallback.sources.options_flow, ...(bundle.sources?.options_flow ?? {}) },
      government_contracts: { ...fallback.sources.government_contracts, ...(bundle.sources?.government_contracts ?? {}) },
      institutional_activity: { ...fallback.sources.institutional_activity, ...(bundle.sources?.institutional_activity ?? {}) },
      macro_positioning: { ...fallback.sources.macro_positioning, ...(bundle.sources?.macro_positioning ?? {}) },
    },
    drivers: Array.isArray(bundle.drivers) && bundle.drivers.length > 0 ? bundle.drivers.slice(0, 4) : fallback.drivers,
  };
}

function normalizeOptionsFlowSummary(bundle: OptionsFlowSummary | null | undefined, ticker: string, lookbackDays = 30): OptionsFlowSummary {
  const fallback = { ...inactiveOptionsFlowSummary(ticker), lookback_days: lookbackDays };
  if (!bundle) return fallback;
  const effectiveLookback = Number.isFinite(bundle.lookback_days) ? bundle.lookback_days : lookbackDays;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    lookback_days: effectiveLookback,
    signals: Array.isArray(bundle.signals) && bundle.signals.length > 0 ? bundle.signals.slice(0, 4) : fallback.signals,
    metrics: {
      ...fallback.metrics,
      ...(bundle.metrics ?? {}),
    },
  };
}

function normalizeSignalFreshness(bundle: SignalFreshnessBundle | null | undefined, ticker: string, lookbackDays = 30): SignalFreshnessBundle {
  const fallback = inactiveSignalFreshnessBundle(ticker, lookbackDays);
  if (!bundle) return fallback;
  const effectiveLookback = Number.isFinite(bundle.lookback_days) ? bundle.lookback_days : lookbackDays;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    lookback_days: effectiveLookback,
    timing: {
      ...fallback.timing,
      ...(bundle.timing ?? {}),
    },
  };
}

function normalizeTechnicalIndicators(bundle: TechnicalIndicators | null | undefined): TechnicalIndicators {
  const fallback = inactiveTechnicalIndicators();
  if (!bundle) return fallback;
  return {
    ...fallback,
    ...bundle,
    rsi: { ...fallback.rsi, ...(bundle.rsi ?? {}) },
    macd: { ...fallback.macd, ...(bundle.macd ?? {}) },
    ema_trend: { ...fallback.ema_trend, ...(bundle.ema_trend ?? {}) },
  };
}

const confirmationSourceLabels: Record<ConfirmationSourceKey, string> = {
  congress: "Congress",
  insiders: "Insiders",
  signals: "Signals",
  price_volume: "Price / Volume",
  fundamentals: "Fundamentals",
  options_flow: "Options Flow",
  government_contracts: "Government Contracts",
  institutional_activity: "Institutional Activity",
  macro_positioning: "Macro Positioning",
};

const confirmationSourceOrder: ConfirmationSourceKey[] = [
  "congress",
  "insiders",
  "signals",
  "price_volume",
  "fundamentals",
  "options_flow",
  "government_contracts",
  "institutional_activity",
  "macro_positioning",
];

function sourceStateClass(direction: ConfirmationScoreBundle["direction"] | "inactive"): string {
  if (direction === "bullish") return "text-emerald-300";
  if (direction === "bearish") return "text-rose-300";
  if (direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function technicalToneClass(tone: "bullish" | "bearish" | "mixed" | "inactive" | "unavailable"): string {
  if (tone === "bullish") return "text-emerald-300";
  if (tone === "bearish") return "text-rose-300";
  if (tone === "mixed") return "text-amber-300";
  if (tone === "unavailable") return "text-slate-500";
  return "text-slate-400";
}

function sourceUnavailable(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): boolean {
  const status = (source.status ?? "").toLowerCase();
  return !source.present && ["unavailable", "not_configured", "disabled", "provider_error", "error"].includes(status);
}

function sourceStateLabel(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  if (sourceUnavailable(source)) return "UNAVAILABLE";
  if (source.present && source.score_contribution && source.score_contribution > 0) return "BULLISH SUPPORT";
  return source.present ? source.direction.toUpperCase() : "INACTIVE";
}

function formatConfirmationSourceList(keys: ConfirmationSourceKey[]): string {
  if (keys.length === 0) return "No active sources";
  return keys.map((key) => confirmationSourceLabels[key]).join(" + ");
}

function alignedConfirmationSources(bundle: ConfirmationScoreBundle): ConfirmationSourceKey[] {
  if (bundle.direction === "neutral" || bundle.direction === "mixed") {
    return confirmationSourceOrder.filter((key) => bundle.sources[key].present);
  }
  return confirmationSourceOrder.filter((key) => {
    const source = bundle.sources[key];
    return source.present && source.direction === bundle.direction;
  });
}

function sourceEntitlement(entitlements: TickerSourceEntitlements | null | undefined, source: ConfirmationSourceKey): TickerSourceEntitlement | null {
  return entitlements?.[source] ?? null;
}

function sourceIsLocked(entitlements: TickerSourceEntitlements | null | undefined, source: ConfirmationSourceKey): boolean {
  return Boolean(sourceEntitlement(entitlements, source)?.locked);
}

function displaySourceEntitlementsForTickerContext(
  activityEntitlements: TickerSourceEntitlements | null | undefined,
  fallbackEntitlements: TickerSourceEntitlements,
  allowAuthHintOverride: boolean,
): TickerSourceEntitlements {
  if (!activityEntitlements) return fallbackEntitlements;
  if (!allowAuthHintOverride) return activityEntitlements;
  const merged = { ...activityEntitlements };
  for (const source of confirmationSourceOrder) {
    const activityMeta = activityEntitlements[source];
    const fallbackMeta = fallbackEntitlements[source];
    if (activityMeta?.locked && fallbackMeta?.locked === false) {
      merged[source] = fallbackMeta;
    }
  }
  return merged;
}

function lockFeatureLabel(requiredPlan?: TickerSourceEntitlement["required_plan"]): string {
  if (requiredPlan === "free") return "Create a free account";
  return requiredPlan === "pro" ? "Pro feature" : "Premium feature";
}

function confirmationBandForDisplayScore(score: number): ConfirmationScoreBundle["band"] {
  if (score <= 19) return "inactive";
  if (score <= 39) return "weak";
  if (score <= 59) return "moderate";
  if (score <= 79) return "strong";
  return "exceptional";
}

function displayDirectionForSources(sources: ConfirmationScoreBundle["sources"]): ConfirmationScoreBundle["direction"] {
  const directionalSources = confirmationSourceOrder.filter((source) => (
    source !== "government_contracts"
    && sources[source].present
    && sources[source].direction !== "neutral"
  ));
  const directions = new Set(directionalSources.map((source) => sources[source].direction));
  if (directions.has("mixed") || (directions.has("bullish") && directions.has("bearish"))) return "mixed";
  if (directions.has("bullish")) return "bullish";
  if (directions.has("bearish")) return "bearish";
  return "neutral";
}

function displayScoreForSources(sources: ConfirmationScoreBundle["sources"]): number {
  const directionalSources = confirmationSourceOrder.filter((source) => (
    source !== "government_contracts"
    && sources[source].present
    && sources[source].direction !== "neutral"
  ));
  const directionalScore = directionalSources.length > 0
    ? directionalSources.reduce((sum, source) => sum + Math.max(sources[source].strength, sources[source].quality), 0) / directionalSources.length
    : 0;
  const supportScore = sources.government_contracts.present
    ? sources.government_contracts.score_contribution ?? Math.max(1, Math.min(sources.government_contracts.strength, 20))
    : 0;
  return Math.max(0, Math.min(100, Math.round(directionalScore + supportScore)));
}

function displayConfirmationBundleForEntitlements(
  bundle: ConfirmationScoreBundle,
  entitlements: TickerSourceEntitlements | null | undefined,
): ConfirmationScoreBundle {
  const lockedSources = confirmationSourceOrder.filter((source) => sourceIsLocked(entitlements, source));
  if (lockedSources.length === 0) return bundle;
  const sources = { ...bundle.sources };
  const lockedActiveSources = lockedSources.filter((source) => bundle.sources[source].present);
  for (const source of lockedSources) {
    const meta = sourceEntitlement(entitlements, source);
    sources[source] = {
      ...sources[source],
      present: false,
      direction: "neutral",
      strength: 0,
      quality: 0,
      freshness_days: null,
      label: lockFeatureLabel(meta?.required_plan),
    };
  }
  const visibleActiveSources = confirmationSourceOrder.filter((source) => sources[source].present);
  if (visibleActiveSources.length === 0 && lockedActiveSources.length > 0) {
    return {
      ...bundle,
      score: 0,
      band: "inactive",
      direction: "neutral",
      status: "Locked source context",
      explanation: "Additional Premium/Pro context is locked for this ticker.",
      sources,
      drivers: ["Additional Premium/Pro context locked"],
      active_sources: [],
      source_details: {},
    };
  }
  if (lockedActiveSources.length === 0) return { ...bundle, sources };
  const displayScore = displayScoreForSources(sources);
  const displayDirection = displayDirectionForSources(sources);
  return {
    ...bundle,
    score: displayScore,
    band: confirmationBandForDisplayScore(displayScore),
    direction: displayDirection,
    status: visibleActiveSources.length > 0 ? "Visible context" : "Inactive",
    explanation: visibleActiveSources.length > 0
      ? "Visible confirmation context is based on unlocked sources."
      : "No unlocked confirmation sources are active for this lookback.",
    sources,
    active_sources: visibleActiveSources,
  };
}

function inactiveOrUnalignedSourceLine(bundle: ConfirmationScoreBundle, alignedSources: ConfirmationSourceKey[]): string {
  const aligned = new Set(alignedSources);
  const parts = confirmationSourceOrder
    .filter((key) => !aligned.has(key))
    .map((key) => {
      const source = bundle.sources[key];
      if (!source.present) return `${confirmationSourceLabels[key]} inactive`;
      return `${confirmationSourceLabels[key]} ${source.direction}`;
    });
  return parts.length > 0 ? parts.join(" · ") : "All tracked sources aligned";
}

function setupTimingLabel(freshness: SignalFreshnessBundle): string {
  if (freshness.timing.active_source_count <= 0) return "Timing inactive";
  if (freshness.timing.freshest_source_days === null && freshness.timing.stalest_active_source_days === null) return "Timing limited";
  if (freshness.freshness_state === "stale") return "Older setup";
  if (freshness.freshness_state === "maturing") return "Maturing setup";
  return "Fresh setup";
}

function timingDetailLine(freshness: SignalFreshnessBundle): string {
  const timing = freshness.timing;
  if (timing.active_source_count > 0 && timing.freshest_source_days === null && timing.stalest_active_source_days === null) {
    return `${timing.active_source_count} active source${timing.active_source_count === 1 ? "" : "s"} · dates limited`;
  }
  const freshest = timing.freshest_source_days === null ? "--" : `${timing.freshest_source_days}d`;
  const oldest = timing.stalest_active_source_days === null ? "--" : `${timing.stalest_active_source_days}d`;
  const overlap = timing.overlap_window_days === null ? "--" : `${timing.overlap_window_days}d`;
  return `${freshest} freshest · ${oldest} oldest · ${overlap} overlap`;
}

function overviewTimestamp(freshness: SignalFreshnessBundle): string {
  const freshest = freshness.timing.freshest_source_days;
  if (freshest === null || freshest === undefined) return "Updated --";
  if (freshest === 0) return "Updated today";
  return `Updated ${freshest}d ago`;
}

function overviewHeadline(bundle: ConfirmationScoreBundle): string {
  if (bundle.direction === "bearish") return "Bearish confirmation";
  if (bundle.direction === "bullish") return "Bullish confirmation";
  if (bundle.direction === "mixed") return "Conflicted confirmation";
  if (bundle.sources.government_contracts.present) return "Positive support building";
  return "No active confirmation";
}

function overviewSubheadline(alignedSources: ConfirmationSourceKey[]): string {
  if (alignedSources.length <= 0) return "No active sources aligned.";
  return `${alignedSources.length} active source${alignedSources.length === 1 ? "" : "s"} aligned.`;
}

function capitalizeWord(value: string): string {
  if (!value) return value;
  return `${value.slice(0, 1).toUpperCase()}${value.slice(1)}`;
}

function confirmationDirectionDisplay(value: string): string {
  return value === "mixed" ? "Conflicted" : capitalizeWord(value);
}

function overviewScoreLine(bundle: ConfirmationScoreBundle): string {
  if (bundle.band === "inactive" && bundle.direction === "neutral") {
    return `${Math.round(bundle.score)} / 100 · Inactive`;
  }
  return `${Math.round(bundle.score)} / 100 · ${capitalizeWord(bundle.band)} ${confirmationDirectionDisplay(bundle.direction)}`;
}

function overviewBullets({
  confirmationBundle,
  alignedSources,
}: {
  confirmationBundle: ConfirmationScoreBundle;
  alignedSources: ConfirmationSourceKey[];
}): string[] {
  const bullets = new Set<string>();
  const activeLabels = Array.from(new Set(alignedSources.map((key) => confirmationSourceLabels[key])));
  if (activeLabels.length > 0) bullets.add(`Active sources: ${activeLabels.join(" · ")}`);
  if (confirmationBundle.sources.government_contracts.present) {
    const governmentSummary = normalizeUpperCardWindowCopy(
      confirmationBundle.sources.government_contracts.detail ?? confirmationBundle.sources.government_contracts.summary,
      confirmationBundle.lookback_days,
    ) ?? `Government contracts are active in the ${contextWindowNoun(confirmationBundle.lookback_days)}.`;
    if (confirmationBundle.direction === "bearish") {
      bullets.add("Government contracts add positive support, while other sources remain bearish.");
    } else {
      bullets.add(governmentSummary);
    }
  }
  if (confirmationBundle.sources.insiders.present) {
    if (confirmationBundle.sources.insiders.direction === "bearish") bullets.add("Insider activity: active / sell-skewed");
    else if (confirmationBundle.sources.insiders.direction === "bullish") bullets.add("Insider activity: active / buy-skewed");
    else bullets.add("Insider activity: active / balanced");
  }
  if (confirmationBundle.sources.signals.present) {
    if (confirmationBundle.sources.signals.direction === "bearish") bullets.add("Signals: confirmed bearish");
    else if (confirmationBundle.sources.signals.direction === "bullish") bullets.add("Signals: confirmed bullish");
    else bullets.add("Signals: mixed");
  }
  if (confirmationBundle.sources.price_volume.present) {
    if (confirmationBundle.sources.price_volume.direction === "bearish") bullets.add("Price / Volume: bearish tape");
    else if (confirmationBundle.sources.price_volume.direction === "bullish") bullets.add("Price / Volume: bullish tape");
    else bullets.add("Price / Volume: mixed tape");
  }
  if (confirmationBundle.sources.institutional_activity.present) {
    if (confirmationBundle.sources.institutional_activity.direction === "bearish") bullets.add("Institutional Activity: active / reduction");
    else if (confirmationBundle.sources.institutional_activity.direction === "bullish") bullets.add("Institutional Activity: active / accumulation");
    else bullets.add("Institutional Activity: active / mixed");
  }
  if (confirmationBundle.sources.macro_positioning.present) {
    const summary = confirmationBundle.sources.macro_positioning.summary ?? confirmationBundle.sources.macro_positioning.detail;
    if (summary) bullets.add(`Macro Positioning: ${summary}`);
    else if (confirmationBundle.sources.macro_positioning.direction === "bearish") bullets.add("Macro Positioning: Macro headwinds.");
    else if (confirmationBundle.sources.macro_positioning.direction === "bullish") bullets.add("Macro Positioning: Bullish backdrop.");
    else bullets.add("Macro Positioning: Neutral macro backdrop.");
  }
  if (confirmationBundle.sources.fundamentals.present) {
    if (confirmationBundle.sources.fundamentals.direction === "bearish") bullets.add("Fundamentals: pressure");
    else if (confirmationBundle.sources.fundamentals.direction === "bullish") bullets.add("Fundamentals: strength");
    else bullets.add("Fundamentals: mixed");
  }
  if (confirmationBundle.sources.congress.present) {
    if (confirmationBundle.sources.congress.direction === "bearish") bullets.add("Congress activity: active / sell-skewed");
    else if (confirmationBundle.sources.congress.direction === "bullish") bullets.add("Congress activity: active / buy-skewed");
    else bullets.add("Congress activity: active / mixed");
  }
  return Array.from(bullets).slice(0, 5);
}

function overviewMutedLine(bundle: ConfirmationScoreBundle): string | null {
  if (bundle.sources.government_contracts.present && bundle.direction === "neutral") {
    return "Government contracts are active, but broader directional confirmation is still limited.";
  }
  if (!bundle.sources.price_volume.present && !bundle.sources.fundamentals.present && !bundle.sources.options_flow.present) {
    return "Price / volume, fundamentals, and options flow are inactive.";
  }
  return null;
}

function overviewCaveat(bundle: ConfirmationScoreBundle): string {
  if (bundle.status === "Visible context") {
    return "Additional Premium/Pro context is available behind locked source cards.";
  }
  if (bundle.direction === "bearish" && bundle.sources.government_contracts.present) {
    return "Government contracts add bullish support, but broader sources still lean bearish.";
  }
  if (!bundle.sources.signals.present) return "Signal activity is not active in this window.";
  return "Signal activity is active in this window.";
}

function priceVolumeSummary(
  source: ConfirmationScoreBundle["sources"]["price_volume"],
  technicalIndicators: TechnicalIndicators,
  context?: TickerSignalsSummaryResponse["price_volume"] | null,
  lookbackDays = SIGNAL_WINDOW_DAYS,
): { state: string; summary: string; diagnostics: string[]; tone: "bullish" | "bearish" | "mixed" | "inactive" | "unavailable" } {
  const diagnostics = [
    technicalIndicators.rsi.message,
    technicalIndicators.macd.message,
    technicalIndicators.ema_trend.message,
  ];
  const contextStatus = typeof context?.status === "string" ? context.status.toLowerCase() : null;
  const contextDirection = typeof context?.direction === "string" ? context.direction.toLowerCase() : null;
  const contextSummary = normalizeUpperCardWindowCopy(context?.summary ?? context?.title, lookbackDays) ?? "";
  const priceVolumeRows = compactPriceVolumeRows(context, technicalIndicators);
  const inactiveSummary = `No strong price/volume signal in the ${lastContextWindowLabel(lookbackDays)}.`;
  if (contextStatus === "active") {
    const direction = contextDirection === "bullish" || contextDirection === "bearish" || contextDirection === "mixed"
      ? contextDirection
      : null;
    return {
      state: direction === "bullish" || direction === "bearish" ? direction.toUpperCase() : direction === "mixed" ? "MIXED" : "ACTIVE",
      summary: contextSummary || "Tape confirmation is active",
      diagnostics: priceVolumeRows,
      tone: direction ?? "mixed",
    };
  }
  if (contextStatus === "inactive") {
    return {
      state: "INACTIVE",
      summary: inactiveSummary,
      diagnostics: priceVolumeRows,
      tone: "inactive",
    };
  }
  if (contextStatus === "limited") {
    const summary = contextSummary || "Limited price history.";
    return {
      state: "LIMITED",
      summary,
      diagnostics: priceVolumeRows,
      tone: "unavailable",
    };
  }
  if (contextStatus === "loading") {
    const summary = contextSummary || "Loading price and volume data.";
    return {
      state: "LOADING",
      summary,
      diagnostics: priceVolumeRows,
      tone: "unavailable",
    };
  }
  if (contextStatus === "unavailable") {
    const summary = contextSummary || "Price and volume unavailable.";
    return {
      state: "UNAVAILABLE",
      summary,
      diagnostics: priceVolumeRows,
      tone: "unavailable",
    };
  }
  const indicatorsUnavailable = diagnostics.every((item) => item.toLowerCase().includes("unavailable"));
  const hasTechnicalInputs = Number(technicalIndicators.price_points ?? 0) > 0;
  if (!source.present && indicatorsUnavailable) {
    const insufficientHistory = [technicalIndicators.rsi, technicalIndicators.macd, technicalIndicators.ema_trend].some(
      (item) => item.reason === "insufficient_price_history",
    );
    if (hasTechnicalInputs) {
      const summary = insufficientHistory || technicalIndicators.price_points < 35 ? "Limited price history." : inactiveSummary;
      return {
        state: insufficientHistory || technicalIndicators.price_points < 35 ? "LIMITED" : "INACTIVE",
        summary,
        diagnostics: priceVolumeRows,
        tone: insufficientHistory || technicalIndicators.price_points < 35 ? "unavailable" : "inactive",
      };
    }
    const summary = insufficientHistory ? "Limited price history." : "Loading price and volume data.";
    return {
      state: "UNAVAILABLE",
      summary,
      diagnostics: priceVolumeRows,
      tone: "unavailable",
    };
  }
  if (!source.present) {
    return {
      state: "INACTIVE",
      summary: inactiveSummary,
      diagnostics: priceVolumeRows,
      tone: "inactive",
    };
  }
  if (source.direction === "bearish") {
    return {
      state: "BEARISH",
      summary: "Bearish days with elevated volume",
      diagnostics: priceVolumeRows,
      tone: "bearish",
    };
  }
  if (source.direction === "bullish") {
    return {
      state: "BULLISH",
      summary: "Bullish days with elevated volume",
      diagnostics: priceVolumeRows,
      tone: "bullish",
    };
  }
  return {
    state: "MIXED",
    summary: "Tape confirmation is mixed",
    diagnostics: priceVolumeRows,
    tone: "mixed",
  };
}

function formatUpperCardPrice(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "\u2014";
}

function formatUpperCardSignedPercent(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "\u2014";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function formatUpperCardMultiple(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(2)}x` : "\u2014";
}

type UpperCardTechnicalReading = TechnicalIndicators["rsi"] | NonNullable<NonNullable<TickerSignalsSummaryResponse["price_volume"]>["rsi"]>;

function formatUpperCardRsi(reading: UpperCardTechnicalReading | null | undefined): string {
  return typeof reading?.value === "number" && Number.isFinite(reading.value) ? reading.value.toFixed(1) : "\u2014";
}

function formatUpperCardMacd(reading: UpperCardTechnicalReading | null | undefined): string {
  const signal = typeof reading?.signal === "string" ? reading.signal.toLowerCase() : "";
  if (signal === "bullish") return "MACD bullish";
  if (signal === "bearish") return "MACD bearish";
  if (signal === "neutral" || signal === "flat" || signal === "mixed") return "MACD neutral";
  return "MACD \u2014";
}

function compactPriceVolumeRows(
  context: TickerSignalsSummaryResponse["price_volume"] | null | undefined,
  technicalIndicators: TechnicalIndicators,
): string[] {
  const rsi = context?.rsi ?? technicalIndicators.rsi;
  const macd = context?.macd ?? technicalIndicators.macd;
  return [
    `Latest close ${formatUpperCardPrice(context?.latest_close)}`,
    `1D change ${formatUpperCardSignedPercent(context?.change_pct_1d)}`,
    `Vol vs 30D ${formatUpperCardMultiple(context?.volume_vs_avg)}`,
    `RSI ${formatUpperCardRsi(rsi)}`,
    formatUpperCardMacd(macd),
  ];
}

const FUNDAMENTALS_METRICS = [
  {
    key: "revenue_growth",
    label: "Revenue Growth",
    tooltip: "Measures reported revenue growth. This is not acquisition-adjusted organic growth unless specifically stated.",
  },
  {
    key: "return_on_equity",
    label: "ROE",
    tooltip: "Measures net income as a percentage of shareholders' equity. Higher and rising ROE can indicate efficient capital deployment.",
  },
  {
    key: "ev_to_ebitda",
    label: "EV/EBITDA",
    tooltip: "Compares enterprise value to EBITDA. Lower values relative to peers can suggest cheaper valuation; higher values can suggest expensive expectations.",
  },
  {
    key: "operating_margin_expansion",
    label: "Op Margin \u0394",
    tooltip: "Measures whether operating margins are improving or contracting over time.",
  },
  {
    key: "net_debt_to_ebitda",
    label: "Net Debt / EBITDA",
    tooltip: "Measures leverage against operating earnings. Lower values usually indicate a stronger balance sheet.",
  },
] as const;

function fundamentalsToneClass(status: string | null | undefined): string {
  if (status === "bullish") return "text-emerald-300";
  if (status === "bearish") return "text-rose-300";
  if (status === "mixed") return "text-amber-300";
  return "text-slate-500";
}

function fundamentalsStateLabel(status: string | null | undefined): string {
  if (status === "bullish") return "BULLISH";
  if (status === "bearish") return "BEARISH";
  if (status === "mixed") return "MIXED";
  return "UNAVAILABLE";
}

function fundamentalsHeadline(summary: TickerFundamentalsSummary): string {
  const headline = typeof summary?.headline === "string" && summary.headline.trim() ? summary.headline.trim() : null;
  if (headline) return headline;
  const status = summary?.status;
  if (status === "bullish") return "Fundamental strength";
  if (status === "bearish") return "Fundamental pressure";
  if (status === "mixed") return "Mixed fundamental profile";
  return "Fundamentals unavailable";
}

function FundamentalsCard({ summary }: { summary: TickerFundamentalsSummary }) {
  const status = typeof summary?.status === "string" ? summary.status.toLowerCase() : "unavailable";
  const metrics = summary?.metrics ?? {};
  return (
    <div className={`${cardClassName} h-full !rounded-lg p-4`}>
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className={fundamentalsToneClass(status)}>
            <IntelligenceIcon kind="fundamentals" />
          </span>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Fundamentals</p>
        </div>
        <p className={`text-xs font-semibold uppercase tracking-[0.14em] ${fundamentalsToneClass(status)}`}>
          {fundamentalsStateLabel(status)}
        </p>
      </div>
      <p className="mt-3 text-sm font-semibold text-slate-100">{fundamentalsHeadline(summary)}</p>
      <div className="mt-3 grid gap-1.5">
        {FUNDAMENTALS_METRICS.map((metric) => {
          const value = metrics[metric.key];
          const display = typeof value?.display === "string" && value.display.trim() ? value.display : "\u2014";
          const state = typeof value?.state === "string" ? value.state : "unavailable";
          return (
            <div key={metric.key} title={metric.tooltip} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3 text-xs">
              <span className="min-w-0 truncate text-slate-400">{metric.label}</span>
              <span className={`font-semibold tabular-nums ${fundamentalsToneClass(state === "neutral" ? "mixed" : state)}`}>{display}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function sourceCardToneClass(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  return source.present ? sourceStateClass(source.direction) : "text-slate-500";
}

function sourceCardBorderClass(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  if (!source.present) return "border-white/10 bg-white/[0.025]";
  if (source.direction === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  if (source.direction === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  return "border-amber-400/20 bg-amber-400/[0.04]";
}

function optionsFlowToneClass(state: OptionsFlowSummary["state"]): string {
  if (state === "bullish") return "text-emerald-300";
  if (state === "bearish") return "text-rose-300";
  if (state === "mixed") return "text-amber-300";
  return "text-slate-500";
}

function optionsFlowBorderClass(summary: OptionsFlowSummary): string {
  if (summary.state === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  if (summary.state === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  if (summary.state === "mixed") return "border-amber-400/20 bg-amber-400/[0.04]";
  return "border-white/10 bg-white/[0.025]";
}

function optionsFlowDiagnostics(summary: OptionsFlowSummary): string[] {
  if (Array.isArray(summary.signals) && summary.signals.length > 0) return summary.signals.slice(0, 4);
  if (summary.state === "inactive") return [`No notable options flow in the ${lastContextWindowLabel(summary.lookback_days)}.`];
  if (summary.state === "unavailable") return ["Options flow unavailable"];
  return [summary.summary || "Options flow is active"];
}

function insiderSourceBody(buys: number, sells: number, source: ConfirmationScoreBundle["sources"]["insiders"], lookbackDays: number): string {
  if (!source.present) return `No notable insider activity in the ${lastContextWindowLabel(lookbackDays)}.`;
  if (sells > buys) return "Active / sell-skewed";
  if (buys > sells) return "Active / buy-skewed";
  return "Active / balanced";
}

function insiderSourceSupport(buys: number, sells: number, lookbackDays: number): string {
  if (buys + sells <= 0) return `No qualifying insider buys or sells found in the ${contextWindowNoun(lookbackDays)}.`;
  if (sells > buys) return `${sells - buys} net sells · ${contextWindowLabel(lookbackDays)}`;
  if (buys > sells) return `${buys - sells} net buys · ${contextWindowLabel(lookbackDays)}`;
  return `${buys + sells} trades · ${contextWindowLabel(lookbackDays)}`;
}

function congressSourceSupport(buys: number, sells: number, lookbackDays: number): string {
  if (buys + sells <= 0) return `No qualifying Congress trades found in the ${contextWindowNoun(lookbackDays)}.`;
  if (sells > buys) return `${sells - buys} net sells · ${contextWindowLabel(lookbackDays)}`;
  if (buys > sells) return `${buys - sells} net buys · ${contextWindowLabel(lookbackDays)}`;
  return `${buys + sells} trades · ${contextWindowLabel(lookbackDays)}`;
}

function sourceCardBody(key: "congress" | "signals", source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey], topSignal: TickerActivityData["topSignal"], lookbackDays: number): string {
  if (!source.present) {
    return key === "congress"
      ? `No notable Congress activity in the ${lastContextWindowLabel(lookbackDays)}.`
      : `No active signal stack in the ${lastContextWindowLabel(lookbackDays)}.`;
  }
  if (key === "signals") return topSignal ? "Signal conviction active" : "Signal source active";
  return source.direction === "bearish" ? "Active / sell-skewed" : source.direction === "bullish" ? "Active / buy-skewed" : "Active / mixed";
}

function summaryCount(context: TickerSignalsSummaryResponse["insiders"] | TickerSignalsSummaryResponse["congress"] | null, key: "buy_count" | "sell_count"): number {
  const value = context?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function sourceFromTopSignal(
  source: ConfirmationScoreBundle["sources"]["signals"],
  topSignal: TickerActivityData["topSignal"],
): ConfirmationScoreBundle["sources"]["signals"] {
  if (!source.present || !topSignal) return source;
  const side = normalizeTradeSide(topSignal.trade_type);
  const direction = side === "buy" ? "bullish" : side === "sell" ? "bearish" : source.direction === "neutral" ? "mixed" : source.direction;
  return {
    ...source,
    present: true,
    direction,
    label: topSignal.smart_band ? `${topSignal.smart_band} smart signal` : "Signal conviction active",
  };
}

type IntelligenceIconKind =
  | "congress"
  | "government-contract"
  | "insider-buy"
  | "insider-sell"
  | "signals"
  | "price-volume"
  | "fundamentals"
  | "flow"
  | "people"
  | "eye";

function IntelligenceIcon({ kind, className = "h-4 w-4" }: { kind: IntelligenceIconKind; className?: string }) {
  if (kind === "eye") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z" />
        <circle cx="12" cy="12" r="2.5" />
      </svg>
    );
  }
  if (kind === "congress") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 9h16" />
        <path d="M5 19h14" />
        <path d="M7 9v10" />
        <path d="M12 9v10" />
        <path d="M17 9v10" />
        <path d="M3 21h18" />
        <path d="M12 3 4 7h16l-8-4Z" />
      </svg>
    );
  }
  if (kind === "government-contract") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 20h16" />
        <path d="M6 20V9" />
        <path d="M12 20V9" />
        <path d="M18 20V9" />
        <path d="M3 9h18" />
        <path d="M12 4 4 8h16l-8-4Z" />
      </svg>
    );
  }
  if (kind === "signals") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 12h2.5l2-5 3.5 10 3-7 2 2H20" />
        <path d="M4 19h16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "price-volume") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 17 9 12l3 3 7-8" />
        <path d="M5 21V9" opacity="0.45" />
        <path d="M11 21v-5" opacity="0.45" />
        <path d="M17 21V7" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "fundamentals") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 12h3l2-5 4 10 2.5-5H20" />
        <path d="M18 4v5" opacity="0.55" />
        <path d="M15.5 6.5h5" opacity="0.55" />
      </svg>
    );
  }
  if (kind === "flow") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M7 7h9.5a3.5 3.5 0 0 1 0 7H8" />
        <path d="m11 4-4 3 4 3" />
        <path d="M17 17H7.5a3.5 3.5 0 0 1 0-7H16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "people") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M16 19v-1.5a3.5 3.5 0 0 0-3.5-3.5h-5A3.5 3.5 0 0 0 4 17.5V19" />
        <path d="M10 10a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" />
        <path d="M20 19v-1a3 3 0 0 0-2.2-2.9" opacity="0.55" />
        <path d="M16 4.4a3 3 0 0 1 0 5.8" opacity="0.55" />
      </svg>
    );
  }
  const isSell = kind === "insider-sell";
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" />
      <path d="M5 20a7 7 0 0 1 14 0" />
      <path d={isSell ? "M18 8v7" : "M18 15V8"} />
      <path d={isSell ? "m15 12 3 3 3-3" : "m15 11 3-3 3 3"} />
    </svg>
  );
}

function SourceEvidenceCard({
  title,
  icon,
  source,
  body,
  support,
}: {
  title: string;
  icon: IntelligenceIconKind;
  source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey];
  body: string;
  support: string;
}) {
  return (
    <div className={`rounded-lg border px-4 py-4 ${sourceCardBorderClass(source)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${sourceCardToneClass(source)}`}>
            <IntelligenceIcon kind={icon} className="h-3.5 w-3.5" />
          </span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">{title}</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${sourceCardToneClass(source)}`}>{sourceStateLabel(source)}</p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{body}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{support}</p>
    </div>
  );
}

function LockedSourceEvidenceCard({
  title,
  icon,
  requiredPlan,
  support,
}: {
  title: string;
  icon: IntelligenceIconKind;
  requiredPlan: "premium" | "pro";
  support: string;
}) {
  const label = lockFeatureLabel(requiredPlan);
  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.025] px-4 py-4">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="shrink-0 text-slate-500">
            <IntelligenceIcon kind={icon} className="h-3.5 w-3.5" />
          </span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">{title}</p>
        </div>
        <p className="shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500">LOCKED</p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{label}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{support}</p>
    </div>
  );
}

function OptionsFlowCard({ summary }: { summary: OptionsFlowSummary }) {
  const contractCount = summary.metrics.observed_contracts ?? 0;
  const freshnessDays = summary.metrics.freshness_days;
  const body = summary.state === "inactive"
    ? `No notable options flow in the ${lastContextWindowLabel(summary.lookback_days)}.`
    : normalizeUpperCardWindowCopy(summary.summary, summary.lookback_days) ?? summary.summary;
  const detail = summary.state === "inactive"
    ? `Options flow context is based on the fixed ${contextWindowNoun(summary.lookback_days)}.`
    : summary.state === "unavailable"
      ? "Flow unavailable"
      : `${contractCount > 0 ? `${contractCount} contracts` : "Recent flow"} · ${freshnessDays === null ? contextWindowLabel(summary.lookback_days) : `${freshnessDays}d fresh`}`;
  return (
    <div className={`rounded-lg border px-4 py-4 ${optionsFlowBorderClass(summary)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${optionsFlowToneClass(summary.state)}`}>
            <IntelligenceIcon kind="flow" className="h-3.5 w-3.5" />
          </span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">Options Flow</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${optionsFlowToneClass(summary.state)}`}>
          {summary.state.toUpperCase()}
        </p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{body}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{detail}</p>
    </div>
  );
}

function GovernmentContractsCard({
  source,
  lookbackDays,
}: {
  source: ConfirmationScoreBundle["sources"]["government_contracts"];
  lookbackDays: number;
}) {
  const isActive = source.present;
  const body = isActive ? "Government contracts active" : "No major government contracts";
  const detail = isActive
    ? normalizeUpperCardWindowCopy(source.detail ?? source.summary, lookbackDays) ?? `Government contracts are active in the ${contextWindowNoun(lookbackDays)}.`
    : `No qualifying contracts found in the ${lastContextWindowLabel(lookbackDays)}.`;

  return (
    <div className={`rounded-lg border px-4 py-4 ${isActive ? "border-sky-400/20 bg-sky-400/[0.045]" : "border-white/10 bg-white/[0.025]"}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${isActive ? "text-sky-300" : "text-slate-500"}`}>
            <IntelligenceIcon kind="government-contract" className="h-3.5 w-3.5" />
          </span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">Government Contracts</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${isActive ? "text-sky-300" : "text-slate-500"}`}>
          {isActive ? "BULLISH SUPPORT" : "INACTIVE"}
        </p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{body}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{detail}</p>
    </div>
  );
}

function hrefWithFilters(
  symbol: string,
  lookback: Lookback,
  source: SourceFilter,
  side: SideFilter,
  extra?: Record<string, string | number | null | undefined>,
): string {
  const q = new URLSearchParams();
  q.set("lookback", lookback);
  q.set("source", source);
  q.set("side", side);
  Object.entries(extra ?? {}).forEach(([key, value]) => {
    if (value === null || value === undefined) return;
    q.set(key, String(value));
  });
  const base = tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
  return `${base}?${q.toString()}`;
}

function lookbackStartDateKey(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - Math.max(days - 1, 0));
  return date.toISOString().slice(0, 10);
}

function InlineEmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-white/15 bg-white/[0.02] px-4 py-3">
      <p className="text-sm text-slate-400">{message}</p>
    </div>
  );
}

function ActivityCard({ children }: { children: ReactNode }) {
  return (
    <div className="w-full max-w-full min-w-0 overflow-hidden rounded-2xl border border-white/10 bg-white/5 px-3 py-2.5 sm:px-4">
      {children}
    </div>
  );
}

function ActivityScrollRegion({ children }: { children: ReactNode }) {
  return (
    <div
      data-activity-scroll-region
      className={[
        "min-w-0 max-w-full max-h-[35rem] space-y-3 overflow-y-auto pr-1",
        "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
        "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
        "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60",
      ].join(" ")}
    >
      {children}
    </div>
  );
}

function ActivityHeaderStat({
  href,
  label,
  value,
  toneClass,
}: {
  href: string;
  label: string;
  value: number;
  toneClass: string;
}) {
  return (
    <Link
      href={href}
      prefetch={false}
      className="inline-grid min-w-[4.75rem] grid-cols-[1fr_auto] items-center gap-2 rounded-md border border-white/10 bg-slate-950/70 px-2.5 py-1.5 transition hover:border-white/20 hover:bg-slate-900/80"
    >
      <span className="truncate text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">{label}</span>
      <span className={`text-sm font-semibold tabular-nums ${toneClass}`}>{value}</span>
    </Link>
  );
}

function ActivityHeaderStats({
  symbol,
  lookback,
  source,
  buys,
  sells,
}: {
  symbol: string;
  lookback: Lookback;
  source: Extract<SourceFilter, "congress" | "insider">;
  buys: number;
  sells: number;
}) {
  return (
    <div className="flex shrink-0 flex-wrap items-center justify-start gap-2 sm:justify-center">
      <ActivityHeaderStat
        href={hrefWithFilters(symbol, lookback, source, "buy")}
        label="Buys"
        value={buys}
        toneClass="text-emerald-300"
      />
      <ActivityHeaderStat
        href={hrefWithFilters(symbol, lookback, source, "sell")}
        label="Sells"
        value={sells}
        toneClass="text-rose-300"
      />
    </div>
  );
}

function InstitutionalActivityCard({
  event,
}: {
  event: EventsResponse["items"][number];
}) {
  const payload = eventPayload(event);
  const holderName = institutionalHolderName(event);
  const cik = event.member_bioguide_id?.trim() || payloadString(payload, "cik");
  const holderHref = cik ? `/institution/${encodeURIComponent(cik)}` : null;
  const action = institutionalActionLabel(event);
  const value = institutionalValue(event);
  const valueText = value !== null ? formatCurrency(value) : "Value unavailable";
  const reportPeriod = institutionalReportPeriod(event);
  const filingDate = institutionalDate(event);
  const summary = payloadString(payload, "summary") || payloadString(payload, "title") || null;
  const metaLine = [filingDate ? `Filed ${formatDateShort(filingDate)}` : null, reportPeriod].filter(Boolean).join(" · ");

  return (
    <ActivityCard>
      <div className="grid min-w-0 gap-x-4 gap-y-2 sm:grid-cols-[minmax(180px,1.5fr)_minmax(120px,.8fr)_minmax(120px,.8fr)_auto] sm:items-center">
        <div className="min-w-0">
          {holderHref ? (
            <Link href={holderHref} prefetch={false} className="block truncate text-sm font-semibold text-emerald-200">
              {holderName}
            </Link>
          ) : (
            <p className="truncate text-sm font-semibold text-slate-100">{holderName}</p>
          )}
          <p className="mt-1 truncate text-xs text-slate-400">{metaLine || "13F filing"}</p>
        </div>
        <div className="min-w-0">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">Reported value</div>
          <div className="truncate text-sm font-semibold tabular-nums text-white">{valueText}</div>
        </div>
        <div className="min-w-0">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">Source</div>
          <div className="truncate text-sm font-semibold text-slate-200">13F holdings</div>
        </div>
        <div className="flex justify-start sm:justify-end">
          <Badge tone={institutionalTone(event)}>{action}</Badge>
        </div>
      </div>
      {summary ? (
        <p className="mt-3 max-w-full overflow-hidden break-words text-ellipsis text-sm leading-6 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2]">
          {summary}
        </p>
      ) : null}
    </ActivityCard>
  );
}

function GovernmentContractActivityCard({
  contract,
}: {
  contract: TickerGovernmentContractItem;
}) {
  const agency = contract.awarding_agency?.trim() || contract.funding_agency?.trim() || "Government Contract";
  const agencyHref = departmentHref(agency);
  const awardDate = contract.period_start ?? contract.award_date ?? null;
  const recipient = contract.recipient_name?.trim() || contract.raw_recipient_name?.trim() || null;
  const amount = readNumeric(contract.award_amount);
  const description = contract.description?.trim() || null;
  const sourceUrl = contract.source_url?.trim() || null;
  const contractValue = amount !== null ? formatCurrency(amount) : "Value unavailable";
  const dateText = awardDate ? `Start Date: ${formatDateShort(awardDate)}` : null;
  const metaLine = [dateText, recipient].filter((value) => Boolean(value) && value !== "â€”").join(" · ");

  return (
    <ActivityCard>
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          {agencyHref ? (
            <Link href={agencyHref} prefetch={false} className="block truncate text-sm font-semibold text-slate-100 hover:text-emerald-200">
              {agency}
            </Link>
          ) : (
            <p className="truncate text-sm font-semibold text-slate-100">{agency}</p>
          )}
          <p className="mt-1 truncate text-xs text-slate-400">{metaLine || formatDateShort(awardDate)}</p>
        </div>
        <div className="shrink-0 text-right">
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">Contract Value</p>
          <p className="mt-1 text-sm font-semibold tabular-nums text-white">{contractValue}</p>
        </div>
      </div>
      {description ? (
        <p className="mt-3 max-w-full overflow-hidden break-words text-ellipsis text-sm leading-6 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2]">
          {description}
        </p>
      ) : null}
      <div className="mt-3 flex justify-end">
        {sourceUrl ? (
          <Link
            href={sourceUrl}
            target="_blank"
            rel="noreferrer"
            prefetch={false}
            className="text-xs font-semibold text-emerald-200 underline-offset-4 transition hover:text-emerald-100 hover:underline"
          >
            View contract
          </Link>
        ) : (
          <span className="text-xs text-slate-500">Link unavailable</span>
        )}
      </div>
    </ActivityCard>
  );
}

function ActivityCardGrid({
  identity,
  sideBadge,
  dateLabel,
  price,
  priceSubtext,
  tradeValue,
  pnl,
  pnlClassName,
  signal,
  showGainLoss = true,
}: {
  identity: ReactNode;
  sideBadge: ReactNode;
  dateLabel: ReactNode;
  price: ReactNode;
  priceSubtext?: ReactNode;
  tradeValue: ReactNode;
  pnl: ReactNode;
  pnlClassName?: string;
  signal: ReactNode;
  showGainLoss?: boolean;
}) {
  const metricLabelClassName = "text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500";
  const metricValueClassName = "truncate text-sm font-semibold tabular-nums";
  const gainLossLabelNode = (
    <span
      className="cursor-help whitespace-nowrap"
      title={tickerGainLossTooltip}
      aria-label={`${gainLossLabel}: ${tickerGainLossTooltip}`}
    >
      {gainLossLabel}
    </span>
  );

  return (
    <div className={[
      "grid min-w-0 gap-x-3 gap-y-2 sm:items-center",
      showGainLoss
        ? "sm:grid-cols-[minmax(150px,1.45fr)_minmax(76px,.7fr)_minmax(104px,.9fr)_minmax(88px,.65fr)_minmax(84px,auto)] lg:grid-cols-[minmax(170px,1.65fr)_minmax(84px,.72fr)_minmax(120px,.95fr)_minmax(92px,.68fr)_minmax(92px,auto)]"
        : "sm:grid-cols-[minmax(170px,1.6fr)_minmax(92px,.7fr)_minmax(128px,.95fr)_minmax(92px,auto)] lg:grid-cols-[minmax(190px,1.8fr)_minmax(104px,.72fr)_minmax(140px,.95fr)_minmax(100px,auto)]",
    ].join(" ")}>
      <div className="min-w-0 sm:col-start-1 sm:row-start-1">{identity}</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-2 sm:row-start-1`}>Price</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-3 sm:row-start-1`}>Trade value</div>
      {showGainLoss ? <div className={`${metricLabelClassName} hidden sm:block sm:col-start-4 sm:row-start-1`}>{gainLossLabelNode}</div> : null}
      <div className={`flex min-w-0 items-center justify-start sm:row-start-1 sm:justify-end ${showGainLoss ? "sm:col-start-5" : "sm:col-start-4"}`}>{sideBadge}</div>

      <div className="text-xs text-slate-400 sm:col-start-1 sm:row-start-2">{dateLabel}</div>
      <div className="min-w-0 sm:col-start-2 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Price</div>
        <div className={`${metricValueClassName} text-white`}>{price}</div>
        {priceSubtext ? <div className="mt-0.5 truncate text-[11px] tabular-nums text-slate-500">{priceSubtext}</div> : null}
      </div>
      <div className="min-w-0 sm:col-start-3 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Trade value</div>
        <div className={`${metricValueClassName} text-white`}>{tradeValue}</div>
      </div>
      {showGainLoss ? (
        <div className="min-w-0 sm:col-start-4 sm:row-start-2">
          <div className={`${metricLabelClassName} sm:hidden`}>{gainLossLabelNode}</div>
          <div className={`${metricValueClassName} ${pnlClassName ?? "text-slate-400"}`}>{pnl}</div>
        </div>
      ) : null}
      <div className={`flex min-w-0 items-center justify-start sm:row-start-2 sm:justify-end ${showGainLoss ? "sm:col-start-5" : "sm:col-start-4"}`}>{signal}</div>
    </div>
  );
}

function DeferredTickerSummarySkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-7">
        {Array.from({ length: 7 }).map((_, idx) => (
          <div key={idx} className={`${cardClassName} p-4`}>
            <SkeletonBlock className="h-3 w-28" />
            <SkeletonBlock className="mt-3 h-7 w-20" />
          </div>
        ))}
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className={`${cardClassName} p-4 md:col-span-2 xl:col-span-3`}>
          <SkeletonBlock className="h-3 w-28" />
          <div className="mt-3 flex gap-2">
            {Array.from({ length: 4 }).map((_, idx) => <SkeletonBlock key={idx} className="h-8 w-20 rounded-lg" />)}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <SkeletonBlock className="h-3 w-32" />
          <SkeletonBlock className="mt-3 h-7 w-16" />
        </div>
      </div>
      <section className={`${cardClassName} p-4`}>
        <SkeletonBlock className="h-3 w-40" />
        <SkeletonBlock className="mt-3 h-64 w-full" />
      </section>
    </div>
  );
}

async function resolveTickerActivityData({
  eventsPromise,
  congressEventsPromise,
  insiderEventsPromise,
  institutionalEventsPromise,
  governmentContractsPromise,
  signalSummaryRequest,
  signalsUnavailable,
  lookbackStartKey,
  side,
}: {
  eventsPromise?: ReturnType<typeof getEvents>;
  congressEventsPromise?: ReturnType<typeof getEvents>;
  insiderEventsPromise?: ReturnType<typeof getEvents>;
  institutionalEventsPromise?: ReturnType<typeof getEvents>;
  governmentContractsPromise?: ReturnType<typeof getTickerGovernmentContracts>;
  signalSummaryRequest?: Promise<TickerSignalsSummaryResponse>;
  signalsUnavailable?: SignalGateState | null;
  lookbackStartKey: string;
  side: SideFilter;
}): Promise<TickerActivityData> {
  const [eventsRes, congressEventsRes, insiderEventsRes, institutionalEventsRes, governmentContractsRes, signalsResult] = await Promise.all([
    eventsPromise ?? Promise.resolve(emptyEventsResponse()),
    congressEventsPromise ?? Promise.resolve(emptyEventsResponse()),
    insiderEventsPromise ?? Promise.resolve(emptyEventsResponse()),
    institutionalEventsPromise ?? Promise.resolve(emptyEventsResponse()),
    governmentContractsPromise ?? Promise.resolve({
      symbol: null,
      status: "ok",
      source_status: "ok",
      items: [] as TickerGovernmentContractItem[],
      total: 0,
      contract_count: 0,
      page: 0,
      limit: GOVERNMENT_CONTRACTS_PAGE_SIZE,
      has_next: false,
    }),
    signalSummaryRequest
      ? signalSummaryRequest
          .then((response) => ({
            response,
            resolved: true,
            unavailable: response.source_entitlements?.signals?.locked
              ? signalsUnavailable ?? signalGateForAuthenticatedFreeUser()
              : null,
          }))
          .catch(() => ({
            response: { items: [] as SignalItem[] },
            resolved: false,
            unavailable: signalsUnavailable ?? { reason: "unavailable" as const, message: "Ticker signals are temporarily unavailable." },
          }))
      : Promise.resolve({
          response: { items: [] as SignalItem[] },
          resolved: false,
          unavailable: signalsUnavailable ?? null,
        }),
  ]);
  const signalsRes = signalsResult.response as TickerSignalsSummaryResponse;

  const events = dedupeByKey(eventsRes.items ?? [], (event) => {
    const stableIdentity = stableEventIdentity(event);
    if (stableIdentity) return `stable|${stableIdentity}`;

    const actor =
      canonicalize(event.member_bioguide_id) ||
      canonicalize(event.member_name) ||
      canonicalize(resolveInsiderName(event));
    const sideValue = normalizeTradeSide(event.trade_type) ?? canonicalize(event.trade_type);

    return [
      canonicalize(event.event_type),
      canonicalize(event.source),
      canonicalize(event.symbol ?? event.ticker),
      actor,
      sideValue,
      toDateKey(event.ts) ?? "",
      payloadDateKey(event.payload),
      normalizedAmountLabel(event.amount_min, event.amount_max),
    ].join("|");
  });

  const signalActivityRows = (signalsRes.items ?? []).filter((signal) => isTickerSignalKind(signal.kind));
  const confirmationSignals = dedupeByKey(signalActivityRows, (signal) => [
    canonicalize(signal.kind),
    canonicalize(signal.symbol),
    canonicalize(signal.who),
    canonicalize(signal.member_bioguide_id),
    normalizeTradeSide(signal.trade_type) ?? canonicalize(signal.trade_type),
    toDateKey(signal.ts) ?? "",
    normalizedAmountLabel(signal.amount_min, signal.amount_max),
    canonicalize(signal.smart_band),
    String(signal.smart_score ?? ""),
    String(signal.unusual_multiple ?? ""),
  ].join("|"));
  const signals = dedupeByKey(signalActivityRows, (signal) => [
    canonicalize(signal.kind),
    canonicalize(signal.symbol),
    canonicalize(signal.who),
    canonicalize(signal.member_bioguide_id),
    normalizeTradeSide(signal.trade_type) ?? canonicalize(signal.trade_type),
    toDateKey(signal.ts) ?? "",
    normalizedAmountLabel(signal.amount_min, signal.amount_max),
    canonicalize(signal.smart_band),
    String(signal.smart_score ?? ""),
    String(signal.unusual_multiple ?? ""),
  ].join("|")).filter((signal) => {
    const key = toDateKey(signal.ts);
    return Boolean(key && key >= lookbackStartKey);
  });

  const filteredEvents = side === "all"
    ? events
    : events.filter((event) => normalizeTradeSide(event.trade_type) === side);

  const metricCongressEvents = events.filter((event) => event.event_type === "congress_trade");
  const metricInsiderEvents = events.filter((event) => event.event_type === "insider_trade");
  const congressEvents = visibleActivityItems(congressEventsRes, ACTIVITY_PAGE_SIZE);
  const insiderEvents = visibleActivityItems(insiderEventsRes, ACTIVITY_PAGE_SIZE);
  const institutionalEvents = visibleActivityItems(institutionalEventsRes, ACTIVITY_PAGE_SIZE)
    .filter((event) => isInstitutionalActivityEventType(event.event_type));
  const congressActivityPage = activityPageMeta(congressEventsRes, 0, ACTIVITY_PAGE_SIZE);
  const insiderActivityPage = activityPageMeta(insiderEventsRes, 0, ACTIVITY_PAGE_SIZE);
  const institutionalActivityPage = activityPageMeta(institutionalEventsRes, 0, ACTIVITY_PAGE_SIZE);
  const institutionalEventsAvailability = institutionalEventsRes as EventsResponse & { availability_status?: string | null };
  const institutionalEventsStatus = institutionalEventsAvailability.status ?? institutionalEventsAvailability.availability_status ?? "ok";
  const governmentContracts = governmentContractsRes.items ?? [];
  const governmentContractsTotal = typeof governmentContractsRes.total === "number"
    ? governmentContractsRes.total
    : typeof governmentContractsRes.contract_count === "number"
      ? governmentContractsRes.contract_count
      : governmentContracts.length;
  const governmentContractsPage = typeof governmentContractsRes.page === "number" ? governmentContractsRes.page : 0;
  const governmentContractsLimit = typeof governmentContractsRes.limit === "number" ? governmentContractsRes.limit : GOVERNMENT_CONTRACTS_PAGE_SIZE;
  const governmentContractsHasNext = Boolean(governmentContractsRes.has_next);
  const governmentContractsStatus = governmentContractsRes.status ?? governmentContractsRes.source_status ?? "ok";
  const congressBuys = metricCongressEvents.filter((event) => normalizeTradeSide(event.trade_type) === "buy").length;
  const congressSells = metricCongressEvents.filter((event) => normalizeTradeSide(event.trade_type) === "sell").length;
  const insiderBuys = metricInsiderEvents.filter((event) => normalizeTradeSide(event.trade_type) === "buy").length;
  const insiderSells = metricInsiderEvents.filter((event) => normalizeTradeSide(event.trade_type) === "sell").length;
  const topSignal = [...confirmationSignals].sort((a, b) => (b.smart_score ?? 0) - (a.smart_score ?? 0))[0];
  const congressParticipantEvents = side === "all"
    ? congressEvents
    : congressEvents.filter((event) => normalizeTradeSide(event.trade_type) === side);
  const insiderParticipantEvents = side === "all"
    ? insiderEvents
    : insiderEvents.filter((event) => normalizeTradeSide(event.trade_type) === side);
  const congressParticipantMap = new Map<string, ParticipantStats>();
  const insiderParticipantMap = new Map<string, ParticipantStats>();

  for (const event of congressParticipantEvents) {
    const who = (event.member_name ?? "Unknown Member").trim();
    const memberId = asTrimmedString(event.member_bioguide_id);
    const participantKey = memberId ? `member:${memberId}` : `name:${who.toLowerCase()}`;
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    const existing = congressParticipantMap.get(participantKey) ?? { name: who, memberId, trades: 0, buys: 0, sells: 0, netFlow: 0 };
    existing.trades += 1;
    if (sideValue === "buy") existing.buys += 1;
    if (sideValue === "sell") existing.sells += 1;
    if (Number.isFinite(amount) && amount > 0) {
      existing.netFlow += sideValue === "sell" ? -amount : sideValue === "buy" ? amount : 0;
    }
    if (!existing.memberId && memberId) existing.memberId = memberId;
    if (!existing.chamber) existing.chamber = resolveCongressChamber(event);
    if (!existing.party) existing.party = resolveCongressParty(event);
    if (!existing.state) existing.state = resolveCongressState(event);
    const safeHref = memberHref({ name: event.member_name ?? undefined, memberId: event.member_bioguide_id ?? undefined });
    if (safeHref && safeHref !== "/member/UNKNOWN" && !existing.href) existing.href = safeHref;
    congressParticipantMap.set(participantKey, existing);
  }

  for (const event of insiderParticipantEvents) {
    const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
    const who = display.insiderName || resolveInsiderName(event);
    const reportingCik = display.reportingCik ?? resolveInsiderReportingCik(event);
    const role = display.role ?? resolveInsiderRole(event);
    const participantKey = reportingCik ? `cik:${reportingCik}` : `name:${who.toLowerCase()}`;
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    const existing = insiderParticipantMap.get(participantKey) ?? { name: who, trades: 0, buys: 0, sells: 0, netFlow: 0 };
    existing.trades += 1;
    if (sideValue === "buy") existing.buys += 1;
    if (sideValue === "sell") existing.sells += 1;
    if (Number.isFinite(amount) && amount > 0) {
      existing.netFlow += sideValue === "sell" ? -amount : sideValue === "buy" ? amount : 0;
    }
    if (reportingCik && !existing.reportingCik) existing.reportingCik = reportingCik;
    if (!existing.role) existing.role = role;
    insiderParticipantMap.set(participantKey, existing);
  }

  const topCongressParticipants = [...congressParticipantMap.values()].sort((a, b) => b.trades - a.trades);
  const topInsiderParticipants = [...insiderParticipantMap.values()].sort((a, b) => b.trades - a.trades);

  return {
    events: filteredEvents,
    signals,
    signalsTotal: signals.length,
    signalsUnavailable: signalsResult.unavailable,
    congressEvents,
    congressEventsTotal: congressActivityPage.total,
    congressEventsPage: congressActivityPage.page,
    congressEventsLimit: congressActivityPage.limit,
    congressEventsHasNext: congressActivityPage.hasNext,
    insiderEvents,
    insiderEventsTotal: insiderActivityPage.total,
    insiderEventsPage: insiderActivityPage.page,
    insiderEventsLimit: insiderActivityPage.limit,
    insiderEventsHasNext: insiderActivityPage.hasNext,
    institutionalEvents,
    institutionalEventsTotal: institutionalActivityPage.total,
    institutionalEventsPage: institutionalActivityPage.page,
    institutionalEventsLimit: institutionalActivityPage.limit,
    institutionalEventsHasNext: institutionalActivityPage.hasNext,
    institutionalEventsStatus,
    governmentContracts,
    governmentContractsTotal,
    governmentContractsPage,
    governmentContractsLimit,
    governmentContractsHasNext,
    governmentContractsStatus,
    priceVolumeContext: signalsRes.price_volume ?? null,
    fundamentalsContext: signalsRes.fundamentals ?? null,
    sourceEntitlements: signalsRes.source_entitlements ?? null,
    confirmationScoreBundle: signalsRes.confirmation_score_bundle ?? null,
    signalFreshness: signalsRes.signal_freshness ?? null,
    signalSummaryResolved: signalsResult.resolved,
    effectiveWindowDays: typeof signalsRes.effective_window_days === "number"
      ? signalsRes.effective_window_days
      : typeof signalsRes.lookback_days === "number"
        ? signalsRes.lookback_days
        : null,
    summaryInsiders: signalsRes.insiders ?? null,
    summaryCongress: signalsRes.congress ?? null,
    congressBuys,
    congressSells,
    insiderBuys,
    insiderSells,
    topSignal,
    topCongressParticipants,
    topInsiderParticipants,
  };
}

async function DeferredTickerContent({
  activityPromise,
  normalizedSymbol,
  decisionLayer,
  lookback,
  source,
  side,
  activityDetailsDeferred,
  signalsAuthPending,
  topMembers,
  confirmationScoreBundle,
  optionsFlowSummary,
  technicalIndicators,
  fallbackSourceEntitlements,
  allowAuthHintEntitlementOverride,
  canViewProTickerContext,
  hasAuthForEntitlementDisplay,
  canViewPremiumMetrics,
  tickerConfirmationGate,
}: {
  activityPromise: Promise<TickerActivityData>;
  normalizedSymbol: string;
  decisionLayer?: TickerDecisionLayer | null;
  lookback: Lookback;
  source: SourceFilter;
  side: SideFilter;
  activityDetailsDeferred: boolean;
  signalsAuthPending: boolean;
  topMembers: NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["top_members"]>;
  confirmationScoreBundle: ConfirmationScoreBundle | null | undefined;
  optionsFlowSummary: OptionsFlowSummary | null | undefined;
  technicalIndicators: TechnicalIndicators | null | undefined;
  fallbackSourceEntitlements: TickerSourceEntitlements;
  allowAuthHintEntitlementOverride: boolean;
  canViewProTickerContext: boolean;
  hasAuthForEntitlementDisplay: boolean;
  canViewPremiumMetrics: boolean;
  tickerConfirmationGate?: TickerConfirmationGate | null;
}) {
  const {
    events,
    signals,
    signalsTotal,
    signalsUnavailable,
    congressEvents,
    congressEventsTotal,
    congressEventsPage,
    congressEventsLimit,
    congressEventsHasNext,
    insiderEvents,
    insiderEventsTotal,
    insiderEventsPage,
    insiderEventsLimit,
    insiderEventsHasNext,
    institutionalEvents,
    institutionalEventsTotal,
    institutionalEventsPage,
    institutionalEventsLimit,
    institutionalEventsHasNext,
    institutionalEventsStatus,
    governmentContracts,
    governmentContractsTotal,
    governmentContractsPage,
    governmentContractsLimit,
    governmentContractsHasNext,
    governmentContractsStatus,
    congressBuys,
    congressSells,
    insiderBuys,
    insiderSells,
    topSignal,
    priceVolumeContext,
    fundamentalsContext,
    sourceEntitlements: activitySourceEntitlements,
    confirmationScoreBundle: activityConfirmationScoreBundle,
    signalSummaryResolved,
    effectiveWindowDays,
    summaryInsiders,
    summaryCongress,
    topCongressParticipants,
    topInsiderParticipants,
  } = await activityPromise;
  const selectedLookbackDays = Number(lookback);
  const effectiveLookbackDays = effectiveWindowDays ?? SIGNAL_WINDOW_DAYS;
  let confirmationBundle = normalizeConfirmationBundle(
    activityConfirmationScoreBundle ?? confirmationScoreBundle,
    normalizedSymbol,
    effectiveLookbackDays,
  );
  if (!activityConfirmationScoreBundle && confirmationBundle.lookback_days !== effectiveLookbackDays) {
    confirmationBundle = { ...confirmationBundle, lookback_days: effectiveLookbackDays };
  }
  let optionsFlow = normalizeOptionsFlowSummary(optionsFlowSummary, normalizedSymbol, effectiveLookbackDays);
  if (optionsFlow.lookback_days !== effectiveLookbackDays) {
    optionsFlow = { ...optionsFlow, lookback_days: effectiveLookbackDays };
  }
  const normalizedTechnicals = normalizeTechnicalIndicators(technicalIndicators);
  const sourceEntitlements = displaySourceEntitlementsForTickerContext(
    activitySourceEntitlements,
    fallbackSourceEntitlements,
    allowAuthHintEntitlementOverride,
  );
  const visibleConfirmationBundle = displayConfirmationBundleForEntitlements(confirmationBundle, sourceEntitlements);
  const signalsCardLocked = sourceIsLocked(sourceEntitlements, "signals");
  const institutionalCardLocked = !canViewProTickerContext && sourceIsLocked(sourceEntitlements, "institutional_activity");
  const optionsFlowCardLocked = !canViewProTickerContext && sourceIsLocked(sourceEntitlements, "options_flow");
  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";
  const showInstitutional = source === "all" || source === "institutional";
  const showGovernmentContracts = source === "all" || source === "government_contract";
  const institutionalEventsUnavailable = institutionalEventsStatus === "unavailable";
  const governmentContractsUnavailable = governmentContractsStatus === "unavailable";
  const signalSourceEvents = events.filter((event) => event.event_type === "congress_trade" || event.event_type === "insider_trade");
  const activityPnlByEventId = new Map<number, number | null>(
    [...signalSourceEvents, ...congressEvents, ...insiderEvents].map((event) => [event.id, readNumeric(event.pnl_pct)]),
  );
  const activityEventById = new Map<number, (typeof congressEvents)[number] | (typeof insiderEvents)[number]>(
    [...signalSourceEvents, ...congressEvents, ...insiderEvents].map((event) => [event.id, event]),
  );
  const tickerReturnTo = tickerHref(normalizedSymbol) ?? `/ticker/${normalizedSymbol}`;
  const signalGateHref = signalsUnavailable?.reason === "unavailable"
    ? tickerReturnTo
    : "/pricing";
  const signalGateLabel = "View Premium";
  const signalGateTitle = "Signal activity requires premium";
  const institutionalGateMessage = hasAuthForEntitlementDisplay
    ? "Subscribe to pro to review 13F holder activity for this ticker."
    : "Create an account and subscribe to pro to review 13F holder activity for this ticker.";
  const confirmationLookbackDays = confirmationBundle.lookback_days;
  const canReuseSignalSummary = signalSummaryResolved && !signalsAuthPending;
  const priceVolume = priceVolumeSummary(confirmationBundle.sources.price_volume, normalizedTechnicals, priceVolumeContext, confirmationLookbackDays);
  const priceVolumeChange = typeof priceVolumeContext?.change_pct_1d === "number" ? priceVolumeContext.change_pct_1d : null;
  const priceVolumeChangeTone = priceVolumeChange === null
    ? "text-slate-500"
    : priceVolumeChange < 0
      ? "text-rose-300"
      : priceVolumeChange > 0
        ? "text-emerald-300"
        : "text-slate-400";
  const insiderCardSource = confirmationBundle.sources.insiders;
  const congressCardSource = confirmationBundle.sources.congress;
  const signalsCardSource = sourceFromTopSignal(confirmationBundle.sources.signals, topSignal);
  const summaryInsiderBuys = summaryCount(summaryInsiders, "buy_count");
  const summaryInsiderSells = summaryCount(summaryInsiders, "sell_count");
  const summaryCongressBuys = summaryCount(summaryCongress, "buy_count");
  const summaryCongressSells = summaryCount(summaryCongress, "sell_count");

  return (
    <>
      <section className="grid min-w-0 grid-cols-1 items-start gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(320px,366px)] xl:items-stretch">
        <div className="min-w-0 xl:flex xl:min-h-0 xl:h-full">
          <TickerContextCard
            key={normalizedSymbol}
            symbol={normalizedSymbol}
            canViewOwnership={canViewProTickerContext}
            className="min-w-0 xl:h-full xl:w-full"
            overview={
              <TickerOverviewPanel
                confirmationBundle={confirmationBundle}
                sourceDisplayBundle={visibleConfirmationBundle}
                decisionLayer={decisionLayer}
                confirmationGate={tickerConfirmationGate}
              />
            }
          />
        </div>

        <div className="min-w-0 xl:flex xl:min-h-0 xl:h-full">
          <div className="grid gap-3 xl:h-full xl:w-full xl:auto-rows-min">
            <div className="grid items-stretch gap-3">
              <div className={`${cardClassName} h-full !rounded-lg p-5`}>
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className={technicalToneClass(priceVolume.tone)}>
                      <IntelligenceIcon kind="price-volume" />
                    </span>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Price / Volume</p>
                  </div>
                  <p className={`text-xs font-semibold uppercase tracking-[0.14em] ${technicalToneClass(priceVolume.tone)}`}>
                    {priceVolume.state}
                  </p>
                </div>
                <div className="mt-3 flex items-end gap-3">
                  <p className="text-3xl font-semibold tabular-nums text-white">{formatUpperCardPrice(priceVolumeContext?.latest_close)}</p>
                  <p className={`pb-1 text-sm font-semibold tabular-nums ${priceVolumeChangeTone}`}>{formatUpperCardSignedPercent(priceVolumeChange)}</p>
                </div>
                <p className="mt-3 text-sm font-semibold text-slate-100">{priceVolume.summary}</p>
                <div className="mt-3 grid gap-1.5">
                  {priceVolume.diagnostics.slice(2).map((diagnostic) => (
                    <p key={diagnostic} className="text-xs text-slate-400">{diagnostic}</p>
                  ))}
                </div>
              </div>
              <FundamentalsCard summary={fundamentalsContext} />
            </div>

            <div className="grid gap-3 xl:h-full xl:auto-rows-min">
              <SourceEvidenceCard
                title="Insiders"
                icon={insiderCardSource.direction === "bearish" ? "insider-sell" : "insider-buy"}
                source={insiderCardSource}
                body={insiderSourceBody(summaryInsiderBuys, summaryInsiderSells, insiderCardSource, confirmationLookbackDays)}
                support={insiderSourceSupport(summaryInsiderBuys, summaryInsiderSells, confirmationLookbackDays)}
              />
              <SourceEvidenceCard
                title="Congress"
                icon="congress"
                source={congressCardSource}
                body={sourceCardBody("congress", congressCardSource, topSignal, confirmationLookbackDays)}
                support={congressSourceSupport(summaryCongressBuys, summaryCongressSells, confirmationLookbackDays)}
              />
              {institutionalCardLocked ? (
                <LockedSourceEvidenceCard
                  title="Institutional"
                  icon="people"
                  requiredPlan="pro"
                  support="Institutional activity unlocks with Pro."
                />
              ) : (
                <TickerInstitutionalSourceCardClient
                  symbol={normalizedSymbol}
                  side={side}
                  lookbackDays={confirmationLookbackDays}
                  initialSource={confirmationBundle.sources.institutional_activity}
                  canViewInstitutional={canViewProTickerContext}
                  initialResolved={canReuseSignalSummary}
                />
              )}
              {signalsCardLocked ? (
                <LockedSourceEvidenceCard
                  title="Signals"
                  icon="signals"
                  requiredPlan="premium"
                  support="Signal stack details unlock with Premium."
                />
              ) : (
                <TickerSignalsSourceCardClient
                  symbol={normalizedSymbol}
                  side={side}
                  lookbackDays={confirmationLookbackDays}
                  lookbackStartKey={lookbackStartDateKey(confirmationLookbackDays)}
                  initialSource={signalsCardSource}
                  initialResolved={canReuseSignalSummary}
                  initialTopSignal={
                    topSignal
                      ? {
                          smart_score: topSignal.smart_score ?? null,
                          smart_band: topSignal.smart_band ?? null,
                          trade_type: topSignal.trade_type ?? null,
                        }
                      : null
                  }
                />
              )}
              {optionsFlowCardLocked ? (
                <LockedSourceEvidenceCard
                  title="Options Flow"
                  icon="flow"
                  requiredPlan="pro"
                  support="Options flow unlocks with Pro."
                />
              ) : (
                <OptionsFlowCard summary={optionsFlow} />
              )}
              <GovernmentContractsCard
                source={confirmationBundle.sources.government_contracts}
                lookbackDays={confirmationLookbackDays}
              />
            </div>
          </div>
        </div>
      </section>
      <div className="grid gap-3 md:grid-cols-3">
        <div className={`${cardClassName} p-4`}>
          <div className="flex items-center justify-between gap-3">
            <p className="text-xs uppercase tracking-widest text-slate-400">Activity view</p>
            <p className="text-xs text-slate-500">All / Congress / Insiders / Signals / Institutional / Gov Contracts</p>
          </div>
          <div className="mt-3 flex flex-wrap rounded-xl border border-white/10 bg-slate-950/80 p-1">
            {([
              ["all", "All"],
              ["congress", "Congress"],
              ["insider", "Insiders"],
              ["signals", "Signals"],
              ["institutional", "Institutional"],
              ["government_contract", "Gov Contracts"],
            ] as const).map(([value, label]) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, lookback, value, side)}
                prefetch={false}
                className={`rounded-lg px-3 py-1.5 text-xs font-semibold ${
                  source === value
                    ? "bg-emerald-400/15 text-emerald-200"
                    : "text-slate-300 hover:bg-white/5"
                }`}
              >
                {label}
              </Link>
            ))}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Chart range</p>
          <div className="flex flex-wrap gap-2">
            {(["30", "90", "180", "365"] as const).map((value) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, value, source, side)}
                prefetch={false}
                className={`rounded-full border px-3 py-1 text-xs font-semibold ${
                  lookback === value
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-900/60 text-slate-300"
                }`}
              >
                {value}D
              </Link>
            ))}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Trade side</p>
          <div className="flex flex-wrap gap-2">
            {(["all", "buy", "sell"] as const).map((value) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, lookback, source, value)}
                prefetch={false}
                className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase ${
                  side === value
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-900/60 text-slate-300"
                }`}
              >
                {value}
              </Link>
            ))}
          </div>
        </div>
      </div>

      <TickerChartLoader symbol={normalizedSymbol} days={selectedLookbackDays} />
      <TickerDeferredActivityRefresh enabled={activityDetailsDeferred} symbol={normalizedSymbol} />

      <div className="grid gap-6 xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <div className="min-w-0 space-y-6">
          {showCongress ? (
            <section id="congress-activity" className={`${cardClassName} scroll-mt-6`}>
              <div className="mb-4 grid gap-3 sm:grid-cols-[1fr_auto_auto] sm:items-center">
                <h2 className="text-lg font-semibold text-white">Congress activity</h2>
                <ActivityHeaderStats
                  symbol={normalizedSymbol}
                  lookback={lookback}
                  source="congress"
                  buys={congressBuys}
                  sells={congressSells}
                />
                <span id="congress-activity-status" className="text-xs text-slate-400">
                  {activityCountLabel(congressEventsTotal, congressEvents.length, "event")}
                </span>
              </div>
              <div className="space-y-3">
                {congressEvents.length === 0 ? (
                  <TickerActivityDetailClient kind="congress" symbol={normalizedSymbol} lookbackDays={selectedLookbackDays} side={side} statusElementId="congress-activity-status" canViewPremiumMetrics={canViewPremiumMetrics} />
                ) : (
                  <>
                    <ActivityScrollRegion>
                      {congressEvents.map((event) => {
                        const memberName = event.member_name ?? "Unknown";
                        const memberLink = memberName.trim() && memberName !== "Unknown"
                          ? memberHref({ name: memberName, memberId: event.member_bioguide_id ?? undefined })
                          : null;
                        const chamber = chamberBadge(resolveCongressChamber(event));
                        const affiliation = formatCongressAffiliationText(resolveCongressParty(event), resolveCongressState(event));
                        const signal = resolveSmartSignalValue(event as Record<string, unknown>);
                        const strengthLabel = formatSignalStrengthText(signal.band);
                        const displayPrice = resolveCongressTradePrice(event);
                        const pnl = readNumeric(event.pnl_pct);

                        return (
                          <ActivityCard key={event.id}>
                            <ActivityCardGrid
                              identity={
                                <div className="flex flex-wrap items-center gap-2">
                                  {memberLink ? (
                                    <Link href={memberLink} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                      {memberName}
                                    </Link>
                                  ) : (
                                    <span className="text-sm font-semibold text-slate-100">{memberName}</span>
                                  )}
                                  <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge>
                                  {affiliation ? <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{affiliation}</span> : null}
                                  <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{strengthLabel}</span>
                                </div>
                              }
                              sideBadge={<Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>}
                              dateLabel={formatDateShort(resolveCongressReportDate(event))}
                              price={displayPrice !== null ? formatCurrency(displayPrice) : "-"}
                              tradeValue={formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                              pnl={pnl !== null ? formatPnl(pnl) : "-"}
                              pnlClassName={pnl !== null ? pnlClass(pnl) : "text-slate-400"}
                              showGainLoss={false}
                              signal={
                                canViewPremiumMetrics ? (
                                  <SmartSignalPill score={signal.score} band={signal.band} size="compact" />
                                ) : (
                                  <LockedSmartSignalPill band={signal.band} size="compact" />
                                )
                              }
                            />
                          </ActivityCard>
                        );
                      })}
                    </ActivityScrollRegion>
                    <TickerActivityPaginationFooter
                      sectionId="congress-activity"
                      pageParam="congress_page"
                      page={congressEventsPage}
                      limit={congressEventsLimit}
                      total={congressEventsTotal}
                      itemCount={congressEvents.length}
                      hasNext={congressEventsHasNext}
                    />
                  </>
                )}
              </div>
            </section>
          ) : null}

          {showInsider ? (
            <section id="insider-activity" className={`${cardClassName} scroll-mt-6`}>
              <div className="mb-4 grid gap-3 sm:grid-cols-[1fr_auto_auto] sm:items-start">
                <div>
                  <h2 className="text-lg font-semibold text-white">Insider activity</h2>
                  <p className="mt-1 text-xs text-slate-500">
                    Displayed quotes are USD. Current foreign prices use spot FX where applicable; historical foreign filing prices use trade-date FX and ADR ratios when normalized.
                  </p>
                </div>
                <ActivityHeaderStats
                  symbol={normalizedSymbol}
                  lookback={lookback}
                  source="insider"
                  buys={insiderBuys}
                  sells={insiderSells}
                />
                <span id="insider-activity-status" className="text-xs text-slate-400">
                  {activityCountLabel(insiderEventsTotal, insiderEvents.length, "event")}
                </span>
              </div>
              <div className="space-y-3">
                {insiderEvents.length === 0 ? (
                  <TickerActivityDetailClient kind="insider" symbol={normalizedSymbol} lookbackDays={selectedLookbackDays} side={side} statusElementId="insider-activity-status" canViewPremiumMetrics={canViewPremiumMetrics} />
                ) : (
                  <>
                    <ActivityScrollRegion>
                      {insiderEvents.map((event) => {
                        const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
                        const insiderProfileHref = insiderHref(display.insiderName, display.reportingCik ?? resolveInsiderReportingCik(event));
                        const insiderRoleRaw = display.role ?? resolveInsiderRole(event);
                        const insiderRoleBadge = resolveInsiderRoleBadge(insiderRoleRaw);
                        const insiderRoleTone = insiderRoleBadgeTone(insiderRoleBadge);
                        const strengthLabel = formatSignalStrengthText(display.signal.band);

                        return (
                        <ActivityCard key={event.id}>
                          <ActivityCardGrid
                            identity={
                              <div className="flex flex-wrap items-center gap-2">
                                {insiderProfileHref ? (
                                  <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                    {display.insiderName}
                                  </Link>
                                ) : (
                                  <span className="text-sm font-semibold text-slate-100">{display.insiderName}</span>
                                )}
                                <Badge tone={insiderRoleTone} className="px-2 py-0.5 text-[10px]">{insiderRoleBadge}</Badge>
                                <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{strengthLabel}</span>
                              </div>
                            }
                            sideBadge={<Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>}
                            dateLabel={formatDateShort(display.filingDate ?? resolveInsiderFilingDate(event))}
                            price={formatActivityPrice(display.displayPrice)}
                            tradeValue={display.tradeValue !== null ? formatCurrency(display.tradeValue) : formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                            pnl={display.pnl !== null ? formatPnl(display.pnl) : "-"}
                            pnlClassName={display.pnl !== null ? pnlClass(display.pnl) : "text-slate-400"}
                            showGainLoss={false}
                            signal={
                              canViewPremiumMetrics ? (
                                <SmartSignalPill score={display.signal.score} band={display.signal.band} size="compact" />
                              ) : (
                                <LockedSmartSignalPill band={display.signal.band} size="compact" />
                              )
                            }
                          />
                        </ActivityCard>
                        );
                      })}
                    </ActivityScrollRegion>
                    <TickerActivityPaginationFooter
                      sectionId="insider-activity"
                      pageParam="insider_page"
                      page={insiderEventsPage}
                      limit={insiderEventsLimit}
                      total={insiderEventsTotal}
                      itemCount={insiderEvents.length}
                      hasNext={insiderEventsHasNext}
                    />
                  </>
                )}
              </div>
            </section>
          ) : null}

          {showSignals && (signalsAuthPending || !signalsUnavailable) ? (
            <div id="signals-activity" className="scroll-mt-6">
              <TickerSignalActivityClient
                symbol={normalizedSymbol}
                side={side}
                lookbackDays={selectedLookbackDays}
                returnTo={tickerReturnTo}
                className={cardClassName}
                initialItems={null}
                initialTotal={null}
                initialState={null}
              />
            </div>
          ) : showSignals ? (
            <section id="signals-activity" className={`${cardClassName} scroll-mt-6`}>
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Signal activity</h2>
                <span className="text-xs text-slate-400">
                  {signalsUnavailable ? (signalsUnavailable.reason === "unavailable" ? "unavailable" : "locked") : activityCountLabel(signalsTotal, signals.length, "signal")}
                </span>
              </div>
              <div className="space-y-3">
                {signalsUnavailable ? (
                  <div className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
                    <p className="text-sm font-semibold text-white">{signalGateTitle}</p>
                    <p className="mt-1 text-sm text-slate-400">{signalsUnavailable.message}</p>
                    {signalsUnavailable.reason === "unavailable" ? null : (
                      <Link
                        href={signalGateHref}
                        prefetch={false}
                        className="mt-3 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
                      >
                        {signalGateLabel}
                      </Link>
                    )}
                  </div>
                ) : signals.length === 0 ? (
                  <p className="text-sm text-slate-400">No abnormal signal activity found for this ticker in the selected lookback.</p>
                ) : (
                  <>
                    <ActivityScrollRegion>
                    {signals.slice(0, 20).map((signal) => {
                      const isInsiderSignal = signal.kind === "insider";
                      const isCongressSignal = signal.kind === "congress";
                      const sourceEvent = activityEventById.get(signal.event_id) ?? null;
                      const insiderDisplay = sourceEvent && isInsiderSignal
                        ? resolveInsiderActivityDisplay(sourceEvent as Record<string, unknown>)
                        : null;
                      const displayName = isInsiderSignal
                        ? getInsiderDisplayName(signal.who, insiderDisplay?.insiderName) ?? "Unknown"
                        : signal.who?.trim() || "Unknown";
                      const insiderProfileHref = isInsiderSignal
                        ? insiderHref(displayName, signal.reporting_cik ?? insiderDisplay?.reportingCik ?? null)
                        : null;
                      const insiderRoleBadge = isInsiderSignal
                        ? resolveInsiderRoleBadge(signal.position ?? insiderDisplay?.role ?? null)
                        : null;
                      const congressChamberValue = isCongressSignal
                        ? signal.chamber ?? (sourceEvent ? resolveCongressChamber(sourceEvent) : null)
                        : null;
                      const congressPartyValue = isCongressSignal
                        ? signal.party ?? (sourceEvent ? resolveCongressParty(sourceEvent) : null)
                        : null;
                      const congressStateValue = isCongressSignal && sourceEvent ? resolveCongressState(sourceEvent) : null;
                      const congressChamber = isCongressSignal ? chamberBadge(congressChamberValue) : null;
                      const congressAffiliation = isCongressSignal ? formatCongressAffiliationText(congressPartyValue, congressStateValue) : null;
                      const strengthLabel = formatSignalStrengthText(signal.smart_band);
                      const displayPrice =
                        sourceEvent && isInsiderSignal
                          ? resolveInsiderActivityDisplay(sourceEvent as Record<string, unknown>).price
                          : sourceEvent
                            ? resolveCongressTradePrice(sourceEvent)
                            : readNumeric((signal as any).estimated_price) ?? readNumeric((signal as any).price);
                      const pnl =
                        activityPnlByEventId.get(signal.event_id) ??
                        readNumeric((signal as any).pnl_pct) ??
                        readNumeric((signal as any).pnlPct);

                      return (
                      <ActivityCard key={`${signal.kind}-${signal.event_id}-${signal.ts}`}>
                        <ActivityCardGrid
                          identity={
                            <div className="flex flex-wrap items-center gap-2">
                              {isInsiderSignal && insiderProfileHref ? (
                                <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                  {displayName}
                                </Link>
                              ) : (
                                <span className="text-sm font-semibold text-slate-100">{displayName}</span>
                              )}
                              {isInsiderSignal && insiderRoleBadge ? (
                                <Badge tone={insiderRoleBadgeTone(insiderRoleBadge)} className="px-2 py-0.5 text-[10px]">{insiderRoleBadge}</Badge>
                              ) : isCongressSignal && congressChamberValue && congressChamber ? (
                                <Badge tone={congressChamber.tone} className="px-2 py-0.5 text-[10px]">{congressChamber.label}</Badge>
                              ) : isCongressSignal ? (
                                <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Congress</span>
                              ) : (
                                <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{signal.kind ?? "Signal"}</span>
                              )}
                              {isCongressSignal && congressAffiliation ? (
                                <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{congressAffiliation}</span>
                              ) : null}
                              <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{strengthLabel}</span>
                            </div>
                          }
                          sideBadge={<Badge tone={transactionTone(signal.trade_type)}>{formatTransactionLabel(signal.trade_type)}</Badge>}
                          dateLabel={formatDateShort(signal.ts)}
                          price={displayPrice !== null ? formatCurrency(displayPrice) : "-"}
                          tradeValue={formatCurrencyRange(signal.amount_min ?? null, signal.amount_max ?? null)}
                          pnl={pnl !== null ? formatPnl(pnl) : "-"}
                          pnlClassName={pnl !== null ? pnlClass(pnl) : "text-slate-400"}
                          showGainLoss={false}
                          signal={
                            canViewPremiumMetrics ? (
                              <SmartSignalPill score={signal.smart_score ?? null} band={signal.smart_band ?? null} size="compact" />
                            ) : (
                              <LockedSmartSignalPill band={signal.smart_band ?? null} size="compact" />
                            )
                          }
                        />
                      </ActivityCard>
                      );
                    })}
                    </ActivityScrollRegion>
                    <div className="border-t border-white/10 pt-3">
                      <span className="text-xs text-slate-500">
                        Showing {signals.length > 0 ? 1 : 0}-{signals.length}
                      </span>
                    </div>
                  </>
                )}
              </div>
            </section>
          ) : null}

          {showInstitutional ? (
            <section id="institutional-activity" className={`${cardClassName} scroll-mt-6`}>
              <div className="mb-4 flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Institutional activity</h2>
                  <p className="mt-1 text-xs text-slate-500">
                    13F filings disclose quarter-end holdings and may not reflect real-time trading.
                  </p>
                </div>
                <span className="text-xs text-slate-400">
                  {activityCountLabel(institutionalEventsTotal, institutionalEvents.length, "event", institutionalEventsUnavailable)}
                </span>
              </div>
              <div className="space-y-3">
                {institutionalCardLocked ? (
                  <div className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
                    <p className="text-sm font-semibold text-white">Institutional activity requires Pro.</p>
                    <p className="mt-1 text-sm text-slate-400">{institutionalGateMessage}</p>
                    <Link
                      href="/pricing"
                      prefetch={false}
                      className="mt-3 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
                    >
                      View Pro
                    </Link>
                  </div>
                ) : institutionalEventsUnavailable ? (
                  <p className="text-sm text-slate-400">Institutional activity is temporarily unavailable.</p>
                ) : institutionalEvents.length === 0 ? (
                  <p className="text-sm text-slate-400">No institutional holder activity found for this ticker in the selected lookback.</p>
                ) : (
                  <>
                    <ActivityScrollRegion>
                      {institutionalEvents.map((event) => (
                        <InstitutionalActivityCard key={event.id} event={event} />
                      ))}
                    </ActivityScrollRegion>
                    <TickerActivityPaginationFooter
                      sectionId="institutional-activity"
                      pageParam="institutional_page"
                      page={institutionalEventsPage}
                      limit={institutionalEventsLimit}
                      total={institutionalEventsTotal}
                      itemCount={institutionalEvents.length}
                      hasNext={institutionalEventsHasNext}
                    />
                  </>
                )}
              </div>
            </section>
          ) : null}

          {showGovernmentContracts ? (
            <section id="government-contracts-activity" className={`${cardClassName} w-full max-w-full min-w-0 overflow-hidden scroll-mt-6`}>
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Government contracts activity</h2>
                <span className="text-xs text-slate-400">{governmentContractsTotal} contract{governmentContractsTotal === 1 ? "" : "s"}</span>
              </div>
              <div className="min-w-0 space-y-3">
                {governmentContractsUnavailable ? (
                  <p className="text-sm text-slate-400">Government contract activity unavailable.</p>
                ) : governmentContractsTotal === 0 ? (
                  <p className="text-sm text-slate-400">
                    {activityDetailsDeferred ? "Loading government contract activity." : "No government contracts in selected window."}
                  </p>
                ) : (
                  <>
                    <ActivityScrollRegion>
                      {governmentContracts.map((contract, index) => (
                        <GovernmentContractActivityCard key={contract.award_id ?? `${contract.period_start}-${index}`} contract={contract} />
                      ))}
                    </ActivityScrollRegion>
                    <TickerActivityPaginationFooter
                      sectionId="government-contracts-activity"
                      pageParam="contracts_page"
                      page={governmentContractsPage}
                      limit={governmentContractsLimit}
                      total={governmentContractsTotal}
                      itemCount={governmentContracts.length}
                      hasNext={governmentContractsHasNext}
                    />
                  </>
                )}
              </div>
            </section>
          ) : null}
        </div>

        <div className="min-w-0 space-y-5">
          <ExpandableTickerSection
            id="top-congress-traders"
            title="Top Congress traders"
            className={cardClassName}
            emptyState={<InlineEmptyState message="No Congress participants in current window." />}
          >
            {topCongressParticipants.map((participant) => {
                  const match = topMembers.find((member) => {
                    if (participant.memberId && (member.bioguide_id === participant.memberId || member.member_id === participant.memberId)) return true;
                    return member.name === participant.name;
                  });
                  const resolvedHref = participant.href ?? (match ? memberHref({ name: match.name, memberId: match.bioguide_id }) : undefined);
                  const bias = biasLabel(participant.buys, participant.sells);
                  const chamberValue = participant.chamber ?? match?.chamber ?? null;
                  const partyValue = participant.party ?? match?.party ?? null;
                  const state = participant.state ?? match?.state ?? null;
                  const chamber = chamberBadge(chamberValue);
                  const affiliation = formatCongressAffiliationText(partyValue, state);
                  const rowClassName = `${compactInteractiveSurfaceClassName} block px-3 py-2.5 text-sm`;

                  const content = (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <span className={`block truncate text-sm font-semibold ${compactInteractiveTitleClassName}`}>{participant.name}</span>
                          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                            {chamberValue ? <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge> : null}
                            {affiliation ? <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{affiliation}</span> : null}
                          </div>
                        </div>
                        <div className="text-right">
                          <span className="text-sm font-semibold tabular-nums text-slate-200">{participant.trades}</span>
                          <p className="text-[11px] text-slate-500">Trades</p>
                        </div>
                      </div>
                      <div className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-400">
                        <span className={`font-semibold tabular-nums ${biasTextClass(bias.tone)}`}>{bias.label}</span>
                        <span className={`font-semibold tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                          {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                        </span>
                      </div>
                    </>
                  );

                  if (resolvedHref) {
                    return (
                      <Link key={participant.memberId ?? participant.name} href={resolvedHref} prefetch={false} className={rowClassName}>
                        {content}
                      </Link>
                    );
                  }

                  return (
                    <div key={participant.memberId ?? participant.name} className={rowClassName}>
                      {content}
                    </div>
                  );
                })}
          </ExpandableTickerSection>

          <ExpandableTickerSection
            id="top-insiders"
            title="Top insiders"
            className={cardClassName}
            emptyState={<InlineEmptyState message="No insiders in current window." />}
          >
            {topInsiderParticipants.map((participant) => {
                  const bias = biasLabel(participant.buys, participant.sells);
                  const href = insiderHref(participant.name, participant.reportingCik);
                  const roleBadge = resolveInsiderRoleBadge(participant.role);
                  const roleTone = insiderRoleBadgeTone(roleBadge);
                  const content = (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <span className={`block truncate font-semibold ${compactInteractiveTitleClassName}`}>{participant.name}</span>
                          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                            <Badge tone={roleTone} className="px-2 py-0.5 text-[10px]">{roleBadge}</Badge>
                          </div>
                        </div>
                        <div className="text-right">
                          <span className="text-sm font-semibold tabular-nums text-slate-200">{participant.trades}</span>
                          <p className="text-[11px] text-slate-500">Trades</p>
                        </div>
                      </div>
                      <div className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-400">
                        <span className={`font-semibold tabular-nums ${biasTextClass(bias.tone)}`}>{bias.label}</span>
                        <span className={`font-semibold tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                          {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                        </span>
                      </div>
                    </>
                  );

                  if (href) {
                    return (
                      <Link
                        key={participant.reportingCik ?? participant.name}
                        href={href}
                        prefetch={false}
                        className={`${compactInteractiveSurfaceClassName} block w-full px-3 py-2.5 text-sm`}
                      >
                        {content}
                      </Link>
                    );
                  }

                  return (
                    <div
                      key={participant.reportingCik ?? participant.name}
                      className={`${compactInteractiveSurfaceClassName} block w-full px-3 py-2.5 text-sm`}
                    >
                      {content}
                    </div>
                  );
                })}
          </ExpandableTickerSection>
        </div>
      </div>
    </>
  );
}

export default async function TickerPage({ params, searchParams }: Props) {
  const { symbol } = await params;
  const sp = (await searchParams) ?? {};
  const requestHeaders = await headers();
  const lookback = clampLookback(one(sp, "lookback"));
  const source = clampSource(one(sp, "source"));
  const side = clampSide(one(sp, "side"));
  const congressPage = clampPage(one(sp, "congress_page"));
  const insiderPage = clampPage(one(sp, "insider_page"));
  const institutionalPage = clampPage(one(sp, "institutional_page"));
  const contractsPage = clampPage(one(sp, "contracts_page"));
  const normalizedSymbol = normalizedTickerSymbolForRoute(symbol);
  const canonicalTickerUrl = canonicalTickerUrlForSymbol(normalizedSymbol);
  const activityDetailsRequested = one(sp, "activity_details") === "1";
  const lookbackDays = Number(lookback);
  const authState = await optionalPageAuthState();
  const authToken = authState.token;
  const entitlements = authToken
    ? await getEntitlements(authToken, { source: "TickerPage" }).catch(() => null)
    : entitlementsFromTierHint(authState.entitlementHint);
  const useAnonymousTickerSsrShell = shouldUseAnonymousTickerSsrShell({
    requestHeaders,
    authToken,
    hasAuthHint: authState.hasAuthHint,
    activityDetailsRequested,
  });
  const activeTickerSsrRequest = !useAnonymousTickerSsrShell;

  const contextBundleResult = useAnonymousTickerSsrShell
    ? {
        bundle: null as TickerContextBundle | null,
        profile: fallbackTickerProfile(normalizedSymbol),
        fallbackMessage: "Ticker data is loading. Try refreshing shortly.",
      }
    : await getTickerContextBundle(normalizedSymbol, {
        side,
        limit: 3,
        lookback_days: lookbackDays,
        authToken: authToken ?? undefined,
        activeUser: activeTickerSsrRequest,
        source: "TickerContextBundle",
        requestSource: "ssr",
      })
        .then((bundle) => ({ bundle, profile: bundle as TickerProfileResponse, fallbackMessage: null as string | null }))
        .catch((error) => {
          if (error instanceof ApiError && error.status === 404) return { bundle: null as TickerContextBundle | null, profile: null, fallbackMessage: null };
          if (isRecoverableTickerProfileError(error)) {
            console.error("[ticker-context-bundle] shell fallback", {
              symbol: normalizedSymbol,
              status: error instanceof ApiError ? error.status : null,
              name: error instanceof Error ? error.name : "unknown",
            });
            return getTickerProfile(normalizedSymbol, { source: "TickerProfileFallback" })
              .then((profile) => ({
                bundle: null as TickerContextBundle | null,
                profile,
                fallbackMessage: "Ticker data is loading. Try refreshing shortly.",
              }))
              .catch((profileError) => {
                if (profileError instanceof ApiError && profileError.status === 404) {
                  return { bundle: null as TickerContextBundle | null, profile: null, fallbackMessage: null };
                }
                return {
                  bundle: null as TickerContextBundle | null,
                  profile: fallbackTickerProfile(normalizedSymbol),
                  fallbackMessage: "Ticker data is loading. Try refreshing shortly.",
                };
              });
          }
          throw error;
        });
  const profile = contextBundleResult.profile;
  if (!profile) return <MissingTickerSearchFallback symbol={normalizedSymbol} />;
  const contextBundle = contextBundleResult.bundle;
  const shellFallbackMessage = contextBundleResult.fallbackMessage;

  const shouldLoadSignals = source === "all" || source === "signals";
  const signalActivityAuthPending = shouldLoadSignals && !authToken && authState.hasAuthHint;
  const hasAuthForEntitlementDisplay = Boolean(authToken || authState.hasAuthHint);
  const canViewSignalActivity = hasAuthForEntitlementDisplay ? canUseSignalActivity(entitlements) : false;
  const canViewTickerConfirmation = hasAuthForEntitlementDisplay ? canUseTickerConfirmation(entitlements) : false;
  const canViewPremiumMetrics = hasAuthForEntitlementDisplay && entitlements
    ? hasEntitlement(entitlements, "premium_feed_metrics")
    : false;
  const canViewProContext = hasAuthForEntitlementDisplay && canUseProTickerContext(entitlements);
  const fallbackSourceEntitlements = tickerContextSourceEntitlements(entitlements, hasAuthForEntitlementDisplay);
  const signalGateState = !shouldLoadSignals || signalActivityAuthPending
    ? null
    : !authToken
      ? signalGateForUnauthenticatedUser()
      : canViewSignalActivity
        ? null
        : signalGateForAuthenticatedFreeUser();
  const tickerConfirmationGate: TickerConfirmationGate | null = canViewTickerConfirmation
    ? null
    : {
        locked: true,
        href: "/pricing",
        label: "Upgrade to Premium",
        message: "Confirmation score, active-source alignment, and freshness setup are available with Premium or Pro.",
      };
  const loadFreshSignalSummary = () => getTickerSignalsSummary(normalizedSymbol, {
    side,
    limit: 3,
    lookback_days: lookbackDays,
    authToken: authToken ?? undefined,
    activeUser: activeTickerSsrRequest,
    source: "TickerSignalsSummary",
  }).catch((error) => {
    if (contextBundle?.signals_summary) return contextBundle.signals_summary;
    throw error;
  });
  const headerMetadata = tickerHeaderMetadata(profile.ticker);
  const headerExchange = cleanTickerHeaderMetadata(profile.ticker.exchange_short_name ?? profile.ticker.exchange);
  const headerCurrency = cleanTickerHeaderMetadata(contextBundle?.identity?.currency);
  const tickerName = profile.ticker.name?.trim();
  const showTickerName = Boolean(tickerName && tickerName.toUpperCase() !== profile.ticker.symbol.toUpperCase());
  const limitedDataMessage = profile.ticker.limited_data_state ? profile.ticker.limited_data_message ?? "Limited price history available" : null;
  const deferTickerActivityDetails = useAnonymousTickerSsrShell || shouldDeferAnonymousTickerActivityDetails({
    requestHeaders,
    authToken,
    hasAuthHint: authState.hasAuthHint,
    activityDetailsRequested,
  });
  const activityPromise = (async () => {
    if (deferTickerActivityDetails) {
      return resolveTickerActivityData({
        signalSummaryRequest: loadFreshSignalSummary(),
        signalsUnavailable: signalGateState,
        lookbackStartKey: lookbackStartDateKey(lookbackDays),
        side,
      });
    }
    const shouldFetchGovernmentContracts = source === "all" || source === "government_contract";
    const shouldFetchCongressActivity = source === "all" || source === "congress";
    const shouldFetchInsiderActivity = source === "all" || source === "insider";
    const shouldFetchInstitutionalActivity = canViewProContext && (source === "all" || source === "institutional");
    const tradeType = sideToTradeType(side);
    const congressActivity =
      shouldFetchCongressActivity
        ? await getEvents({
            symbol: normalizedSymbol,
            recent_days: lookbackDays,
            limit: ACTIVITY_FETCH_SIZE,
            offset: congressPage * ACTIVITY_PAGE_SIZE,
            enrich_prices: 1,
            tape: "congress",
            source: "TickerCongressActivity",
            ...(tradeType ? { trade_type: tradeType } : {}),
          }).catch((error) => {
            console.error("[ticker-congress-activity] unavailable", {
              symbol: normalizedSymbol,
              status: error instanceof ApiError ? error.status : null,
              name: error instanceof Error ? error.name : "unknown",
            });
            return emptyEventsResponse(congressPage, ACTIVITY_PAGE_SIZE);
          })
        : undefined;
    const insiderActivity =
      shouldFetchInsiderActivity
        ? await getEvents({
            symbol: normalizedSymbol,
            recent_days: lookbackDays,
            limit: ACTIVITY_FETCH_SIZE,
            offset: insiderPage * ACTIVITY_PAGE_SIZE,
            enrich_prices: 1,
            tape: "insider",
            source: "TickerInsiderActivity",
            ...(tradeType ? { trade_type: tradeType } : {}),
          }).catch((error) => {
            console.error("[ticker-insider-activity] unavailable", {
              symbol: normalizedSymbol,
              status: error instanceof ApiError ? error.status : null,
              name: error instanceof Error ? error.name : "unknown",
            });
            return emptyEventsResponse(insiderPage, ACTIVITY_PAGE_SIZE);
          })
        : undefined;
    const institutionalActivity =
      shouldFetchInstitutionalActivity
        ? await getEvents({
            symbol: normalizedSymbol,
            recent_days: lookbackDays,
            limit: ACTIVITY_FETCH_SIZE,
            offset: institutionalPage * ACTIVITY_PAGE_SIZE,
            enrich_prices: 0,
            tape: "institutional",
            authToken: authToken ?? undefined,
            source: "TickerInstitutionalActivity",
            requestSource: "ssr",
            routeFamily: "ticker",
          }).catch((error) => {
            console.error("[ticker-institutional-activity] unavailable", {
              symbol: normalizedSymbol,
              status: error instanceof ApiError ? error.status : null,
              name: error instanceof Error ? error.name : "unknown",
            });
            return emptyEventsResponse(institutionalPage, ACTIVITY_PAGE_SIZE);
          })
        : undefined;
    const governmentContracts =
      shouldFetchGovernmentContracts
        ? await getTickerGovernmentContracts(normalizedSymbol, {
            lookback_days: lookbackDays,
            min_amount: 1_000_000,
            limit: GOVERNMENT_CONTRACTS_PAGE_SIZE,
            page: contractsPage,
            activeUser: activeTickerSsrRequest,
            source: "TickerGovernmentContracts",
          }).catch((error) => {
            console.error("[ticker-government-contracts] unavailable", error);
            return {
              symbol: normalizedSymbol,
              status: "unavailable",
              source_status: "unavailable",
              items: [],
              total: 0,
              contract_count: 0,
              page: contractsPage,
              limit: GOVERNMENT_CONTRACTS_PAGE_SIZE,
              has_next: false,
            };
          })
        : undefined;
    const boundedEvents = [
      ...((congressActivity?.items ?? []) as EventsResponse["items"]),
      ...((insiderActivity?.items ?? []) as EventsResponse["items"]),
    ];
    return resolveTickerActivityData({
      eventsPromise: Promise.resolve({
        ...emptyEventsResponse(),
        items: boundedEvents,
        total: boundedEvents.length,
        item_count: boundedEvents.length,
      }),
      congressEventsPromise: congressActivity ? Promise.resolve(congressActivity) : undefined,
      insiderEventsPromise: insiderActivity ? Promise.resolve(insiderActivity) : undefined,
      institutionalEventsPromise: institutionalActivity ? Promise.resolve(institutionalActivity) : undefined,
      governmentContractsPromise: governmentContracts ? Promise.resolve(governmentContracts) : undefined,
      signalSummaryRequest: loadFreshSignalSummary(),
      signalsUnavailable: signalGateState,
      lookbackStartKey: lookbackStartDateKey(lookbackDays),
      side,
    });
  })();

  return (
    <div className="space-y-6">
      <EntitlementHintRefresh enabled={!authToken && authState.hasAuthHint} renderedTier={entitlements?.tier ?? null} />
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="min-w-0 basis-full max-w-[calc(100vw-2rem)] lg:basis-auto lg:max-w-full">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Ticker intelligence</p>
          <h1 className="max-w-full break-words text-2xl font-semibold text-white [overflow-wrap:anywhere] sm:text-3xl">
            <span>{profile.ticker.symbol}</span>
            {showTickerName ? <span className="text-slate-400"> / {tickerName}</span> : null}
            <span className="ml-2 align-middle text-xl font-normal text-slate-500">☆</span>
          </h1>
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
            {headerExchange ? <span className={pillClassName}>{headerExchange}</span> : null}
            {headerMetadata.length ? (
              <p className="min-w-0 rounded-md bg-slate-900/45 px-3 py-1 text-[11px] font-medium tracking-[0.02em] text-slate-400 sm:max-w-[44rem] sm:truncate">
                {headerMetadata.join(" / ")}
              </p>
            ) : null}
            {headerCurrency ? <span className={pillClassName}>{headerCurrency}</span> : null}
          </div>
          {limitedDataMessage ? (
            <p className="mt-3 text-sm font-medium text-amber-200">{limitedDataMessage}</p>
          ) : null}
        </div>
        <div className="grid w-[calc(100vw-2rem)] flex-none grid-cols-2 gap-2 [&>*]:w-full [&>*>button]:w-full [&>a]:justify-center [&>button]:justify-center sm:flex sm:w-auto sm:flex-initial sm:flex-wrap sm:items-center sm:justify-end sm:[&>*]:w-auto sm:[&>*>button]:w-auto">
          <AddTickerToWatchlist symbol={normalizedSymbol} />
          <Link href={`/compare/${encodeURIComponent(normalizedSymbol)}/_`} className={ghostButtonClassName}>Compare</Link>
          <ShareLinks canonicalUrl={canonicalTickerUrl} />
          <Link href="/?mode=all" className={ghostButtonClassName}>Back to feed</Link>
        </div>
      </div>
      {shellFallbackMessage ? (
        <div className="rounded-lg border border-amber-300/20 bg-amber-300/10 px-4 py-3 text-sm font-medium text-amber-100">
          {shellFallbackMessage}
        </div>
      ) : null}
      <Suspense fallback={<DeferredTickerSummarySkeleton />}>
        <DeferredTickerContent
          activityPromise={activityPromise}
          normalizedSymbol={normalizedSymbol}
          decisionLayer={contextBundle?.decision_layer ?? null}
          lookback={lookback}
          source={source}
          side={side}
          activityDetailsDeferred={deferTickerActivityDetails}
          signalsAuthPending={signalActivityAuthPending}
          topMembers={profile.top_members ?? []}
          confirmationScoreBundle={profile.confirmation_score_bundle}
          optionsFlowSummary={profile.options_flow_summary}
          technicalIndicators={profile.technical_indicators}
          fallbackSourceEntitlements={fallbackSourceEntitlements}
          allowAuthHintEntitlementOverride={authState.hasAuthHint}
          canViewProTickerContext={canViewProContext}
          hasAuthForEntitlementDisplay={hasAuthForEntitlementDisplay}
          canViewPremiumMetrics={canViewPremiumMetrics}
          tickerConfirmationGate={tickerConfirmationGate}
        />
      </Suspense>
    </div>
  );
}

