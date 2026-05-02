import { FeedFiltersServer } from "@/components/feed/FeedFiltersServer";
import { FeedList } from "@/components/feed/FeedList";
import { FeedDebugVisibility } from "@/components/feed/FeedDebugVisibility";
import { FeedMountLogger } from "@/components/feed/FeedMountLogger";
import { FeedClientProbe } from "@/components/feed/FeedClientProbe";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { API_BASE, getEvents, getTickerProfiles } from "@/lib/api";
import type { EventsResponse } from "@/lib/api";
import type { FeedItem } from "@/lib/types";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { Suspense } from "react";

export const dynamic = "force-dynamic";

// PR summary: Home feed is now backed by /api/events. The unified tape currently shows only seeded demo events; production
// trades require backfill/dual-write from the legacy trade store.
function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

const feedParamKeys = ["symbol", "member", "chamber", "party", "trade_type", "role", "ownership", "min_amount", "max_amount", "recent_days", "department"] as const;

type FeedParamKey = (typeof feedParamKeys)[number];
type FeedMode = "congress" | "insider" | "government_contracts" | "all";
type SearchParamsInput = Record<string, string | string[] | undefined>;
type CompanyNameMap = Record<string, string>;

const validModes = ["all", "congress", "insider", "government_contracts"] as const;

function isValidMode(value: string): value is FeedMode {
  return (validModes as readonly string[]).includes(value);
}

function feedParamsForMode(mode: FeedMode, params: Record<FeedParamKey, string>): Record<FeedParamKey, string> {
  if (mode !== "government_contracts") return params;
  return {
    ...params,
    member: "",
    chamber: "",
    party: "",
    trade_type: "",
    role: "",
    ownership: "",
    department: params.department,
  };
}

export async function generateMetadata({
  searchParams,
}: {
  searchParams?: Promise<SearchParamsInput>;
}): Promise<Metadata> {
  const sp = (await searchParams) ?? {};
  const modeParam = getParam(sp, "mode");
  const mode = isValidMode(modeParam) ? modeParam : "all";

  return {
    alternates: {
      canonical: `/?mode=${mode}`,
    },
  };
}

function buildEventsUrl(params: Record<string, string | number | boolean>, tape: string) {
  const url = new URL("/api/events", API_BASE);

  if (tape === "insider") {
    url.searchParams.set("event_type", "insider_trade");
  } else if (tape === "congress") {
    url.searchParams.set("event_type", "congress_trade");
  } else if (tape === "government_contracts" || tape === "government_contract") {
    url.searchParams.set("event_type", "government_contract");
  } else {
    url.searchParams.delete("event_type");
  }

  Object.entries(params).forEach(([key, value]) => {
    const trimmed = String(value).trim();
    if (!trimmed) return;
    url.searchParams.set(key, trimmed);
  });

  return url.toString();
}

type SignalOverlayMap = Record<string, { score: number; band: string }>;

function DebugMountLogger({
  enabled,
  name,
  detail,
}: {
  enabled: boolean;
  name: string;
  detail?: Record<string, unknown>;
}) {
  if (!enabled) return null;
  return <FeedMountLogger name={name} enabled={true} detail={detail} />;
}

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && !Number.isNaN(value)) return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function firstTrimmedString(...values: unknown[]): string | null {
  for (const value of values) {
    const trimmed = asTrimmedString(value);
    if (trimmed) return trimmed;
  }
  return null;
}

function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const parsed = asNumber(value);
    if (parsed !== null) return parsed;
  }
  return null;
}

function insiderRole(payload: any): string | null {
  const raw =
    asTrimmedString(payload?.raw?.typeOfOwner) ??
    asTrimmedString(payload.role) ??
    asTrimmedString(payload?.raw?.officerTitle) ??
    asTrimmedString(payload?.raw?.insiderRole) ??
    asTrimmedString(payload?.raw?.position);

  if (!raw) return null;
  const s = raw.toUpperCase();
  if (s.includes("CEO")) return "CEO";
  if (s.includes("CFO")) return "CFO";
  if (s.includes("COO")) return "COO";
  if (s.includes("CTO")) return "CTO";
  if (s.includes("PRESIDENT")) return "PRES";
  if (s.includes("VP")) return "VP";
  if (s.includes("DIRECTOR")) return "DIR";
  if (s.includes("OFFICER")) return "OFFICER";
  return "INSIDER";
}

function normalizeInsiderDirection(payload: any): "Purchase" | "Sale" | null {
  const t = asTrimmedString(payload?.raw?.transactionType)?.toUpperCase();
  if (t) {
    if (t.includes("SALE")) return "Sale";
    if (t.includes("PURCHASE")) return "Purchase";
    return null;
  }
  const ad = asTrimmedString(payload?.raw?.acquisitionOrDisposition)?.toUpperCase();
  if (ad === "A") return "Purchase";
  if (ad === "D") return "Sale";
  return null;
}

function parsePayload(payload: unknown): any {
  if (typeof payload === "string") {
    try {
      return JSON.parse(payload);
    } catch {
      return {};
    }
  }
  if (payload && typeof payload === "object") return payload;
  return {};
}

function formatOwnershipLabel(value: unknown): string | null {
  const raw = asTrimmedString(value);
  if (!raw) return null;
  const cleaned = raw.toUpperCase();
  if (cleaned === "D" || cleaned === "DIRECT") return "Direct";
  if (cleaned === "I" || cleaned === "INDIRECT") return "Indirect";
  return raw;
}

const governmentContractCompanyFallbacks: CompanyNameMap = {
  BA: "Boeing Co",
  LMT: "Lockheed Martin Corp",
  NVDA: "NVIDIA Corporation",
};

function companyNameForGovernmentContract(symbol: string | null, payload: Record<string, any>, companyNames: CompanyNameMap): string {
  if (!symbol) {
    return (
      firstTrimmedString(payload.company_name, payload.companyName, payload.recipient_name, payload.raw_recipient_name) ??
      "Company unavailable"
    );
  }
  const normalized = symbol.trim().toUpperCase();
  return (
    firstTrimmedString(companyNames[normalized], payload.company_name, payload.companyName) ??
    governmentContractCompanyFallbacks[normalized] ??
    firstTrimmedString(payload.recipient_name, payload.raw_recipient_name) ??
    normalized
  );
}

function mapEventToFeedItem(
  event: {
  id: number;
  event_type: string;
  ts: string;
  ticker?: string | null;
  source?: string | null;
  headline?: string | null;
  summary?: string | null;
  url?: string | null;
  amount_min?: number | null;
  amount_max?: number | null;
  estimated_price?: number | null;
  current_price?: number | null;
  pnl_pct?: number | null;
  pnl_source?: string | null;
  quote_is_stale?: boolean | null;
  quote_asof_ts?: string | null;
  member_net_30d?: number | null;
  symbol_net_30d?: number | null;
  payload?: any;
},
  companyNames: CompanyNameMap = {},
): FeedItem | null {
  if (event.event_type === "congress_trade") {
    const payload = parsePayload(event.payload);
    const memberPayload = payload.member ?? {};
    const memberBioguide =
      asTrimmedString(memberPayload.bioguide_id) ??
      (typeof memberPayload.bioguide_id === "number" ? String(memberPayload.bioguide_id) : null) ??
      event.source ??
      "event";
    const memberName =
      asTrimmedString(memberPayload.name) ?? asTrimmedString(payload.member_name) ?? event.source ?? "Congressional Trade";
    const memberChamber = asTrimmedString(memberPayload.chamber) ?? event.source ?? event.event_type;
    const memberParty = asTrimmedString(memberPayload.party);
    const memberState = asTrimmedString(memberPayload.state);
    const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
    const securityName = asTrimmedString(payload.security_name) ?? event.headline ?? event.summary ?? event.event_type;
    const assetClass = asTrimmedString(payload.asset_class) ?? "Security";
    const sector = asTrimmedString(payload.sector);
    const transactionType = asTrimmedString(payload.transaction_type) ?? event.event_type;
    const ownerType = asTrimmedString(payload.owner_type) ?? "Unknown";
    const tradeDate = asTrimmedString(payload.trade_date) ?? event.ts ?? null;
    const reportDate = asTrimmedString(payload.report_date) ?? event.ts ?? null;
    const amountMin = asNumber(payload.amount_range_min);
    const amountMax = asNumber(payload.amount_range_max);
    const estimatedPrice =
      typeof (event as any).estimated_price === "number"
        ? (event as any).estimated_price
        : asNumber(payload.estimated_price);
    const currentPrice =
      typeof (event as any).current_price === "number"
        ? (event as any).current_price
        : asNumber(payload.current_price);
    const pnlPct =
      typeof (event as any).pnl_pct === "number"
        ? (event as any).pnl_pct
        : asNumber(payload.pnl_pct);
    const documentUrl = asTrimmedString(payload.document_url) ?? event.url ?? null;
    const memberNet30d =
      typeof (event as any).member_net_30d === "number"
        ? (event as any).member_net_30d
        : asNumber(payload.member_net_30d);

    return {
      id: event.id,
      kind: event.event_type,
      member: {
        bioguide_id: memberBioguide,
        name: memberName,
        chamber: memberChamber,
        party: memberParty,
        state: memberState,
      },
      security: {
        symbol,
        name: securityName,
        asset_class: assetClass,
        sector,
      },
      transaction_type: transactionType,
      owner_type: ownerType,
      trade_date: tradeDate,
      report_date: reportDate,
      amount_range_min: amountMin,
      amount_range_max: amountMax,
      estimated_price: estimatedPrice,
      current_price: currentPrice,
      pnl_pct: pnlPct,
      pnl_source: (event as any).pnl_source ?? null,
      quote_is_stale: typeof (event as any).quote_is_stale === "boolean" ? (event as any).quote_is_stale : null,
      quote_asof_ts: typeof (event as any).quote_asof_ts === "string" ? (event as any).quote_asof_ts : null,
      member_net_30d: memberNet30d,
      confirmation_30d: (event as any).confirmation_30d ?? null,
    };
  }

  if (event.event_type === "insider_trade") {
    const payload = parsePayload(event.payload);
    const direction = normalizeInsiderDirection(payload);
    if (!direction) return null;
    const display = resolveInsiderActivityDisplay(event as unknown as Record<string, unknown>);
    const symbol = asTrimmedString(event.ticker) ?? display.symbol ?? asTrimmedString(payload.symbol);
    const insiderName =
      display.insiderName ??
      asTrimmedString(event.source) ??
      "Insider";
    const ownership = formatOwnershipLabel(payload.ownership) ?? formatOwnershipLabel(payload?.raw?.directOrIndirect);
    const transactionType = direction;
    const role = display.role ?? insiderRole(payload);
    const companyName = display.companyName !== "-" ? display.companyName : asTrimmedString(event.headline) ?? asTrimmedString(event.summary);
    const companyNameDiffersFromTicker =
      companyName && symbol
        ? companyName.toUpperCase() !== symbol.toUpperCase()
        : Boolean(companyName);
    const securityName =
      (companyNameDiffersFromTicker ? companyName : null) ??
      companyName ??
      asTrimmedString(payload.security_name) ??
      symbol ??
      event.headline ??
      event.summary ??
      "Insider Trade";
    const securityClass = asTrimmedString(payload?.raw?.securityName) ?? "Insider Trade";
    const price = display.price ?? asNumber(payload.price);
    const amountMin = asNumber((event as any).amount_min) ?? null;
    const amountMax = asNumber((event as any).amount_max) ?? null;
    const currentPrice =
      typeof (event as any).current_price === "number"
        ? (event as any).current_price
        : asNumber(payload.current_price);
    const pnlPct =
      typeof (event as any).pnl_pct === "number"
        ? (event as any).pnl_pct
        : asNumber(payload.pnl_pct);
    const memberNet30d =
      typeof (event as any).member_net_30d === "number"
        ? (event as any).member_net_30d
        : asNumber(payload.member_net_30d);
    const symbolNet30d =
      typeof (event as any).symbol_net_30d === "number"
        ? (event as any).symbol_net_30d
        : asNumber(payload.symbol_net_30d);
    const filingDate = asTrimmedString(payload.filing_date) ?? event.ts ?? null;
    const transactionDate =
      asTrimmedString(payload.transaction_date) ?? asTrimmedString(payload?.raw?.transactionDate) ?? null;

    return {
      id: event.id,
      member: {
        bioguide_id: `insider-${symbol ?? event.id}`,
        name: insiderName,
        chamber: "insider",
      },
      security: {
        symbol,
        name: securityName,
        asset_class: securityClass,
      },
      transaction_type: transactionType,
      owner_type: ownership ?? "Insider",
      trade_date: transactionDate,
      report_date: filingDate,
      amount_range_min: amountMin,
      amount_range_max: amountMax,
      kind: "insider_trade",
      current_price: currentPrice,
      pnl_pct: pnlPct,
      pnl_source: (event as any).pnl_source ?? null,
      quote_is_stale: typeof (event as any).quote_is_stale === "boolean" ? (event as any).quote_is_stale : null,
      quote_asof_ts: typeof (event as any).quote_asof_ts === "string" ? (event as any).quote_asof_ts : null,
      member_net_30d: memberNet30d,
      symbol_net_30d: symbolNet30d,
      confirmation_30d: (event as any).confirmation_30d ?? null,
      insider: {
        name: insiderName,
        ownership,
        filing_date: filingDate,
        transaction_date: transactionDate,
        price,
        display_price: display.price,
        reported_price: display.reportedPrice,
        reported_price_currency: display.reportedPriceCurrency,
        role,
        reporting_cik: display.reportingCik ?? asTrimmedString(payload.reporting_cik) ?? asTrimmedString(payload?.raw?.reportingCik) ?? null,
      },
    };
  }

  if (event.event_type === "institutional_buy") {
    const payload = parsePayload(event.payload);
    const symbol = asTrimmedString(event.ticker) ?? asTrimmedString(payload.symbol);
    const institutionName =
      asTrimmedString(payload.institution_name) ??
      asTrimmedString(payload?.raw?.holder) ??
      asTrimmedString(payload?.raw?.institutionName) ??
      asTrimmedString(event.source) ??
      "Institution";
    const securityName =
      asTrimmedString(payload?.raw?.companyName) ??
      asTrimmedString(payload.company_name) ??
      symbol ??
      "Institutional Position";
    const amountMax =
      asNumber((event as any).amount_max) ??
      asNumber(payload.market_value) ??
      null;
    const amountMin =
      asNumber((event as any).amount_min) ??
      amountMax;
    const filingDate = asTrimmedString(payload.filing_date) ?? event.ts ?? null;
    const reportDate = asTrimmedString(payload.report_date) ?? filingDate;

    return {
      id: event.id,
      kind: "institutional_buy",
      member: {
        bioguide_id: asTrimmedString(payload.institution_cik) ?? `institution-${event.id}`,
        name: institutionName,
        chamber: "institutional",
      },
      security: {
        symbol,
        name: securityName,
        asset_class: "Institutional Filing",
      },
      transaction_type: "Holding Increase",
      owner_type: "Institutional Filing",
      trade_date: reportDate,
      report_date: filingDate,
      amount_range_min: amountMin,
      amount_range_max: amountMax,
    };
  }

  if (event.event_type === "government_contract") {
    const payload = parsePayload(event.payload);
    const isFundingAction =
      payload.event_subtype === "funding_action" ||
      Boolean(payload.modification_number) ||
      Boolean(payload.action_date);
    const agency =
      firstTrimmedString(payload.awarding_agency, payload.department, payload.agency, payload.funding_agency) ??
      "Government Contract";
    const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
    const companyName = companyNameForGovernmentContract(symbol, payload, companyNames);
    const title =
      firstTrimmedString(
        payload.title,
        payload.description,
        payload.award_description,
        payload.contract_description,
        event.headline,
        event.summary,
      ) ?? "Government contract award";
    const value = firstNumber(
      payload.obligated_amount,
      payload.transaction_obligated_amount,
      payload.award_amount,
      payload.contract_value,
      payload.amount,
      (event as any).amount_max,
      (event as any).amount_min,
    );
    const reportDate =
      firstTrimmedString(
        payload.report_date,
        payload.action_date,
        payload.created_at,
      ) ??
      event.ts ??
      null;

    return {
      id: event.id,
      kind: "government_contract",
      member: {
        bioguide_id: `government-contract-${event.id}`,
        name: agency,
        chamber: "government_contract",
      },
      security: {
        symbol,
        name: companyName,
        asset_class: "Government Contract",
      },
      contract_description: title,
      transaction_type: isFundingAction ? "Government Contract Funding" : "Government Contract",
      owner_type: agency,
      trade_date: null,
      report_date: reportDate,
      amount_range_min: value,
      amount_range_max: value,
      payload,
      url: firstTrimmedString(payload.source_url, payload.url),
    };
  }

  return {
    id: event.id,
    member: {
      bioguide_id: event.source ?? "event",
      name: event.source ?? "Congressional Event",
      chamber: event.event_type ?? "event",
    },
    security: {
      symbol: event.ticker ?? null,
      name: event.headline ?? event.summary ?? event.event_type,
      asset_class: event.event_type,
    },
    transaction_type: event.event_type,
    owner_type: "event",
    trade_date: event.ts,
    report_date: event.ts,
    amount_range_min: null,
    amount_range_max: null,
  };
}


type FeedResultsSectionProps = {
  feedMode: FeedMode;
  queryDebug: boolean;
  debugLifecycle: boolean;
  page: number;
  pageSize: 25 | 50 | 100;
  activeParams: Record<FeedParamKey, string>;
};

function FeedResultsSectionSkeleton() {
  return (
    <section className="space-y-4" aria-live="polite" aria-busy="true">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="space-y-2">
          <SkeletonBlock className="h-6 w-40" />
          <SkeletonBlock className="h-4 w-56" />
        </div>
      </div>
      <div className="space-y-3">
        {Array.from({ length: 5 }).map((_, idx) => (
          <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-2">
                <SkeletonBlock className="h-3 w-20" />
                <SkeletonBlock className="h-5 w-48" />
              </div>
              <SkeletonBlock className="h-6 w-16 rounded-full" />
            </div>
            <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
              {Array.from({ length: 4 }).map((__, statIdx) => (
                <SkeletonBlock key={statIdx} className="h-3 w-full" />
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

async function FeedResultsSection({ feedMode, queryDebug, debugLifecycle, page, pageSize, activeParams }: FeedResultsSectionProps) {
  const requestParams = {
    ...activeParams,
    limit: pageSize,
    page_size: pageSize,
    offset: (page - 1) * pageSize,
  };

  const requestUrl = buildEventsUrl(requestParams, feedMode);
  const debug: {
    request_url: string;
    events_returned: number;
    fetch_error: string | null;
  } = {
    request_url: requestUrl,
    events_returned: 0,
    fetch_error: null,
  };

  let events: EventsResponse = { items: [], limit: null, offset: null, total: null };
  try {
    events = await getEvents({ ...requestParams, tape: feedMode });
  } catch (err) {
    debug.fetch_error = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    console.error("[feed] fetch failed:", err);
  }

  debug.events_returned = events.items.length;

  const governmentContractSymbols = Array.from(
    new Set(
      events.items
        .filter((event) => event.event_type === "government_contract")
        .map((event) => {
          const payload = parsePayload(event.payload);
          return asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
        })
        .filter((symbol): symbol is string => Boolean(symbol))
        .map((symbol) => symbol.toUpperCase()),
    ),
  );
  let companyNames: CompanyNameMap = {};
  if (governmentContractSymbols.length > 0) {
    try {
      const profiles = await getTickerProfiles(governmentContractSymbols);
      companyNames = Object.fromEntries(
        Object.entries(profiles)
          .map(([symbol, profile]) => [symbol.toUpperCase(), asTrimmedString(profile?.ticker?.name)] as const)
          .filter((entry): entry is readonly [string, string] => Boolean(entry[1])),
      );
    } catch (err) {
      console.error("[feed] ticker profiles unavailable for government contracts:", err);
    }
  }

  const items = [...events.items]
    .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
    .map((event) => {
      const feedItem = mapEventToFeedItem(event, companyNames);
      if (!feedItem) return null;
      const payload = parsePayload(event.payload);
      const tradeTicker = asTrimmedString(payload.symbol) ?? event.ticker ?? null;
      const tradeUrl =
        feedItem.kind === "government_contract"
          ? firstTrimmedString(payload.source_url, payload.url, payload.award_url, event.url)
          : asTrimmedString(payload.document_url) ?? event.url ?? null;
      return {
        ...feedItem,
        title: event.headline ?? event.summary ?? event.event_type,
        ticker: tradeTicker,
        timestamp: event.ts,
        source: event.source ?? null,
        url: tradeUrl,
        payload,
        smart_score: (event as any).smart_score ?? null,
        smart_band: (event as any).smart_band ?? null,
        pnl_source: (event as any).pnl_source ?? null,
        quote_is_stale: (event as any).quote_is_stale ?? null,
        quote_asof_ts: (event as any).quote_asof_ts ?? null,
      };
    })
    .filter(Boolean) as FeedItem[];

  const total = typeof events.total === "number" ? events.total : null;
  const totalPages = total ? Math.max(1, Math.ceil(total / pageSize)) : 1;

  const signalOverlay: SignalOverlayMap = {};

  return (
    <section className="space-y-4">
      <DebugMountLogger enabled={debugLifecycle} name="FeedResultsSection" detail={{ feedMode, page, pageSize }} />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-white">Latest events</h2>
          <p className="text-sm text-slate-400">Showing {items.length} events on page {page}.</p>
        </div>
      </div>
      {queryDebug ? (
      <FeedDebugVisibility initialQueryDebug={queryDebug}>
        <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4 text-xs text-slate-300">
            <div className="font-semibold text-slate-100">Debug feed request</div>
            <div className="mt-2 text-slate-400">
              <span className="font-semibold text-slate-200">request_url:</span>{" "}
              <span className="break-all font-mono text-[11px]">{debug.request_url}</span>
            </div>
            <div className="mt-2 text-slate-400">
              <span className="font-semibold text-slate-200">events_returned:</span> {debug.events_returned}
            </div>
            {debug.fetch_error ? (
              <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-2 text-red-300">
                <div className="font-semibold">fetch_error:</div>
                <pre className="mt-1 whitespace-pre-wrap text-xs">{debug.fetch_error}</pre>
              </div>
            ) : null}
            <div className="mt-3 space-y-2">
              {events.items.slice(0, 3).map((event) => {
                const payload = parsePayload(event.payload);
                const memberPayload = payload.member ?? {};
                const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker) ?? "—";
                const memberName =
                  asTrimmedString(memberPayload.name) ??
                  asTrimmedString(payload.member_name) ??
                  asTrimmedString(event.source) ??
                  "—";
                const tradeType =
                  asTrimmedString(payload.transaction_type) ?? asTrimmedString(event.event_type) ?? "—";
                const amountMin =
                  asNumber((event as any).amount_min) ??
                  asNumber(payload.amount_range_min) ??
                  asNumber(payload.amount_min) ??
                  asNumber(payload.amount) ??
                  null;
                const amountMax =
                  asNumber((event as any).amount_max) ??
                  asNumber(payload.amount_range_max) ??
                  asNumber(payload.amount_max) ??
                  null;
                return (
                  <div key={event.id} className="rounded-lg border border-slate-800/60 bg-slate-900/40 p-3">
                    <div className="text-slate-200">
                      <span className="font-semibold">Symbol:</span> {symbol}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Member:</span> {memberName}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Trade type:</span> {tradeType}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Amount:</span>{" "}
                      {amountMin !== null ? amountMin : "—"} / {amountMax !== null ? amountMax : "—"}
                    </div>
                  </div>
                );
              })}
            </div>
        </div>
      </FeedDebugVisibility>
      ) : null}
      <div id="feed-top" />
      <div className="min-h-[32rem]">
        <FeedList
          items={items}
          page={page}
          pageSize={pageSize}
          total={total}
          totalPages={totalPages}
          overlaySignals={signalOverlay}
          debugLifecycle={debugLifecycle}
        />
      </div>
    </section>
  );
}

export default async function FeedPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParamsInput>;
}) {
  const sp = (await searchParams) ?? {};
  const modeParam = getParam(sp, "mode");
  if (!modeParam || !isValidMode(modeParam)) {
    redirect("/?mode=all");
  }
  const feedMode = modeParam;
  const queryDebug = getParam(sp, "debug") === "1";
  const debugDisableFeedFilters = getParam(sp, "debug_disable_feed_filters") === "1";
  const debugDisableFeedResults = getParam(sp, "debug_disable_feed_results") === "1";
  const debugPlainFeedShell = getParam(sp, "debug_plain_feed_shell") === "1";
  const debugMoveProbeBelowResults = getParam(sp, "debug_move_probe_below_results") === "1";
  const debugMoveProbeAboveHeader = getParam(sp, "debug_move_probe_above_header") === "1";
  const debugReplaceHeaderWithProbe = getParam(sp, "debug_replace_header_with_probe") === "1";
  const debugServerPlaceholderInFilterSlot = getParam(sp, "debug_server_placeholder_in_filter_slot") === "1";
  const debugDisableTopMountLogger = getParam(sp, "debug_disable_top_mount_logger") === "1";
  const debugDisableAllMountLoggers = getParam(sp, "debug_disable_all_mount_loggers") === "1";
  const debugClientProbeInsideOuterWrapper = getParam(sp, "debug_client_probe_inside_outer_wrapper") === "1";
  const debugClientProbeInsideHeaderWrapper = getParam(sp, "debug_client_probe_inside_header_wrapper") === "1";
  const debugClientProbeBetweenHeaderAndResults = getParam(sp, "debug_client_probe_between_header_and_results") === "1";
  const debugLifecycle =
    queryDebug ||
    debugDisableFeedFilters ||
    debugDisableFeedResults ||
    debugPlainFeedShell ||
    debugMoveProbeBelowResults ||
    debugMoveProbeAboveHeader ||
    debugReplaceHeaderWithProbe ||
    debugServerPlaceholderInFilterSlot ||
    debugDisableTopMountLogger ||
    debugDisableAllMountLoggers ||
    debugClientProbeInsideOuterWrapper ||
    debugClientProbeInsideHeaderWrapper ||
    debugClientProbeBetweenHeaderAndResults ||
    getParam(sp, "debug_lifecycle") === "1";
  const debugMountLoggersEnabled = debugLifecycle && !debugDisableAllMountLoggers;
  const debugTopMountLoggerEnabled = debugMountLoggersEnabled && !debugDisableTopMountLogger;
  const requestedPage = Number(getParam(sp, "page") || "1");
  const page = Number.isFinite(requestedPage) ? Math.max(1, Math.floor(requestedPage)) : 1;
  const requestedPageSize = Number(getParam(sp, "page_size") || getParam(sp, "limit") || "50");
  const pageSize: 25 | 50 | 100 = [25, 50, 100].includes(requestedPageSize) ? (requestedPageSize as 25 | 50 | 100) : 50;
  const activeParams = feedParamsForMode(feedMode, {
    symbol: getParam(sp, "symbol"),
    member: getParam(sp, "member"),
    chamber: getParam(sp, "chamber"),
    party: getParam(sp, "party"),
    trade_type: getParam(sp, "trade_type"),
    role: getParam(sp, "role"),
    ownership: getParam(sp, "ownership"),
    min_amount: getParam(sp, "min_amount"),
    max_amount: getParam(sp, "max_amount"),
    recent_days: getParam(sp, "recent_days"),
    department: getParam(sp, "department"),
  });
  const resultsBoundaryKey = JSON.stringify({
    mode: feedMode,
    page,
    pageSize,
    ...activeParams,
  });
  if (debugPlainFeedShell) {
    return (
      <div className="space-y-4">
        <DebugMountLogger enabled={debugTopMountLoggerEnabled} name="FeedPage" detail={{ feedMode, debugPlainFeedShell: true }} />
        <section className="rounded-2xl border border-amber-400/30 bg-amber-500/10 p-4 text-sm text-amber-100">
          <div className="font-semibold">debug_plain_feed_shell=1</div>
          <p className="mt-2">
            Minimal shell only. FeedFilters, FeedResultsSection, suspense-loading visuals, and feed cards are intentionally disabled.
          </p>
        </section>
        <div className="rounded-2xl border border-white/15 bg-white/5 p-4 text-sm text-slate-300">
          <p>Static feed shell diagnostic content.</p>
          <p className="mt-1 text-slate-400">mode={feedMode}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <DebugMountLogger
        name="FeedPage"
        enabled={debugTopMountLoggerEnabled}
        detail={{
          feedMode,
          debugDisableFeedFilters,
          debugDisableFeedResults,
          debugMoveProbeBelowResults,
          debugMoveProbeAboveHeader,
          debugReplaceHeaderWithProbe,
          debugServerPlaceholderInFilterSlot,
          debugDisableTopMountLogger,
          debugDisableAllMountLoggers,
          debugClientProbeInsideOuterWrapper,
          debugClientProbeInsideHeaderWrapper,
          debugClientProbeBetweenHeaderAndResults,
        }}
      />
      <DebugMountLogger enabled={debugTopMountLoggerEnabled} name="FeedPageOuterWrapper" detail={{ wrapper: "top-outer-page-div" }} />

      {debugClientProbeInsideOuterWrapper ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeInsideOuterWrapperSlot" />
          <FeedClientProbe label="inside-outer-wrapper" />
        </div>
      ) : null}

      {debugMoveProbeAboveHeader ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeAboveHeaderSlot" />
          <FeedClientProbe label="above-header" />
        </div>
      ) : null}

      {debugReplaceHeaderWithProbe ? (
        <section className="flex flex-col gap-6">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedHeaderWrapper" detail={{ mode: "replaced_with_client_probe" }} />
          <FeedClientProbe label="header-replacement" />
        </section>
      ) : (
        <section className="flex flex-col gap-6">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedHeaderWrapper" detail={{ mode: "normal-header" }} />
          {debugClientProbeInsideHeaderWrapper ? (
            <div className="space-y-2">
              <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeInsideHeaderWrapperSlot" />
              <FeedClientProbe label="inside-header-wrapper" />
            </div>
          ) : null}
          <div className="flex flex-col gap-2">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
            <h1 className="text-4xl font-semibold text-white sm:text-5xl">Unified political & insider trade feed.</h1>
            <p className="max-w-2xl text-sm text-slate-400">
              One feed, one API: switch between Congress, Insider, or All and apply mode-aware filters for fast signal discovery.
            </p>
          </div>
          <div className="contents">
            <DebugMountLogger
              name="FeedFilterSlotWrapper"
              enabled={debugMountLoggersEnabled}
              detail={{
                slot: "header-filter-area",
                debugDisableFeedFilters,
                debugMoveProbeBelowResults,
                debugServerPlaceholderInFilterSlot,
              }}
            />
            {debugDisableFeedFilters ? (
              <div className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-3 text-xs text-amber-100">
                debug_disable_feed_filters=1 (FeedFilters disabled)
              </div>
            ) : debugMoveProbeBelowResults ? null : debugServerPlaceholderInFilterSlot ? (
              <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-3 text-xs text-slate-300">
                debug_server_placeholder_in_filter_slot=1 (server-rendered placeholder only)
              </div>
            ) : (
              <FeedFiltersServer mode={feedMode} params={activeParams} />
            )}
          </div>
        </section>
      )}

      {debugClientProbeBetweenHeaderAndResults ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeBetweenHeaderAndResultsSlot" />
          <FeedClientProbe label="between-header-and-results" />
        </div>
      ) : null}

      {debugDisableFeedResults ? (
        <section className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-4 text-sm text-amber-100">
          debug_disable_feed_results=1 (FeedResultsSection / cards disabled)
        </section>
      ) : (
        <div className="space-y-3">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedResultsSectionWrapper" detail={{ wrapper: "results-section-wrapper" }} />
          <Suspense key={resultsBoundaryKey} fallback={<FeedResultsSectionSkeleton />}>
            <FeedResultsSection
              feedMode={feedMode}
              queryDebug={queryDebug}
              debugLifecycle={debugMountLoggersEnabled}
              page={page}
              pageSize={pageSize}
              activeParams={activeParams}
            />
          </Suspense>
          {debugMoveProbeBelowResults ? (
            <div className="space-y-2">
              <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeBelowResultsSlot" />
              <FeedClientProbe label="below-results" />
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
