import { FeedFilters } from "@/components/feed/FeedFilters";
import { FeedList } from "@/components/feed/FeedList";
import { FeedDebugVisibility } from "@/components/feed/FeedDebugVisibility";
import { API_BASE, getFeed } from "@/lib/api";
import type { EventsResponse } from "@/lib/api";
import type { FeedItem } from "@/lib/types";

export const dynamic = "force-dynamic";

// PR summary: Home feed is now backed by /api/events. The unified tape currently shows only seeded demo events; production
// trades require backfill/dual-write from the legacy trade store.
function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

const feedParamKeys = ["symbol", "member", "chamber", "party", "trade_type", "role", "ownership", "min_amount", "recent_days"] as const;

type FeedParamKey = (typeof feedParamKeys)[number];

function buildEventsUrl(params: Record<string, string | number | boolean>, tape: string) {
  const url = new URL("/api/events", API_BASE);

  if (tape === "insider") {
    url.searchParams.set("event_type", "insider_trade");
  } else if (tape === "congress") {
    url.searchParams.set("event_type", "congress_trade");
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
  payload?: any;
}
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
    const documentUrl = asTrimmedString(payload.document_url) ?? event.url ?? null;

    return {
      id: event.id,
      kind: "congress_trade",
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
    };
  }

  if (event.event_type === "insider_trade") {
    const payload = parsePayload(event.payload);
    const direction = normalizeInsiderDirection(payload);
    if (!direction) return null;
    const symbol = asTrimmedString(event.ticker) ?? asTrimmedString(payload.symbol);
    const insiderName =
      asTrimmedString(payload.insider_name) ??
      asTrimmedString(payload?.raw?.reportingName) ??
      asTrimmedString(event.source) ??
      "Insider";
    const ownership = formatOwnershipLabel(payload.ownership) ?? formatOwnershipLabel(payload?.raw?.directOrIndirect);
    const transactionType = direction;
    const role = insiderRole(payload);
    const companyName = asTrimmedString(payload?.raw?.companyName);
    const companyNameDiffersFromTicker = companyName && symbol ? companyName.toUpperCase() !== symbol.toUpperCase() : Boolean(companyName);
    const securityName =
      (companyNameDiffersFromTicker ? companyName : null) ??
      symbol ??
      asTrimmedString(payload.security_name) ??
      event.headline ??
      event.summary ??
      "Insider Trade";
    const securityClass = asTrimmedString(payload?.raw?.securityName) ?? "Insider Trade";
    const price = null;
    const amountMin = asNumber((event as any).amount_min) ?? null;
    const amountMax = asNumber((event as any).amount_max) ?? null;
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
      insider: {
        name: insiderName,
        ownership,
        filing_date: filingDate,
        transaction_date: transactionDate,
        price,
        role,
      },
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


export default async function FeedPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
  }) {
  const sp = (await searchParams) ?? {};

  const tape = getParam(sp, "tape") || "all";
  const queryDebug = getParam(sp, "debug") === "1";
  const requestedPage = Number(getParam(sp, "page") || "1");
  const page = Number.isFinite(requestedPage) ? Math.max(1, Math.floor(requestedPage)) : 1;
  const requestedPageSize = Number(getParam(sp, "limit") || "50");
  const pageSize: 25 | 50 | 100 = [25, 50, 100].includes(requestedPageSize) ? (requestedPageSize as 25 | 50 | 100) : 50;
  const activeParams: Record<FeedParamKey, string> = {
    symbol: getParam(sp, "symbol"),
    member: getParam(sp, "member"),
    chamber: getParam(sp, "chamber"),
    party: getParam(sp, "party"),
    trade_type: getParam(sp, "trade_type"),
    role: getParam(sp, "role"),
    ownership: getParam(sp, "ownership"),
    min_amount: getParam(sp, "min_amount"),
    recent_days: getParam(sp, "recent_days"),
  };

  const requestParams = {
    ...activeParams,
    limit: pageSize,
    offset: (page - 1) * pageSize,
    include_total: "true",
  };

  const requestUrl = buildEventsUrl(requestParams, tape);
  const debug: {
    request_url: string;
    events_returned: number;
    fetch_error: string | null;
  } = {
    request_url: requestUrl,
    events_returned: 0,
    fetch_error: null,
  };

  let events: EventsResponse = { items: [], next_cursor: null, total: null };

  try {
    events = await getFeed({ ...requestParams, tape });
  } catch (err) {
    debug.fetch_error = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    console.error("[feed] fetch failed:", err);
  }

  debug.events_returned = events.items.length;

  const items = [...events.items]
    .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
    .map((event) => {
      const feedItem = mapEventToFeedItem(event);
      if (!feedItem) return null;
      const payload = parsePayload(event.payload);
      const tradeTicker = asTrimmedString(payload.symbol) ?? event.ticker ?? null;
      const tradeUrl = asTrimmedString(payload.document_url) ?? event.url ?? null;
      return {
        ...feedItem,
        title: event.headline ?? event.summary ?? event.event_type,
        ticker: tradeTicker,
        timestamp: event.ts,
        source: event.source ?? null,
        url: tradeUrl,
        payload,
      };
    })
    .filter(Boolean) as FeedItem[];

  const total = typeof events.total === "number" ? events.total : null;
  const totalPages = total ? Math.max(1, Math.ceil(total / pageSize)) : 1;

  return (
    <div className="space-y-8">
      <section className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
          <h1 className="text-4xl font-semibold text-white sm:text-5xl">Unified political & insider trade feed.</h1>
          <p className="max-w-2xl text-sm text-slate-400">
            One feed, one API: switch between Congress, Insider, or All and apply mode-aware filters for fast signal discovery.
          </p>
        </div>

        <FeedFilters events={events.items} resultsCount={items.length} />
      </section>

      <section className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-xl font-semibold text-white">Latest events</h2>
            <p className="text-sm text-slate-400">Showing {items.length} events on page {page}.</p>
          </div>
        </div>
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
        <div id="feed-top" />
        <FeedList items={items} page={page} pageSize={pageSize} total={total} totalPages={totalPages} />
      </section>
    </div>
  );
}
