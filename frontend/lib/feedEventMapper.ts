import { INSTITUTIONAL_ACTIVITY_EVENT_TYPES, type EventItem } from "@/lib/api";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import type { FeedItem } from "@/lib/types";

export type FeedSortBy = "filed_after" | "amount" | "pnl" | "signal";
export type FeedSortDir = "asc" | "desc";
export type CompanyNameMap = Record<string, string>;

const institutionalActivityEventTypes = new Set<string>(INSTITUTIONAL_ACTIVITY_EVENT_TYPES);

const governmentContractCompanyFallbacks: CompanyNameMap = {
  BA: "Boeing Co",
  LMT: "Lockheed Martin Corp",
  NVDA: "NVIDIA Corporation",
};

const badEventIdentityLabels = new Set([
  "congress_trade",
  "congress_treasury_trade",
  "congress_crypto_trade",
  "insider_trade",
  "institutional_buy",
  "institutional_accumulation",
  "institutional_distribution",
  "new_institutional_position",
  "major_holder_reduction",
  "major_holder_exit",
  "cluster_accumulation",
  "cluster_distribution",
  "smart_money_confirmation",
  "crowded_long",
  "contrarian_accumulation",
  "government_contract",
  "event",
  "security",
]);

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

export function parsePayload(payload: unknown): Record<string, any> {
  if (typeof payload === "string") {
    try {
      return JSON.parse(payload);
    } catch {
      return {};
    }
  }
  if (payload && typeof payload === "object") return payload as Record<string, any>;
  return {};
}

function safeIdentityText(...values: unknown[]): string | null {
  for (const value of values) {
    const text = asTrimmedString(value);
    if (text && !badEventIdentityLabels.has(text.toLowerCase())) return text;
  }
  return null;
}

function safeTickerText(...values: unknown[]): string | null {
  const text = safeIdentityText(...values);
  return text ? text.toUpperCase() : null;
}

function congressFallbackName(source?: string | null): string {
  const normalized = (source ?? "").trim().toLowerCase();
  if (normalized.includes("house")) return "House disclosure";
  if (normalized.includes("senate")) return "Senate disclosure";
  return "Congressional Trade";
}

function congressFallbackChamber(source?: string | null): string {
  const normalized = (source ?? "").trim().toLowerCase();
  if (normalized.includes("house")) return "house";
  if (normalized.includes("senate")) return "senate";
  return "congress";
}

function insiderRole(payload: Record<string, any>): string | null {
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

function normalizeInsiderDirection(payload: Record<string, any>): "Purchase" | "Sale" | null {
  const t = firstTrimmedString(
    payload.transaction_type,
    payload.transactionType,
    payload.trade_type,
    payload.tradeType,
    payload?.raw?.transactionType,
  )?.toUpperCase();
  if (t) {
    if (t.startsWith("S") || t.includes("SALE")) return "Sale";
    if (t.startsWith("P") || t.includes("PURCHASE")) return "Purchase";
    return null;
  }
  const ad = firstTrimmedString(payload.acquisitionOrDisposition, payload.acquisition_or_disposition, payload?.raw?.acquisitionOrDisposition)?.toUpperCase();
  if (ad === "A") return "Purchase";
  if (ad === "D") return "Sale";
  return null;
}

function formatOwnershipLabel(value: unknown): string | null {
  const raw = asTrimmedString(value);
  if (!raw) return null;
  const cleaned = raw.toUpperCase();
  if (cleaned === "D" || cleaned === "DIRECT") return "Direct";
  if (cleaned === "I" || cleaned === "INDIRECT") return "Indirect";
  return raw;
}

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

function companyNameForSymbol(symbol: string | null, payload: Record<string, any>, companyNames: CompanyNameMap): string {
  if (!symbol) {
    return firstTrimmedString(payload.company_name, payload.companyName, payload.issuer_name, payload.issuerName) ?? "Company unavailable";
  }
  const normalized = symbol.trim().toUpperCase();
  return firstTrimmedString(companyNames[normalized], payload.company_name, payload.companyName, payload.issuer_name, payload.issuerName) ?? normalized;
}

function institutionalDisplayName(value: unknown): string | null {
  const text = asTrimmedString(value);
  if (!text) return null;
  const normalized = text.toLowerCase();
  if (normalized === "institutional activity" || normalized === "institutional" || normalized === "institution" || normalized === "13f filing") return null;
  return text;
}

function institutionalTransactionLabel(eventType: string, payload: Record<string, any>, tradeType?: string | null): string {
  const cleanTradeType = asTrimmedString(tradeType);
  if (cleanTradeType && cleanTradeType.toLowerCase() !== "13f filing") return cleanTradeType;
  const normalized = eventType.toLowerCase();
  const direction = asTrimmedString(payload.direction)?.toLowerCase();
  const valueDelta = asNumber(payload.value_delta_usd);
  if (normalized === "new_institutional_position") return "New Position";
  if (normalized === "major_holder_exit") return "Reported Exit";
  if (normalized.includes("reduction") || normalized.includes("distribution") || direction === "bearish") return "Reported Reduction";
  if (normalized.includes("accumulation") || normalized === "institutional_buy" || direction === "bullish") return "Reported Increase";
  if (valueDelta !== null) {
    if (valueDelta < 0) return "Reported Reduction";
    if (valueDelta > 0) return "Reported Increase";
  }
  return "Reported Activity";
}

function feedItemAmountValue(item: FeedItem): number | null {
  return firstNumber(item.amount_range_max, item.amount_range_min);
}

function feedItemDateValue(item: FeedItem): number | null {
  const rawDate = item.report_date ?? item.trade_date ?? (item as any).timestamp ?? null;
  if (!rawDate) return null;
  const time = new Date(rawDate).getTime();
  return Number.isFinite(time) ? time : null;
}

function feedItemPnlValue(item: FeedItem): number | null {
  return firstNumber((item as any).gain_loss_percent, (item as any).gainLossPercent, (item as any).pnl_pct, (item as any).pnlPct, (item as any).pnl);
}

function feedItemSignalValue(item: FeedItem): number | null {
  return firstNumber((item as any).smart_score, (item as any).smartScore);
}

export function sortFeedItems(items: FeedItem[], sortBy: string, sortDir: string): FeedItem[] {
  const normalizedSort: FeedSortBy = sortBy === "amount" || sortBy === "pnl" || sortBy === "signal" || sortBy === "filed_after" ? sortBy : "filed_after";
  const direction: FeedSortDir = sortDir === "asc" ? "asc" : "desc";
  const multiplier = direction === "asc" ? 1 : -1;
  const valueFor = (item: FeedItem) => {
    if (normalizedSort === "amount") return feedItemAmountValue(item);
    if (normalizedSort === "pnl") return feedItemPnlValue(item);
    if (normalizedSort === "signal") return feedItemSignalValue(item);
    return feedItemDateValue(item);
  };
  return [...items].sort((left, right) => {
    const leftValue = valueFor(left);
    const rightValue = valueFor(right);
    if (leftValue === null && rightValue === null) return Number(right.id ?? 0) - Number(left.id ?? 0);
    if (leftValue === null) return 1;
    if (rightValue === null) return -1;
    if (leftValue === rightValue) return Number(right.id ?? 0) - Number(left.id ?? 0);
    return (leftValue - rightValue) * multiplier;
  });
}

export function redactPremiumFeedMetrics(items: FeedItem[], canViewPremiumMetrics: boolean): FeedItem[] {
  if (canViewPremiumMetrics) return items;
  return items.map((item) => {
    const pnlValue = feedItemPnlValue(item);
    const signalValue = feedItemSignalValue(item);
    const payload = parsePayload((item as any).payload);
    return {
      ...item,
      payload: {
        ...payload,
        pnl_pct: undefined,
        pnlPct: undefined,
        pnl: undefined,
        smart_score: undefined,
        smartScore: undefined,
      },
      pnl_pct: pnlValue === null ? null : pnlValue < 0 ? -0.1 : pnlValue > 0 ? 0.1 : 0,
      return_pct: null,
      smart_score: signalValue === null ? null : undefined,
    } as FeedItem;
  });
}

export function feedProfileSymbols(events: EventItem[]): string[] {
  return Array.from(
    new Set(
      events
        .filter((event) => event.event_type === "government_contract" || institutionalActivityEventTypes.has(event.event_type))
        .map((event) => {
          const payload = parsePayload(event.payload);
          return asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
        })
        .filter((symbol): symbol is string => Boolean(symbol))
        .map((symbol) => symbol.toUpperCase()),
    ),
  );
}

export function mapEventToFeedItem(event: EventItem, companyNames: CompanyNameMap = {}): FeedItem | null {
  if (event.event_type === "congress_trade" || event.event_type === "congress_treasury_trade" || event.event_type === "congress_crypto_trade") {
    const payload = parsePayload(event.payload);
    const memberPayload = payload.member ?? {};
    const memberBioguide = asTrimmedString(memberPayload.bioguide_id) ?? (typeof memberPayload.bioguide_id === "number" ? String(memberPayload.bioguide_id) : null) ?? event.source ?? "event";
    const symbol = safeTickerText(payload.symbol, payload.ticker, event.symbol, event.ticker);
    return {
      id: event.id,
      kind: event.event_type as FeedItem["kind"],
      member: {
        bioguide_id: memberBioguide,
        name: asTrimmedString(memberPayload.name) ?? asTrimmedString(payload.member_name) ?? congressFallbackName(event.source),
        chamber: asTrimmedString(memberPayload.chamber) ?? congressFallbackChamber(event.source),
        party: asTrimmedString(memberPayload.party),
        state: asTrimmedString(memberPayload.state),
      },
      security: {
        symbol,
        name:
          safeIdentityText(
            payload.company_name,
            payload.companyName,
            payload.issuer_name,
            payload.issuerName,
            payload.security_name,
            payload.securityName,
            payload.security_description,
            payload.securityDescription,
            payload.description,
            event.headline,
            event.summary,
          ) ?? "Unresolved security",
        asset_class: asTrimmedString(payload.asset_class) ?? "Security",
        sector: asTrimmedString(payload.sector),
      },
      transaction_type: asTrimmedString(payload.transaction_type) ?? event.event_type,
      owner_type: asTrimmedString(payload.owner_type) ?? "Unknown",
      trade_date: asTrimmedString(payload.trade_date) ?? event.ts ?? null,
      report_date: asTrimmedString(payload.report_date) ?? event.ts ?? null,
      amount_range_min: asNumber(payload.amount_range_min),
      amount_range_max: asNumber(payload.amount_range_max),
      estimated_price: typeof event.estimated_price === "number" ? event.estimated_price : asNumber(payload.estimated_price),
      current_price: typeof event.current_price === "number" ? event.current_price : asNumber(payload.current_price),
      pnl_pct: event.gain_loss_percent ?? event.pnl_pct ?? asNumber(payload.pnl_pct),
      gain_loss_percent: event.gain_loss_percent ?? asNumber(payload.gain_loss_percent),
      gain_loss_amount: event.gain_loss_amount ?? null,
      gain_loss_status: (event.gain_loss_status as FeedItem["gain_loss_status"]) ?? null,
      gain_loss_as_of: event.gain_loss_as_of ?? null,
      pnl_source: (event.pnl_source as FeedItem["pnl_source"]) ?? null,
      quote_is_stale: typeof (event as any).quote_is_stale === "boolean" ? (event as any).quote_is_stale : null,
      quote_asof_ts: typeof (event as any).quote_asof_ts === "string" ? (event as any).quote_asof_ts : null,
      member_net_30d: event.member_net_30d ?? asNumber(payload.member_net_30d),
      symbol_net_30d: event.symbol_net_30d ?? asNumber(payload.symbol_net_30d),
      confirmation_30d: event.confirmation_30d ?? null,
    };
  }

  if (event.event_type === "insider_trade") {
    const payload = parsePayload(event.payload);
    const direction = normalizeInsiderDirection(payload);
    if (!direction) return null;
    const display = resolveInsiderActivityDisplay(event as unknown as Record<string, unknown>);
    const symbol = asTrimmedString(event.ticker) ?? display.symbol ?? asTrimmedString(payload.symbol);
    const insiderName = display.insiderName ?? asTrimmedString(event.source) ?? "Insider";
    const companyName = display.companyName !== "-" ? display.companyName : asTrimmedString(event.headline) ?? asTrimmedString(event.summary);
    const companyNameDiffersFromTicker = companyName && symbol ? companyName.toUpperCase() !== symbol.toUpperCase() : Boolean(companyName);
    const filingDate = asTrimmedString(payload.filing_date) ?? event.ts ?? null;
    const transactionDate = asTrimmedString(payload.transaction_date) ?? asTrimmedString(payload?.raw?.transactionDate) ?? null;
    const ownership = formatOwnershipLabel(payload.ownership) ?? formatOwnershipLabel(payload?.raw?.directOrIndirect);
    return {
      id: event.id,
      kind: "insider_trade",
      member: {
        bioguide_id: `insider-${symbol ?? event.id}`,
        name: insiderName,
        chamber: "insider",
      },
      security: {
        symbol,
        name: (companyNameDiffersFromTicker ? companyName : null) ?? companyName ?? asTrimmedString(payload.security_name) ?? symbol ?? event.headline ?? event.summary ?? "Insider Trade",
        asset_class: asTrimmedString(payload?.raw?.securityName) ?? "Insider Trade",
      },
      transaction_type: direction,
      owner_type: ownership ?? "Insider",
      trade_date: transactionDate,
      report_date: filingDate,
      amount_range_min: asNumber(event.amount_min),
      amount_range_max: asNumber(event.amount_max),
      current_price: event.current_price ?? asNumber(payload.current_price),
      pnl_pct: event.gain_loss_percent ?? event.pnl_pct ?? asNumber(payload.pnl_pct),
      gain_loss_percent: event.gain_loss_percent ?? asNumber(payload.gain_loss_percent),
      gain_loss_amount: event.gain_loss_amount ?? null,
      gain_loss_status: (event.gain_loss_status as FeedItem["gain_loss_status"]) ?? null,
      gain_loss_as_of: event.gain_loss_as_of ?? null,
      pnl_source: (event.pnl_source as FeedItem["pnl_source"]) ?? null,
      quote_is_stale: typeof (event as any).quote_is_stale === "boolean" ? (event as any).quote_is_stale : null,
      quote_asof_ts: typeof (event as any).quote_asof_ts === "string" ? (event as any).quote_asof_ts : null,
      member_net_30d: event.member_net_30d ?? asNumber(payload.member_net_30d),
      symbol_net_30d: event.symbol_net_30d ?? asNumber(payload.symbol_net_30d),
      confirmation_30d: event.confirmation_30d ?? null,
      insider: {
        name: insiderName,
        ownership,
        filing_date: filingDate,
        transaction_date: transactionDate,
        price: display.price ?? asNumber(payload.price),
        display_price: display.price,
        reported_price: display.reportedPrice,
        reported_price_currency: display.reportedPriceCurrency,
        role: display.role ?? insiderRole(payload),
        reporting_cik: display.reportingCik ?? asTrimmedString(payload.reporting_cik) ?? asTrimmedString(payload?.raw?.reportingCik) ?? null,
      },
    };
  }

  if (institutionalActivityEventTypes.has(event.event_type)) {
    const payload = parsePayload(event.payload);
    const symbol = asTrimmedString(event.ticker) ?? asTrimmedString(payload.symbol);
    const amountMax = asNumber(event.amount_max) ?? asNumber(payload.reported_value_usd) ?? asNumber(payload.market_value);
    const reportPeriod = asTrimmedString(payload.report_period) ?? (payload.report_quarter && payload.report_year ? `Q${payload.report_quarter} ${payload.report_year}` : null);
    return {
      id: event.id,
      kind: event.event_type as FeedItem["kind"],
      member: {
        bioguide_id: asTrimmedString(payload.institution_cik) ?? asTrimmedString(event.member_bioguide_id) ?? `institution-${event.id}`,
        name:
          institutionalDisplayName(payload.holder_name) ??
          institutionalDisplayName(payload.institution_name) ??
          institutionalDisplayName(event.member_name) ??
          institutionalDisplayName(payload?.raw?.holder) ??
          institutionalDisplayName(payload?.raw?.institutionName) ??
          "Institution unavailable",
        chamber: "institutional",
      },
      security: {
        symbol,
        name: companyNameForSymbol(symbol, payload, companyNames),
        asset_class: "13F filing",
      },
      transaction_type: institutionalTransactionLabel(event.event_type, payload, event.trade_type),
      owner_type: "13F filing",
      payload,
      trade_date: null,
      report_date: asTrimmedString(payload.filing_date) ?? event.ts ?? null,
      amount_range_min: asNumber(event.amount_min) ?? amountMax,
      amount_range_max: amountMax,
      institutional: {
        report_period: reportPeriod,
        value_delta_usd: asNumber(payload.value_delta_usd),
      },
    };
  }

  if (event.event_type === "government_contract") {
    const payload = parsePayload(event.payload);
    const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
    const isFundingAction = payload.event_subtype === "funding_action" || Boolean(payload.modification_number) || Boolean(payload.action_date);
    const value = firstNumber(payload.obligated_amount, payload.transaction_obligated_amount, payload.award_amount, payload.contract_value, payload.amount, event.amount_max, event.amount_min);
    return {
      id: event.id,
      kind: "government_contract",
      member: {
        bioguide_id: `government-contract-${event.id}`,
        name: firstTrimmedString(payload.awarding_agency, payload.department, payload.agency, payload.funding_agency) ?? "Government Contract",
        chamber: "government_contract",
      },
      security: {
        symbol,
        name: companyNameForGovernmentContract(symbol, payload, companyNames),
        asset_class: "Government Contract",
      },
      contract_description: firstTrimmedString(payload.title, payload.description, payload.award_description, payload.contract_description, event.headline, event.summary) ?? "Government contract award",
      transaction_type: isFundingAction ? "Government Contract Funding" : "Government Contract",
      owner_type: firstTrimmedString(payload.awarding_agency, payload.department, payload.agency, payload.funding_agency) ?? "Government Contract",
      trade_date: null,
      report_date: firstTrimmedString(payload.report_date, payload.action_date, payload.created_at) ?? event.ts ?? null,
      amount_range_min: value,
      amount_range_max: value,
      payload,
      url: firstTrimmedString(payload.source_url, payload.url),
    };
  }

  return {
    id: event.id,
    kind: "event",
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

export function eventToRenderedFeedItem(event: EventItem, companyNames: CompanyNameMap = {}): FeedItem | null {
  const feedItem = mapEventToFeedItem(event, companyNames);
  if (!feedItem) return null;
  const payload = parsePayload(event.payload);
  const tradeTicker =
    feedItem.kind === "congress_treasury_trade" || feedItem.kind === "congress_crypto_trade"
      ? null
      : safeTickerText(payload.symbol, payload.ticker, event.symbol, event.ticker);
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
    gain_loss_percent: (event as any).gain_loss_percent ?? null,
    gain_loss_amount: (event as any).gain_loss_amount ?? null,
    gain_loss_status: (event as any).gain_loss_status ?? null,
    gain_loss_as_of: (event as any).gain_loss_as_of ?? null,
    pnl_source: (event as any).pnl_source ?? null,
    quote_is_stale: (event as any).quote_is_stale ?? null,
    quote_asof_ts: (event as any).quote_asof_ts ?? null,
  } as FeedItem;
}
