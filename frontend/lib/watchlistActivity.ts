import type { EventItem, SignalItem } from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import type { FeedItem } from "@/lib/types";

export type ActivityMode = "all" | "congress" | "insider" | "government_contracts" | "signals";

export type WatchlistActivityState = {
  mode: ActivityMode;
  recentDays: string;
  limit: number;
  onlyNew: boolean;
  newSince: string;
};

export function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

export function parseMode(value: string): ActivityMode {
  return value === "congress" || value === "insider" || value === "government_contracts" || value === "signals" ? value : "all";
}

export function recentDaysToSince(value: string): string | undefined {
  const days = Number(value);
  if (!Number.isFinite(days) || days < 1) return undefined;
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}

function parseDateValue(value?: string | null): number | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function resolveWatchlistEventSince({
  recentDays,
  onlyNew,
  newSince,
}: Pick<WatchlistActivityState, "recentDays" | "onlyNew" | "newSince">): string | undefined {
  const windowSince = recentDaysToSince(recentDays);
  if (!onlyNew) return windowSince;

  const unreadSinceMs = parseDateValue(newSince);
  const windowSinceMs = parseDateValue(windowSince);

  if (unreadSinceMs === null) return windowSince;
  if (windowSinceMs === null) return newSince;
  return unreadSinceMs >= windowSinceMs ? newSince : windowSince;
}

function payloadText(payload: any, keys: string[]): string | null {
  for (const key of keys) {
    const value = payload?.[key] ?? payload?.raw?.[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

export function eventToFeedItem(event: EventItem): FeedItem {
  const payload = event.payload ?? {};
  const isInsider = event.event_type === "insider_trade";
  const symbol = event.symbol ?? event.ticker ?? payloadText(payload, ["symbol", "ticker"]);
  const insiderName = payloadText(payload, ["insider_name", "insiderName"]) ?? event.member_name ?? "Unknown insider";
  const securityName =
    payloadText(payload, ["company_name", "companyName", "security_name", "securityName"]) ??
    symbol ??
    "Unknown";

  return {
    id: event.id,
    kind: event.event_type as FeedItem["kind"],
    member: {
      bioguide_id: event.member_bioguide_id ?? "",
      name: isInsider ? insiderName : event.member_name ?? "Unknown",
      chamber: event.chamber ?? "",
      party: event.party ?? null,
      state: null,
    },
    security: {
      symbol,
      name: formatCompanyName(securityName) || securityName,
      asset_class: payloadText(payload, ["asset_class", "securityName"]) ?? "stock",
      sector: payloadText(payload, ["sector"]),
    },
    transaction_type: event.trade_type ?? "",
    owner_type: payloadText(payload, ["owner_type", "ownership"]) ?? (isInsider ? "insider" : ""),
    trade_date: payloadText(payload, ["transaction_date", "transactionDate", "trade_date", "tradeDate"]),
    report_date: payloadText(payload, ["filing_date", "filingDate", "report_date", "reportDate"]) ?? event.ts,
    amount_range_min: event.amount_min ?? null,
    amount_range_max: event.amount_max ?? null,
    estimated_trade_value: event.estimated_trade_value ?? null,
    estimated_price: event.estimated_price ?? null,
    estimated_shares: event.estimated_shares ?? null,
    current_price: event.current_price ?? null,
    display_price: event.display_price ?? null,
    reported_price: event.reported_price ?? null,
    reported_price_currency: event.reported_price_currency ?? null,
    pnl_pct: event.gain_loss_percent ?? event.pnl_pct ?? null,
    gain_loss_percent: event.gain_loss_percent ?? null,
    gain_loss_amount: event.gain_loss_amount ?? null,
    gain_loss_status: (event.gain_loss_status as FeedItem["gain_loss_status"]) ?? null,
    gain_loss_as_of: event.gain_loss_as_of ?? null,
    pnl_source: (event.pnl_source as FeedItem["pnl_source"]) ?? null,
    outcome_status: event.outcome_status ?? null,
    outcome_skip_reason: event.outcome_skip_reason ?? null,
    outcome_methodology: event.outcome_methodology ?? null,
    outcome_error: event.outcome_error ?? null,
    price_basis: event.price_basis ?? null,
    smart_score: event.smart_score ?? null,
    smart_band: event.smart_band ?? null,
    member_net_30d: event.member_net_30d ?? null,
    symbol_net_30d: event.symbol_net_30d ?? null,
    confirmation_30d: event.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name: insiderName,
          ownership: payloadText(payload, ["owner_type", "ownership"]),
          filing_date: payloadText(payload, ["filing_date", "filingDate"]),
          transaction_date: payloadText(payload, ["transaction_date", "transactionDate"]),
          price: typeof payload.price === "number" ? payload.price : null,
          display_price: typeof payload.display_price === "number" ? payload.display_price : null,
          reported_price: typeof payload.reported_price === "number" ? payload.reported_price : null,
          reported_price_currency: payloadText(payload, ["reported_price_currency", "reportedPriceCurrency"]),
          role: payloadText(payload, ["role", "position", "officerTitle", "typeOfOwner"]),
          reporting_cik: payloadText(payload, ["reporting_cik", "reportingCik"]),
        }
      : undefined,
  };
}

export function signalToFeedItem(signal: SignalItem): FeedItem {
  const isInsider = signal.kind === "insider";
  const eventType = isInsider ? "insider_trade" : "congress_trade";
  const name = signal.who ?? (isInsider ? "Unknown insider" : "Unknown member");

  return {
    id: signal.event_id,
    kind: eventType,
    member: {
      bioguide_id: signal.member_bioguide_id ?? "",
      name,
      chamber: signal.chamber ?? "",
      party: signal.party ?? null,
      state: null,
    },
    security: {
      symbol: signal.symbol ?? null,
      name: signal.symbol ?? "Unknown",
      asset_class: "stock",
      sector: null,
    },
    transaction_type: signal.trade_type ?? "",
    owner_type: isInsider ? "insider" : "",
    trade_date: signal.ts,
    report_date: signal.ts,
    amount_range_min: signal.amount_min ?? null,
    amount_range_max: signal.amount_max ?? null,
    estimated_price: signal.estimated_price ?? signal.price ?? null,
    current_price: signal.current_price ?? null,
    pnl_pct: signal.gain_loss_percent ?? signal.pnl_pct ?? signal.pnlPct ?? null,
    gain_loss_percent: signal.gain_loss_percent ?? null,
    gain_loss_amount: signal.gain_loss_amount ?? null,
    gain_loss_status: (signal.gain_loss_status as FeedItem["gain_loss_status"]) ?? null,
    gain_loss_as_of: signal.gain_loss_as_of ?? null,
    pnl_source: (signal.pnl_source as FeedItem["pnl_source"]) ?? null,
    outcome_status: signal.outcome_status ?? null,
    outcome_skip_reason: signal.outcome_skip_reason ?? null,
    outcome_methodology: signal.outcome_methodology ?? null,
    outcome_error: signal.outcome_error ?? null,
    price_basis: signal.price_basis ?? null,
    smart_score: signal.smart_score ?? null,
    smart_band: signal.smart_band ?? null,
    confirmation_30d: signal.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name,
          role: signal.position ?? null,
          reporting_cik: signal.reporting_cik ?? null,
        }
      : undefined,
  };
}
