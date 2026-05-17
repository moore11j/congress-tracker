import type { EventItem } from "@/lib/api";
import type { MonitoringAlert } from "@/lib/types";

const PROVIDER_LABELS = new Set(["api", "data source", "data_source", "event source", "fmp", "provider", "sec", "source", "vendor"]);

type MonitoringTitleEvent = Pick<EventItem, "event_type" | "headline" | "member_name" | "source" | "summary" | "symbol" | "ticker" | "trade_type"> & {
  transaction_type?: string | null;
};

type UnknownRecord = Record<string, unknown>;

function text(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned || null;
}

function record(value: unknown): UnknownRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as UnknownRecord) : {};
}

function looksLikeProviderLabel(value: string | null | undefined): boolean {
  if (!value) return false;
  const normalized = value.trim().toLowerCase().replace(/[-_]/g, " ");
  return PROVIDER_LABELS.has(normalized);
}

function firstValidName(...values: unknown[]): string | null {
  for (const value of values) {
    const cleaned = text(value);
    if (cleaned && !looksLikeProviderLabel(cleaned)) return cleaned;
  }
  return null;
}

export function resolveInsiderName(payload: UnknownRecord, eventMemberName?: string | null): string | null {
  const raw = record(payload.raw);
  const insider = record(payload.insider);
  const reportingOwner = record(payload.reporting_owner);
  const reportingOwnerCamel = record(payload.reportingOwner);
  const owner = record(payload.owner);

  return firstValidName(
    payload.reporting_owner_name,
    payload.reportingOwnerName,
    payload.owner_name,
    payload.ownerName,
    payload.insider_name,
    payload.insiderName,
    payload.person_name,
    payload.personName,
    reportingOwner.name,
    reportingOwner.owner_name,
    reportingOwnerCamel.name,
    reportingOwnerCamel.ownerName,
    owner.name,
    owner.owner_name,
    insider.name,
    raw.reporting_owner_name,
    raw.reportingOwnerName,
    raw.owner_name,
    raw.ownerName,
    raw.insider_name,
    raw.insiderName,
    raw.person_name,
    raw.personName,
    eventMemberName,
  );
}

export function normalizeMonitoringTradeSide(...values: unknown[]): "purchase" | "sale" | null {
  for (const value of values) {
    const cleaned = text(value);
    if (!cleaned) continue;
    const normalized = cleaned.toLowerCase();
    if (
      normalized === "p" ||
      normalized === "purchase" ||
      normalized.startsWith("p-") ||
      normalized.includes("purchase") ||
      normalized === "buy" ||
      normalized === "bought" ||
      normalized === "acquired" ||
      normalized === "acquisition" ||
      normalized === "a" ||
      normalized.includes("acquir")
    ) {
      return "purchase";
    }
    if (
      normalized === "s" ||
      normalized === "sale" ||
      normalized.startsWith("s-") ||
      normalized.includes("sale") ||
      normalized === "sell" ||
      normalized === "sold" ||
      normalized === "disposition" ||
      normalized === "dispose" ||
      normalized === "disposed" ||
      normalized === "d" ||
      normalized.includes("disposition")
    ) {
      return "sale";
    }
  }
  return null;
}

export function buildMonitoringEventTitle(event: MonitoringTitleEvent, payloadInput?: unknown): string {
  const payload = record(payloadInput);
  const symbol = text(event.symbol) ?? text(event.ticker) ?? text(payload.symbol) ?? text(payload.ticker);
  const formattedSymbol = symbol ? symbol.toUpperCase() : null;

  if (event.event_type === "insider_trade") {
    const raw = record(payload.raw);
    const insiderName = resolveInsiderName(payload, event.member_name) ?? "Insider";
    const side =
      normalizeMonitoringTradeSide(
        event.trade_type,
        event.transaction_type,
        payload.trade_type,
        payload.tradeType,
        payload.transaction_type,
        payload.transactionType,
        payload.side,
        raw.transaction_type,
        raw.transactionType,
        raw.side,
      ) ?? "trade";
    return [formattedSymbol, insiderName, side].filter(Boolean).join(" - ");
  }

  const member = record(payload.member);
  const name =
    text(event.member_name) ??
    text(payload.member_name) ??
    text(member.name) ??
    text(payload.insider_name) ??
    text(payload.insiderName) ??
    text(payload.reporting_owner_name) ??
    text(payload.reportingOwnerName) ??
    text(event.source) ??
    text(event.event_type);
  const tradeType = text(event.trade_type) ?? text(event.transaction_type) ?? text(payload.transaction_type) ?? text(payload.transactionType);
  return [formattedSymbol, name, tradeType].filter(Boolean).join(" - ") || event.headline || event.summary || event.event_type;
}

export function displayMonitoringAlertTitle(alert: MonitoringAlert): string {
  if (alert.alert_type !== "insider_trade") return alert.title;
  const payload = record(alert.payload);
  const eventPayload = record(payload.event);
  const rebuilt = buildMonitoringEventTitle(
    {
      event_type: alert.alert_type,
      member_name: text(eventPayload.member_name),
      source: null,
      symbol: text(alert.symbol) ?? text(eventPayload.symbol) ?? text(eventPayload.ticker),
      ticker: text(eventPayload.ticker),
      trade_type: text(eventPayload.trade_type),
      transaction_type: text(eventPayload.transaction_type) ?? text(eventPayload.transactionType),
      headline: null,
      summary: null,
    },
    eventPayload,
  );
  return rebuilt || alert.title;
}
