import { getInsiderDisplayName } from "@/lib/insider";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  formatReportedInsiderPrice,
  resolveInsiderDisplayPrice,
  resolveInsiderDisplayValue,
  resolveInsiderReportedPrice,
} from "@/lib/insiderTradeDisplay";

export function readTradeText(record: Record<string, unknown>, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

export function readTradeNumber(record: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string") {
      const parsed = Number(value.replace(/[$,% ,]/g, "").trim());
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return null;
}

function objectRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function firstNestedText(record: Record<string, unknown>, ...keys: string[]): string | null {
  const payload = objectRecord(record.payload);
  const raw = objectRecord(payload.raw);
  const insider = objectRecord(payload.insider);
  const candidates = [record, payload, insider, raw];
  for (const candidate of candidates) {
    const value = readTradeText(candidate, ...keys);
    if (value) return value;
  }
  return null;
}

function isGenericSecurityName(value: string | null): boolean {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return true;
  const exact = new Set([
    "common stock",
    "ordinary shares",
    "ordinary share",
    "class a",
    "class b",
    "preferred stock",
    "restricted stock",
    "stock option",
    "stock options",
  ]);
  if (exact.has(normalized) || normalized.includes("common stock")) return true;
  return [
    "right to buy",
    "right to purchase",
    "stock option",
    "restricted stock unit",
  ].some((fragment) => normalized.includes(fragment));
}

function shouldShowReportedPrice(
  record: Record<string, unknown>,
  reportedPrice: number | null,
  displayPrice: number | null,
  reportedCurrency: string | null,
): boolean {
  if (reportedPrice === null) return false;
  const payload = objectRecord(record.payload);
  const normalization = objectRecord(
    record.price_normalization ??
      record.priceNormalization ??
      payload.price_normalization ??
      payload.priceNormalization,
  );
  const status = readTradeText(normalization, "status");
  const reportedBasis = readTradeText(record, "reported_share_basis", "reportedShareBasis") ??
    readTradeText(payload, "reported_share_basis", "reportedShareBasis") ??
    readTradeText(normalization, "raw_share_basis");
  const displayBasis = readTradeText(record, "display_share_basis", "displayShareBasis") ??
    readTradeText(payload, "display_share_basis", "displayShareBasis") ??
    readTradeText(normalization, "display_share_basis");

  if (status === "normalized") return true;
  if (reportedCurrency && reportedCurrency.trim().toUpperCase() !== "USD") return true;
  if (reportedBasis && displayBasis && reportedBasis !== displayBasis) return true;
  return displayPrice !== null && reportedPrice !== displayPrice;
}

export function displayTickerSymbol(symbol?: string | null): string {
  if (!symbol) return "-";
  const trimmed = symbol.trim();
  if (!trimmed) return "-";
  if (!trimmed.includes(":")) return trimmed;
  return trimmed.split(":", 2)[1]?.trim() || trimmed;
}

export function resolveInsiderActivityDisplay(record: Record<string, unknown>) {
  const payload = objectRecord(record.payload);
  const displayInput = {
    ...record,
    amount_range_min: readTradeNumber(record, "amount_range_min", "amount_min", "amountMin"),
    amount_range_max: readTradeNumber(record, "amount_range_max", "amount_max", "amountMax"),
    amount_min: readTradeNumber(record, "amount_min", "amountMin"),
    amount_max: readTradeNumber(record, "amount_max", "amountMax"),
    estimated_price: readTradeNumber(record, "display_price", "displayPrice", "estimated_price", "price"),
    payload: Object.keys(payload).length ? payload : record,
  };

  const symbol = firstNestedText(record, "symbol", "ticker");
  const companyName =
    firstNestedText(record, "company_name", "companyName") ??
    (isGenericSecurityName(firstNestedText(record, "security_name", "securityName"))
      ? null
      : firstNestedText(record, "security_name", "securityName")) ??
    symbol ??
    "-";
  const securityName = firstNestedText(record, "security_name", "securityName");
  const insiderName =
    getInsiderDisplayName(
      firstNestedText(record, "insider_name", "insiderName", "reporting_name", "reportingName"),
      firstNestedText(record, "member_name"),
      firstNestedText(record, "who"),
    ) ?? "Unknown Insider";
  const reportingCik = firstNestedText(record, "reporting_cik", "reportingCik", "reportingCIK");
  const role = firstNestedText(record, "role", "typeOfOwner", "officerTitle", "insiderRole", "position");
  const transactionDate = firstNestedText(record, "transaction_date", "transactionDate", "trade_date", "tradeDate");
  const filingDate = firstNestedText(record, "filing_date", "filingDate", "report_date", "reportDate");
  const tradeType = firstNestedText(record, "trade_type", "tradeType", "transaction_type", "transactionType");
  const price = resolveInsiderDisplayPrice(displayInput);
  const reported = resolveInsiderReportedPrice(displayInput);
  const reportedLabel = formatReportedInsiderPrice(reported.price, reported.currency);
  const showReportedLabel = shouldShowReportedPrice(record, reported.price, price, reported.currency);
  const tradeValue =
    readTradeNumber(record, "trade_value", "tradeValue") ??
    resolveInsiderDisplayValue(displayInput) ??
    readTradeNumber(record, "amount_max", "amount_min", "amountMax", "amountMin");
  const pnl = readTradeNumber(record, "pnl_pct", "pnlPct", "pnl");
  const signal = resolveSmartSignalValue(record);

  return {
    symbol,
    displaySymbol: displayTickerSymbol(symbol),
    companyName,
    securityName,
    insiderName,
    reportingCik,
    role,
    transactionDate,
    filingDate,
    tradeType,
    price,
    reportedPrice: reported.price,
    reportedPriceCurrency: reported.currency,
    reportedLabel: reportedLabel && showReportedLabel ? reportedLabel : null,
    tradeValue,
    pnl,
    signal,
    hasSignal: signal.score !== null || Boolean(signal.band),
  };
}
