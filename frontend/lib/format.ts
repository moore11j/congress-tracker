import type { BadgeTone } from "@/components/Badge";

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

export function formatCurrency(value: number | null) {
  if (value === null || Number.isNaN(value)) return "—";
  return currencyFormatter.format(value);
}

export function formatCurrencyRange(min: number | null, max: number | null) {
  return `${formatCurrency(min)} – ${formatCurrency(max)}`;
}

export function formatDateShort(iso: string | null) {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
}

export function formatSymbol(symbol?: string | null) {
  if (!symbol) return "—";
  const trimmed = symbol.trim().toUpperCase();
  return trimmed.length ? trimmed : "—";
}

export function formatTransactionLabel(value?: string | null) {
  const cleaned = (value ?? "").toLowerCase();
  if (!cleaned) return "—";
  if (cleaned === "purchase") return "Purchase";
  if (cleaned === "sale") return "Sale";
  if (cleaned === "exchange") return "Exchange";
  return cleaned.replace(/_/g, " ").replace(/\b\w/g, (s) => s.toUpperCase());
}

export function transactionTone(value?: string | null): BadgeTone {
  const cleaned = (value ?? "").toLowerCase();
  if (cleaned === "sale") return "neg";
  if (cleaned === "purchase") return "pos";
  return "neutral";
}

export function chamberBadge(chamber?: string | null): { label: string; tone: BadgeTone } {
  const cleaned = (chamber ?? "").toLowerCase();
  if (cleaned === "house") return { label: "House", tone: "house" };
  if (cleaned === "senate") return { label: "Senate", tone: "senate" };
  return { label: "—", tone: "neutral" };
}

export function partyBadge(party?: string | null): { label: string; tone: BadgeTone } {
  const cleaned = (party ?? "").trim().toLowerCase();
  if (!cleaned) return { label: "—", tone: "neutral" };
  if (cleaned.startsWith("d")) return { label: "D", tone: "dem" };
  if (cleaned.startsWith("r")) return { label: "R", tone: "rep" };
  if (cleaned.includes("ind")) return { label: "I", tone: "ind" };
  return { label: cleaned.slice(0, 6).toUpperCase(), tone: "neutral" };
}

export function memberTag(party?: string | null, state?: string | null) {
  const p = partyBadge(party).label;
  const s = state?.trim().toUpperCase();
  if (p !== "—" && s) return `${p}-${s}`;
  if (s) return s;
  if (p !== "—") return p;
  return "Unknown";
}
