import Link from "next/link";
import type { FeedItem } from "@/lib/types";
import { Badge } from "@/components/Badge";
import {
  chamberBadge,
  formatCurrency,
  formatCurrencyRange,
  formatDateShort,
  formatSymbol,
  formatTransactionLabel,
  memberTag,
  partyBadge,
  transactionTone,
} from "@/lib/format";

type FeedCardInsiderItem = FeedItem & {
  trade_type?: string | null;
  payload?: {
    transaction_type?: string | null;
    shares?: number | string | null;
    price?: number | string | null;
    raw?: {
      transactionType?: string | null;
      securitiesTransacted?: number | string | null;
      price?: number | string | null;
      typeOfOwner?: string | null;
      securityName?: string | null;
    };
  };
  insider?: FeedItem["insider"] & {
    transaction_type?: string | null;
    shares?: number | string | null;
  };
};

function parseNum(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const cleaned = v.replace(/,/g, "").trim();
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function extractRole(rawTypeOfOwner: string | undefined): string | null {
  if (!rawTypeOfOwner) return null;
  const value = rawTypeOfOwner.toUpperCase();

  if (value.includes("CEO")) return "CEO";
  if (value.includes("CFO")) return "CFO";
  if (value.includes("COO")) return "COO";
  if (value.includes("PRESIDENT")) return "President";
  if (value.includes("VP")) return "VP";
  if (value.includes("DIRECTOR")) return "Director";
  if (value.includes("OFFICER")) return "Officer";
  return null;
}

function normalizeSecurityClass(securityName: string | undefined): string | null {
  if (!securityName) return null;
  const trimmed = securityName.trim();
  if (!trimmed) return null;
  const value = trimmed.toLowerCase();

  if (value.includes("common")) return "Common";
  if (value.includes("preferred")) return "Preferred";
  if (value.includes("unit")) return "Units";
  if (value.includes("bond") || value.includes("note")) return "Debt";
  if (value.includes("option")) return "Option";

  return trimmed.length <= 20 ? trimmed : null;
}

function formatMoney(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function computeTotalValue(shares?: number | null, price?: number | null): number | null {
  if (!shares || !price || shares <= 0 || price <= 0) return null;
  return shares * price;
}

function getInsiderKind(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;
  const raw =
    insiderItem.trade_type ??
    insiderItem.insider?.transaction_type ??
    insiderItem.payload?.raw?.transactionType ??
    item.transaction_type ??
    "";
  const t = raw.toUpperCase();

  if (t.startsWith("P-") || t.startsWith("P") || t.includes("PURCHASE")) return "purchase";
  if (t.startsWith("S-") || t.startsWith("S") || t.includes("SALE")) return "sale";
  return null;
}

function getInsiderValue(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;

  const shares = parseNum(insiderItem.payload?.shares ?? insiderItem.insider?.shares ?? insiderItem.payload?.raw?.securitiesTransacted);
  const price = parseNum(insiderItem.payload?.price ?? insiderItem.insider?.price ?? insiderItem.payload?.raw?.price ?? item.insider?.price);
  const total = computeTotalValue(shares, price);

  return { total, shares, price };
}

export function FeedCard({ item }: { item: FeedItem }) {
  if (!item) return null;

  const isCongress = item.kind !== "insider_trade";
  const isInsider = item.kind === "insider_trade";
  const chamber = chamberBadge(item.member?.chamber ?? "—");
  const party = partyBadge(item.member?.party ?? null);
  const tag = memberTag(item.member?.party ?? null, item.member?.state ?? null);
  const insiderKind = isInsider ? getInsiderKind(item) : null;
  const insiderValue = isInsider ? getInsiderValue(item) : null;

  const insiderItem = item as FeedCardInsiderItem;
  const securityClass = isInsider ? normalizeSecurityClass(insiderItem.payload?.raw?.securityName ?? item.security?.name ?? undefined) : null;
  const insiderRole = isInsider
    ? extractRole(insiderItem.payload?.raw?.typeOfOwner ?? item.insider?.role ?? undefined) ?? "Insider"
    : null;

  return (
    <div className="rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-4">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              {isInsider ? (
                <span className="text-lg font-semibold text-white">{item.insider?.name ?? item.member?.name ?? "—"}</span>
              ) : (
                <Link href={`/member/${item.member?.bioguide_id ?? "event"}`} className="text-lg font-semibold text-white hover:text-emerald-200">
                  {item.member?.name ?? "—"}
                </Link>
              )}
              {isInsider ? <Badge tone="dem">{insiderRole}</Badge> : <Badge tone={party.tone}>{tag}</Badge>}
              {isCongress ? <Badge tone={chamber.tone}>{chamber.label}</Badge> : null}
            </div>
            <div className="flex flex-wrap items-center gap-3 text-sm text-slate-300">
              <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Security</span>
              {item.security?.symbol ? (
                <Link
                  href={`/ticker/${formatSymbol(item.security.symbol ?? "—")}`}
                  className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-emerald-100"
                >
                  {formatSymbol(item.security.symbol ?? "—")}
                </Link>
              ) : (
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-slate-200">
                  —
                </span>
              )}
              {isInsider ? (
                <span className="text-slate-200">{insiderItem.payload?.raw?.securityName ?? item.security?.name ?? "—"}</span>
              ) : (
                <span className="text-slate-200">{item.security?.name ?? "—"}</span>
              )}
              {(isInsider ? Boolean(securityClass) : true) ? <span className="text-slate-500">•</span> : null}
              <span className="text-slate-400">{isInsider ? (securityClass ?? "Security") : (item.security?.asset_class ?? "—")}</span>
              {item.security?.sector ? (
                <>
                  <span className="text-slate-500">•</span>
                  <span className="text-slate-400">{item.security.sector}</span>
                </>
              ) : null}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-400">
            <span>
              {isInsider ? "Transaction" : "Trade"}: <span className="text-slate-200">{item.trade_date ? formatDateShort(item.trade_date) : "—"}</span>
            </span>
            <span>
              {isInsider ? "Filing" : "Report"}: <span className="text-slate-200">{item.report_date ? formatDateShort(item.report_date) : "—"}</span>
            </span>
            {isInsider ? (
              <span>
                Ownership: <span className="text-slate-200">{item.insider?.ownership ?? item.owner_type ?? "—"}</span>
              </span>
            ) : null}
          </div>
        </div>

        <div className="flex flex-col items-start gap-3 text-left lg:items-end lg:text-right">
          <Badge tone={isInsider ? (insiderKind === "purchase" ? "pos" : "neg") : transactionTone(item.transaction_type)}>
            {isInsider
              ? insiderKind === "purchase"
                ? "Purchase"
                : insiderKind === "sale"
                  ? "Sale"
                  : "—"
              : (formatTransactionLabel(item.transaction_type) ?? "—")}
          </Badge>
          <div className="text-lg font-semibold text-white">
            {isInsider
              ? insiderValue?.total
                ? formatMoney(insiderValue.total)
                : "—"
              : (formatCurrencyRange(item.amount_range_min, item.amount_range_max) ?? "—")}
          </div>
          {isInsider ? (
            <div className="text-xs text-slate-400">
              {insiderValue?.shares && insiderValue?.price
                ? `${insiderValue.shares.toLocaleString()} shares @ ${new Intl.NumberFormat("en-US", {
                    style: "currency",
                    currency: "USD",
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  }).format(insiderValue.price)}`
                : insiderValue?.price
                  ? `@ ${new Intl.NumberFormat("en-US", {
                      style: "currency",
                      currency: "USD",
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    }).format(insiderValue.price)}`
                  : insiderValue?.shares
                    ? `${insiderValue.shares.toLocaleString()} shares`
                    : "—"}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
