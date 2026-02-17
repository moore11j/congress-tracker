import Link from "next/link";
import type { FeedItem } from "@/lib/types";
import { Badge } from "@/components/Badge";
import {
  chamberBadge,
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
  amount_min?: number | string | null;
  amount_max?: number | string | null;
  payload?: {
    transaction_type?: string | null;
    transaction_date?: string | null;
    filing_date?: string | null;
    shares?: number | string | null;
    price?: number | string | null;
    raw?: {
      transactionType?: string | null;
      transactionDate?: string | null;
      filingDate?: string | null;
      acquisitionOrDisposition?: string | null;
      securitiesTransacted?: number | string | null;
      transactionShares?: number | string | null;
      price?: number | string | null;
      typeOfOwner?: string | null;
      officerTitle?: string | null;
      insiderRole?: string | null;
      position?: string | null;
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
    const cleaned = v.replace(/[$,]/g, "").trim();
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function formatPrice(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(n);
}

function formatShares(n: number): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: Number.isInteger(n) ? 0 : 2,
  }).format(n);
}

function formatMoney(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatYMD(ymd?: string | null): string {
  if (!ymd) return "—";
  const s = ymd.slice(0, 10);
  const [y, m, d] = s.split("-").map(Number);
  if (!y || !m || !d) return s;
  const dt = new Date(Date.UTC(y, m - 1, d));
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
    timeZone: "UTC",
  }).format(dt);
}

function daysBetweenYMD(a?: string | null, b?: string | null): number | null {
  if (!a || !b) return null;
  const aa = a.slice(0, 10);
  const bb = b.slice(0, 10);
  const [ay, am, ad] = aa.split("-").map(Number);
  const [by, bm, bd] = bb.split("-").map(Number);
  if (!ay || !am || !ad || !by || !bm || !bd) return null;
  const t1 = Date.UTC(ay, am - 1, ad);
  const t2 = Date.UTC(by, bm - 1, bd);
  const diff = Math.round((t2 - t1) / (1000 * 60 * 60 * 24));
  return Number.isFinite(diff) ? diff : null;
}


function normalizeSecurityClass(securityName: string | undefined): string | null {
  if (!securityName) return null;
  const trimmed = securityName.trim();
  if (!trimmed) return null;
  const value = trimmed.toLowerCase();
  if (value === "common stock") return "Common";
  if (value === "preferred stock") return "Preferred";
  return trimmed;
}

function getInsiderKind(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;
  const rawDirection =
    insiderItem.trade_type ??
    insiderItem.insider?.transaction_type ??
    insiderItem.payload?.transaction_type ??
    insiderItem.payload?.raw?.transactionType ??
    item.transaction_type ??
    "";
  const t = rawDirection.toUpperCase();
  const ad = insiderItem.payload?.raw?.acquisitionOrDisposition?.toUpperCase() ?? "";

  if (t.startsWith("P-") || t.startsWith("P") || t.includes("PURCHASE")) return "purchase";
  if (t.startsWith("S-") || t.startsWith("S") || t.includes("SALE")) return "sale";
  if (ad === "A") return "purchase";
  if (ad === "D") return "sale";
  return null;
}

function getInsiderValue(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;

  const totalValue = parseNum(item.amount_range_min ?? insiderItem.amount_min ?? item.amount_range_max ?? insiderItem.amount_max);
  const shares = parseNum(insiderItem.payload?.shares ?? insiderItem.payload?.raw?.securitiesTransacted);
  const price = parseNum(insiderItem.insider?.price ?? insiderItem.payload?.price ?? insiderItem.payload?.raw?.price);

  return {
    totalValue,
    price: price && price > 0 ? price : null,
    shares: shares && shares > 0 ? shares : null,
  };
}

function getInsiderRoleBadge(item: FeedItem): string {
  const insiderItem = item as FeedCardInsiderItem;
  const raw =
    insiderItem.insider?.role ??
    insiderItem.payload?.raw?.typeOfOwner ??
    insiderItem.payload?.raw?.officerTitle ??
    insiderItem.payload?.raw?.insiderRole ??
    insiderItem.payload?.raw?.position ??
    "INSIDER";
  const s = raw.toUpperCase();

  if (/\bCHIEF EXECUTIVE OFFICER\b|\bCEO\b/.test(s)) return "CEO";
  if (/\bCHIEF FINANCIAL OFFICER\b|\bCFO\b/.test(s)) return "CFO";
  if (/\bCHIEF OPERATING OFFICER\b|\bCOO\b/.test(s)) return "COO";
  if (/\bCHIEF TECHNOLOGY OFFICER\b|\bCTO\b/.test(s)) return "CTO";
  if (/\bCHIEF COMPLIANCE OFFICER\b|\bCCO\b/.test(s)) return "CCO";
  if (/\bCHIEF LEGAL OFFICER\b|\bCLO\b/.test(s)) return "CLO";
  if (/\bCHIEF ACCOUNTING OFFICER\b|\bCAO\b/.test(s)) return "CAO";
  if (/\bEXECUTIVE VICE PRESIDENT\b|\bEXEC\s+VP\b|\bEVP\b/.test(s)) return "EVP";
  if (/\bSENIOR VICE PRESIDENT\b|\bSR\s+VP\b|\bSVP\b/.test(s)) return "SVP";
  if (/\bPRESIDENT\b/.test(s)) return "PRES";
  if (/\bVICE PRESIDENT\b|\bVP\b/.test(s)) return "VP";
  if (/\bDIRECTOR\b/.test(s)) return "DIR";
  if (/\bOFFICER\b/.test(s)) return "OFFICER";
  return "INSIDER";
}

export function FeedCard({ item }: { item: FeedItem }) {
  if (!item) return null;

  const isCongress = item.kind === "congress_trade";
  const isInsider = item.kind === "insider_trade";
  const chamber = chamberBadge(item.member?.chamber ?? "—");
  const party = partyBadge(item.member?.party ?? null);
  const tag = memberTag(item.member?.party ?? null, item.member?.state ?? null);
  const insiderKind = isInsider ? getInsiderKind(item) : null;
  const insiderValue = isInsider ? getInsiderValue(item) : null;
  const insiderAmount = insiderValue?.totalValue ?? null;
  const insiderPrice = insiderValue?.price ?? null;
  const insiderShares = insiderValue?.shares ?? null;

  const insiderItem = item as FeedCardInsiderItem;
  const securityClass = isInsider ? normalizeSecurityClass(insiderItem.payload?.raw?.securityName ?? undefined) : null;
  const insiderRoleBadge = isInsider ? getInsiderRoleBadge(item) : null;
  const insiderTxDate =
    insiderItem.payload?.transaction_date ?? insiderItem.payload?.raw?.transactionDate ?? item.trade_date;
  const insiderFilingDate =
    insiderItem.payload?.filing_date ?? insiderItem.payload?.raw?.filingDate ?? item.report_date;
  const lagDays = isCongress ? daysBetweenYMD(item.trade_date, item.report_date) : null;
  const congressEstimatedPrice = isCongress ? parseNum(item.estimated_price) : null;

  if (isInsider && !insiderKind) return null;

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
              {isInsider ? <Badge tone="dem">{insiderRoleBadge}</Badge> : <Badge tone={party.tone}>{tag}</Badge>}
              {isCongress ? <Badge tone={chamber.tone}>{chamber.label}</Badge> : null}
            </div>
            <div className="flex flex-wrap items-center gap-3 text-sm text-slate-300">
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
              <span className="text-slate-200">{item.security?.name ?? "—"}</span>
              {isInsider && securityClass ? <span className="text-slate-500">•</span> : null}
              {isInsider && securityClass ? <span className="text-slate-400">{securityClass}</span> : null}
              {isCongress ? <span className="text-slate-500">•</span> : null}
              {isCongress ? <span className="text-slate-400">{item.security?.asset_class ?? "—"}</span> : null}
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
              {isInsider ? "Transaction" : "Trade"}: <span className="text-slate-200">{isInsider ? formatYMD(insiderTxDate) : item.trade_date ? formatDateShort(item.trade_date) : "—"}</span>
            </span>
            <span>
              {isInsider ? "Filing" : "Report"}: <span className="text-slate-200">{isInsider ? formatYMD(insiderFilingDate) : item.report_date ? formatDateShort(item.report_date) : "—"}</span>
              {isCongress && lagDays !== null && lagDays >= 0 ? (
                <span className="ml-2">
                  Filed after: <span className="text-slate-200">{lagDays}d</span>
                </span>
              ) : null}
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
              ? insiderAmount !== null
                ? formatMoney(insiderAmount)
                : "—"
              : (formatCurrencyRange(item.amount_range_min, item.amount_range_max) ?? "—")}
          </div>
          {isCongress && congressEstimatedPrice !== null ? (
            <div className="mt-1 text-xs text-slate-400">Est. Trade Price: {formatPrice(congressEstimatedPrice)}</div>
          ) : null}
          {isInsider && (insiderPrice !== null || insiderShares !== null) ? (
            <div className="text-xs text-slate-400">
              {insiderShares !== null && insiderPrice !== null
                ? `${formatShares(insiderShares)} shares @ ${formatPrice(insiderPrice)}`
                : insiderPrice !== null
                  ? `@ ${formatPrice(insiderPrice)}`
                  : `${formatShares(insiderShares ?? 0)} shares`}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
