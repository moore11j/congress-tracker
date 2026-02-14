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

function formatMoney(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}


function normalizeSecurityClass(securityName: string | undefined): string | null {
  if (!securityName) return null;
  const trimmed = securityName.trim();
  if (!trimmed) return null;
  const value = trimmed.toLowerCase();

  if (value === "common stock" || value === "common") return "Common";
  if (value === "preferred stock") return "Preferred";

  return trimmed;
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

  const amt = parseNum(insiderItem.amount_min ?? insiderItem.amount_max);
  return { amt };
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
  const insiderAmount = insiderValue?.amt ?? null;

  const insiderItem = item as FeedCardInsiderItem;
  const securityClass = isInsider ? normalizeSecurityClass(insiderItem.payload?.raw?.securityName ?? undefined) : null;

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
              {isInsider ? <Badge tone="dem">{item.insider?.role ?? "INSIDER"}</Badge> : <Badge tone={party.tone}>{tag}</Badge>}
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
              ? insiderAmount !== null
                ? formatMoney(insiderAmount)
                : "—"
              : (formatCurrencyRange(item.amount_range_min, item.amount_range_max) ?? "—")}
          </div>
        </div>
      </div>
    </div>
  );
}
