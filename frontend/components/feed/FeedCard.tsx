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

  const shares = parseNum(insiderItem.insider?.shares ?? insiderItem.payload?.raw?.securitiesTransacted);
  const price = parseNum(insiderItem.insider?.price ?? insiderItem.payload?.raw?.price ?? item.insider?.price);
  if (shares === null || price === null || shares <= 0 || price <= 0) return null;

  const total = shares * price;
  if (total < 1001) return null;

  return { total, shares, price };
}

export function FeedCard({ item }: { item: FeedItem }) {
  const chamber = chamberBadge(item.member.chamber);
  const party = partyBadge(item.member.party);
  const tag = memberTag(item.member.party, item.member.state);
  const isInsider = item.kind === "insider_trade";
  const insiderKind = isInsider ? getInsiderKind(item) : null;
  const insiderValue = isInsider ? getInsiderValue(item) : null;

  if (isInsider && (!insiderKind || !insiderValue)) {
    return null;
  }

  const insiderItem = item as FeedCardInsiderItem;
  const insiderRole = isInsider
    ? (item.insider?.role ?? insiderItem.payload?.raw?.typeOfOwner ?? "Insider").replace(/officer:\s*/i, "").trim() || "Insider"
    : null;

  return (
    <div className="rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-4">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              {isInsider ? (
                <span className="text-lg font-semibold text-white">{item.insider?.name ?? item.member.name}</span>
              ) : (
                <Link href={`/member/${item.member.bioguide_id}`} className="text-lg font-semibold text-white hover:text-emerald-200">
                  {item.member.name}
                </Link>
              )}
              {isInsider ? <Badge tone="neutral">{insiderRole}</Badge> : <Badge tone={party.tone}>{tag}</Badge>}
              {isInsider ? null : <Badge tone={chamber.tone}>{chamber.label}</Badge>}
            </div>
            <div className="flex flex-wrap items-center gap-3 text-sm text-slate-300">
              <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Security</span>
              {item.security.symbol ? (
                <Link
                  href={`/ticker/${formatSymbol(item.security.symbol)}`}
                  className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-emerald-100"
                >
                  {formatSymbol(item.security.symbol)}
                </Link>
              ) : (
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-slate-200">
                  {formatSymbol(item.security.symbol)}
                </span>
              )}
              {isInsider ? (insiderItem.payload?.raw?.securityName ? <span className="text-slate-200">{insiderItem.payload.raw.securityName}</span> : null) : <span className="text-slate-200">{item.security.name}</span>}
              {(isInsider ? Boolean(insiderItem.payload?.raw?.securityName) : true) ? <span className="text-slate-500">•</span> : null}
              <span className="text-slate-400">{item.security.asset_class}</span>
              {item.security.sector ? (
                <>
                  <span className="text-slate-500">•</span>
                  <span className="text-slate-400">{item.security.sector}</span>
                </>
              ) : null}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-400">
            <span>
              {isInsider ? "Transaction" : "Trade"}: <span className="text-slate-200">{formatDateShort(item.trade_date)}</span>
            </span>
            <span>
              {isInsider ? "Filing" : "Report"}: <span className="text-slate-200">{formatDateShort(item.report_date)}</span>
            </span>
            {isInsider ? (
              <span>
                Ownership: <span className="text-slate-200">{item.insider?.ownership ?? item.owner_type}</span>
              </span>
            ) : null}
          </div>
        </div>

        <div className="flex flex-col items-start gap-3 text-left lg:items-end lg:text-right">
          <Badge tone={isInsider ? (insiderKind === "purchase" ? "pos" : "neg") : transactionTone(item.transaction_type)}>
            {isInsider ? (insiderKind === "purchase" ? "Purchase" : "Sale") : formatTransactionLabel(item.transaction_type)}
          </Badge>
          <div className="text-lg font-semibold text-white">
            {isInsider ? formatCurrency(insiderValue!.total) : formatCurrencyRange(item.amount_range_min, item.amount_range_max)}
          </div>
          {isInsider && insiderValue?.shares && insiderValue?.price ? (
            <div className="text-xs text-slate-400">
              {`${insiderValue.shares.toLocaleString()} shares @ ${formatCurrency(insiderValue.price)}`}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
