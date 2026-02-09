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

const insiderPriceFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function insiderAmountLabel(item: FeedItem): string {
  const hasValue = item.amount_range_min !== null && item.amount_range_max !== null;
  if (hasValue) {
    return formatCurrency(item.amount_range_max);
  }
  const price = item.insider?.price;
  if (typeof price === "number" && !Number.isNaN(price) && price > 0) {
    return insiderPriceFormatter.format(price);
  }
  return "—";
}

export function FeedCard({ item }: { item: FeedItem }) {
  const chamber = chamberBadge(item.member.chamber);
  const party = partyBadge(item.member.party);
  const tag = memberTag(item.member.party, item.member.state);
  const isInsider = item.kind === "insider_trade";

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
              {isInsider ? <Badge tone="neutral">INSIDER TRADE</Badge> : <Badge tone={party.tone}>{tag}</Badge>}
              {isInsider ? (item.insider?.role ? <Badge tone="neutral">{item.insider.role}</Badge> : null) : <Badge tone={chamber.tone}>{chamber.label}</Badge>}
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
              <span className="text-slate-200">{item.security.name}</span>
              <span className="text-slate-500">•</span>
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
          <Badge tone={transactionTone(item.transaction_type)}>
            {formatTransactionLabel(item.transaction_type)}
          </Badge>
          <div className="text-lg font-semibold text-white">
            {isInsider ? insiderAmountLabel(item) : formatCurrencyRange(item.amount_range_min, item.amount_range_max)}
          </div>
        </div>
      </div>
    </div>
  );
}
