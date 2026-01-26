import Link from "next/link";
import { Badge } from "@/components/Badge";
import { getMemberProfile } from "@/lib/api";
import {
  cardClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import {
  chamberBadge,
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  memberTag,
  partyBadge,
  transactionTone,
} from "@/lib/format";

export default async function MemberPage({
  params,
}: {
  params: Promise<{ bioguide_id: string }>;
}) {
  const { bioguide_id } = await params;
  const data = await getMemberProfile(bioguide_id);
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Member profile</p>
          <h1 className="text-3xl font-semibold text-white">{data.member.name}</h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <Badge tone={party.tone}>{memberTag(data.member.party, data.member.state)}</Badge>
            <Badge tone={chamber.tone}>{chamber.label}</Badge>
            <span className={pillClassName}>Bioguide {data.member.bioguide_id}</span>
          </div>
        </div>
        <Link href="/" className={ghostButtonClassName}>
          Back to feed
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Top tickers</h2>
          <div className="mt-4 space-y-3">
            {data.top_tickers.length === 0 ? (
              <p className="text-sm text-slate-400">No ticker concentration yet.</p>
            ) : (
              data.top_tickers.map((ticker) => (
                <Link
                  key={ticker.symbol}
                  href={`/ticker/${ticker.symbol}`}
                  className="flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-200 hover:border-emerald-400/40"
                >
                  <span>{ticker.symbol}</span>
                  <span className="text-xs text-slate-400">{ticker.trades} trades</span>
                </Link>
              ))
            )}
          </div>
        </div>

        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <div className="mt-4 space-y-4">
            {data.trades.length === 0 ? (
              <p className="text-sm text-slate-400">No recent trades for this member.</p>
            ) : (
              data.trades.map((trade) => (
                <div key={trade.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    {trade.symbol ? (
                      <Link href={`/ticker/${trade.symbol}`} className="text-sm font-semibold text-emerald-200">
                        {trade.symbol}
                      </Link>
                    ) : (
                      <span className="text-sm font-semibold text-slate-200">Unknown ticker</span>
                    )}
                    <Badge tone={transactionTone(trade.transaction_type)}>
                      {formatTransactionLabel(trade.transaction_type)}
                    </Badge>
                  </div>
                  <div className="mt-1 text-xs text-slate-400">{trade.security_name}</div>
                  <div className="mt-2 text-xs text-slate-400">
                    Trade {formatDateShort(trade.trade_date)} • Report {formatDateShort(trade.report_date)}
                  </div>
                  <div className="mt-2 text-sm font-semibold text-white">
                    {formatCurrencyRange(trade.amount_range_min, trade.amount_range_max)}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
