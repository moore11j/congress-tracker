import Link from "next/link";
import { Badge } from "@/components/Badge";
import { getTickerProfile } from "@/lib/api";
import {
  cardClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import { formatCurrencyRange, formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";

type Props = {
  params: Promise<{ symbol: string }>;
};

export default async function TickerPage({ params }: Props) {
  const { symbol } = await params;
  const data = await getTickerProfile(symbol);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Ticker profile</p>
          <h1 className="text-3xl font-semibold text-white">
            {data.ticker.symbol}
            <span className="text-slate-400"> · {data.ticker.name}</span>
          </h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <span className={pillClassName}>{data.ticker.asset_class}</span>
            {data.ticker.sector ? <span className={pillClassName}>{data.ticker.sector}</span> : null}
          </div>
        </div>
        <Link href="/" className={ghostButtonClassName}>
          Back to feed
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Top members trading this ticker</h2>
          <div className="mt-4 space-y-3">
            {data.top_members.length === 0 ? (
              <p className="text-sm text-slate-400">No member activity yet.</p>
            ) : (
              data.top_members.map((member) => (
                <Link
                  key={member.bioguide_id}
                  href={`/member/${member.bioguide_id}`}
                  className="flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-200 hover:border-emerald-400/40"
                >
                  <span>{member.bioguide_id}</span>
                  <span className="text-xs text-slate-400">{member.trades} trades</span>
                </Link>
              ))
            )}
          </div>
        </div>

        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <div className="mt-4 space-y-4">
            {data.trades.length === 0 ? (
              <p className="text-sm text-slate-400">No recent trades for this ticker.</p>
            ) : (
              data.trades.map((trade) => (
                <div key={trade.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <Link href={`/member/${trade.member.bioguide_id}`} className="text-sm font-semibold text-emerald-200">
                      {trade.member.name}
                    </Link>
                    <Badge tone={transactionTone(trade.transaction_type)}>
                      {formatTransactionLabel(trade.transaction_type)}
                    </Badge>
                  </div>
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
