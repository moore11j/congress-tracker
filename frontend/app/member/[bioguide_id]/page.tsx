import Link from "next/link";
import { Badge } from "@/components/Badge";
import { getMemberPerformance, getMemberProfile } from "@/lib/api";
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
  formatStateDistrict,
  partyBadge,
  transactionTone,
} from "@/lib/format";

type Props = {
  params: Promise<{ bioguide_id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function pct(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}

function pct0(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function tone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "text-slate-400";
  if (n > 0) return "text-emerald-400";
  if (n < 0) return "text-rose-400";
  return "text-slate-300";
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { bioguide_id } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getParam(sp, "lb");
  const lb = lbRaw === "90" || lbRaw === "180" || lbRaw === "3650" ? Number(lbRaw) : 365;

  const data = await getMemberProfile(bioguide_id);
  const perf = await getMemberPerformance(bioguide_id, lb);
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);
  const options = [
    { label: "90D", value: 90 },
    { label: "180D", value: 180 },
    { label: "1Y", value: 365 },
    { label: "All", value: 3650 },
  ];

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Member profile</p>
          <h1 className="text-3xl font-semibold text-white">{data.member.name}</h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <Badge tone={party.tone}>{party.label}</Badge>
            <Badge tone={chamber.tone}>{chamber.label}</Badge>
            <span className={pillClassName}>{formatStateDistrict(data.member.state, data.member.district)}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {options.map((o) => (
            <Link
              key={o.value}
              href={`/member/${bioguide_id}?lb=${o.value}`}
              className={`relative rounded-full border px-3 py-1.5 text-xs transition-colors ${
                o.value === lb
                  ? "border-white/30 bg-white/[0.06] font-medium text-white"
                  : "border-white/10 text-white/60 hover:border-white/20 hover:text-white/80"
              }`}
            >
              {o.value === lb && (
                <span className="absolute left-2 right-2 -top-[2px] h-[2px] rounded-full bg-white/60" />
              )}
              {o.label}
            </Link>
          ))}
          <Link href="/" className={ghostButtonClassName}>
            Back to feed
          </Link>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-xs">
        <span className="text-white/40">Lookback:</span>
        <span className="tabular-nums text-white/80">{lb === 3650 ? "All" : `${lb}D`}</span>

        <span className="text-white/20">|</span>

        <span className="text-white/40">Avg:</span>
        <span className={`tabular-nums ${tone(perf.avg_return)}`}>{pct(perf.avg_return)}</span>

        <span className="text-white/40">Med:</span>
        <span className={`tabular-nums ${tone(perf.median_return)}`}>{pct(perf.median_return)}</span>

        <span className="text-white/40">Win:</span>
        <span className="tabular-nums text-white/80">{pct0(perf.win_rate)}</span>

        <span className="text-white/40">n:</span>
        <span className="tabular-nums text-white/80">{perf.trade_count ?? 0}</span>

        <span className="text-white/40">α S&P:</span>
        <span className={`tabular-nums ${tone(perf.avg_alpha)}`}>
          {perf.avg_alpha == null ? "—" : pct(perf.avg_alpha)}
        </span>
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
