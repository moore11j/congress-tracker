import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { ApiError, getDepartmentProfile, type DepartmentContractItem, type DepartmentProfileResponse } from "@/lib/api";
import { cardClassName, ghostButtonClassName, tickerLinkClassName } from "@/lib/styles";
import { formatCurrency, formatDateShort } from "@/lib/format";
import { tickerHref } from "@/lib/ticker";

type Props = {
  params: Promise<{ slug: string }>;
};

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  try {
    const department = await getDepartmentProfile(slug, { limit: 1 });
    return {
      title: `${department.name} Contract Awards | Walnut Market Terminal`,
      description: `Public-market contract exposure from ${department.name} awards.`,
    };
  } catch {
    return {
      title: "Government Department | Walnut Market Terminal",
    };
  }
}

export default async function DepartmentPage({ params }: Props) {
  const { slug } = await params;
  let department: DepartmentProfileResponse;
  try {
    department = await getDepartmentProfile(slug, { limit: 12 });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) notFound();
    throw error;
  }

  const summary = department.summary;
  const topRecipient = summary.topTicker
    ? `${summary.topTicker}${summary.topCompany ? ` - ${summary.topCompany}` : ""}`
    : "-";

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <section className={`${cardClassName} min-w-0 space-y-5`}>
        <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
          <div className="min-w-0 max-w-4xl">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Government Department</p>
            <h1 className="mt-2 break-words text-3xl font-semibold text-white md:text-4xl">{department.name}</h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300">
              Public-market contract exposure from {department.name} awards.
            </p>
          </div>
          <div className="flex min-w-0 flex-wrap gap-2">
            <Link href="/?mode=government_contracts" className={ghostButtonClassName} prefetch={false}>
              Government contracts feed
            </Link>
            <Link href="/screener?government_contracts_active=true&government_contracts_lookback_days=365" className={ghostButtonClassName} prefetch={false}>
              Screener overlay
            </Link>
          </div>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <StatCard label="Total Awarded" value={summary.totalAwarded === null ? "-" : formatCurrencyCompact(summary.totalAwarded)} />
          <StatCard label="Contracts" value={String(summary.contractCount ?? 0)} />
          <StatCard label="Linked Tickers" value={String(summary.linkedTickerCount ?? 0)} />
          <StatCard label="Latest Award" value={formatDateShort(summary.latestAwardDate)} />
          <StatCard label="Top Recipient" value={topRecipient} compact />
        </div>
      </section>

      <section className={`${cardClassName} min-w-0`}>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0">
            <h2 className="text-lg font-semibold text-white">Awarded Tickers</h2>
            <p className="text-sm text-slate-400">Public companies linked to awards from this department.</p>
          </div>
          <span className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">Sorted by total awarded</span>
        </div>

        {department.tickers.length === 0 ? (
          <EmptyState>No linked public-company awards found for this department yet.</EmptyState>
        ) : (
          <div className="min-w-0 overflow-x-auto">
            <table className="min-w-full divide-y divide-white/10 text-left text-sm">
              <thead className="text-xs uppercase tracking-[0.14em] text-slate-500">
                <tr>
                  <th className="py-3 pr-4 font-semibold">Ticker</th>
                  <th className="px-4 py-3 font-semibold">Company</th>
                  <th className="px-4 py-3 text-right font-semibold">Total Awarded</th>
                  <th className="px-4 py-3 text-right font-semibold">Contract Count</th>
                  <th className="px-4 py-3 font-semibold">Latest Award</th>
                  <th className="px-4 py-3 font-semibold">Top Contract / Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {department.tickers.map((ticker) => (
                  <tr key={ticker.symbol} className="align-top text-slate-300">
                    <td className="py-3 pr-4">
                      <Link href={tickerHref(ticker.symbol) ?? "#"} className={tickerLinkClassName} prefetch={false}>
                        {ticker.symbol}
                      </Link>
                    </td>
                    <td className="px-4 py-3 font-medium text-slate-100">{ticker.companyName}</td>
                    <td className="px-4 py-3 text-right font-semibold tabular-nums text-white">{formatCurrencyCompact(ticker.totalAwarded)}</td>
                    <td className="px-4 py-3 text-right tabular-nums">{ticker.contractCount}</td>
                    <td className="px-4 py-3 whitespace-nowrap">{formatDateShort(ticker.latestAwardDate)}</td>
                    <td className="max-w-md px-4 py-3 leading-6 text-slate-400">{ticker.topDescription ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {department.trend && department.trend.length > 1 ? <TrendPanel department={department} /> : null}

      <div className="grid min-w-0 gap-6 xl:grid-cols-2">
        <ContractPanel title="Recent Contracts" items={department.recentContracts} />
        <ContractPanel title="Largest Contracts" items={department.largestContracts} />
      </div>
    </div>
  );
}

function StatCard({ label, value, compact = false }: { label: string; value: string; compact?: boolean }) {
  return (
    <div className="min-w-0 rounded-2xl border border-white/10 bg-slate-950/45 p-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{label}</p>
      <p className={`mt-2 truncate font-semibold text-white ${compact ? "text-sm" : "text-xl tabular-nums"}`}>{value}</p>
    </div>
  );
}

function ContractPanel({ title, items }: { title: string; items: DepartmentContractItem[] }) {
  return (
    <section className={`${cardClassName} min-w-0`}>
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      {items.length === 0 ? (
        <EmptyState>No contract activity available for this department yet.</EmptyState>
      ) : (
        <div className="mt-4 space-y-3">
          {items.map((item) => (
            <ContractRow key={`${item.id}-${item.awardId ?? ""}`} item={item} />
          ))}
        </div>
      )}
    </section>
  );
}

function ContractRow({ item }: { item: DepartmentContractItem }) {
  const href = tickerHref(item.symbol);
  return (
    <div className="rounded-2xl border border-white/10 bg-slate-950/45 p-4">
      <div className="flex min-w-0 flex-col justify-between gap-3 md:flex-row md:items-start">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            {href && item.symbol ? (
              <Link href={href} className={tickerLinkClassName} prefetch={false}>
                {item.symbol}
              </Link>
            ) : null}
            <span className="min-w-0 truncate text-sm font-semibold text-white">{item.companyName ?? item.recipientName}</span>
          </div>
          <p className="mt-1 text-xs text-slate-500">{item.recipientName}</p>
        </div>
        <div className="shrink-0 text-left md:text-right">
          <p className="text-sm font-semibold tabular-nums text-white">{item.amount === null ? "Value unavailable" : formatCurrency(item.amount)}</p>
          <p className="mt-1 text-xs text-slate-400">{formatDateShort(item.date)}</p>
        </div>
      </div>
      <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-slate-500">
        <span>{item.department}</span>
        {item.agency ? <span>{item.agency}</span> : null}
        {item.awardId ? <span>Award ID {item.awardId}</span> : null}
      </div>
      {item.description ? <p className="mt-3 line-clamp-2 text-sm leading-6 text-slate-400">{item.description}</p> : null}
    </div>
  );
}

function TrendPanel({ department }: { department: DepartmentProfileResponse }) {
  const points = department.trend ?? [];
  const max = Math.max(...points.map((point) => point.totalAwarded), 1);
  return (
    <section className={`${cardClassName} min-w-0`}>
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-white">Awarded Value Trend</h2>
        <p className="text-sm text-slate-400">Monthly buckets from available contract records.</p>
      </div>
      <div className="flex h-44 min-w-0 items-end gap-1 overflow-x-auto border-b border-slate-700/80 pb-2">
        {points.slice(-24).map((point) => (
          <div key={point.period} className="group flex h-full min-w-8 flex-col justify-end">
            <div
              className="rounded-t bg-emerald-400/60 transition group-hover:bg-emerald-300"
              style={{ height: `${Math.max(6, (point.totalAwarded / max) * 100)}%` }}
              title={`${point.period}: ${formatCurrencyCompact(point.totalAwarded)} across ${point.contractCount} contract${point.contractCount === 1 ? "" : "s"}`}
            />
          </div>
        ))}
      </div>
    </section>
  );
}

function EmptyState({ children }: { children: string }) {
  return (
    <div className="mt-4 rounded-2xl border border-dashed border-white/10 bg-slate-950/30 p-6 text-sm text-slate-400">
      {children}
    </div>
  );
}

function formatCurrencyCompact(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(abs >= 100_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(abs >= 100_000 ? 0 : 1)}K`;
  return formatCurrency(value);
}
