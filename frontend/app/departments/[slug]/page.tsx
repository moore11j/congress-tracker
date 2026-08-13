import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import {
  ApiError,
  getDepartmentProfile,
  type DepartmentBreakdownItem,
  type DepartmentContractItem,
  type DepartmentProfileResponse,
  type DepartmentTickerItem,
  type DepartmentTrendPoint,
} from "@/lib/api";
import { ShareLinks } from "@/components/member/ShareLinks";
import { AddWatchlistTarget } from "@/components/watchlists/AddWatchlistTarget";
import { tickerLinkClassName } from "@/lib/styles";
import { formatCurrency, formatDateShort } from "@/lib/format";
import { tickerHref } from "@/lib/ticker";
import { departmentHref } from "@/lib/departments";
import { WALNUT_APP_URL, appCanonicalUrl, appPageMetadata } from "@/lib/marketingMetadata";
import { conciseSeoDescription, conciseSeoTitle, departmentHasIndexableContent, noindexFollowMetadata } from "@/lib/seoQuality";

type Props = {
  params: Promise<{ slug: string }>;
};

const panelClassName = "min-w-0 rounded-lg border border-slate-700/70 bg-slate-950/55 shadow-[0_18px_60px_-42px_rgba(20,184,166,0.9)] ring-1 ring-white/[0.025]";
const departmentButtonClassName =
  "inline-flex h-10 items-center justify-center gap-2 rounded-lg border border-slate-700/80 bg-slate-950/45 px-4 text-sm font-semibold text-slate-100 transition hover:border-slate-500 hover:bg-slate-900/70 hover:text-white";
const palette = ["#5ee0a5", "#38bdf8", "#818cf8", "#f8b84e", "#f08a8a", "#c4b5fd", "#94a3b8"];

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? WALNUT_APP_URL;
}

function departmentSeoName(name: string): string {
  if (/^department of defense$/i.test(name)) return "DoD";
  return name;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  const fallbackCanonicalPath = `/departments/${encodeURIComponent(slug)}`;
  try {
    const department = await getDepartmentProfile(slug, { limit: 1 });
    const canonicalPath = departmentHref(department.name) ?? fallbackCanonicalPath;
    const fallbackTitle = `${departmentSeoName(department.name)} Contracts | Walnut Markets`;
    const fallbackDescription = `Research ${department.name} contract awards, linked public companies, ticker exposure and award timing in Walnut Markets.`;
    const title = conciseSeoTitle(fallbackTitle, "Government Contracts | Walnut Markets");
    const description = conciseSeoDescription(fallbackDescription, "Research government contract awards, linked public companies, ticker exposure and award timing in Walnut Markets.");
    if (!departmentHasIndexableContent(department)) {
      return {
        ...noindexFollowMetadata(title, description),
        metadataBase: new URL(WALNUT_APP_URL),
        alternates: { canonical: appCanonicalUrl(canonicalPath) },
      };
    }
    return appPageMetadata(canonicalPath, {
      title,
      description,
      alternates: { canonical: appCanonicalUrl(canonicalPath) },
      openGraph: { type: "website", title, description, url: appCanonicalUrl(canonicalPath) },
    });
  } catch {
    const title = "Government Department Contracts by Public Company | Walnut Markets";
    const description = "Research government department contract awards, linked public companies, and ticker exposure in Walnut Markets.";
    return {
      ...noindexFollowMetadata(title, description),
      metadataBase: new URL(WALNUT_APP_URL),
      alternates: { canonical: appCanonicalUrl(fallbackCanonicalPath) },
    };
  }
}

export default async function DepartmentPage({ params }: Props) {
  const { slug } = await params;
  let department: DepartmentProfileResponse;
  try {
    department = await getDepartmentProfile(slug, { limit: 15, stalePageCache: true, source: "DepartmentProfilePage" });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) notFound();
    throw error;
  }

  const summary = department.summary;
  const topRecipient = summary.topTicker
    ? `${summary.topTicker}${summary.topCompany ? ` - ${summary.topCompany}` : ""}`
    : "-";
  const canonicalDepartmentPath = departmentHref(department.name) ?? `/departments/${encodeURIComponent(slug)}`;
  const canonicalDepartmentUrl = new URL(canonicalDepartmentPath, getSiteUrl()).toString();
  const totalAwarded = summary.totalAwarded ?? 0;
  const topTickerAwarded = department.tickers[0]?.totalAwarded ?? null;
  const categoryBreakdown = normalizeBreakdown(department.categoryBreakdown);
  const typeBreakdown = normalizeBreakdown(department.typeBreakdown);
  const topPrograms = normalizeBreakdown(department.topPrograms).slice(0, 5);

  return (
    <div className="relative -mx-4 min-w-0 overflow-hidden px-4 pb-6 text-slate-100 sm:-mx-6 sm:px-6 lg:-mx-8 lg:px-8">
      <div className="pointer-events-none absolute inset-0 -z-10 bg-[radial-gradient(circle_at_20%_0%,rgba(20,184,166,0.15),transparent_32%),linear-gradient(135deg,rgba(15,23,42,0.97),rgba(2,6,23,1)_42%,rgba(3,7,18,1))]" />
      <div className="pointer-events-none absolute inset-x-0 top-0 -z-10 h-72 bg-[linear-gradient(180deg,rgba(14,165,233,0.08),transparent)]" />

      <section className="min-w-0 pt-2">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
          <div className="flex min-w-0 gap-4">
            <DepartmentSeal name={department.name} />
            <div className="min-w-0">
              <nav className="flex min-w-0 flex-wrap items-center gap-2 text-sm text-slate-400">
                <Link href="/departments" className="transition hover:text-slate-200" prefetch={false}>
                  Government Departments
                </Link>
                <span className="text-slate-600">&gt;</span>
                <span className="truncate text-slate-200">{department.name}</span>
              </nav>
              <div className="mt-5 flex min-w-0 flex-wrap items-center gap-3">
                <h1 className="break-words text-4xl font-semibold leading-tight text-white md:text-5xl">{department.name} Government Contracts</h1>
                <IconFrame tone="muted" label="Tracked department">
                  <Icon name="star" />
                </IconFrame>
              </div>
              <p className="mt-3 max-w-3xl text-base leading-7 text-slate-200">
                Public-market contract exposure from {department.name} awards.
              </p>
              <p className="mt-1 text-sm text-slate-400">
                Data sourced from <span className="text-sky-300">USAspending.gov</span>
                <span className="mx-2 text-slate-600">-</span>
                Last updated {formatDateShort(summary.latestAwardDate)}
              </p>
            </div>
          </div>

          <div className="flex min-w-0 flex-wrap gap-3 xl:justify-end">
            <Link href="/?mode=government_contracts" className={departmentButtonClassName} prefetch={false}>
              <Icon name="feed" />
              Contracts Feed
            </Link>
            <AddWatchlistTarget
              targetType="department"
              targetValue={department.name}
              targetLabel={department.name}
              buttonLabel="Follow Department"
              className={departmentButtonClassName}
            />
            <ShareLinks canonicalUrl={canonicalDepartmentUrl} buttonClassName={departmentButtonClassName} />
            <Link href="/screener?government_contracts_active=true&government_contracts_lookback_days=365" className={departmentButtonClassName} prefetch={false}>
              <Icon name="grid" />
              Screener Overlay
            </Link>
          </div>
        </div>

        <div className="mt-7 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
          <MetricCard tone="emerald" icon="dollar" label="Total Awarded" value={summary.totalAwarded === null ? "-" : formatCurrencyCompact(summary.totalAwarded)} description="Total value of contracts awarded" />
          <MetricCard tone="sky" icon="file" label="Contracts" value={formatInteger(summary.contractCount ?? 0)} description="Total contract awards" />
          <MetricCard tone="violet" icon="building" label="Linked Tickers" value={formatInteger(summary.linkedTickerCount ?? 0)} description="Public companies receiving awards" />
          <MetricCard tone="amber" icon="calendar" label="Latest Award" value={formatDateShort(summary.latestAwardDate)} description="Most recent contract date" />
          <MetricCard tone="lime" icon="trophy" label="Top Recipient" value={topRecipient} description={topTickerAwarded === null ? "No awarded value available" : `${formatCurrencyCompact(topTickerAwarded)} awarded`} compact />
        </div>
      </section>

      <section className="mt-4 grid gap-3 xl:grid-cols-[1fr_1.03fr_1.03fr]">
        <TrendPanel points={department.trend ?? []} totalAwarded={totalAwarded} departmentName={department.name} />
        <DonutPanel title="Awards By Category" items={categoryBreakdown} total={totalAwarded} cta="View all categories" />
        <DonutPanel title="Awards By Type" items={typeBreakdown} total={totalAwarded} cta="View all award types" />
      </section>

      <section className="mt-4 grid min-w-0 gap-3 xl:grid-cols-[minmax(0,2.1fr)_minmax(20rem,0.8fr)]">
        <AwardedTickersPanel tickers={department.tickers} totalAwarded={totalAwarded} />
        <TopProgramsPanel programs={topPrograms} totalAwarded={totalAwarded} />
      </section>

      <section className="mt-4 grid min-w-0 gap-3 xl:grid-cols-2">
        <ContractPanel title="Recent Contracts" items={department.recentContracts} />
        <ContractPanel title="Largest Contracts" items={department.largestContracts} />
      </section>

      <div className={`${panelClassName} mt-1 flex items-start gap-3 px-4 py-3 text-xs leading-5 text-slate-400`}>
        <span className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-sky-400/60 text-sky-300">i</span>
        <p>
          Contract data sourced from USAspending.gov. Awards may include obligated, funded, or potential contract values. Not all awards are publicly disclosed. Data is updated daily.
        </p>
      </div>
    </div>
  );
}

function MetricCard({
  icon,
  label,
  value,
  description,
  tone,
  compact = false,
}: {
  icon: IconName;
  label: string;
  value: string;
  description: string;
  tone: Tone;
  compact?: boolean;
}) {
  return (
    <div className={`${panelClassName} flex min-h-28 min-w-0 gap-4 p-4`}>
      <IconFrame tone={tone} label={label}>
        <Icon name={icon} />
      </IconFrame>
      <div className="min-w-0">
        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-400">{label}</p>
        <p className={`mt-2 truncate font-semibold text-white ${compact ? "text-base" : "text-2xl tabular-nums text-emerald-300"}`}>{value}</p>
        <p className="mt-1 line-clamp-2 text-sm leading-5 text-slate-400">{description}</p>
      </div>
    </div>
  );
}

function TrendPanel({ points, totalAwarded, departmentName }: { points: DepartmentTrendPoint[]; totalAwarded: number; departmentName: string }) {
  const chartPoints = points.slice(-12);
  const deltaPct = periodDeltaPct(chartPoints);
  return (
    <section className={`${panelClassName} min-h-[18.5rem] p-4`}>
      <PanelHeader title="Awarded Over Time" />
      <div className="mt-4 flex items-start justify-between gap-4">
        <div>
          <p className="text-3xl font-semibold tabular-nums text-white">{formatCurrencyCompact(totalAwarded)}</p>
          <p className="mt-1 text-sm text-slate-400">Total</p>
        </div>
        <div className="text-right">
          <p className={`text-lg font-semibold tabular-nums ${deltaPct === null || deltaPct >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
            {deltaPct === null ? "-" : `${deltaPct >= 0 ? "+" : ""}${deltaPct.toFixed(1)}%`}
          </p>
          <p className="text-sm text-slate-400">vs prior period</p>
        </div>
      </div>
      <TrendChart points={chartPoints} />
      <p className="mt-3 text-xs leading-5 text-slate-400">Includes obligations for contracts awarded by {departmentName}.</p>
    </section>
  );
}

function TrendChart({ points }: { points: DepartmentTrendPoint[] }) {
  if (points.length < 2) return <EmptyState>No awarded trend available yet.</EmptyState>;
  const width = 520;
  const height = 150;
  const padX = 22;
  const padY = 14;
  const max = Math.max(...points.map((point) => point.totalAwarded), 1);
  const coordinates = points.map((point, index) => {
    const x = padX + (index / Math.max(points.length - 1, 1)) * (width - padX * 2);
    const y = height - padY - (point.totalAwarded / max) * (height - padY * 2);
    return { x, y, point };
  });
  const line = coordinates.map(({ x, y }) => `${x},${y}`).join(" ");
  const area = `${padX},${height - padY} ${line} ${width - padX},${height - padY}`;
  const ticks = [1, 0.75, 0.5, 0.25, 0];

  return (
    <div className="mt-5 min-w-0 overflow-hidden">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Awarded value trend" className="h-40 w-full">
        <defs>
          <linearGradient id="department-trend-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#34d399" stopOpacity="0.42" />
            <stop offset="100%" stopColor="#34d399" stopOpacity="0" />
          </linearGradient>
        </defs>
        {ticks.map((tick) => {
          const y = padY + tick * (height - padY * 2);
          return <line key={tick} x1={padX} x2={width - padX} y1={y} y2={y} stroke="rgba(148,163,184,0.14)" strokeWidth="1" />;
        })}
        <polygon points={area} fill="url(#department-trend-fill)" />
        <polyline points={line} fill="none" stroke="#5ee0a5" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
        {coordinates.map(({ x, y, point }) => (
          <circle key={point.period} cx={x} cy={y} r="5" fill="#5ee0a5">
            <title>{`${formatTrendPeriod(point.period)}: ${formatCurrencyCompact(point.totalAwarded)}`}</title>
          </circle>
        ))}
      </svg>
      <div className="grid grid-cols-4 text-xs text-slate-400">
        {labelPeriods(points).map((period) => (
          <span key={period}>{formatTrendPeriod(period)}</span>
        ))}
      </div>
    </div>
  );
}

function DonutPanel({ title, items, total, cta }: { title: string; items: DepartmentBreakdownItem[]; total: number; cta: string }) {
  return (
    <section className={`${panelClassName} min-h-[18.5rem] p-4`}>
      <PanelHeader title={title} />
      {items.length === 0 ? (
        <EmptyState>No breakdown data available yet.</EmptyState>
      ) : (
        <div className="mt-5 grid items-center gap-5 md:grid-cols-[9rem_minmax(0,1fr)]">
          <DonutChart items={items} total={total} />
          <div className="min-w-0 space-y-3">
            {items.map((item, index) => (
              <BreakdownRow key={item.label} item={item} total={total} color={palette[index % palette.length]} />
            ))}
          </div>
        </div>
      )}
      <div className="mt-5 flex justify-center">
        <button type="button" className="h-10 rounded-lg border border-slate-700/80 px-4 text-sm font-semibold text-slate-100 transition hover:border-slate-500 hover:text-white">
          {cta}
        </button>
      </div>
    </section>
  );
}

function DonutChart({ items, total }: { items: DepartmentBreakdownItem[]; total: number }) {
  const safeTotal = total > 0 ? total : items.reduce((sum, item) => sum + item.value, 0);
  let offset = 0;
  return (
    <div className="relative mx-auto h-36 w-36">
      <svg viewBox="0 0 120 120" className="h-full w-full -rotate-90">
        <circle cx="60" cy="60" r="42" fill="none" stroke="rgba(30,41,59,0.9)" strokeWidth="18" />
        {items.map((item, index) => {
          const percent = safeTotal > 0 ? Math.max(0, (item.value / safeTotal) * 100) : 0;
          const dashOffset = offset;
          offset += percent;
          return (
            <circle
              key={item.label}
              cx="60"
              cy="60"
              r="42"
              fill="none"
              pathLength="100"
              stroke={palette[index % palette.length]}
              strokeDasharray={`${percent} ${100 - percent}`}
              strokeDashoffset={-dashOffset}
              strokeLinecap="butt"
              strokeWidth="18"
            />
          );
        })}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <p className="text-lg font-semibold tabular-nums text-white">{formatCurrencyCompact(total)}</p>
        <p className="text-xs text-slate-400">Total</p>
      </div>
    </div>
  );
}

function BreakdownRow({ item, total, color }: { item: DepartmentBreakdownItem; total: number; color: string }) {
  const percent = total > 0 ? (item.value / total) * 100 : 0;
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_4.5rem_3.25rem] items-center gap-3 text-sm">
      <span className="flex min-w-0 items-center gap-2 text-slate-300">
        <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ backgroundColor: color }} />
        <span className="truncate">{item.label}</span>
      </span>
      <span className="text-right font-semibold tabular-nums text-white">{formatCurrencyCompact(item.value)}</span>
      <span className="text-right tabular-nums text-slate-400">{percent.toFixed(1)}%</span>
    </div>
  );
}

function AwardedTickersPanel({ tickers, totalAwarded }: { tickers: DepartmentTickerItem[]; totalAwarded: number }) {
  return (
    <section className={`${panelClassName} min-w-0 p-4`}>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-white">Awarded Tickers</h2>
          <p className="mt-1 text-sm text-slate-400">Public companies linked to awards from this department.</p>
        </div>
        <span className="flex items-center gap-2 text-sm text-slate-400">
          Sorted by Total Awarded
          <Icon name="sort" />
        </span>
      </div>

      {tickers.length === 0 ? (
        <EmptyState>No linked public-company awards found for this department yet.</EmptyState>
      ) : (
        <div className="min-w-0 overflow-x-auto">
          <table className="min-w-[58rem] w-full border-collapse text-left text-sm">
            <thead className="border-b border-slate-800 text-[11px] uppercase tracking-[0.18em] text-slate-500">
              <tr>
                <th className="py-3 pr-4 font-semibold">Ticker</th>
                <th className="px-4 py-3 font-semibold">Company</th>
                <th className="px-4 py-3 font-semibold">Total Awarded</th>
                <th className="px-4 py-3 text-right font-semibold">Contract Count</th>
                <th className="px-4 py-3 font-semibold">Latest Award</th>
                <th className="px-4 py-3 font-semibold">Top Contract / Description</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/80">
              {tickers.slice(0, 5).map((ticker, index) => (
                <TickerRow key={ticker.symbol} ticker={ticker} totalAwarded={totalAwarded} index={index} />
              ))}
            </tbody>
          </table>
        </div>
      )}
      {tickers.length > 5 ? (
        <div className="mt-5 flex justify-center">
          <button type="button" className="h-10 rounded-lg border border-slate-700/80 px-4 text-sm font-semibold text-slate-100 transition hover:border-slate-500 hover:text-white">
            View all {tickers.length} companies
          </button>
        </div>
      ) : null}
    </section>
  );
}

function TickerRow({ ticker, totalAwarded, index }: { ticker: DepartmentTickerItem; totalAwarded: number; index: number }) {
  const href = tickerHref(ticker.symbol);
  const width = totalAwarded > 0 ? Math.max(3, Math.min(100, (ticker.totalAwarded / totalAwarded) * 100)) : 0;
  return (
    <tr className="align-middle text-slate-300">
      <td className="py-4 pr-4">
        <div className="flex items-center gap-2">
          <TickerAvatar symbol={ticker.symbol} index={index} />
          <Link href={href ?? "#"} className={tickerLinkClassName} prefetch={false}>
            {ticker.symbol}
          </Link>
        </div>
      </td>
      <td className="px-4 py-4 font-medium text-white">{ticker.companyName}</td>
      <td className="px-4 py-4">
        <div className="flex items-center gap-3">
          <span className="w-16 shrink-0 font-semibold tabular-nums text-white">{formatCurrencyCompact(ticker.totalAwarded)}</span>
          <span className="h-2 w-24 overflow-hidden rounded-full bg-slate-800">
            <span className="block h-full rounded-full bg-emerald-300" style={{ width: `${width}%` }} />
          </span>
        </div>
      </td>
      <td className="px-4 py-4 text-right tabular-nums text-white">{formatInteger(ticker.contractCount)}</td>
      <td className="whitespace-nowrap px-4 py-4">{formatDateShort(ticker.latestAwardDate)}</td>
      <td className="max-w-md px-4 py-4 text-xs uppercase leading-5 text-slate-400">{ticker.topDescription ?? "-"}</td>
    </tr>
  );
}

function TopProgramsPanel({ programs, totalAwarded }: { programs: DepartmentBreakdownItem[]; totalAwarded: number }) {
  const max = Math.max(...programs.map((program) => program.value), 1);
  return (
    <section className={`${panelClassName} min-w-0 p-4`}>
      <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-white">Top Programs</h2>
      <p className="mt-1 text-sm text-slate-400">By total awarded</p>
      {programs.length === 0 ? (
        <EmptyState>No program data available yet.</EmptyState>
      ) : (
        <div className="mt-5 space-y-5">
          {programs.map((program) => (
            <div key={program.label} className="min-w-0">
              <div className="flex items-start justify-between gap-4">
                <p className="line-clamp-2 text-sm leading-5 text-slate-200">{program.label}</p>
                <p className="shrink-0 font-semibold tabular-nums text-white">{formatCurrencyCompact(program.value)}</p>
              </div>
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
                <div className="h-full rounded-full bg-emerald-300" style={{ width: `${Math.max(6, (program.value / max) * 100)}%` }} />
              </div>
            </div>
          ))}
        </div>
      )}
      <div className="mt-5 flex justify-center">
        <button type="button" className="h-10 rounded-lg border border-slate-700/80 px-4 text-sm font-semibold text-slate-100 transition hover:border-slate-500 hover:text-white">
          View all programs
        </button>
      </div>
      <p className="sr-only">Program values are normalized against {formatCurrencyCompact(totalAwarded)} in total department awards.</p>
    </section>
  );
}

function ContractPanel({ title, items }: { title: string; items: DepartmentContractItem[] }) {
  return (
    <section className={`${panelClassName} min-w-0 p-4`}>
      <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-white">{title}</h2>
      {items.length === 0 ? (
        <EmptyState>No contract activity available for this department yet.</EmptyState>
      ) : (
        <div className="mt-4 space-y-3">
          {items.slice(0, 5).map((item) => (
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
    <div className="rounded-lg border border-slate-800 bg-slate-950/45 p-4">
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

function PanelHeader({ title }: { title: string }) {
  return (
    <div className="flex items-center gap-2">
      <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-white">{title}</h2>
      <span className="flex h-4 w-4 items-center justify-center rounded-full border border-slate-600 text-[10px] text-slate-400">i</span>
    </div>
  );
}

function DepartmentSeal({ name }: { name: string }) {
  return (
    <div className="mt-8 hidden h-24 w-24 shrink-0 items-center justify-center rounded-full border-2 border-amber-300/80 bg-[radial-gradient(circle_at_50%_42%,rgba(56,189,248,0.5),rgba(15,23,42,0.78)_52%,rgba(15,23,42,1)_70%)] shadow-[0_0_32px_rgba(14,165,233,0.18)] ring-4 ring-slate-900 sm:flex">
      <div className="flex h-16 w-16 items-center justify-center rounded-full border border-sky-300/50 bg-slate-950/60 text-center text-lg font-bold tracking-wide text-amber-100">
        {departmentAcronym(name)}
      </div>
    </div>
  );
}

function TickerAvatar({ symbol, index }: { symbol: string; index: number }) {
  const color = palette[index % palette.length];
  return (
    <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full border border-white/10 bg-slate-900 text-[10px] font-semibold text-white" style={{ boxShadow: `inset 0 0 0 1px ${color}55` }}>
      {symbol.slice(0, 2)}
    </span>
  );
}

function IconFrame({ children, tone, label }: { children: ReactNode; tone: Tone; label: string }) {
  const toneClassName = {
    emerald: "border-emerald-400/40 text-emerald-300 shadow-emerald-400/10",
    sky: "border-sky-400/40 text-sky-300 shadow-sky-400/10",
    violet: "border-violet-400/40 text-violet-300 shadow-violet-400/10",
    amber: "border-amber-400/40 text-amber-300 shadow-amber-400/10",
    lime: "border-lime-400/40 text-lime-300 shadow-lime-400/10",
    muted: "border-slate-600 text-slate-400 shadow-slate-400/10",
  }[tone];
  return (
    <span aria-label={label} className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-full border bg-slate-950/70 shadow-lg ${toneClassName}`}>
      {children}
    </span>
  );
}

type Tone = "emerald" | "sky" | "violet" | "amber" | "lime" | "muted";
type IconName = "dollar" | "file" | "building" | "calendar" | "trophy" | "feed" | "grid" | "star" | "sort";

function Icon({ name }: { name: IconName }) {
  const paths: Record<IconName, ReactNode> = {
    dollar: <path d="M12 3v18M16 7.5c-1-1-2.3-1.5-4-1.5-2.3 0-4 1.1-4 2.8 0 4.2 8 1.7 8 6.4 0 1.7-1.7 2.8-4 2.8-1.9 0-3.4-.6-4.5-1.8" />,
    file: <path d="M7 3h7l3 3v15H7zM14 3v4h4M10 12h4M10 16h4" />,
    building: <path d="M5 21h14M7 21V5h10v16M10 8h1M13 8h1M10 12h1M13 12h1M10 16h1M13 16h1" />,
    calendar: <path d="M7 3v4M17 3v4M4 9h16M5 5h14v16H5zM8 13h3M8 17h3M14 13h2" />,
    trophy: <path d="M8 4h8v4a4 4 0 0 1-8 0zM8 6H5a3 3 0 0 0 3 3M16 6h3a3 3 0 0 1-3 3M12 12v5M9 21h6M10 17h4" />,
    feed: <path d="M5 5a14 14 0 0 1 14 14M5 11a8 8 0 0 1 8 8M6 18h.01" />,
    grid: <path d="M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z" />,
    star: <path d="m12 3 2.7 5.6 6.1.9-4.4 4.3 1 6.1-5.4-2.9-5.4 2.9 1-6.1-4.4-4.3 6.1-.9z" />,
    sort: <path d="M8 7h8M10 12h6M12 17h4M5 7v10M3 15l2 2 2-2" />,
  };
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8">
      {paths[name]}
    </svg>
  );
}

function EmptyState({ children }: { children: string }) {
  return (
    <div className="mt-4 rounded-lg border border-dashed border-slate-700 bg-slate-950/30 p-6 text-sm text-slate-400">
      {children}
    </div>
  );
}

function normalizeBreakdown(items: DepartmentBreakdownItem[] | null | undefined): DepartmentBreakdownItem[] {
  return (items ?? []).filter((item) => item && item.value > 0).sort((left, right) => right.value - left.value);
}

function labelPeriods(points: DepartmentTrendPoint[]): string[] {
  if (points.length <= 4) return points.map((point) => point.period);
  return [points[0].period, points[Math.floor(points.length / 3)].period, points[Math.floor((points.length * 2) / 3)].period, points[points.length - 1].period];
}

function periodDeltaPct(points: DepartmentTrendPoint[]): number | null {
  if (points.length < 2) return null;
  const current = points[points.length - 1].totalAwarded;
  const previous = points[points.length - 2].totalAwarded;
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous === 0) return null;
  return ((current - previous) / previous) * 100;
}

function formatTrendPeriod(period: string): string {
  const [year, month] = period.split("-");
  if (!year || !month) return period;
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, 1));
  return date.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
}

function departmentAcronym(name: string): string {
  const words = name.replace(/^Department of\s+/i, "").split(/\s+/).filter(Boolean);
  if (name === "NASA") return "NASA";
  const letters = ["D", ...words.slice(0, 2).map((word) => word[0]?.toUpperCase()).filter(Boolean)];
  return letters.join("").slice(0, 4);
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatCurrencyCompact(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(abs >= 100_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(abs >= 100_000 ? 0 : 1)}K`;
  return formatCurrency(value);
}
