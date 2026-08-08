import Link from "next/link";
import type { ReactNode } from "react";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import type { ProfileActivityItem, ProfileMetric, ProfileSectorPeriod, ProfileSummaryCard } from "@/lib/api";
import { cardClassName, ghostButtonClassName, tickerLinkClassName } from "@/lib/styles";
import { formatCurrency, formatDateShort } from "@/lib/format";

const chartColors = ["#34d399", "#38bdf8", "#a78bfa", "#fbbf24", "#fb7185", "#60a5fa", "#a3e635", "#94a3b8"];

export function ProfilePageHeader({
  eyebrow,
  title,
  subtitle,
  actions,
}: {
  eyebrow: string;
  title: string;
  subtitle: string;
  actions?: ReactNode;
}) {
  return (
    <section className="flex min-w-0 flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
      <div className="min-w-0">
        <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300/75">{eyebrow}</p>
        <h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">{title}</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-300/85">{subtitle}</p>
      </div>
      {actions ? <div className="flex min-w-0 flex-wrap gap-2 lg:justify-end">{actions}</div> : null}
    </section>
  );
}

export function SummaryCards({ cards }: { cards: ProfileSummaryCard[] }) {
  return (
    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {cards.map((card) => (
        <Link key={card.kind} href={card.href} prefetch={false} className={`${cardClassName} group min-h-56 p-5 transition hover:border-emerald-300/30 hover:bg-slate-900/85`}>
          <div className="flex items-start justify-between gap-3">
            <span className="flex h-10 w-10 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 text-lg text-emerald-200">
              {iconForKind(card.kind)}
            </span>
            {card.locked ? <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-amber-100">Pro</span> : null}
          </div>
          <h2 className="mt-4 text-lg font-semibold text-white">{card.title}</h2>
          <p className="mt-2 min-h-12 text-sm leading-6 text-slate-400">{card.description}</p>
          <div className="mt-4 grid grid-cols-2 gap-2">
            {card.metrics.slice(0, 2).map((metric) => (
              <MetricMini key={metric.label} metric={metric} />
            ))}
          </div>
          <p className="mt-4 text-sm font-semibold text-emerald-200 group-hover:text-emerald-100">View {card.title} -&gt;</p>
        </Link>
      ))}
    </section>
  );
}

export function MetricGrid({ metrics }: { metrics: ProfileMetric[] }) {
  return (
    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
      {metrics.map((metric) => (
        <div key={metric.label} className={`${cardClassName} min-h-32 p-4`}>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{metric.label}</p>
          <p className="mt-3 truncate text-2xl font-semibold tabular-nums text-white">{formatMetricValue(metric)}</p>
          {typeof metric.change_pct === "number" ? (
            <p className={`mt-2 text-xs font-semibold tabular-nums ${metric.change_pct >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
              {metric.change_pct >= 0 ? "+" : ""}{metric.change_pct.toFixed(1)}% vs prior
            </p>
          ) : (
            <p className="mt-2 text-xs text-slate-500">Latest available period</p>
          )}
        </div>
      ))}
    </section>
  );
}

export function FilterLinks({
  label,
  options,
  active,
}: {
  label: string;
  active: string;
  options: Array<{ label: string; value: string; href: string }>;
}) {
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-2 rounded-2xl border border-white/10 bg-slate-950/40 p-2">
      <span className="px-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</span>
      {options.map((option) => (
        <Link
          key={option.value}
          href={option.href}
          prefetch={false}
          className={`rounded-full px-3 py-1.5 text-xs font-semibold transition ${
            active === option.value ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30" : "text-slate-300 hover:bg-white/5 hover:text-white"
          }`}
        >
          {option.label}
        </Link>
      ))}
    </div>
  );
}

export function ActivityFeed({ items, activeType = "all" }: { items: ProfileActivityItem[]; activeType?: string }) {
  const filters = [
    ["all", "All"],
    ["congress", "Congress"],
    ["insiders", "Insiders"],
    ["institutions", "Institutions"],
    ["departments", "Departments"],
  ] as const;
  return (
    <section className={`${cardClassName} min-w-0 p-0`}>
      <div className="flex flex-col gap-3 border-b border-white/10 p-4 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-white">Latest Profile Activity</h2>
          <p className="text-sm text-slate-400">Recent profile-linked events across Walnut datasets.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          {filters.map(([value, label]) => (
            <Link
              key={value}
              href={value === "all" ? "/profiles" : `/profiles?type=${value}`}
              prefetch={false}
              className={`rounded-full px-3 py-1 text-xs font-semibold ${activeType === value ? "bg-emerald-400/15 text-emerald-100" : "border border-white/10 text-slate-300 hover:text-white"}`}
            >
              {label}
            </Link>
          ))}
        </div>
      </div>
      <ActivityTable items={items} />
    </section>
  );
}

export function ActivityTable({ items }: { items: ProfileActivityItem[] }) {
  if (!items.length) return <EmptyState>No profile activity available for this filter.</EmptyState>;
  return (
    <div className="min-w-0 overflow-x-auto">
      <table className="min-w-[64rem] w-full text-left text-sm">
        <thead className="border-b border-white/10 text-[11px] uppercase tracking-[0.16em] text-slate-500">
          <tr>
            <th className="px-4 py-3 font-semibold">Time</th>
            <th className="px-4 py-3 font-semibold">Type</th>
            <th className="px-4 py-3 font-semibold">Profile</th>
            <th className="px-4 py-3 font-semibold">Ticker / Company</th>
            <th className="px-4 py-3 font-semibold">Activity</th>
            <th className="px-4 py-3 text-right font-semibold">Value</th>
            <th className="px-4 py-3 text-right font-semibold">Score</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {items.map((item) => (
            <tr key={String(item.id)} className="text-slate-300">
              <td className="whitespace-nowrap px-4 py-3 text-slate-400">{formatDateShort(item.time ?? null)}</td>
              <td className="px-4 py-3"><TypePill type={item.type} /></td>
              <td className="px-4 py-3">{item.profile_href ? <Link href={item.profile_href} className="font-semibold text-white hover:text-emerald-100" prefetch={false}>{item.profile}</Link> : <span className="font-semibold text-white">{item.profile}</span>}</td>
              <td className="px-4 py-3">
                {item.ticker_href && item.symbol ? <Link href={item.ticker_href} className={tickerLinkClassName} prefetch={false}>{item.symbol}</Link> : <span className="font-mono text-slate-200">{item.symbol ?? "-"}</span>}
                <span className="ml-2 text-slate-500">{item.company && item.company !== item.symbol ? item.company : null}</span>
              </td>
              <td className="px-4 py-3 text-slate-200">{item.activity ?? "-"}</td>
              <td className="px-4 py-3 text-right font-semibold tabular-nums text-white">{formatCompactCurrency(item.value)}</td>
              <td className="px-4 py-3 text-right tabular-nums text-slate-300">{formatNumber(item.metric)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function DataPanel({
  title,
  subtitle,
  rows,
  columns,
  cta,
  empty,
}: {
  title: string;
  subtitle?: string;
  rows: Array<Record<string, unknown>>;
  columns: Array<{ key: string; label: string; align?: "right"; format?: "currency" | "number" | "percent" | "date" | "link" }>;
  cta?: { href: string; label: string };
  empty: string;
}) {
  return (
    <section className={`${cardClassName} min-w-0 p-0`}>
      <div className="border-b border-white/10 p-4">
        <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-white">{title}</h2>
        {subtitle ? <p className="mt-1 text-sm text-slate-400">{subtitle}</p> : null}
      </div>
      {!rows.length ? (
        <EmptyState>{empty}</EmptyState>
      ) : (
        <div className="min-w-0 overflow-x-auto">
          <table className="min-w-[42rem] w-full text-left text-sm">
            <thead className="border-b border-white/10 text-[11px] uppercase tracking-[0.16em] text-slate-500">
              <tr>
                {columns.map((column) => (
                  <th key={column.key} className={`px-4 py-3 font-semibold ${column.align === "right" ? "text-right" : ""}`}>{column.label}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((row, index) => (
                <tr key={`${title}-${index}`} className="text-slate-300">
                  {columns.map((column) => (
                    <td key={column.key} className={`px-4 py-3 ${column.align === "right" ? "text-right tabular-nums" : ""}`}>
                      {cellValue(row, column)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {cta ? (
        <div className="border-t border-white/10 p-4">
          <Link href={cta.href} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100" prefetch={false}>{cta.label} -&gt;</Link>
        </div>
      ) : null}
    </section>
  );
}

export function SectorStackedChart({ title, rows, note }: { title: string; rows: ProfileSectorPeriod[]; note?: string }) {
  return (
    <section className={`${cardClassName} min-w-0`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-white">{title}</h2>
          <p className="mt-1 text-sm text-slate-400">Allocation share by reported value.</p>
        </div>
        <div className="inline-flex rounded-xl border border-white/10 bg-slate-950/50 p-1">
          {["Top 5", "Top 8", "All"].map((label, index) => (
            <span key={label} className={`rounded-lg px-3 py-1 text-xs font-semibold ${index === 1 ? "bg-emerald-400/15 text-emerald-100" : "text-slate-500"}`}>{label}</span>
          ))}
        </div>
      </div>
      {!rows.length ? (
        <EmptyState>No sector exposure available for this period.</EmptyState>
      ) : (
        <div className="mt-5 space-y-4">
          {rows.map((row) => (
            <div key={row.period} className="grid gap-2 md:grid-cols-[5rem_minmax(0,1fr)] md:items-center">
              <div className="text-xs font-semibold text-slate-400">{row.period}</div>
              <div className="flex h-8 min-w-0 overflow-hidden rounded-lg border border-white/10 bg-slate-950/60">
                {row.segments.slice(0, 8).map((segment, index) => (
                  <span
                    key={`${row.period}-${segment.label}`}
                    title={`${segment.label}: ${segment.percent.toFixed(1)}%`}
                    className="h-full"
                    style={{ width: `${Math.max(segment.percent, 1)}%`, backgroundColor: chartColors[index % chartColors.length] }}
                  />
                ))}
              </div>
            </div>
          ))}
          <Legend labels={rows[rows.length - 1]?.segments.slice(0, 8).map((segment) => segment.label) ?? []} />
        </div>
      )}
      {note ? <p className="mt-4 text-xs leading-5 text-slate-500">{note}</p> : null}
    </section>
  );
}

export function TimeSeriesPanel({ title, rows, empty }: { title: string; rows: Array<Record<string, unknown>>; empty: string }) {
  const max = Math.max(...rows.map((row) => numeric(row.value)), 1);
  return (
    <section className={`${cardClassName} min-w-0`}>
      <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-white">{title}</h2>
      {!rows.length ? (
        <EmptyState>{empty}</EmptyState>
      ) : (
        <div className="mt-5 flex h-56 items-end gap-2 overflow-x-auto border-b border-l border-white/10 px-2 pt-4">
          {rows.map((row) => {
            const value = numeric(row.value);
            return (
              <div key={String(row.period)} className="flex min-w-14 flex-1 flex-col items-center justify-end gap-2">
                <div className="w-full rounded-t bg-emerald-300/80" style={{ height: `${Math.max(4, (value / max) * 180)}px` }} title={`${String(row.period)} ${formatCompactCurrency(value)}`} />
                <span className="text-[10px] text-slate-500">{String(row.period)}</span>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

export function LockedPanel({ message }: { message?: string | null }) {
  return (
    <section className={`${cardClassName} border-emerald-300/20 bg-emerald-300/5`}>
      <p className="text-base font-semibold text-emerald-100">Institutional detail requires Pro.</p>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">{message ?? "Upgrade to unlock reported 13F holdings, portfolio changes, and institution-level rankings."}</p>
      <Link href="/pricing" className={`${ghostButtonClassName} mt-4 border-emerald-300/30 bg-emerald-300/10 text-emerald-100`} prefetch={false}>Upgrade to Pro</Link>
    </section>
  );
}

export function LandingLoading({ eyebrow }: { eyebrow: string }) {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300/75">{eyebrow}</p>
        <SkeletonBlock className="mt-3 h-9 w-80" />
        <SkeletonBlock className="mt-3 h-4 w-full max-w-3xl" />
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, index) => (
          <div key={index} className={cardClassName}><SkeletonBlock className="h-20 w-full" /></div>
        ))}
      </div>
      <div className={cardClassName}><SkeletonTable columns={6} rows={6} /></div>
    </div>
  );
}

function MetricMini({ metric }: { metric: ProfileMetric }) {
  return (
    <div className="rounded-xl border border-white/10 bg-slate-950/45 p-3">
      <p className="truncate text-lg font-semibold tabular-nums text-white">{formatMetricValue(metric)}</p>
      <p className="mt-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">{metric.label}</p>
    </div>
  );
}

function TypePill({ type }: { type: string }) {
  return <span className="rounded-full border border-white/10 bg-white/[0.04] px-2.5 py-1 text-xs font-semibold text-slate-200">{type}</span>;
}

function EmptyState({ children }: { children: string }) {
  return <div className="m-4 rounded-xl border border-dashed border-white/10 bg-slate-950/35 p-5 text-sm text-slate-400">{children}</div>;
}

function Legend({ labels }: { labels: string[] }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-2">
      {labels.map((label, index) => (
        <span key={label} className="inline-flex items-center gap-2 text-xs text-slate-400">
          <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: chartColors[index % chartColors.length] }} />
          {label}
        </span>
      ))}
    </div>
  );
}

function cellValue(row: Record<string, unknown>, column: { key: string; format?: string }) {
  const href = typeof row.href === "string" ? row.href : null;
  const value = row[column.key];
  if (column.format === "currency") return <span className="font-semibold text-white">{formatCompactCurrency(numericOrNull(value))}</span>;
  if (column.format === "number") return formatNumber(numericOrNull(value));
  if (column.format === "percent") return formatPercent(numericOrNull(value));
  if (column.format === "date") return formatDateShort(typeof value === "string" ? value : null);
  if (column.format === "link" && href && typeof value === "string") {
    return <Link href={href} className={value === String(row.symbol) ? tickerLinkClassName : "font-semibold text-white hover:text-emerald-100"} prefetch={false}>{value}</Link>;
  }
  if (column.key === "largest_holding") {
    const holding = value && typeof value === "object" ? value as Record<string, unknown> : null;
    return holding ? `${String(holding.symbol ?? "")} ${formatCompactCurrency(numericOrNull(holding.value))}` : "-";
  }
  return typeof value === "string" || typeof value === "number" ? String(value) : "-";
}

function iconForKind(kind: string) {
  if (kind === "congress") return "C";
  if (kind === "insiders") return "I";
  if (kind === "institutions") return "13F";
  if (kind === "departments") return "G";
  return "P";
}

export function formatMetricValue(metric: ProfileMetric) {
  return metric.format === "currency" ? formatCompactCurrency(metric.value) : formatNumber(metric.value);
}

export function formatCompactCurrency(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000_000) return `$${(value / 1_000_000_000_000).toFixed(abs >= 10_000_000_000_000 ? 0 : 1)}T`;
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(abs >= 100_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(abs >= 100_000 ? 0 : 1)}K`;
  return formatCurrency(value);
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function numeric(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function numericOrNull(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
