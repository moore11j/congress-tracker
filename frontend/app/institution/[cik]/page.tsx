import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import {
  getInstitutionActivity,
  getInstitutionFilings,
  getInstitutionHoldings,
  getInstitutionProfile,
  type InstitutionActivityItem,
  type InstitutionFilingItem,
  type InstitutionHoldingItem,
  type InstitutionProfileResponse,
} from "@/lib/api";
import { HoldingsAllocationChart } from "@/components/institution/HoldingsAllocationChart";
import { normalizeInstitutionCik } from "@/lib/institution";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { withServerTimeout } from "@/lib/serverTimeout";
import { tickerHref } from "@/lib/ticker";
import { cardClassName, ghostButtonClassName, tickerLinkClassName } from "@/lib/styles";
import { formatCurrency, formatDateShort } from "@/lib/format";

type Props = {
  params: Promise<{ cik: string }>;
};

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { cik: rawCik } = await params;
  const cik = normalizeInstitutionCik(rawCik);
  if (!cik) {
    return { title: "Institution | Walnut Market Terminal" };
  }
  try {
    const profile = await getInstitutionProfile(cik, { source: "InstitutionMetadata" });
    return {
      title: `${profile.holder_name ?? "Institution"} 13F Profile | Walnut Market Terminal`,
      description: "Reported 13F holdings, filing history, and quarter-over-quarter institutional activity.",
    };
  } catch {
    return { title: "Institution | Walnut Market Terminal" };
  }
}

export default async function InstitutionPage({ params }: Props) {
  const { cik: rawCik } = await params;
  const cik = normalizeInstitutionCik(rawCik);
  if (!cik) notFound();

  const authState = await optionalPageAuthState();
  const authToken = authState.token ?? undefined;
  const profile = await withServerTimeout(
    getInstitutionProfile(cik, { authToken, source: "InstitutionProfilePage" }),
    "institution:profile",
    12000,
  ).catch(() => unavailableInstitutionProfile(cik));
  if (profile.locked) {
    return <LockedInstitutionProfile cik={cik} />;
  }

  const [holdings, activity, filings] = await Promise.all([
    withServerTimeout(
      getInstitutionHoldings(cik, {
        year: profile.latest_report_year ?? undefined,
        quarter: profile.latest_report_quarter ?? undefined,
        limit: 50,
        authToken,
        source: "InstitutionProfileHoldings",
      }),
      "institution:holdings",
      10000,
    ).catch(() => ({ items: [] })),
    withServerTimeout(
      getInstitutionActivity(cik, { limit: 25, authToken, source: "InstitutionProfileActivity" }),
      "institution:activity",
      8000,
    ).catch(() => ({ items: [] })),
    withServerTimeout(
      getInstitutionFilings(cik, { limit: 25, authToken, source: "InstitutionProfileFilings" }),
      "institution:filings",
      8000,
    ).catch(() => ({ items: [] })),
  ]);

  const unavailable = profile.availability_status === "unavailable" || profile.status === "no_data";
  const name = profile.holder_name ?? "Institution unavailable";
  const reportPeriod = profile.latest_report_year && profile.latest_report_quarter
    ? `Q${profile.latest_report_quarter} ${profile.latest_report_year}`
    : "Unavailable";
  const allocationHoldings = profile.top_holdings?.length ? profile.top_holdings : (holdings.items ?? []);
  const hasRetryableFiling = (filings.items ?? []).some((item) => (item.status ?? "").toLowerCase() === "retryable");

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <section className={`${cardClassName} min-w-0 space-y-5`}>
        <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
          <div className="min-w-0 max-w-4xl">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Institutional Activity</p>
            <h1 className="mt-2 break-words text-3xl font-semibold text-white md:text-4xl">{name}</h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300">
              Reported 13F holdings and quarter-over-quarter changes by filing date.
            </p>
          </div>
          <div className="flex min-w-0 flex-wrap gap-2">
            <Link href="/feed?mode=institutional" className={ghostButtonClassName} prefetch={false}>
              Institutional feed
            </Link>
            <Link href="/signals?mode=institutional" className={ghostButtonClassName} prefetch={false}>
              Institutional signals
            </Link>
          </div>
        </div>

        {unavailable ? (
          <UnavailableProfile />
        ) : null}

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
          <StatCard label="CIK" value={cik} compact />
          <StatCard label="Latest Filing Date" value={formatDateShort(profile.latest_filing_date ?? null)} />
          <StatCard label="Latest Report Period" value={reportPeriod} />
          <StatCard label="Total Reported Value" value={formatCompactCurrency(profile.total_reported_value_usd ?? profile.total_reported_value)} />
          <StatCard label="Holdings" value={formatCount(profile.holdings_count)} />
          <StatCard label="Source" value={profile.source_label ?? "Institutional Activity"} compact />
        </div>
      </section>

      <HoldingsAllocationChart
        holdings={allocationHoldings}
        totalReportedValue={profile.total_reported_value_usd ?? profile.total_reported_value}
        holdingsCount={profile.holdings_count}
        reportPeriod={reportPeriod}
        filingDate={profile.latest_filing_date}
        hasRetryableFiling={hasRetryableFiling}
      />
      <ActivitySection items={activity.items ?? []} />
      <HoldingsSection items={holdings.items ?? []} />
      <FilingsSection items={filings.items ?? []} />
    </div>
  );
}

function unavailableInstitutionProfile(cik: string): InstitutionProfileResponse {
  return {
    status: "temporarily_unavailable",
    cik,
    holder_name: null,
    latest_filing_date: null,
    latest_report_year: null,
    latest_report_quarter: null,
    total_reported_value: null,
    total_reported_value_usd: null,
    holdings_count: 0,
    source_label: "Institutional Activity",
    availability_status: "unavailable",
    locked: false,
    top_holdings: [],
  };
}

function LockedInstitutionProfile({ cik }: { cik: string }) {
  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <section className={`${cardClassName} min-w-0 space-y-4`}>
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Institutional Activity</p>
        <h1 className="break-words text-3xl font-semibold text-white md:text-4xl">Institutional profile locked</h1>
        <div className="rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-5">
          <p className="text-base font-semibold text-emerald-100">Institutional profiles are available on Pro.</p>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">
            Unlock reported 13F holdings, portfolio allocation, and recent institutional activity.
          </p>
          <div className="mt-4 flex flex-wrap items-center gap-2">
            <Link href="/pricing" className={ghostButtonClassName} prefetch={false}>
              Upgrade to Pro
            </Link>
            <span className="text-xs text-slate-500">CIK {cik}</span>
          </div>
        </div>
      </section>
    </div>
  );
}

function UnavailableProfile() {
  return (
    <div className="rounded-2xl border border-dashed border-white/10 bg-slate-950/30 p-5">
      <p className="text-sm font-semibold text-slate-100">Institutional profile unavailable.</p>
      <p className="mt-1 text-sm leading-6 text-slate-400">
        No processed 13F holdings are available for this institution yet.
      </p>
    </div>
  );
}

function ActivitySection({ items }: { items: InstitutionActivityItem[] }) {
  return (
    <section className={`${cardClassName} min-w-0`}>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Recent Institutional Activity</h2>
          <p className="text-sm text-slate-400">Quarter-over-quarter changes reported by filing date.</p>
        </div>
        <span className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">13F filing activity</span>
      </div>
      {items.length === 0 ? (
        <EmptyState>No recent institutional activity available.</EmptyState>
      ) : (
        <div className="space-y-3">
          {items.map((item) => (
            <div key={`${item.id}-${item.symbol}`} className="rounded-2xl border border-white/10 bg-slate-950/45 p-4">
              <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto_auto] md:items-center">
                <div className="min-w-0">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    {item.symbol && tickerHref(item.symbol) ? (
                      <Link href={tickerHref(item.symbol)!} className={tickerLinkClassName} prefetch={false}>
                        {item.symbol}
                      </Link>
                    ) : (
                      <span className="font-mono text-sm font-semibold text-slate-100">{item.symbol ?? "-"}</span>
                    )}
                    <span className={`text-xs font-semibold uppercase tracking-[0.12em] ${actionTextClass(normalizeActivityAction(item.action))}`}>
                      {normalizeActivityAction(item.action)}
                    </span>
                  </div>
                  <p className="mt-1 truncate text-sm text-slate-400">{item.issuer_name ?? "Company unavailable"}</p>
                </div>
                <div className="text-sm text-slate-300 md:text-right">
                  <p>Filing date: <span className="text-slate-100">{formatDateShort(item.filing_date ?? null)}</span></p>
                  <p>Report period: <span className="text-slate-100">{item.report_period ?? "Unavailable"}</span></p>
                </div>
                <div className="text-left md:text-right">
                  <p className="text-base font-semibold tabular-nums text-white">{formatCompactCurrency(item.current_value_usd)}</p>
                  <p className={`mt-1 text-xs tabular-nums ${deltaTextClass(item.value_delta_usd)}`}>
                    Change {formatSignedMoneyCompact(item.value_delta_usd ?? 0)}
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function HoldingsSection({ items }: { items: InstitutionHoldingItem[] }) {
  return (
    <section className={`${cardClassName} min-w-0`}>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Most Recent Reported Holdings</h2>
          <p className="text-sm text-slate-400">Sorted by reported value.</p>
        </div>
      </div>
      {items.length === 0 ? (
        <EmptyState>No reported holdings available for this period.</EmptyState>
      ) : (
        <div className="min-w-0 overflow-x-auto">
          <table className="min-w-full divide-y divide-white/10 text-left text-sm">
            <thead className="text-xs uppercase tracking-[0.14em] text-slate-500">
              <tr>
                <th className="py-3 pr-4 font-semibold">Symbol</th>
                <th className="px-4 py-3 font-semibold">Company</th>
                <th className="px-4 py-3 text-right font-semibold">Reported Value</th>
                <th className="px-4 py-3 text-right font-semibold">Shares</th>
                <th className="px-4 py-3 text-right font-semibold">Weight %</th>
                <th className="px-4 py-3 text-right font-semibold">Ownership %</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {items.map((item) => (
                <tr key={`${item.id}-${item.symbol}-${item.cusip}`} className="align-top text-slate-300">
                  <td className="py-3 pr-4">
                    {item.symbol && tickerHref(item.symbol) ? (
                      <Link href={tickerHref(item.symbol)!} className={tickerLinkClassName} prefetch={false}>
                        {item.symbol}
                      </Link>
                    ) : (
                      <span className="font-mono font-semibold text-slate-100">{item.symbol ?? "-"}</span>
                    )}
                  </td>
                  <td className="px-4 py-3 font-medium text-slate-100">{item.issuer_name ?? "Company unavailable"}</td>
                  <td className="px-4 py-3 text-right font-semibold tabular-nums text-white">{formatCompactCurrency(item.value_usd)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatNumber(item.shares)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatPct(item.portfolio_weight)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatPct(item.ownership_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function FilingsSection({ items }: { items: InstitutionFilingItem[] }) {
  return (
    <section className={`${cardClassName} min-w-0`}>
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-white">Filing History</h2>
        <p className="text-sm text-slate-400">13F filing dates and processing status.</p>
      </div>
      {items.length === 0 ? (
        <EmptyState>Filing detected; holdings are not available yet.</EmptyState>
      ) : (
        <div className="min-w-0 overflow-x-auto">
          <table className="min-w-full divide-y divide-white/10 text-left text-sm">
            <thead className="text-xs uppercase tracking-[0.14em] text-slate-500">
              <tr>
                <th className="py-3 pr-4 font-semibold">Year / Quarter</th>
                <th className="px-4 py-3 font-semibold">Filing Date</th>
                <th className="px-4 py-3 font-semibold">Form Type</th>
                <th className="px-4 py-3 text-right font-semibold">Holdings</th>
                <th className="px-4 py-3 font-semibold">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {items.map((item) => (
                <tr key={`${item.id}-${item.accession_number}`} className="text-slate-300">
                  <td className="py-3 pr-4 font-semibold text-slate-100">Q{item.report_quarter ?? "-"} {item.report_year ?? "-"}</td>
                  <td className="px-4 py-3 whitespace-nowrap">{formatDateShort(item.filing_date ?? null)}</td>
                  <td className="px-4 py-3">{item.form_type ?? "13F filing"}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(item.holdings_count)}</td>
                  <td className="px-4 py-3">{item.status ?? (item.processed_at ? "processed" : "retryable")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
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

function EmptyState({ children }: { children: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-white/10 bg-slate-950/30 p-6 text-sm text-slate-400">
      {children}
    </div>
  );
}

function formatCompactCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(abs >= 10_000 ? 0 : 1)}K`;
  return formatCurrency(value);
}

function formatSignedMoneyCompact(value: number): string {
  if (value > 0) return `+${formatCompactCurrency(value)}`;
  if (value < 0) return `-${formatCompactCurrency(Math.abs(value))}`;
  return "$0";
}

function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Math.round(value).toLocaleString();
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Math.round(value).toLocaleString();
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value.toFixed(2)}%`;
}

function actionTextClass(action?: string | null): string {
  const normalized = (action ?? "").toLowerCase();
  if (normalized.includes("reduction") || normalized.includes("exit")) return "text-rose-300";
  if (normalized.includes("increase") || normalized.includes("new")) return "text-emerald-300";
  return "text-slate-300";
}

function normalizeActivityAction(action?: string | null): string {
  const normalized = (action ?? "").toLowerCase().replace(/[_-]+/g, " ");
  if (normalized.includes("new position")) return "New Position";
  if (normalized.includes("exit")) return "Reported Exit";
  if (normalized.includes("reduction") || normalized.includes("reduced") || normalized.includes("decrease")) {
    return "Reported Reduction";
  }
  if (normalized.includes("increase") || normalized.includes("accumulation")) return "Reported Increase";
  return "Reported Activity";
}

function deltaTextClass(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "text-slate-400";
  if (value < 0) return "text-rose-300";
  if (value > 0) return "text-emerald-300";
  return "text-slate-400";
}
