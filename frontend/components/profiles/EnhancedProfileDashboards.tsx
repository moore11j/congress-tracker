import Link from "next/link";
import type { ReactNode } from "react";
import type { CongressOverviewResponse, InsidersOverviewResponse, InstitutionsOverviewResponse, ProfileActivityItem, ProfileMetric, ProfileSectorPeriod, ProfilesSummaryResponse } from "@/lib/api";

const COLORS = ["#42d3a7", "#3b82f6", "#a855f7", "#f6b91a", "#fb7185", "#60a5fa", "#a3e635", "#94a3b8", "#2dd4bf", "#f97316"];
const PROFILE_COLORS: Record<string, string> = { Congress: "#42d3a7", Insider: "#3b82f6", Institution: "#a855f7", Department: "#f6b91a" };

type Row = Record<string, unknown>;

export function EnhancedProfilesOverview({ data }: { data: ProfilesSummaryResponse }) {
  const activity = (data.activity ?? []).filter((item) => item.profile && item.profile !== "Profile unavailable");
  const categories = ["Congress", "Insider", "Institution", "Department"];
  const counts = categories.map((type) => activity.filter((item) => item.type === type).length);
  const total = counts.reduce((sum, value) => sum + value, 0);

  return (
    <main className="relative min-w-0 space-y-3 overflow-hidden pb-3">
      <OverviewGlow />
      <header className="relative z-10 pt-2">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-emerald-300">Profiles</p>
        <h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">Follow the market&apos;s major players</h1>
        <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">Track activity across Congress, corporate insiders, institutions, and government departments.</p>
      </header>
      <section className="relative z-10 grid gap-3 xl:grid-cols-4">
        {data.cards.map((card) => <OverviewCard key={card.kind} card={card} />)}
      </section>
      <section className="relative z-10 grid gap-3 xl:grid-cols-[1.25fr_.8fr_1.1fr]">
        <Panel title="Latest activity by profile type" action="Live database data">
          <ActivityBars items={activity} />
        </Panel>
        <Panel title="Where the activity is">
          <Donut values={counts} colors={categories.map((category) => PROFILE_COLORS[category])} label="Events" value={formatNumber(total)} />
          <div className="mt-4 space-y-2.5">
            {categories.map((category, index) => <LegendRow key={category} label={category} color={PROFILE_COLORS[category]} value={total ? `${((counts[index] / total) * 100).toFixed(1)}%` : "-"} />)}
          </div>
        </Panel>
        <Panel title="Current profile coverage">
          <div className="space-y-3 pt-1">
            {data.cards.map((card, index) => {
              const primary = card.metrics[0];
              return <div key={card.kind} className="grid grid-cols-[7rem_minmax(0,1fr)_4.25rem] items-center gap-3 text-xs">
                <span className="truncate text-slate-300">{card.title}</span>
                <div className="h-2.5 overflow-hidden rounded-sm bg-slate-800"><div className="h-full rounded-sm" style={{ width: `${total ? Math.max(7, (counts[index] / total) * 100) : 7}%`, backgroundColor: COLORS[index] }} /></div>
                <span className="text-right font-semibold tabular-nums text-white">{formatMetric(primary)}</span>
              </div>;
            })}
          </div>
        </Panel>
      </section>
      <section className="relative z-10 grid gap-3 xl:grid-cols-[1.16fr_1.84fr]">
        <Panel title="Latest profile activity" action={<Link href="/feed" className="text-emerald-200 hover:text-emerald-100">View feed -&gt;</Link>}><CompactActivity items={activity} /></Panel>
        <Panel title="Most active profiles">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {(data.directories?.length ? data.directories : data.cards.map((card) => ({ kind: card.kind, title: card.title, href: card.href, primary_title: card.title, primary_items: activity.filter((item) => profileKind(item.type) === card.kind).sort((left, right) => (right.value ?? 0) - (left.value ?? 0)).slice(0, 5).map((item) => ({ label: item.profile, value: item.value, value_format: "currency", href: item.profile_href })) }))).map((directory) => <DirectoryLeaders key={directory.kind} title={directory.primary_title || directory.title} href={directory.href} rows={directory.primary_items ?? []} />)}
          </div>
        </Panel>
      </section>
    </main>
  );
}

export function EnhancedCongressDashboard({ data, chamberFilter }: { data: CongressOverviewResponse; chamberFilter: ReactNode }) {
  return <ProfileDashboard
    flavor="congress"
    eyebrow="CONGRESS"
    title="Congress Trading"
    subtitle="Track disclosed trades, portfolio activity, and market positioning across members of Congress."
    filter={chamberFilter}
    comparison={`previous ${data.period_days} days`}
    snapshotTitle="Congress trading snapshot"
    snapshotSeries={periodTotals(data.sector_exposure)}
    snapshotLabel="Reported activity across the selected period"
    metrics={data.summary}
    primary={{ title: "Top members by portfolio value", rows: data.top_members, columns: [["name", "Member", "link"], ["party", "Party"], ["chamber", "Chamber"], ["estimated_portfolio_value", "Est. portfolio", "currency"], ["trades", "Trades", "number"]] }}
    secondary={{ title: "Most traded stocks", rows: data.most_traded_stocks, columns: [["symbol", "Ticker", "link"], ["company", "Company"], ["buy_value", "Buy value", "currency"], ["sell_value", "Sell value", "currency"], ["net_value", "Net value", "currency"]] }}
    sectorRows={data.sector_exposure}
    sectorTitle="Sector exposure over time"
    left={{ title: "Top buyers", rows: data.top_buyers, columns: [["name", "Member", "link"], ["trades", "Trades", "number"], ["value", "Buy value", "currency"], ["last_activity", "Latest", "date"]] }}
    right={{ title: "Top sellers", rows: data.top_sellers, columns: [["name", "Member", "link"], ["trades", "Trades", "number"], ["value", "Sell value", "currency"], ["last_activity", "Latest", "date"]] }}
    recent={data.recent_disclosures}
    note={data.note}
  />;
}

export function EnhancedInsiderDashboard({ data, periodFilter }: { data: InsidersOverviewResponse; periodFilter: ReactNode }) {
  return <ProfileDashboard
    flavor="insiders"
    eyebrow="INSIDERS"
    title="Corporate Insider Activity"
    subtitle="Track purchases and sales from executives, directors, and major shareholders."
    filter={periodFilter}
    comparison={`previous ${data.period_days} days`}
    snapshotTitle="Insider trading snapshot"
    snapshotSeries={periodTotals(data.sector_activity)}
    snapshotLabel="Net activity across verified open-market transactions"
    metrics={data.summary}
    primary={{ title: "Top insiders by net buying", rows: data.top_insiders, columns: [["name", "Insider", "link"], ["company", "Company"], ["role", "Role"], ["net_buy_value", "Net buy value", "currency"], ["trades", "Trades", "number"]] }}
    secondary={{ title: "Most traded stocks", rows: data.most_traded_stocks, columns: [["symbol", "Ticker", "link"], ["company", "Company"], ["buy_value", "Buy value", "currency"], ["sell_value", "Sell value", "currency"], ["net_value", "Net value", "currency"]] }}
    sectorRows={data.sector_activity}
    sectorTitle="Insider activity by sector"
    left={{ title: "Recent open-market purchases", rows: data.recent_purchases as Row[], columns: [["profile", "Insider", "link"], ["symbol", "Ticker", "link"], ["activity", "Action"], ["value", "Value", "currency"], ["time", "Date", "date"]] }}
    right={{ title: "Cluster buying", rows: data.cluster_buying, columns: [["symbol", "Ticker", "link"], ["company", "Company"], ["unique_insiders", "Insiders", "number"], ["buy_value", "Buy value", "currency"]] }}
    recent={data.largest_buys}
  />;
}

export function EnhancedInstitutionDashboard({ data, period }: { data: InstitutionsOverviewResponse; period: string }) {
  if (data.locked) {
    return <ProfileDashboard flavor="institutions" eyebrow="INSTITUTIONS" title="Institutional Holdings" subtitle="Track institutional portfolios, quarterly position changes, accumulation, and sector exposure." filter={<PeriodBadge label={period} />} comparison="previous comparable quarter" snapshotTitle="Institutional holdings snapshot" snapshotSeries={[]} snapshotLabel="Institutional detail is available with Pro." metrics={data.summary} primary={{ title: "Top institutions", rows: [], columns: [] }} secondary={{ title: "Top increased positions", rows: [], columns: [] }} sectorRows={[]} sectorTitle="Sector exposure over time" left={{ title: "Most widely held stocks", rows: [], columns: [] }} right={{ title: "Recent notable filings", rows: [], columns: [] }} recent={[]} lockedMessage={data.message} />;
  }
  return <ProfileDashboard
    flavor="institutions"
    eyebrow="INSTITUTIONS"
    title="Institutional Holdings"
    subtitle="Track institutional portfolios, quarterly position changes, accumulation, and sector exposure."
    filter={<PeriodBadge label={period} />}
    comparison="previous comparable quarter"
    snapshotTitle="Institutional holdings snapshot"
    snapshotSeries={periodTotals(data.sector_exposure)}
    snapshotLabel="Aggregate reported portfolio value by filing quarter"
    metrics={data.summary}
    primary={{ title: "Top institutions by portfolio value", rows: data.top_institutions, columns: [["name", "Institution", "link"], ["portfolio_value", "Portfolio value", "currency"], ["previous_value", "Previous quarter", "currency"], ["qoq_change", "QoQ change", "percent"], ["positions", "Positions", "number"]] }}
    secondary={{ title: "Top increased positions", rows: data.position_changes, columns: [["symbol", "Ticker", "link"], ["company", "Company"], ["current_value", "Current value", "currency"], ["previous_value", "Previous value", "currency"], ["increase_value", "Increase", "currency"]] }}
    sectorRows={data.sector_exposure}
    sectorTitle="Sector exposure over time"
    left={{ title: "Most widely held stocks", rows: data.most_widely_held, columns: [["symbol", "Ticker", "link"], ["company", "Company"], ["value", "Total value", "currency"], ["holders", "Institutions", "number"]] }}
    right={{ title: "Recent notable filings", rows: data.recent_filings, columns: [["name", "Institution", "link"], ["symbol", "Ticker", "link"], ["action", "Action"], ["value", "Value", "currency"], ["filing_date", "Date", "date"]] }}
    recent={data.largest_new_positions as unknown as ProfileActivityItem[]}
  />;
}

type TableSpec = { title: string; rows: Row[]; columns: Array<[string, string, CellFormat?]> };
type CellFormat = "currency" | "number" | "date" | "percent" | "link";

type DashboardFlavor = "congress" | "insiders" | "institutions";

function ProfileDashboard({ flavor, eyebrow, title, subtitle, filter, comparison, snapshotTitle, snapshotSeries, snapshotLabel, metrics, primary, secondary, sectorRows, sectorTitle, left, right, recent, note, lockedMessage }: { flavor: DashboardFlavor; eyebrow: string; title: string; subtitle: string; filter: ReactNode; comparison: string; snapshotTitle: string; snapshotSeries: Array<{ label: string; value: number }>; snapshotLabel: string; metrics: ProfileMetric[]; primary: TableSpec; secondary: TableSpec; sectorRows: ProfileSectorPeriod[]; sectorTitle: string; left: TableSpec; right: TableSpec; recent: ProfileActivityItem[]; note?: string; lockedMessage?: string | null }) {
  const snapshotStats = [primary.rows[0], secondary.rows[0], left.rows[0], right.rows[0]];
  return <main className="relative min-w-0 space-y-3 overflow-hidden pb-3">
    <OverviewGlow />
    <header className="relative z-10 flex flex-col gap-3 pt-2 md:flex-row md:items-end md:justify-between">
      <div><p className="text-xs font-semibold uppercase tracking-[0.28em] text-emerald-300">{eyebrow}</p><h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">{title}</h1><p className="mt-2 text-sm leading-6 text-slate-300">{subtitle}</p></div>
      {filter}
    </header>
      <section className="relative z-10 grid gap-3 xl:grid-cols-[1.68fr_.9fr]">
        <Panel title={snapshotTitle} subtitle={snapshotLabel} action="Live data"><TrendChart series={snapshotSeries} /></Panel>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-2">
        {snapshotStats.map((row, index) => <SnapshotCard key={index} row={row} label={snapshotLabels(flavor)[index]} />)}
      </div>
    </section>
    <MetricCards metrics={metrics} comparison={comparison} />
    {lockedMessage ? <Panel title="Institutional detail"><p className="text-sm leading-6 text-slate-300">{lockedMessage}</p><Link href="/pricing" className="mt-4 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">View Pro access -&gt;</Link></Panel> : <>
      <section className="grid gap-3 xl:grid-cols-2"><DataTable {...primary} /><DataTable {...secondary} /></section>
      <section className="grid gap-3 xl:grid-cols-[1.05fr_.85fr_.8fr]">
        <SectorPanel title={sectorTitle} rows={sectorRows} note={note} />
        <Panel title="Net activity by sector"><SectorMomentum rows={sectorRows} /></Panel>
        <Panel title={flavor === "institutions" ? "Increases vs decreases mix" : "Buy vs sell mix"}><ActivityMix flavor={flavor} metrics={metrics} /></Panel>
      </section>
      <section className="grid gap-3 xl:grid-cols-[.9fr_1fr_1.1fr]"><DataTable {...left} /><DataTable {...right} /><Panel title="Top moving sectors"><SectorMovers rows={sectorRows} /></Panel></section>
      <section className="grid gap-3 xl:grid-cols-[1.15fr_.85fr]"><Panel title="Activity over time" subtitle="Reported value by period"><ActivityColumns series={snapshotSeries} /></Panel><Panel title="Recent notable activity"><CompactActivity items={recent} /></Panel></section>
    </>}
  </main>;
}

function OverviewCard({ card }: { card: ProfilesSummaryResponse["cards"][number] }) {
  return <Link href={card.href} prefetch={false} className="group min-w-0 overflow-hidden rounded-lg border border-slate-700/70 bg-slate-950/65 p-4 transition hover:border-emerald-300/45 hover:bg-slate-900/80">
    <div className="flex items-start justify-between gap-3"><span className="flex h-10 w-10 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 text-lg text-emerald-200">{profileIcon(card.kind)}</span><span className="text-[11px] font-semibold uppercase tracking-[.16em] text-emerald-300">Live</span></div>
    <h2 className="mt-3 text-base font-semibold text-white">{card.title}</h2><p className="mt-1 min-h-10 text-xs leading-5 text-slate-300">{card.description}</p>
    <div className="mt-3 grid grid-cols-2 gap-2">{card.metrics.slice(0, 2).map((metric) => <div key={metric.label} className="rounded-md border border-slate-700/80 bg-slate-950/80 p-3"><p className="text-lg font-semibold tabular-nums text-white">{formatMetric(metric)}</p><p className="mt-1 truncate text-[10px] font-semibold uppercase tracking-[.12em] text-slate-500">{metric.label}</p></div>)}</div>
    <span className="mt-3 inline-flex text-sm font-semibold text-emerald-200">View {card.title} -&gt;</span>
  </Link>;
}

function Panel({ title, subtitle, action, children }: { title: string; subtitle?: string; action?: ReactNode; children: ReactNode }) {
  return <section className="min-w-0 rounded-lg border border-slate-700/70 bg-slate-950/65 p-4 shadow-[0_18px_50px_rgba(0,0,0,.18)]"><div className="flex flex-wrap items-start justify-between gap-2"><div><h2 className="text-sm font-semibold uppercase tracking-[.15em] text-white">{title}</h2>{subtitle ? <p className="mt-1 text-xs text-slate-400">{subtitle}</p> : null}</div>{action ? <span className="text-[10px] font-semibold uppercase tracking-[.12em] text-slate-400">{action}</span> : null}</div><div className="mt-4">{children}</div></section>;
}

function MetricCards({ metrics, comparison }: { metrics: ProfileMetric[]; comparison: string }) { return <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">{metrics.map((metric) => <div key={metric.label} className="min-w-0 rounded-lg border border-slate-700/70 bg-slate-950/65 p-4"><p className="truncate text-[10px] font-semibold uppercase tracking-[.16em] text-slate-500">{metric.label}</p><p className="mt-3 truncate text-2xl font-semibold tabular-nums text-white">{formatMetric(metric)}</p><p className={`mt-2 text-xs font-semibold tabular-nums ${typeof metric.change_pct === "number" ? metric.change_pct >= 0 ? "text-emerald-300" : "text-rose-300" : "text-slate-500"}`}>{typeof metric.change_pct === "number" ? `${metric.change_pct >= 0 ? "+" : ""}${metric.change_pct.toFixed(1)}% vs ${comparison}` : "Latest available period"}</p></div>)}</section>; }

function DataTable({ title, rows, columns }: TableSpec) { return <Panel title={title}>{!rows.length ? <p className="py-8 text-sm text-slate-400">No database records are available for this selection.</p> : <div className="overflow-x-auto"><table className="min-w-[38rem] w-full text-left text-xs"><thead className="border-b border-white/10 text-[10px] uppercase tracking-[.13em] text-slate-500"><tr>{columns.map(([, label]) => <th key={label} className="px-2 py-2.5 font-semibold">{label}</th>)}</tr></thead><tbody className="divide-y divide-white/5">{rows.slice(0, 10).map((row, index) => <tr key={index}>{columns.map(([key, , format]) => <td key={key} className="max-w-44 truncate px-2 py-2.5 text-slate-300">{renderCell(row, key, format)}</td>)}</tr>)}</tbody></table></div>}</Panel>; }

function SectorPanel({ title, rows, note }: { title: string; rows: ProfileSectorPeriod[]; note?: string }) { const labels = sectorLabels(rows); return <Panel title={title} subtitle="Latest-quarter ordering is kept across every period."><div className="space-y-3">{rows.map((row) => <div key={row.period} className="grid grid-cols-[3.5rem_minmax(0,1fr)] items-center gap-2"><span className="text-[10px] font-semibold text-slate-400">{row.period}</span><div className="flex h-8 overflow-hidden rounded-sm bg-slate-900">{orderedSegments(row, labels).map((segment) => <span key={segment.label} title={`${segment.label}: ${segment.percent.toFixed(1)}%`} style={{ width: `${Math.max(segment.percent, .8)}%`, backgroundColor: colorFor(segment.label, labels) }} />)}</div></div>)}</div><div className="mt-4 flex flex-wrap gap-x-3 gap-y-2">{labels.map((label) => <span key={label} className="inline-flex items-center gap-1.5 text-[10px] text-slate-400"><i className="h-2 w-2 rounded-full" style={{ backgroundColor: colorFor(label, labels) }} />{label}</span>)}</div>{note ? <p className="mt-4 text-xs leading-5 text-slate-500">{note}</p> : null}</Panel>; }

function SectorBreakdown({ rows }: { rows: ProfileSectorPeriod[] }) { const labels = sectorLabels(rows); const latest = rows[rows.length - 1]; return latest ? <div className="space-y-3">{orderedSegments(latest, labels).slice(0, 8).map((segment) => <div key={segment.label} className="grid grid-cols-[minmax(0,1fr)_3rem] gap-3 text-xs"><span className="flex min-w-0 items-center gap-2 truncate text-slate-300"><i className="h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: colorFor(segment.label, labels) }} />{segment.label}</span><span className="text-right font-semibold text-white">{segment.percent.toFixed(1)}%</span></div>)}</div> : <p className="py-8 text-sm text-slate-400">No sector records are available.</p>; }

function TrendChart({ series }: { series: Array<{ label: string; value: number }> }) { if (!series.length) return <p className="flex h-40 items-center justify-center text-sm text-slate-400">No time-series records are available.</p>; const max = Math.max(...series.map((point) => point.value), 1); const points = series.map((point, index) => `${(index / Math.max(series.length - 1, 1)) * 100},${100 - (point.value / max) * 85}`).join(" "); return <div><svg viewBox="0 0 100 100" preserveAspectRatio="none" className="h-40 w-full overflow-visible"><defs><linearGradient id="profile-trend" x1="0" x2="0" y1="0" y2="1"><stop stopColor="#42d3a7" stopOpacity=".35" /><stop offset="1" stopColor="#42d3a7" stopOpacity="0" /></linearGradient></defs><polyline points={`0,100 ${points} 100,100`} fill="url(#profile-trend)" stroke="none" /><polyline points={points} fill="none" stroke="#55e3b0" strokeWidth="1.5" vectorEffect="non-scaling-stroke" /></svg><div className="mt-2 flex justify-between gap-2 text-[10px] text-slate-500"><span>{series[0].label}</span><span>{series[series.length - 1].label}</span></div></div>; }

function ActivityBars({ items }: { items: ProfileActivityItem[] }) { const groups = ["Congress", "Insider", "Institution", "Department"].map((type) => ({ type, value: items.filter((item) => item.type === type).length })); const max = Math.max(...groups.map((group) => group.value), 1); return <div className="flex h-44 items-end gap-4 border-b border-white/10 px-3">{groups.map((group) => <div key={group.type} className="flex flex-1 flex-col items-center gap-2"><div className="w-full rounded-t" style={{ height: `${Math.max(4, (group.value / max) * 135)}px`, backgroundColor: PROFILE_COLORS[group.type] }} /><span className="text-[10px] text-slate-500">{group.type}</span></div>)}</div>; }

function Donut({ values, colors, label, value }: { values: number[]; colors: string[]; label: string; value: string }) { const total = values.reduce((sum, item) => sum + item, 0); let offset = 0; return <div className="relative mx-auto h-36 w-36"><svg viewBox="0 0 42 42" className="h-full w-full -rotate-90">{values.map((item, index) => { const dash = total ? (item / total) * 100 : 0; const segment = <circle key={index} cx="21" cy="21" r="15.915" fill="none" stroke={colors[index]} strokeWidth="7" strokeDasharray={`${dash} ${100 - dash}`} strokeDashoffset={-offset} />; offset += dash; return segment; })}</svg><div className="absolute inset-0 flex flex-col items-center justify-center"><b className="text-lg tabular-nums text-white">{value}</b><span className="text-[9px] uppercase tracking-[.14em] text-slate-500">{label}</span></div></div>; }

function SnapshotCard({ row, label }: { row: Row | undefined; label: string }) { if (!row) return <div className="rounded-lg border border-slate-700/70 bg-slate-950/65 p-4"><p className="text-[10px] uppercase tracking-[.14em] text-slate-500">{label}</p><p className="mt-3 text-sm text-slate-400">No record available</p></div>; const title = stringAt(row, ["name", "symbol", "profile", "company"]) ?? "Live result"; const detail = valueAt(row, ["net_value", "net_buy_value", "increase_value", "portfolio_value", "value", "buy_value", "trades"]); const recentDate = stringAt(row, ["filing_date", "last_transaction", "last_activity", "recent_activity"]); return <div className="rounded-lg border border-slate-700/70 bg-slate-950/65 p-4"><p className="text-[10px] uppercase tracking-[.14em] text-slate-500">{label}</p><p className="mt-2 truncate text-sm font-semibold text-white">{title}</p><p className="mt-2 text-sm font-semibold tabular-nums text-emerald-300">{detail !== null ? formatUnknown(detail) : recentDate ? formatDate(recentDate) : "-"}</p></div>; }

function CompactActivity({ items }: { items: ProfileActivityItem[] }) { return !items.length ? <p className="py-8 text-sm text-slate-400">No recent database activity is available.</p> : <div className="space-y-0.5">{items.slice(0, 6).map((item) => <div key={String(item.id)} className="grid grid-cols-[minmax(0,1fr)_4.5rem_5rem] items-center gap-3 border-b border-white/5 py-2 text-xs"><span className="truncate font-semibold text-emerald-200">{item.profile}</span><span className="truncate text-slate-400">{item.symbol ?? item.activity ?? "-"}</span><span className="text-right font-semibold tabular-nums text-white">{formatUnknown(item.value)}</span></div>)}</div>; }

function DirectoryLeaders({ title, href, rows }: { title: string; href: string; rows: Array<{ label: string; value?: number | null; value_format?: string; href?: string | null }> }) { return <div className="min-w-0 border-l border-white/10 pl-3 first:border-l-0 first:pl-0"><h3 className="truncate text-xs font-semibold text-white">{title}</h3><div className="mt-2 space-y-1.5">{rows.slice(0, 5).map((row, index) => <div key={`${row.label}-${index}`} className="grid grid-cols-[1rem_minmax(0,1fr)_4.5rem] gap-1 text-xs"><span className="text-slate-500">{index + 1}</span><span className="truncate text-slate-300">{row.label}</span><span className="text-right tabular-nums text-white">{row.value_format === "currency" ? formatMoney(row.value) : formatNumber(row.value)}</span></div>)}</div><Link href={href} className="mt-3 inline-flex text-xs font-semibold text-emerald-200">View {title} -&gt;</Link></div>; }

function PeriodBadge({ label }: { label: string }) { return <span className="rounded-full border border-white/10 bg-slate-950/60 px-4 py-2 text-xs font-semibold text-slate-200">{label}</span>; }
function ActivityMix({ flavor, metrics }: { flavor: DashboardFlavor; metrics: ProfileMetric[] }) { const labels = flavor === "institutions" ? ["Increases", "Decreases"] : ["Buy value", "Sell value"]; const values = flavor === "institutions" ? [metricValue(metrics, "Total Position Increases"), metricValue(metrics, "Total Position Decreases")] : [metricValue(metrics, "Total Buy Value") ?? metricValue(metrics, "Buy Value"), metricValue(metrics, "Total Sell Value") ?? metricValue(metrics, "Sell Value")]; const total = values.reduce<number>((sum, value) => sum + Math.max(value ?? 0, 0), 0); if (!total) return <p className="py-8 text-sm text-slate-400">No matching database activity is available.</p>; const primaryPct = ((values[0] ?? 0) / total) * 100; return <div className="flex items-center gap-4"><Donut values={values.map((value) => Math.max(value ?? 0, 0))} colors={["#42d3a7", "#fb7185"]} label={labels[0]} value={`${primaryPct.toFixed(1)}%`} /><div className="min-w-0 flex-1 space-y-3">{labels.map((label, index) => <LegendRow key={label} label={label} color={index ? "#fb7185" : "#42d3a7"} value={flavor === "institutions" ? formatNumber(values[index]) : formatMoney(values[index])} />)}<p className="border-t border-white/10 pt-3 text-xs text-slate-400">Total {flavor === "institutions" ? "position changes" : "reported value"}: <span className="font-semibold text-white">{flavor === "institutions" ? formatNumber(total) : formatMoney(total)}</span></p></div></div>; }
function SectorMomentum({ rows }: { rows: ProfileSectorPeriod[] }) { const movements = sectorMovements(rows); return !movements.length ? <p className="py-8 text-sm text-slate-400">No comparable sector periods are available.</p> : <div className="space-y-2.5">{movements.slice(0, 8).map((item) => <div key={item.label} className="grid grid-cols-[minmax(0,1fr)_3.5rem] items-center gap-3 text-xs"><span className="truncate text-slate-300">{item.label}</span><span className={`text-right font-semibold tabular-nums ${item.value >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{item.value >= 0 ? "+" : ""}{formatMoney(item.value)}</span></div>)}</div>; }
function SectorMovers({ rows }: { rows: ProfileSectorPeriod[] }) { const movements = sectorMovements(rows); return !movements.length ? <p className="py-8 text-sm text-slate-400">No comparable sector periods are available.</p> : <div className="space-y-2.5">{movements.slice(0, 6).map((item) => <div key={item.label}><div className="flex items-center justify-between gap-3 text-xs"><span className="truncate text-slate-300">{item.label}</span><span className={item.value >= 0 ? "font-semibold text-emerald-300" : "font-semibold text-rose-300"}>{item.value >= 0 ? "+" : ""}{formatMoney(item.value)}</span></div><div className="mt-1.5 h-1 overflow-hidden rounded bg-slate-800"><div className="h-full rounded" style={{ width: `${Math.max(5, item.share * 100)}%`, backgroundColor: item.value >= 0 ? "#42d3a7" : "#fb7185" }} /></div></div>)}</div>; }
function ActivityColumns({ series }: { series: Array<{ label: string; value: number }> }) { if (!series.length) return <p className="flex h-36 items-center justify-center text-sm text-slate-400">No time-series records are available.</p>; const max = Math.max(...series.map((point) => Math.abs(point.value)), 1); return <div><div className="flex h-36 items-end gap-2 border-b border-white/10 px-1">{series.map((point) => <div key={point.label} className="flex min-w-0 flex-1 items-end"><div title={`${point.label}: ${formatMoney(point.value)}`} className="w-full rounded-t-sm bg-emerald-400/70" style={{ height: `${Math.max(3, (Math.abs(point.value) / max) * 132)}px` }} /></div>)}</div><div className="mt-2 flex justify-between gap-3 text-[10px] text-slate-500"><span>{series[0].label}</span><span>{series[series.length - 1].label}</span></div></div>; }
function LegendRow({ label, color, value }: { label: string; color: string; value: string }) { return <div className="flex items-center justify-between gap-3 text-xs"><span className="flex items-center gap-2 text-slate-300"><i className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color }} />{label}</span><span className="font-semibold tabular-nums text-white">{value}</span></div>; }
function OverviewGlow() { return <div aria-hidden className="pointer-events-none absolute inset-x-0 top-0 h-48 opacity-60 [background:radial-gradient(ellipse_at_70%_0%,rgba(52,211,153,.16),transparent_38%),radial-gradient(ellipse_at_25%_0%,rgba(59,130,246,.11),transparent_44%)]" />; }
function periodTotals(rows: ProfileSectorPeriod[]) { return rows.map((row) => ({ label: row.period, value: row.segments.reduce((sum, segment) => sum + (Number.isFinite(segment.value) ? segment.value : 0), 0) })); }
function metricValue(metrics: ProfileMetric[], label: string) { return metrics.find((metric) => metric.label === label)?.value ?? null; }
function sectorMovements(rows: ProfileSectorPeriod[]) { const latest = rows[rows.length - 1]; const previous = rows[rows.length - 2]; if (!latest || !previous) return []; const previousByLabel = new Map(previous.segments.map((segment) => [segment.label, segment.value])); const values = latest.segments.map((segment) => ({ label: segment.label, value: segment.value - (previousByLabel.get(segment.label) ?? 0) })); const max = Math.max(...values.map((item) => Math.abs(item.value)), 1); return values.sort((left, right) => Math.abs(right.value) - Math.abs(left.value)).map((item) => ({ ...item, share: Math.abs(item.value) / max })); }
function snapshotLabels(flavor: DashboardFlavor) { return flavor === "congress" ? ["Top member", "Most traded ticker", "Top buyer", "Most active sector"] : flavor === "insiders" ? ["Top net buyer", "Most traded ticker", "Recent purchases", "Cluster buying"] : ["Top institution", "Largest reported increase", "Most widely held stock", "Recent filing"]; }
function sectorLabels(rows: ProfileSectorPeriod[]) { const latest = rows[rows.length - 1]?.segments.map((segment) => segment.label) ?? []; return [...latest, ...Array.from(new Set(rows.flatMap((row) => row.segments.map((segment) => segment.label)))).filter((label) => !latest.includes(label))]; }
function orderedSegments(row: ProfileSectorPeriod, labels: string[]) { const byLabel = new Map(row.segments.map((segment) => [segment.label, segment])); return labels.map((label) => byLabel.get(label)).filter((segment): segment is NonNullable<typeof segment> => Boolean(segment)); }
function colorFor(label: string, labels: string[]) { return COLORS[Math.max(labels.indexOf(label), 0) % COLORS.length]; }
function renderCell(row: Row, key: string, format?: CellFormat) { const value = row[key]; if (format === "link") { const href = typeof row.href === "string" ? row.href : typeof row.profile_href === "string" ? row.profile_href : typeof row.ticker_href === "string" ? row.ticker_href : null; const text = String(value ?? "-"); return href ? <Link href={href} prefetch={false} className="font-semibold text-emerald-200 hover:text-emerald-100">{text}</Link> : <span className="font-semibold text-emerald-200">{text}</span>; } if (format === "currency") return formatMoney(asNumber(value)); if (format === "number") return formatNumber(asNumber(value)); if (format === "percent") return typeof value === "number" ? `${value >= 0 ? "+" : ""}${value.toFixed(1)}%` : "-"; if (format === "date") return typeof value === "string" ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(new Date(value)) : "-"; return String(value ?? "-"); }
function formatMetric(metric?: ProfileMetric) { if (!metric) return "-"; return metric.format === "currency" ? formatMoney(metric.value) : formatNumber(metric.value); }
function formatUnknown(value: unknown) { return typeof value === "number" ? formatMoney(value) : "-"; }
function formatMoney(value?: number | null) { if (typeof value !== "number" || !Number.isFinite(value)) return "-"; return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value); }
function formatDate(value: string) { const parsed = new Date(value); return Number.isNaN(parsed.valueOf()) ? value : new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(parsed); }
function formatNumber(value?: number | null) { return typeof value === "number" && Number.isFinite(value) ? new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value) : "-"; }
function asNumber(value: unknown) { return typeof value === "number" && Number.isFinite(value) ? value : null; }
function stringAt(row: Row, keys: string[]) { for (const key of keys) if (typeof row[key] === "string" && row[key]) return row[key] as string; return null; }
function valueAt(row: Row, keys: string[]) { for (const key of keys) if (typeof row[key] === "number") return row[key]; return null; }
function profileIcon(kind: string) { return kind === "congress" ? "♜" : kind === "insiders" ? "♙" : kind === "institutions" ? "▦" : "◇"; }
function profileKind(type: string) { return type === "Congress" ? "congress" : type === "Insider" ? "insiders" : type === "Institution" ? "institutions" : "departments"; }
