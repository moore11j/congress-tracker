import Link from "next/link";
import type { ReactNode } from "react";

export type InsiderPeriod = "ttm" | "90";

type MetricCard = {
  label: string;
  value: string;
  change: string;
  positive?: boolean;
  spark: number[];
  color: "green" | "red" | "blue";
};

const months = ["Jun '24", "Jul '24", "Aug '24", "Sep '24", "Oct '24", "Nov '24", "Dec '24", "Jan '25", "Feb '25", "Mar '25", "Apr '25", "May '25"];
const netValues = [18, 12, -8, 42, 6, 108, 28, 52, 18, 9, 16, 6];
const tradeLine = [240, 720, 280, 900, 520, 460, 710, 1040, 330, 210, 680, 390];

const metricCards: MetricCard[] = [
  { label: "Open-Market Trades", value: "29,284", change: "+172.6% vs prior 12 months", positive: true, spark: [34, 36, 51, 43, 62, 54, 70, 49, 56, 63, 72, 51], color: "green" },
  { label: "Buy Value", value: "$19B", change: "+504.4% vs prior 12 months", positive: true, spark: [28, 31, 35, 44, 39, 56, 48, 62, 51, 68, 55, 47], color: "green" },
  { label: "Sell Value", value: "$62B", change: "+62.2% vs prior 12 months", positive: true, spark: [70, 54, 47, 38, 44, 58, 52, 72, 49, 64, 61, 55], color: "red" },
  { label: "Active Insiders", value: "6,272", change: "+18.7% vs prior 12 months", positive: true, spark: [39, 53, 48, 61, 50, 67, 42, 55, 57, 52, 72, 64], color: "green" },
  { label: "Average Trade Size", value: "$2.8M", change: "+19.3% vs prior 12 months", positive: true, spark: [42, 39, 54, 47, 50, 57, 56, 55, 68, 49, 44, 52], color: "green" },
];

const topInsiders = [
  ["1", "SaverOne 2014 Ltd", "VisionWave Holdings, Inc.", "Director", "$14B", "8", "Jun 16, 2026"],
  ["2", "Navios Maritime Partners L.P.", "Frangou Angeliki", "Director, 10% Owner", "$854M", "90", "Jul 30, 2026"],
  ["3", "The Vanguard Group, Inc.", "Multiple Companies", "10% Owner", "$612M", "250", "Jun 10, 2026"],
  ["4", "ValueAct Capital Master Fund", "Microsoft Corporation", "10% Owner", "$483M", "34", "May 28, 2026"],
  ["5", "BlackRock, Inc.", "Apple Inc.", "10% Owner", "$412M", "27", "Jun 3, 2026"],
];

const mostTraded = [
  ["1", "SVRE", "SaverOne 2014 Ltd", "$14B", "$0", "$14B", "32"],
  ["2", "PSX", "Phillips 66", "$0", "$7.4B", "-$7.4B", "28"],
  ["3", "CRWV", "CoreWeave Inc.", "$0", "$5.4B", "-$5.4B", "35"],
  ["4", "TSM", "Taiwan Semiconductor", "$2.4B", "$1.1B", "$1.3B", "32"],
  ["5", "AAPL", "Apple Inc.", "$1.6B", "$1.2B", "$0.6B", "24"],
];

const sectorMix = [
  { label: "Technology", color: "#8b5cf6", values: [24, 25, 24, 23, 25] },
  { label: "Industrials", color: "#3b82f6", values: [20, 19, 21, 20, 19] },
  { label: "Financials", color: "#f59e0b", values: [17, 18, 18, 19, 18] },
  { label: "Health Care", color: "#ef4444", values: [13, 12, 12, 12, 13] },
  { label: "Consumer Discretionary", color: "#f97316", values: [10, 10, 9, 10, 9] },
  { label: "Energy", color: "#10b981", values: [8, 8, 8, 8, 8] },
  { label: "Others", color: "#60a5fa", values: [8, 8, 8, 8, 8] },
];

const netSector = [
  ["Technology", 10.2],
  ["Industrials", 6.1],
  ["Financials", 4.7],
  ["Health Care", 1.8],
  ["Consumer Discretionary", 0.7],
  ["Energy", 0.6],
  ["Utilities", -0.4],
  ["Real Estate", -0.8],
  ["Materials", -1.1],
  ["Consumer Staples", -1.6],
  ["Communication Services", -2.4],
] as const;

const roleMix = [
  { label: "CEOs", value: 31.4, color: "#fbbf24" },
  { label: "Directors", value: 27.8, color: "#4ade80" },
  { label: "10% Owners", value: 22.6, color: "#6366f1" },
  { label: "Officers", value: 18.2, color: "#60a5fa" },
];

const movers = [
  ["Technology", "$10.2B", "+142.6%", true, [42, 40, 46, 44, 51, 49, 55, 53, 61, 58, 63, 66]],
  ["Industrials", "$6.1B", "+80.3%", true, [34, 38, 36, 43, 39, 47, 45, 51, 49, 54, 52, 57]],
  ["Financials", "$4.7B", "+64.2%", true, [32, 36, 34, 39, 37, 42, 40, 46, 43, 49, 47, 52]],
  ["Energy", "$0.6B", "-12.4%", false, [54, 50, 52, 47, 49, 43, 45, 40, 42, 38, 39, 35]],
  ["Consumer Staples", "-$1.6B", "-35.7%", false, [60, 58, 55, 53, 50, 48, 45, 43, 40, 38, 35, 32]],
] as const;

const notableTrades = [
  ["SaverOne 2014 Ltd", "SVRE", "Buy", "$250M", "Jun 16, 2026"],
  ["Frangou Angeliki", "PSX", "Sell", "$450M", "Jun 12, 2026"],
  ["The Vanguard Group, Inc.", "AAPL", "Buy", "$120M", "Jun 10, 2026"],
  ["BlackRock, Inc.", "MSFT", "Buy", "$95M", "Jun 9, 2026"],
  ["Taiwan Semiconductor", "TSM", "Buy", "$180M", "Jun 6, 2026"],
];

const activityBars = [
  { label: "Jun '24", buy: 0.9, sell: 1.2, trades: 520 },
  { label: "Jul '24", buy: 1.1, sell: 1.4, trades: 700 },
  { label: "Aug '24", buy: 0.7, sell: 0.9, trades: 560 },
  { label: "Sep '24", buy: 1.0, sell: 1.1, trades: 820 },
  { label: "Oct '24", buy: 1.4, sell: 2.4, trades: 1040 },
  { label: "Nov '24", buy: 3.0, sell: 2.9, trades: 760 },
  { label: "Dec '24", buy: 1.7, sell: 1.5, trades: 650 },
  { label: "Jan '25", buy: 2.6, sell: 1.9, trades: 1090 },
  { label: "Feb '25", buy: 1.1, sell: 1.2, trades: 710 },
  { label: "Mar '25", buy: 1.0, sell: 1.1, trades: 840 },
  { label: "Apr '25", buy: 1.2, sell: 1.3, trades: 780 },
  { label: "May '25", buy: 1.3, sell: 0.8, trades: 930 },
  { label: "Jun '25", buy: 0.8, sell: 0.6, trades: 650 },
];

const statTiles = [
  { label: "Top Net Buyer", title: "VisionWave Holdings, Inc.", detail: "SaverOne 2014 Ltd", value: "$14B", sub: "8 trades", icon: "network" },
  { label: "Most Traded Ticker", title: "SVRE", detail: "SaverOne 2014 Ltd", value: "$14B", sub: "32 insiders", icon: "grid" },
  { label: "Cluster Buying", title: "TSM", detail: "32 insiders", value: "$2.4M", sub: "", icon: "cluster" },
  { label: "Sector Breadth", title: "Technology", detail: "Positive", value: "73.3%", sub: "", icon: "pie" },
];

export function InsiderProfilesDashboard({ selectedPeriod }: { selectedPeriod: InsiderPeriod }) {
  return (
    <section className="relative min-w-0 overflow-hidden pb-4">
      <HeroGlow />
      <header className="relative z-10 flex min-w-0 flex-col gap-4 pt-2 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.32em] text-emerald-300">Insiders</p>
          <h1 className="mt-2 text-4xl font-semibold leading-tight text-white">Corporate Insider Activity</h1>
          <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">Track purchases and sales from executives, directors, and major shareholders.</p>
        </div>
        <PeriodFilter selected={selectedPeriod} />
      </header>

      <main className="relative z-10 mt-4 space-y-3">
        <section className="grid gap-3 rounded-2xl border border-slate-700/70 bg-slate-950/58 p-4 shadow-[0_20px_70px_rgba(15,23,42,0.35)] xl:grid-cols-[1.55fr_0.9fr]">
          <div className="min-w-0">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold uppercase tracking-[0.24em] text-white">Insider Trading Snapshot <span className="text-slate-500">i</span></h2>
                <p className="mt-3 text-xs text-slate-400">Net insider activity (USD)</p>
                <p className="mt-1 text-2xl font-semibold text-emerald-300">$62.8M</p>
                <p className="text-xs font-semibold text-emerald-300">+62.2% vs prior 12 months</p>
              </div>
              <SegmentedControl items={["Value (USD)", "Trades"]} active="Value (USD)" />
            </div>
            <SnapshotChart />
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {statTiles.map((tile) => <SnapshotStat key={tile.label} {...tile} />)}
          </div>
        </section>

        <section className="grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
          {metricCards.map((metric) => <MetricTile key={metric.label} metric={metric} />)}
        </section>

        <section className="grid gap-3 xl:grid-cols-[1fr_1fr]">
          <Panel title="Top Insiders by Net Buying">
            <DataTable
              headers={["Rank", "Insider", "Company", "Role", "Net Buy Value", "Trades", "Last Transaction"]}
              rows={topInsiders.map((row) => [rankBadge(row[0]), <Link key={row[1]} href={`/insider/${slugify(row[1])}`} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200">{row[1]}</Link>, row[2], row[3], row[4], row[5], row[6]])}
              alignRight={[4, 5, 6]}
            />
            <PanelCta href="/feed?mode=insider">View all insiders</PanelCta>
          </Panel>
          <Panel title="Most Traded Stocks">
            <DataTable
              headers={["Rank", "Ticker", "Company", "Insider Buy Value", "Insider Sell Value", "Net Value", "Unique Insiders"]}
              rows={mostTraded.map((row) => [rankBadge(row[0]), <Link key={row[1]} href={`/ticker/${row[1]}`} prefetch={false} className="font-semibold text-white hover:text-emerald-200">{row[1]}</Link>, row[2], row[3], row[4], <span key={row[5]} className={row[5].startsWith("-") ? "text-rose-300" : "text-emerald-300"}>{row[5]}</span>, row[6]])}
              alignRight={[3, 4, 5, 6]}
            />
            <PanelCta href="/feed?mode=insider">View all stocks</PanelCta>
          </Panel>
        </section>

        <section className="grid gap-3 xl:grid-cols-[1fr_0.95fr_1fr]">
          <Panel title="Sector Exposure Over Time">
            <SectorExposureChart />
          </Panel>
          <Panel title="Net Activity by Sector (TTM)">
            <NetSectorChart />
          </Panel>
          <Panel title="Buy vs Sell Mix (TTM)">
            <BuySellDonut />
          </Panel>
        </section>

        <section className="grid gap-3 xl:grid-cols-[0.95fr_0.9fr_1fr]">
          <Panel title="Transaction Mix by Role (TTM)">
            <RoleMix />
          </Panel>
          <Panel title="Top Moving Sectors (vs Prior TTM)">
            <MovingSectors />
          </Panel>
          <Panel title="Recent Notable Trades">
            <RecentTrades />
          </Panel>
        </section>

        <Panel title="Activity Over Time (USD)" action="Monthly">
          <ActivityBars />
        </Panel>
      </main>
    </section>
  );
}

function PeriodFilter({ selected }: { selected: InsiderPeriod }) {
  const options = [
    { label: "TTM", value: "ttm", href: "/insiders" },
    { label: "90D", value: "90", href: "/insiders?period=90" },
  ] as const;
  return (
    <nav className="flex w-fit min-w-0 items-center gap-2 rounded-2xl border border-slate-700/70 bg-slate-950/70 p-2">
      <span className="px-3 text-xs font-semibold uppercase tracking-[0.24em] text-slate-400">Period</span>
      {options.map((option) => (
        <Link
          key={option.value}
          href={option.href}
          prefetch={false}
          className={`rounded-full px-4 py-2 text-xs font-semibold transition ${selected === option.value ? "bg-emerald-400/18 text-emerald-100 ring-1 ring-emerald-300/30" : "text-slate-200 hover:bg-white/5 hover:text-white"}`}
        >
          {option.label}
        </Link>
      ))}
    </nav>
  );
}

function Panel({ title, action, children }: { title: string; action?: string; children: ReactNode }) {
  return (
    <section className="min-w-0 rounded-2xl border border-slate-700/70 bg-slate-950/62 p-4 shadow-[0_18px_55px_rgba(15,23,42,0.24)]">
      <div className="mb-3 flex min-w-0 items-center justify-between gap-3">
        <h2 className="truncate text-sm font-semibold uppercase tracking-[0.2em] text-white">{title} <span className="text-slate-500">i</span></h2>
        {action ? <span className="rounded-lg border border-slate-700 bg-slate-950/80 px-3 py-1.5 text-xs font-medium text-slate-300">{action}</span> : null}
      </div>
      {children}
    </section>
  );
}

function SnapshotStat({ label, title, detail, value, sub, icon }: { label: string; title: string; detail: string; value: string; sub: string; icon: string }) {
  return (
    <article className="min-h-28 rounded-xl border border-slate-700/70 bg-slate-900/50 p-3">
      <div className="flex items-start gap-3">
        <IconBadge icon={icon} />
        <div className="min-w-0">
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
          <p className="mt-2 truncate text-sm font-semibold text-white">{title}</p>
          <p className="mt-1 truncate text-xs text-slate-400">{detail}</p>
          <p className="mt-2 text-lg font-semibold text-emerald-300">{value}</p>
          {sub ? <p className="text-xs text-slate-400">{sub}</p> : null}
        </div>
      </div>
    </article>
  );
}

function MetricTile({ metric }: { metric: MetricCard }) {
  return (
    <article className="relative min-h-24 overflow-hidden rounded-xl border border-slate-700/70 bg-slate-950/62 p-3">
      <div className="relative z-10">
        <p className="truncate text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{metric.label}</p>
        <p className="mt-2 text-2xl font-semibold tabular-nums text-white">{metric.value}</p>
        <p className={`mt-1 text-xs font-semibold ${metric.positive === false ? "text-rose-300" : "text-emerald-300"}`}>{metric.change}</p>
      </div>
      <Sparkline values={metric.spark} color={metric.color} />
    </article>
  );
}

function SnapshotChart() {
  const width = 1160;
  const height = 168;
  const netPath = linePath(netValues, width, height, 8);
  const tradePath = linePath(tradeLine, width, height, 8);
  const netArea = `${netPath} L${width} ${height + 18} L0 ${height + 18} Z`;
  return (
    <svg viewBox={`0 0 ${width + 96} 230`} className="mt-1 h-44 w-full" role="img" aria-label="Insider trading snapshot">
      <defs>
        <linearGradient id="insider-snapshot-area" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#34d399" stopOpacity="0.38" />
          <stop offset="100%" stopColor="#34d399" stopOpacity="0.04" />
        </linearGradient>
      </defs>
      {[-80, -40, 0, 40, 80, 120].map((tick) => {
        const y = height + 18 - ((tick + 80) / 200) * height;
        return (
          <g key={tick}>
            <line x1="58" x2={width + 58} y1={y} y2={y} stroke="#263244" strokeWidth="1" />
            <text x="4" y={y + 4} fill="#94a3b8" fontSize="11">{tick < 0 ? `-$${Math.abs(tick)}M` : tick === 0 ? "$0" : `$${tick}M`}</text>
          </g>
        );
      })}
      {[0, 500, 1000, 1500].map((tick) => {
        const y = height + 18 - (tick / 1500) * height;
        return <text key={tick} x={width + 68} y={y + 4} fill="#94a3b8" fontSize="11">{tick.toLocaleString()}</text>;
      })}
      <path d={netArea} fill="url(#insider-snapshot-area)" transform="translate(58 0)" />
      <path d={netPath} fill="none" stroke="#6ee7b7" strokeWidth="2.5" transform="translate(58 0)" />
      <path d={tradePath} fill="none" stroke="#3b82f6" strokeWidth="2.5" transform="translate(58 0)" />
      {months.map((label, index) => <text key={label} x={58 + (index / (months.length - 1)) * width} y="218" textAnchor="middle" fill="#94a3b8" fontSize="11">{label}</text>)}
      <LegendSvg x={360} y={18} items={[["Net Value (USD)", "#4ade80"], ["Total Trades", "#3b82f6"]]} />
    </svg>
  );
}

function Sparkline({ values, color }: { values: number[]; color: MetricCard["color"] }) {
  const stroke = color === "green" ? "#34d399" : color === "red" ? "#fb7185" : "#60a5fa";
  const fill = color === "green" ? "rgba(52,211,153,0.16)" : color === "red" ? "rgba(251,113,133,0.16)" : "rgba(96,165,250,0.16)";
  const path = linePath(values, 170, 42, 2);
  const area = `${path} L170 48 L0 48 Z`;
  return (
    <svg viewBox="0 0 170 54" className="absolute bottom-1 right-3 h-12 w-32" aria-hidden="true">
      <path d={area} fill={fill} />
      <path d={path} fill="none" stroke={stroke} strokeWidth="2" />
    </svg>
  );
}

function DataTable({ headers, rows, alignRight = [] }: { headers: string[]; rows: ReactNode[][]; alignRight?: number[] }) {
  return (
    <div className="min-w-0 overflow-hidden rounded-lg border border-slate-800/80">
      <table className="w-full table-fixed text-left text-xs">
        <thead className="border-b border-slate-700/70 bg-slate-900/40 text-[10px] uppercase tracking-[0.16em] text-slate-500">
          <tr>
            {headers.map((header, index) => <th key={header} className={`px-3 py-2 font-semibold ${index === 0 ? "w-12" : ""} ${alignRight.includes(index) ? "text-right" : ""}`}>{header}</th>)}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/85 text-slate-300">
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {row.map((cell, index) => <td key={index} className={`truncate px-3 py-2 ${alignRight.includes(index) ? "text-right font-semibold tabular-nums text-slate-100" : ""}`}>{cell}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SectorExposureChart() {
  const periods = ["Q2 '24", "Q3 '24", "Q4 '24", "Q1 '25", "Q2 '25"];
  return (
    <div>
      <Legend items={sectorMix.map((sector) => ({ label: sector.label, color: sector.color }))} />
      <div className="mt-4 grid h-36 grid-cols-[34px_1fr] gap-3">
        <div className="relative text-[10px] text-slate-400">
          {["100%", "75%", "50%", "25%", "0%"].map((label, index) => <span key={label} className="absolute right-0" style={{ top: `${index * 25}%` }}>{label}</span>)}
        </div>
        <div className="relative border-l border-b border-slate-700/70">
          <div className="absolute inset-0 grid grid-rows-4">{[0, 1, 2, 3].map((row) => <span key={row} className="border-t border-slate-800/80" />)}</div>
          <div className="relative flex h-full items-end justify-around gap-4 px-3">
            {periods.map((period, periodIndex) => (
              <div key={period} className="flex h-full flex-1 flex-col items-center justify-end gap-2">
                <div className="flex h-24 w-10 flex-col-reverse overflow-hidden rounded-t-md border border-slate-700/70">
                  {sectorMix.map((sector) => <span key={sector.label} style={{ height: `${sector.values[periodIndex]}%`, backgroundColor: sector.color }} />)}
                </div>
                <span className="text-[11px] text-slate-400">{period}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function NetSectorChart() {
  const max = 12;
  return (
    <div className="space-y-1">
      {netSector.map(([label, value]) => (
        <div key={label} className="grid grid-cols-[135px_1fr_44px] items-center gap-2 text-[11px]">
          <span className="truncate text-slate-300">{label}</span>
          <div className="relative h-3 rounded bg-slate-800/75">
            <span className="absolute left-1/2 top-0 h-full w-px bg-slate-600/70" />
            <span className={`absolute top-0 h-full rounded ${value >= 0 ? "left-1/2 bg-emerald-400/75" : "right-1/2 bg-rose-400/78"}`} style={{ width: `${Math.min(50, (Math.abs(value) / max) * 50)}%` }} />
          </div>
          <span className={`text-right font-semibold tabular-nums ${value >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{value >= 0 ? `$${value.toFixed(1)}B` : `-$${Math.abs(value).toFixed(1)}B`}</span>
        </div>
      ))}
    </div>
  );
}

function BuySellDonut() {
  return (
    <div className="grid min-h-40 items-center gap-5 md:grid-cols-[0.9fr_1fr]">
      <Donut segments={[23.4, 76.6]} colors={["#34d399", "#ef4444"]} label="Buy" value="23.4%" detail="$19B" />
      <div className="space-y-4 text-sm">
        <LegendRow color="#34d399" label="Buy Value" value="$19B (23.4%)" />
        <LegendRow color="#ef4444" label="Sell Value" value="$62B (76.6%)" />
        <div className="border-t border-slate-800 pt-3">
          <p className="text-slate-500">Total</p>
          <p className="mt-1 text-lg font-semibold text-white">$81B</p>
        </div>
      </div>
    </div>
  );
}

function RoleMix() {
  return (
    <div className="grid min-h-32 items-center gap-4 md:grid-cols-[0.8fr_1fr]">
      <Donut segments={roleMix.map((item) => item.value)} colors={roleMix.map((item) => item.color)} label="Total Trades" value="29,284" small />
      <div className="space-y-3">
        {roleMix.map((item) => <LegendRow key={item.label} color={item.color} label={item.label} value={`${item.value.toFixed(1)}%`} />)}
      </div>
    </div>
  );
}

function MovingSectors() {
  return (
    <div className="divide-y divide-slate-800/80">
      <div className="grid grid-cols-[1fr_82px_72px_100px] gap-3 pb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
        <span>Sector</span><span>Net Activity</span><span>vs Prior</span><span>Trend</span>
      </div>
      {movers.map(([label, net, change, positive, spark]) => (
        <div key={label} className="grid grid-cols-[1fr_82px_72px_100px] items-center gap-3 py-1.5 text-xs">
          <span className="truncate text-slate-300">{label}</span>
          <span className={net.startsWith("-") ? "font-semibold text-rose-300" : "font-semibold text-slate-100"}>{net}</span>
          <span className={positive ? "font-semibold text-emerald-300" : "font-semibold text-rose-300"}>{change}</span>
          <MiniTrend values={[...spark]} positive={positive} />
        </div>
      ))}
    </div>
  );
}

function RecentTrades() {
  return (
    <div className="min-w-0">
      <div className="grid grid-cols-[minmax(0,1.2fr)_62px_64px_80px_104px] gap-2 border-b border-slate-700/70 pb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
        <span>Insider</span><span>Ticker</span><span>Action</span><span className="text-right">Value</span><span className="text-right">Date</span>
      </div>
      <div className="divide-y divide-slate-800/85 text-xs">
        {notableTrades.map((row) => (
          <div key={`${row[0]}-${row[1]}`} className="grid grid-cols-[minmax(0,1.2fr)_62px_64px_80px_104px] gap-2 py-2">
            <span className="truncate font-semibold text-slate-100">{row[0]}</span>
            <span className="font-semibold text-slate-300">{row[1]}</span>
            <span><span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${row[2] === "Buy" ? "bg-emerald-400/15 text-emerald-300" : "bg-rose-400/15 text-rose-300"}`}>{row[2]}</span></span>
            <span className="text-right font-semibold tabular-nums text-slate-100">{row[3]}</span>
            <span className="text-right text-slate-300">{row[4]}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActivityBars() {
  const width = 1420;
  const height = 220;
  const baseY = 110;
  const step = width / activityBars.length;
  const linePoints = activityBars.map((item, index) => {
    const x = 70 + index * step + step / 2;
    const y = 18 + (1 - item.trades / 1500) * 165;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <div>
      <div className="mb-2 flex flex-wrap gap-5 text-xs text-slate-300">
        <LegendRow color="#34d399" label="Buy Value (USD)" compact />
        <LegendRow color="#ef4444" label="Sell Value (USD)" compact />
        <LegendRow color="#3b82f6" label="Total Trades" compact />
      </div>
      <svg viewBox={`0 0 ${width + 120} ${height + 45}`} className="h-52 w-full" role="img" aria-label="Insider activity over time">
        {[-3, -1.5, 0, 1.5, 3].map((tick) => {
          const y = baseY - (tick / 3) * 82;
          return (
            <g key={tick}>
              <line x1="70" x2={width + 40} y1={y} y2={y} stroke="#243044" strokeWidth="1" />
              <text x="20" y={y + 4} fill="#94a3b8" fontSize="12">{tick < 0 ? `-$${Math.abs(tick)}B` : tick === 0 ? "$0" : `$${tick}B`}</text>
            </g>
          );
        })}
        {activityBars.map((item, index) => {
          const x = 70 + index * step + step / 2 - 16;
          const buyH = (item.buy / 3) * 82;
          const sellH = (item.sell / 3) * 82;
          return (
            <g key={item.label}>
              <rect x={x} y={baseY - buyH} width="32" height={buyH} rx="2" fill="#34d399" opacity="0.78" />
              <rect x={x} y={baseY} width="32" height={sellH} rx="2" fill="#ef4444" opacity="0.8" />
              <text x={x + 16} y={height + 28} fill="#94a3b8" fontSize="11" textAnchor="middle">{item.label}</text>
            </g>
          );
        })}
        <polyline points={linePoints} fill="none" stroke="#3b82f6" strokeWidth="2.5" />
        {[0, 500, 1000, 1500].map((tick) => {
          const y = 18 + (1 - tick / 1500) * 165;
          return <text key={tick} x={width + 55} y={y + 4} fill="#94a3b8" fontSize="12">{tick === 1500 ? "1,500" : tick}</text>;
        })}
        <text x={width + 100} y="150" fill="#94a3b8" fontSize="11" transform={`rotate(90 ${width + 100} 150)`}>Trades</text>
      </svg>
    </div>
  );
}

function Donut({ segments, colors, label, value, detail, small }: { segments: number[]; colors: string[]; label: string; value: string; detail?: string; small?: boolean }) {
  const total = segments.reduce((sum, item) => sum + item, 0) || 1;
  let offset = 25;
  return (
    <div className={`relative mx-auto ${small ? "h-36 w-36" : "h-44 w-44"}`}>
      <svg viewBox="0 0 120 120" className="h-full w-full -rotate-90">
        <circle cx="60" cy="60" r="43" fill="none" stroke="#1f2937" strokeWidth="16" />
        {segments.map((segment, index) => {
          const length = (segment / total) * 270;
          const current = offset;
          offset += length;
          return <circle key={index} cx="60" cy="60" r="43" fill="none" stroke={colors[index]} strokeWidth="16" strokeDasharray={`${length} 270`} strokeDashoffset={-current} />;
        })}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{label}</p>
        <p className={`${small ? "text-lg" : "text-3xl"} mt-1 font-semibold text-white`}>{value}</p>
        {detail ? <p className="mt-1 text-sm text-slate-300">{detail}</p> : null}
      </div>
    </div>
  );
}

function MiniTrend({ values, positive }: { values: number[]; positive: boolean }) {
  return (
    <svg viewBox="0 0 100 24" className="h-6 w-24" aria-hidden="true">
      <path d={linePath(values, 98, 18, 2)} fill="none" stroke={positive ? "#34d399" : "#ef4444"} strokeWidth="2" />
    </svg>
  );
}

function SegmentedControl({ items, active }: { items: string[]; active: string }) {
  return (
    <div className="flex rounded-full border border-slate-700/70 bg-slate-950/70 p-1">
      {items.map((item) => <span key={item} className={`rounded-full px-5 py-1.5 text-xs font-semibold uppercase ${item === active ? "bg-emerald-400/18 text-emerald-100" : "text-slate-500"}`}>{item}</span>)}
    </div>
  );
}

function IconBadge({ icon }: { icon: string }) {
  const color = icon === "pie" ? "text-cyan-200 border-cyan-300/35 bg-cyan-300/10" : "text-emerald-200 border-emerald-300/35 bg-emerald-300/10";
  return (
    <span className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border ${color}`}>
      <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.8">
        {icon === "network" ? <><circle cx="7" cy="8" r="3" /><circle cx="17" cy="8" r="3" /><circle cx="12" cy="17" r="3" /><path d="M9.5 10.5 11 14M14.5 10.5 13 14" /></> : null}
        {icon === "grid" ? <><rect x="4" y="4" width="6" height="6" rx="2" /><rect x="14" y="4" width="6" height="6" rx="2" /><rect x="4" y="14" width="6" height="6" rx="2" /><rect x="14" y="14" width="6" height="6" rx="2" /></> : null}
        {icon === "cluster" ? <><circle cx="7" cy="8" r="3" /><circle cx="17" cy="8" r="3" /><path d="M4 19c.6-3 2.5-4.5 5.5-4.5M14.5 14.5c3 0 4.9 1.5 5.5 4.5" /></> : null}
        {icon === "pie" ? <><path d="M12 3v9h9" /><path d="M21 12a9 9 0 1 1-9-9" /></> : null}
      </svg>
    </span>
  );
}

function Legend({ items }: { items: Array<{ label: string; color: string }> }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-2 text-xs text-slate-300">
      {items.map((item) => <LegendRow key={item.label} color={item.color} label={item.label} compact />)}
    </div>
  );
}

function LegendRow({ color, label, value, compact }: { color: string; label: string; value?: string; compact?: boolean }) {
  return (
    <div className={`flex items-center gap-2 ${compact ? "" : "justify-between"}`}>
      <span className="h-2.5 w-2.5 shrink-0 rounded-sm" style={{ backgroundColor: color }} />
      <span className="text-slate-300">{label}</span>
      {value ? <span className="font-semibold text-white">{value}</span> : null}
    </div>
  );
}

function LegendSvg({ x, y, items }: { x: number; y: number; items: Array<[string, string]> }) {
  return (
    <g transform={`translate(${x} ${y})`}>
      {items.map(([label, color], index) => (
        <g key={label} transform={`translate(${index * 170} 0)`}>
          <rect x="0" y="-8" width="9" height="9" fill={color} />
          <text x="16" y="0" fill="#cbd5e1" fontSize="12">{label}</text>
        </g>
      ))}
    </g>
  );
}

function PanelCta({ href, children }: { href: string; children: ReactNode }) {
  return <Link href={href} prefetch={false} className="mx-auto mt-2 block w-fit text-xs font-semibold text-emerald-300 hover:text-emerald-100">{children} -&gt;</Link>;
}

function rankBadge(rank: string) {
  const color = rank === "1" ? "bg-amber-300 text-slate-950" : rank === "2" ? "bg-slate-300 text-slate-950" : rank === "3" ? "bg-orange-400 text-slate-950" : "bg-slate-800 text-slate-300";
  return <span className={`inline-flex h-5 w-5 items-center justify-center rounded-full text-[10px] font-bold ${color}`}>{rank}</span>;
}

function HeroGlow() {
  return <div className="pointer-events-none absolute inset-x-[-2rem] top-0 h-72 bg-[radial-gradient(circle_at_20%_18%,rgba(34,211,238,0.12),transparent_30%),radial-gradient(circle_at_80%_4%,rgba(16,185,129,0.12),transparent_28%)]" />;
}

function slugify(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function linePath(values: number[], width: number, height: number, pad: number) {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const scale = max === min ? 1 : max - min;
  return values.map((value, index) => {
    const x = (index / Math.max(1, values.length - 1)) * width;
    const y = pad + height - ((value - min) / scale) * height;
    return `${index === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`;
  }).join(" ");
}
