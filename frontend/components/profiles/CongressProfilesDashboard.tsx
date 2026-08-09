import Link from "next/link";
import type { ReactNode } from "react";

export type Chamber = "all" | "house" | "senate";

type MetricCard = {
  label: string;
  value: string;
  change: string;
  positive?: boolean;
  note?: string;
  spark: number[];
  color: "green" | "red" | "blue";
};

const months = ["Jun '25", "Jul '25", "Aug '25", "Sep '25", "Oct '25", "Nov '25", "Dec '25", "Jan '26", "Feb '26", "Mar '26", "Apr '26", "May '26"];
const snapshotTrend = [760, 1330, 1160, 1490, 1450, 1260, 1860, 1390, 1500, 1330, 1740, 1660, 1950, 1680, 2050];

const metricCards: MetricCard[] = [
  { label: "Total Trades", value: "10,428", change: "-4.0% vs prior 12 months", positive: false, spark: [48, 26, 31, 25, 33, 27, 35, 32, 46, 34, 51, 39, 58, 42, 71], color: "red" },
  { label: "Total Buy Value", value: "$511M", change: "+60.0% vs prior 12 months", positive: true, spark: [24, 31, 26, 28, 34, 30, 43, 38, 50, 42, 57, 49, 63, 55, 72], color: "green" },
  { label: "Total Sell Value", value: "$281M", change: "-63.4% vs prior 12 months", positive: false, spark: [62, 55, 57, 48, 51, 43, 46, 38, 35, 41, 33, 37, 31, 34, 29], color: "red" },
  { label: "Active Members", value: "176", change: "Latest 12 months", spark: [32, 38, 36, 42, 43, 45, 39, 41, 44, 50, 43, 48, 56, 47, 59], color: "blue" },
  { label: "Average Trade Size", value: "$81.4K", change: "Latest 12 months", spark: [58, 42, 36, 47, 44, 53, 50, 46, 61, 55, 66, 57, 62, 49, 54], color: "blue" },
];

const topMembers = [
  ["1", "Nancy Pelosi", "Democrat", "House", "$167M", "39", "Jun 24, 2026", "/member/nancy-pelosi"],
  ["2", "Mark Warner", "Democrat", "Senate", "$142M", "28", "Jun 23, 2026", "/member/mark-warner"],
  ["3", "Mitt Romney", "Republican", "Senate", "$128M", "24", "Jun 24, 2026", "/member/mitt-romney"],
  ["4", "Rick Scott", "Republican", "Senate", "$112M", "21", "Jun 22, 2026", "/member/rick-scott"],
  ["5", "Thomas Carper", "Democrat", "Senate", "$98M", "18", "Jun 22, 2026", "/member/thomas-carper"],
  ["6", "Josh Hawley", "Republican", "Senate", "$92M", "17", "Jun 23, 2026", "/member/josh-hawley"],
  ["7", "Sheldon Whitehouse", "Democrat", "Senate", "$87M", "16", "Jun 23, 2026", "/member/sheldon-whitehouse"],
  ["8", "Joni Ernst", "Republican", "Senate", "$81M", "15", "Jun 24, 2026", "/member/joni-ernst"],
  ["9", "Debbie Stabenow", "Democrat", "Senate", "$76M", "14", "Jun 22, 2026", "/member/debbie-stabenow"],
  ["10", "Jefferson Shreve", "Republican", "House", "$75M", "13", "Jun 24, 2026", "/member/jefferson-shreve"],
];

const mostTraded = [
  ["1", "AAPL", "Apple Inc", "$4.9M", "$102M", "-$97.5M", "33"],
  ["2", "JPM", "JPMorgan Chase & Co", "$75.8M", "$250K", "+$75.6M", "18"],
  ["3", "MSFT", "Microsoft Corp", "$43.6M", "$1.2M", "+$42.4M", "20"],
  ["4", "GOOGL", "Alphabet Inc Class A", "$31.2M", "$420K", "+$30.8M", "16"],
  ["5", "AMZN", "Amazon.com Inc", "$28.7M", "$1.1M", "+$27.6M", "14"],
  ["6", "NVDA", "NVIDIA Corp", "$22.5M", "$2.0M", "+$20.5M", "15"],
  ["7", "META", "Meta Platforms Inc", "$20.1M", "$980K", "+$19.1M", "12"],
  ["8", "BRK.B", "Berkshire Hathaway", "$18.4M", "$210K", "+$18.2M", "9"],
  ["9", "XOM", "Exxon Mobil Corp", "$15.2M", "$320K", "+$14.9M", "11"],
  ["10", "LLY", "Eli Lilly & Co", "$12.9M", "$410K", "+$12.5M", "8"],
];

const sectorMix = [
  { label: "Financials", color: "#3b82f6", values: [28, 27, 29, 31, 30] },
  { label: "Technology", color: "#8b5cf6", values: [24, 25, 23, 22, 24] },
  { label: "Healthcare", color: "#f472b6", values: [12, 13, 12, 11, 12] },
  { label: "Industrials", color: "#ef4444", values: [10, 10, 11, 10, 9] },
  { label: "Consumer Discretionary", color: "#f59e0b", values: [8, 8, 8, 9, 8] },
  { label: "Other", color: "#22c55e", values: [18, 17, 17, 17, 17] },
];

const netSector = [
  ["Financial Services", 162],
  ["Technology", 118],
  ["Health Care", 64],
  ["Industrials", 20],
  ["Consumer Discretionary", 14],
  ["Energy", -6],
  ["Consumer Staples", -11],
  ["Utilities", -17],
  ["Real Estate", -26],
  ["Materials", -38],
] as const;

const movers = [
  ["Technology", "+42.3%", true, [42, 38, 45, 43, 50, 48, 55, 52, 61, 58, 66, 63]],
  ["Financial Services", "+31.7%", true, [34, 37, 35, 42, 39, 45, 46, 48, 52, 49, 55, 57]],
  ["Industrials", "+18.5%", true, [28, 32, 30, 36, 34, 39, 37, 42, 40, 45, 43, 47]],
  ["Health Care", "+12.4%", true, [31, 29, 34, 32, 36, 35, 39, 37, 41, 39, 43, 42]],
  ["Energy", "-8.2%", false, [56, 52, 54, 48, 50, 45, 47, 42, 44, 39, 41, 36]],
  ["Real Estate", "-14.7%", false, [62, 58, 61, 54, 56, 50, 52, 47, 45, 43, 39, 37]],
] as const;

const notableTrades = [
  ["Jefferson Shreve", "AAPL", "Buy", "$1.2M", "Jun 24, 2026"],
  ["Nancy Pelosi", "NVDA", "Buy", "$750K", "Jun 24, 2026"],
  ["Mitt Romney", "JPM", "Buy", "$500K", "Jun 23, 2026"],
  ["Rick Scott", "XOM", "Sell", "$450K", "Jun 23, 2026"],
  ["Mark Warner", "MSFT", "Buy", "$300K", "Jun 23, 2026"],
];

const activityBars = [
  { label: "Jun '25", buy: 45, sell: 38, trades: 860 },
  { label: "Jul '25", buy: 92, sell: 55, trades: 1120 },
  { label: "Aug '25", buy: 56, sell: 35, trades: 1200 },
  { label: "Sep '25", buy: 52, sell: 38, trades: 1210 },
  { label: "Oct '25", buy: 78, sell: 64, trades: 910 },
  { label: "Nov '25", buy: 92, sell: 78, trades: 970 },
  { label: "Dec '25", buy: 85, sell: 82, trades: 820 },
  { label: "Jan '26", buy: 93, sell: 91, trades: 760 },
  { label: "Feb '26", buy: 80, sell: 68, trades: 1040 },
  { label: "Mar '26", buy: 76, sell: 48, trades: 1100 },
  { label: "Apr '26", buy: 87, sell: 65, trades: 900 },
  { label: "May '26", buy: 78, sell: 58, trades: 930 },
];

const statTiles = [
  { label: "Most Active Member", title: "Nancy Pelosi", detail: "39 Trades", icon: "person" },
  { label: "Most Traded Ticker", title: "AAPL", detail: "-$97.5M Net Value", icon: "chart" },
  { label: "Top Buyer", title: "Jefferson Shreve", detail: "$250M Net Buys", icon: "arrow" },
  { label: "Most Active Sector", title: "Financial Services", detail: "31.0% of Trades", icon: "pie" },
];

export function CongressProfilesDashboard({ selectedChamber }: { selectedChamber: Chamber }) {
  return (
    <section className="relative min-w-0 overflow-hidden pb-4">
      <HeroGlow />
      <header className="relative z-10 flex min-w-0 flex-col gap-4 pt-3 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.32em] text-emerald-300">Congress</p>
          <h1 className="mt-3 text-4xl font-semibold leading-tight text-white">Congress Trading</h1>
          <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">Track disclosed trades, portfolio activity, and market positioning across members of Congress.</p>
        </div>
        <ChamberFilter selected={selectedChamber} />
      </header>

      <main className="relative z-10 mt-6 space-y-5">
        <section className="grid gap-4 rounded-2xl border border-slate-700/70 bg-slate-950/58 p-4 shadow-[0_20px_70px_rgba(15,23,42,0.35)] xl:grid-cols-[1.58fr_0.9fr]">
          <div className="min-w-0">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold uppercase tracking-[0.26em] text-white">Congress Trading Snapshot</h2>
                <p className="mt-2 text-xs text-slate-400">Activity trend over the last 12 months</p>
              </div>
              <SegmentedControl items={["6M", "12M", "YTD", "All"]} active="12M" />
            </div>
            <div className="mt-4 grid gap-5 lg:grid-cols-[150px_minmax(0,1fr)]">
              <div>
                <p className="text-3xl font-semibold tabular-nums text-white">10,428</p>
                <p className="mt-2 text-xs text-slate-400">Total Trades</p>
                <p className="mt-4 text-xs font-semibold text-rose-300">-4.0% vs prior 12 months</p>
              </div>
              <SnapshotLine />
            </div>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {statTiles.map((tile) => <SnapshotStat key={tile.label} {...tile} />)}
          </div>
        </section>

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
          {metricCards.map((metric) => <MetricTile key={metric.label} metric={metric} />)}
        </section>

        <section className="grid gap-4 xl:grid-cols-[1.02fr_0.98fr]">
          <Panel title="Top Members by Portfolio Value">
            <DataTable
              headers={["Rank", "Member", "Party", "Chamber", "Est. Portfolio Value", "Trades", "Recent Activity"]}
              rows={topMembers.map((row) => [row[0], <Link key={row[1]} href={row[7]} prefetch={false} className="font-semibold text-emerald-300 hover:text-emerald-100">{row[1]}</Link>, row[2], row[3], row[4], row[5], row[6]])}
              alignRight={[4, 5, 6]}
            />
          </Panel>
          <Panel title="Most Traded Stocks" action="Last 12 Months">
            <DataTable
              headers={["Rank", "Ticker", "Company", "Buy Value", "Sell Value", "Net Value", "Trades"]}
              rows={mostTraded.map((row) => [row[0], <Link key={row[1]} href={`/ticker/${row[1]}`} prefetch={false} className="font-semibold text-white hover:text-emerald-200">{row[1]}</Link>, row[2], row[3], row[4], <span key={row[5]} className={row[5].startsWith("-") ? "text-rose-300" : "text-emerald-300"}>{row[5]}</span>, row[6]])}
              alignRight={[3, 4, 5, 6]}
            />
          </Panel>
        </section>

        <section className="grid gap-4 xl:grid-cols-3">
          <Panel title="Sector Exposure Over Time" action="100% Stacked">
            <SectorExposureChart />
          </Panel>
          <Panel title="Net Activity by Sector" action="Last 12 Months">
            <NetSectorChart />
          </Panel>
          <Panel title="Buy vs Sell Mix" action="Last 12 Months">
            <BuySellDonut />
          </Panel>
        </section>

        <section className="grid gap-4 xl:grid-cols-[0.95fr_0.9fr_1.1fr]">
          <Panel title="Chamber Mix">
            <ChamberMix />
          </Panel>
          <Panel title="Top Moving Sectors">
            <MovingSectors />
          </Panel>
          <Panel title="Recent Notable Trades">
            <RecentTrades />
          </Panel>
        </section>

        <Panel title="Activity Over Time" action="Monthly">
          <ActivityBars />
        </Panel>
      </main>
    </section>
  );
}

function ChamberFilter({ selected }: { selected: Chamber }) {
  const options: Array<{ label: string; value: Chamber; href: string }> = [
    { label: "All", value: "all", href: "/members" },
    { label: "House", value: "house", href: "/members?chamber=house" },
    { label: "Senate", value: "senate", href: "/members?chamber=senate" },
  ];
  return (
    <nav className="flex w-fit min-w-0 items-center gap-2 rounded-2xl border border-slate-700/70 bg-slate-950/70 p-2">
      <span className="px-3 text-xs font-semibold uppercase tracking-[0.24em] text-slate-400">Chamber</span>
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
    <section className="min-w-0 rounded-2xl border border-slate-700/70 bg-slate-950/62 p-5 shadow-[0_18px_55px_rgba(15,23,42,0.24)]">
      <div className="mb-4 flex min-w-0 items-center justify-between gap-3">
        <h2 className="truncate text-sm font-semibold uppercase tracking-[0.22em] text-white">{title} <span className="text-slate-500">i</span></h2>
        {action ? <span className="rounded-lg border border-slate-700 bg-slate-950/80 px-3 py-1.5 text-xs font-medium text-slate-300">{action}</span> : null}
      </div>
      {children}
    </section>
  );
}

function SnapshotStat({ label, title, detail, icon }: { label: string; title: string; detail: string; icon: string }) {
  return (
    <article className="min-h-24 rounded-xl border border-slate-700/70 bg-slate-900/42 p-3">
      <div className="flex items-center gap-3">
        <IconBadge icon={icon} />
        <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">{label}</p>
      </div>
      <p className="mt-2 truncate text-base font-semibold text-white">{title}</p>
      <p className="mt-1 truncate text-xs text-slate-300">{detail}</p>
    </article>
  );
}

function MetricTile({ metric }: { metric: MetricCard }) {
  return (
    <article className="relative min-h-36 overflow-hidden rounded-2xl border border-slate-700/70 bg-slate-950/62 p-4">
      <div className="relative z-10">
        <div className="flex items-center justify-between gap-3">
          <p className="truncate text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-500">{metric.label}</p>
          <span className="flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-slate-600 text-[10px] text-slate-500">i</span>
        </div>
        <p className="mt-4 text-2xl font-semibold tabular-nums text-white">{metric.value}</p>
        <p className={`mt-2 text-xs font-semibold ${metric.positive === false ? "text-rose-300" : metric.positive ? "text-emerald-300" : "text-slate-500"}`}>{metric.change}</p>
      </div>
      <Sparkline values={metric.spark} color={metric.color} />
    </article>
  );
}

function SnapshotLine() {
  const width = 760;
  const height = 198;
  const path = linePath(snapshotTrend, width, height, 14);
  const area = `${path} L${width} ${height + 18} L0 ${height + 18} Z`;
  const ticks = [0, 500, 1000, 1500, 2000];
  return (
    <svg viewBox={`0 0 ${width + 22} 225`} className="h-40 w-full" role="img" aria-label="Congress trading trend">
      <defs>
        <linearGradient id="snapshot-area" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#34d399" stopOpacity="0.42" />
          <stop offset="100%" stopColor="#34d399" stopOpacity="0.03" />
        </linearGradient>
      </defs>
      {ticks.map((tick, index) => {
        const y = height + 18 - (tick / 2000) * height;
        return (
          <g key={tick}>
            <line x1="52" x2={width} y1={y} y2={y} stroke="#263244" strokeWidth="1" />
            <text x="8" y={y + 4} fill="#94a3b8" fontSize="11">{index === 0 ? "0" : index === 4 ? "2K" : `${tick / 1000}K`}</text>
          </g>
        );
      })}
      <path d={area} fill="url(#snapshot-area)" transform="translate(52 0)" />
      <path d={path} fill="none" stroke="#6ee7b7" strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" transform="translate(52 0)" />
      {months.map((label, index) => (
        <text key={label} x={52 + (index / (months.length - 1)) * width} y="218" textAnchor="middle" fill="#94a3b8" fontSize="11">{label}</text>
      ))}
    </svg>
  );
}

function Sparkline({ values, color }: { values: number[]; color: MetricCard["color"] }) {
  const stroke = color === "green" ? "#34d399" : color === "red" ? "#fb7185" : "#60a5fa";
  const fill = color === "green" ? "rgba(52,211,153,0.16)" : color === "red" ? "rgba(251,113,133,0.16)" : "rgba(96,165,250,0.16)";
  const path = linePath(values, 220, 55, 2);
  const area = `${path} L220 62 L0 62 Z`;
  return (
    <svg viewBox="0 0 220 70" className="absolute inset-x-0 bottom-0 h-16 w-full" aria-hidden="true">
      <path d={area} fill={fill} />
      <path d={path} fill="none" stroke={stroke} strokeWidth="2" />
    </svg>
  );
}

function DataTable({ headers, rows, alignRight = [] }: { headers: string[]; rows: ReactNode[][]; alignRight?: number[] }) {
  return (
    <div className="min-w-0 overflow-hidden">
      <table className="w-full table-fixed text-left text-sm">
        <thead className="border-b border-slate-700/70 text-[10px] uppercase tracking-[0.18em] text-slate-500">
          <tr>
            {headers.map((header, index) => (
              <th key={header} className={`py-3 font-semibold ${index === 0 ? "w-12" : ""} ${alignRight.includes(index) ? "text-right" : ""}`}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/85 text-slate-300">
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {row.map((cell, index) => (
                <td key={index} className={`truncate py-2.5 ${alignRight.includes(index) ? "text-right font-semibold tabular-nums text-slate-100" : ""}`}>{cell}</td>
              ))}
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
      <div className="mt-6 grid h-60 grid-cols-[42px_1fr] gap-3">
        <div className="relative text-[11px] text-slate-400">
          {["100%", "75%", "50%", "25%", "0%"].map((label, index) => <span key={label} className="absolute right-0" style={{ top: `${index * 25}%` }}>{label}</span>)}
        </div>
        <div className="relative border-l border-b border-slate-700/70">
          <div className="absolute inset-0 grid grid-rows-4">
            {[0, 1, 2, 3].map((row) => <span key={row} className="border-t border-slate-800/80" />)}
          </div>
          <div className="relative flex h-full items-end justify-around gap-5 px-4">
            {periods.map((period, periodIndex) => (
              <div key={period} className="flex h-full min-w-0 flex-1 flex-col items-center justify-end gap-3">
                <div className="flex h-44 w-12 flex-col-reverse overflow-hidden rounded-t-md border border-slate-700/70">
                  {sectorMix.map((sector) => <span key={sector.label} style={{ height: `${sector.values[periodIndex]}%`, backgroundColor: sector.color }} />)}
                </div>
                <span className="text-xs text-slate-400">{period}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function NetSectorChart() {
  const max = 180;
  return (
    <div className="space-y-2">
      {netSector.map(([label, value]) => (
        <div key={label} className="grid grid-cols-[150px_1fr_52px] items-center gap-3 text-xs">
          <span className="truncate text-slate-300">{label}</span>
          <div className="relative h-5 rounded bg-slate-800/75">
            <span className="absolute left-1/2 top-0 h-full w-px bg-slate-600/70" />
            <span
              className={`absolute top-0 h-full rounded ${value >= 0 ? "left-1/2 bg-emerald-400/75" : "right-1/2 bg-rose-400/78"}`}
              style={{ width: `${Math.min(50, (Math.abs(value) / max) * 50)}%` }}
            />
          </div>
          <span className={`text-right font-semibold tabular-nums ${value >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{value >= 0 ? `$${value}M` : `-$${Math.abs(value)}M`}</span>
        </div>
      ))}
      <div className="grid grid-cols-[150px_1fr_52px] gap-3 pt-2 text-[10px] text-slate-500">
        <span />
        <div className="flex justify-between"><span>-$60M</span><span>$0</span><span>$60M</span><span>$120M</span><span>$180M</span></div>
      </div>
    </div>
  );
}

function BuySellDonut() {
  return (
    <div className="grid min-h-64 items-center gap-5 md:grid-cols-[1fr_0.9fr]">
      <Donut segments={[64, 36]} colors={["#34d399", "#f0526e"]} label="Buy" value="64%" detail="$511M" />
      <div className="space-y-5 text-sm">
        <LegendRow color="#34d399" label="Buy Volume" value="$511M (64%)" />
        <LegendRow color="#f0526e" label="Sell Volume" value="$281M (36%)" />
        <div className="border-t border-slate-800 pt-4">
          <p className="text-slate-500">Total Volume</p>
          <p className="mt-1 text-lg font-semibold text-white">$792M</p>
        </div>
      </div>
    </div>
  );
}

function ChamberMix() {
  return (
    <div>
      <p className="-mt-2 text-xs text-slate-500">Based on Trade Value</p>
      <div className="mt-4 grid min-h-56 items-center gap-5 md:grid-cols-[1fr_0.9fr]">
        <Donut segments={[54.7, 45.3]} colors={["#60a5fa", "#a855f7"]} label="Total" value="$792M" />
        <div className="space-y-5 text-sm">
          <LegendRow color="#60a5fa" label="House" value="$433M (54.7%)" />
          <LegendRow color="#a855f7" label="Senate" value="$359M (45.3%)" />
        </div>
      </div>
    </div>
  );
}

function MovingSectors() {
  return (
    <div>
      <p className="-mt-2 text-xs text-slate-500">vs Prior 12 Months</p>
      <div className="mt-4 divide-y divide-slate-800/80">
        {movers.map(([label, change, positive, spark]) => (
          <div key={label} className="grid grid-cols-[1fr_68px_110px] items-center gap-3 py-2 text-sm">
            <span className="truncate text-slate-300">{label}</span>
            <span className={`font-semibold ${positive ? "text-emerald-300" : "text-rose-300"}`}>{change}</span>
            <MiniTrend values={[...spark]} positive={positive} />
          </div>
        ))}
      </div>
    </div>
  );
}

function RecentTrades() {
  return (
    <div className="min-w-0">
      <div className="grid grid-cols-[minmax(0,1.2fr)_70px_76px_88px_110px] gap-3 border-b border-slate-700/70 pb-3 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
        <span>Member</span>
        <span>Ticker</span>
        <span>Action</span>
        <span className="text-right">Value</span>
        <span className="text-right">Date</span>
      </div>
      <div className="divide-y divide-slate-800/85 text-sm">
        {notableTrades.map((row) => (
          <div key={`${row[0]}-${row[1]}`} className="grid grid-cols-[minmax(0,1.2fr)_70px_76px_88px_110px] gap-3 py-3">
            <span className="truncate font-semibold text-slate-100">{row[0]}</span>
            <span className="font-semibold text-slate-300">{row[1]}</span>
            <span className={row[2] === "Buy" ? "font-semibold text-emerald-300" : "font-semibold text-rose-300"}>{row[2]}</span>
            <span className="text-right font-semibold tabular-nums text-slate-100">{row[3]}</span>
            <span className="text-right text-slate-300">{row[4]}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActivityBars() {
  const width = 1400;
  const height = 310;
  const baseY = 150;
  const step = width / activityBars.length;
  const linePoints = activityBars.map((item, index) => {
    const x = 70 + index * step + step / 2;
    const y = 25 + (1 - item.trades / 1500) * 220;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <div>
      <div className="mb-4 flex flex-wrap gap-5 text-xs text-slate-300">
        <LegendRow color="#34d399" label="Buy Value (USD)" compact />
        <LegendRow color="#f0526e" label="Sell Value (USD)" compact />
        <LegendRow color="#60a5fa" label="Total Trades" compact />
      </div>
      <svg viewBox={`0 0 ${width + 110} ${height + 55}`} className="h-80 w-full" role="img" aria-label="Congress activity over time">
        {[-150, -100, -50, 0, 50, 100, 150].map((tick) => {
          const y = baseY - (tick / 150) * 125;
          return (
            <g key={tick}>
              <line x1="70" x2={width + 40} y1={y} y2={y} stroke="#243044" strokeWidth="1" />
              <text x="20" y={y + 4} fill="#94a3b8" fontSize="12">{tick < 0 ? `-$${Math.abs(tick)}M` : tick === 0 ? "$0" : `$${tick}M`}</text>
            </g>
          );
        })}
        {activityBars.map((item, index) => {
          const x = 70 + index * step + step / 2 - 20;
          const buyH = (item.buy / 150) * 125;
          const sellH = (item.sell / 150) * 125;
          return (
            <g key={item.label}>
              <rect x={x} y={baseY - buyH} width="40" height={buyH} rx="3" fill="#34d399" opacity="0.78" />
              <rect x={x} y={baseY} width="40" height={sellH} rx="3" fill="#f0526e" opacity="0.8" />
              <text x={x + 20} y={height + 28} fill="#94a3b8" fontSize="12" textAnchor="middle">{item.label}</text>
            </g>
          );
        })}
        <polyline points={linePoints} fill="none" stroke="#60a5fa" strokeWidth="3" />
        {linePoints.split(" ").map((point) => {
          const [x, y] = point.split(",");
          return <circle key={point} cx={x} cy={y} r="4" fill="#60a5fa" />;
        })}
        {[0, 500, 750, 1000, 1250, 1500].map((tick) => {
          const y = 25 + (1 - tick / 1500) * 220;
          return <text key={tick} x={width + 55} y={y + 4} fill="#94a3b8" fontSize="12">{tick === 1500 ? "1.5K" : tick >= 1000 ? `${tick / 1000}K` : tick}</text>;
        })}
        <text x={width + 100} y="190" fill="#94a3b8" fontSize="11" transform={`rotate(90 ${width + 100} 190)`}>Total Trades</text>
      </svg>
    </div>
  );
}

function Donut({ segments, colors, label, value, detail }: { segments: number[]; colors: string[]; label: string; value: string; detail?: string }) {
  let offset = 25;
  return (
    <div className="relative mx-auto h-52 w-52">
      <svg viewBox="0 0 120 120" className="h-full w-full -rotate-90">
        <circle cx="60" cy="60" r="43" fill="none" stroke="#1f2937" strokeWidth="16" />
        {segments.map((segment, index) => {
          const length = (segment / 100) * 270;
          const current = offset;
          offset += length;
          return <circle key={index} cx="60" cy="60" r="43" fill="none" stroke={colors[index]} strokeWidth="16" strokeDasharray={`${length} 270`} strokeDashoffset={-current} strokeLinecap="butt" />;
        })}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{label}</p>
        <p className="mt-1 text-3xl font-semibold text-white">{value}</p>
        {detail ? <p className="mt-1 text-sm text-slate-300">{detail}</p> : null}
      </div>
    </div>
  );
}

function MiniTrend({ values, positive }: { values: number[]; positive: boolean }) {
  return (
    <svg viewBox="0 0 110 28" className="h-7 w-28" aria-hidden="true">
      <path d={linePath(values, 108, 22, 2)} fill="none" stroke={positive ? "#34d399" : "#fb7185"} strokeWidth="2" />
    </svg>
  );
}

function SegmentedControl({ items, active }: { items: string[]; active: string }) {
  return (
    <div className="flex rounded-lg border border-slate-700/70 bg-slate-950/70 p-1">
      {items.map((item) => <span key={item} className={`rounded-md px-3 py-1 text-xs font-medium ${item === active ? "bg-emerald-400/18 text-emerald-100" : "text-slate-500"}`}>{item}</span>)}
    </div>
  );
}

function IconBadge({ icon }: { icon: string }) {
  const color = icon === "pie" ? "text-amber-300 border-amber-300/40 bg-amber-300/10" : icon === "chart" ? "text-purple-300 border-purple-300/40 bg-purple-300/10" : "text-emerald-300 border-emerald-300/40 bg-emerald-300/10";
  return (
    <span className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-full border ${color}`}>
      <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.8">
        {icon === "person" ? <><circle cx="12" cy="8" r="3" /><path d="M5 20c1-4 3.3-6 7-6s6 2 7 6" /></> : null}
        {icon === "chart" ? <><path d="M5 18V6" /><path d="M5 18h14" /><path d="m8 15 3-4 3 2 4-6" /></> : null}
        {icon === "arrow" ? <><path d="M7 17 17 7" /><path d="M9 7h8v8" /></> : null}
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
      <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ backgroundColor: color }} />
      <span className="text-slate-300">{label}</span>
      {value ? <span className="font-semibold text-white">{value}</span> : null}
    </div>
  );
}

function HeroGlow() {
  return (
    <div className="pointer-events-none absolute inset-x-[-2rem] top-0 h-80 bg-[radial-gradient(circle_at_22%_16%,rgba(34,211,238,0.12),transparent_30%),radial-gradient(circle_at_78%_5%,rgba(16,185,129,0.12),transparent_28%)]" />
  );
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
