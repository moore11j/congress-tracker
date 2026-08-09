import Link from "next/link";
import type { ReactNode } from "react";

type MetricCard = {
  label: string;
  value: string;
  comparison: string;
  delta: string;
  positive?: boolean;
  spark: number[];
  tone?: "green" | "red";
};

const quarters = ["Q2 '24", "Q3 '24", "Q4 '24", "Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"];
const portfolioValues = [34, 35, 40, 35, 37, 39, 40.5, 43.4];
const positionLine = [112, 129, 151, 166, 154, 160, 171, 188];

const metricCards: MetricCard[] = [
  { label: "Tracked Institutions", value: "487", comparison: "vs Q4 '25", delta: "+12  +2.5%", positive: true, spark: [34, 38, 36, 42, 39, 48, 44, 55, 51, 60], tone: "green" },
  { label: "Total Portfolio Value", value: "$38.0T", comparison: "vs Q4 '25", delta: "+$2.6T  +7.3%", positive: true, spark: [35, 34, 39, 37, 43, 41, 49, 47, 53, 62], tone: "green" },
  { label: "Total Position Increases", value: "146,319", comparison: "vs Q4 '25", delta: "+11,475  +8.5%", positive: true, spark: [30, 34, 33, 41, 38, 45, 43, 52, 47, 61], tone: "green" },
  { label: "Total Position Decreases", value: "14,548", comparison: "vs Q4 '25", delta: "+832  +6.1%", positive: false, spark: [55, 61, 47, 53, 44, 50, 41, 46, 39, 43], tone: "red" },
  { label: "Net Reported Value Change", value: "$10.0T", comparison: "vs Q4 '25", delta: "+$1.8T  +21.9%", positive: true, spark: [28, 30, 34, 33, 38, 36, 43, 48, 55, 66], tone: "green" },
];

const snapshotTiles = [
  { label: "Top Institution", title: "BlackRock", value: "$5.7T", detail: "Portfolio Value", icon: "bank", tone: "green", spark: [42, 39, 48, 44, 54, 49, 58, 52, 62, 57, 66, 69] },
  { label: "Most Accumulated Ticker", title: "NVDA", value: "+$68.2B", detail: "Net Increase (13F)", icon: "chart", tone: "purple", spark: [28, 34, 31, 39, 36, 45, 42, 48, 44, 51, 49, 58] },
  { label: "Largest Reported Increase", title: "Microsoft (MSFT)", value: "+$12.6B", detail: "QoQ Increase", icon: "arrow", tone: "green", spark: [31, 35, 33, 42, 39, 46, 43, 51, 48, 56, 52, 61] },
  { label: "Top Sector Exposure", title: "Technology", value: "28.6%", detail: "Aggregate Exposure", icon: "pie", tone: "amber", spark: [42, 38, 45, 41, 49, 44, 52, 48, 55, 51, 63, 58] },
] as const;

const topInstitutions = [
  ["1", "BlackRock, Inc.", "$5.7T", "$5.2T", "+10.1%", "5,685", "NVDA $336B"],
  ["2", "Vanguard Group", "$4.6T", "$4.1T", "+11.7%", "4,062", "NVDA $337B"],
  ["3", "State Street Corp", "$2.9T", "$2.6T", "+11.0%", "4,260", "NVDA $173B"],
  ["4", "Fidelity Investments", "$2.4T", "$2.2T", "+8.0%", "3,512", "AAPL $156B"],
  ["5", "Geode Capital Management", "$1.6T", "$1.4T", "+13.4%", "4,513", "NVDA $106B"],
  ["6", "Capital Group Companies", "$1.3T", "$1.2T", "+8.3%", "2,874", "MSFT $92B"],
  ["7", "JP Morgan Chase & Co", "$1.2T", "$1.1T", "+9.1%", "7,756", "NVDA $74B"],
  ["8", "Morgan Stanley", "$1.1T", "$1.0T", "+9.5%", "8,227", "AAPL $62B"],
  ["9", "T. Rowe Price Associates", "$998B", "$918B", "+7.4%", "2,631", "MSFT $48B"],
  ["10", "Wellington Management", "$892B", "$819B", "+8.9%", "1,945", "GOOGL $41B"],
];

const increasedPositions = [
  ["1", "NVDA", "NVIDIA Corporation", "$1.7T", "$1.0T", "+$682B", "245"],
  ["2", "MSFT", "Microsoft Corp", "$931B", "$807B", "+$124B", "241"],
  ["3", "AMZN", "Amazon.com Inc", "$668B", "$497B", "+$108B", "249"],
  ["4", "AAPL", "Apple Inc", "$1.5T", "$898B", "+$102B", "229"],
  ["5", "GOOGL", "Alphabet Inc Class A", "$602B", "$421B", "+$101B", "230"],
  ["6", "AVGO", "Broadcom Inc", "$138B", "$36B", "+$102B", "197"],
  ["7", "META", "Meta Platforms Inc", "$374B", "$261B", "+$113B", "172"],
  ["8", "TSLA", "Tesla Inc", "$219B", "$141B", "+$78B", "158"],
  ["9", "BRK.B", "Berkshire Hathaway Inc", "$164B", "$103B", "+$61B", "141"],
  ["10", "JPM", "JPMorgan Chase & Co", "$165B", "$104B", "+$61B", "176"],
];

const sectorSeries = [
  { label: "Technology", color: "#34d399", values: [25, 26, 27, 26, 27, 28, 28, 29] },
  { label: "Financials", color: "#60a5fa", values: [16, 15, 15, 16, 15, 15, 15, 14] },
  { label: "Healthcare", color: "#a855f7", values: [14, 14, 13, 13, 12, 13, 12, 12] },
  { label: "Consumer Discretionary", color: "#fbbf24", values: [13, 13, 14, 13, 13, 13, 14, 14] },
  { label: "Industrials", color: "#22d3ee", values: [10, 10, 10, 11, 10, 10, 11, 10] },
  { label: "Others", color: "#64748b", values: [22, 22, 21, 21, 23, 21, 20, 21] },
];

const netSectorChange = [
  ["Technology", 562],
  ["Financials", 328],
  ["Healthcare", 128],
  ["Industrials", 64],
  ["Consumer Discretionary", 35],
  ["Energy", -18],
  ["Utilities", -67],
  ["Real Estate", -92],
  ["Consumer Staples", -110],
  ["Materials", -130],
] as const;

const widelyHeld = [
  ["1", "AAPL", "Apple Inc", "$1.5T", "229"],
  ["2", "MSFT", "Microsoft Corp", "$931B", "241"],
  ["3", "NVDA", "NVIDIA Corporation", "$1.7T", "245"],
  ["4", "AMZN", "Amazon.com Inc", "$668B", "249"],
  ["5", "GOOGL", "Alphabet Inc Class A", "$602B", "230"],
];

const movingSectors = [
  ["Technology", "+4.25%", "28.6%", true, [38, 40, 39, 44, 42, 48, 46, 51, 49, 54, 53, 58]],
  ["Financials", "+1.97%", "17.3%", true, [35, 34, 37, 36, 40, 39, 42, 41, 45, 44, 46, 49]],
  ["Healthcare", "+1.23%", "12.9%", true, [32, 34, 33, 36, 35, 38, 37, 39, 40, 42, 41, 44]],
  ["Industrials", "+0.98%", "10.4%", true, [31, 30, 32, 34, 33, 35, 36, 37, 36, 39, 38, 41]],
  ["Energy", "-1.24%", "4.3%", false, [50, 48, 49, 46, 44, 45, 42, 40, 41, 38, 36, 35]],
  ["Real Estate", "-1.89%", "3.1%", false, [53, 51, 49, 50, 47, 45, 46, 43, 41, 39, 38, 36]],
] as const;

const notableFilings = [
  ["BlackRock, Inc.", "NVDA", "Buy", "$2.3B", "May 15, 2026"],
  ["Vanguard Group", "AAPL", "Buy", "$1.4B", "May 15, 2026"],
  ["State Street Corp", "MSFT", "Buy", "$987M", "May 15, 2026"],
  ["Fidelity Investments", "AMZN", "Buy", "$842M", "May 15, 2026"],
  ["Capital Group Companies", "META", "Buy", "$612M", "May 14, 2026"],
];

const activityBars = [
  { label: "Q2 '24", inc: 0.65, dec: 0.52, positions: 122 },
  { label: "Q3 '24", inc: 0.82, dec: 0.68, positions: 148 },
  { label: "Q4 '24", inc: 1.08, dec: 0.98, positions: 166 },
  { label: "Q1 '25", inc: 0.96, dec: 0.58, positions: 148 },
  { label: "Q2 '25", inc: 1.02, dec: 0.72, positions: 160 },
  { label: "Q3 '25", inc: 1.06, dec: 0.76, positions: 171 },
  { label: "Q4 '25", inc: 1.04, dec: 0.72, positions: 169 },
  { label: "Q1 '26", inc: 1.12, dec: 0.64, positions: 188 },
];

export function InstitutionProfilesDashboard() {
  return (
    <section className="relative min-w-0 overflow-hidden pb-4">
      <HeroGlow />
      <header className="relative z-10 flex min-w-0 flex-col gap-4 pt-2 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.32em] text-emerald-300">Institutions</p>
          <h1 className="mt-1 text-4xl font-semibold leading-tight text-white">Institutional Holdings</h1>
          <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">Track institutional portfolios, quarterly position changes, accumulation, and sector exposure.</p>
        </div>
        <button type="button" className="flex w-fit items-center gap-2 rounded-2xl border border-slate-700/70 bg-slate-950/70 px-5 py-3 text-sm font-semibold text-slate-100 shadow-lg shadow-slate-950/30">
          Q1 2026 <span aria-hidden="true" className="text-slate-500">v</span>
        </button>
      </header>

      <main className="relative z-10 mt-4 space-y-2.5">
        <section className="grid gap-3 xl:grid-cols-[1.65fr_1fr]">
          <Panel className="min-h-[258px]" title="Institutional Holdings Snapshot" subtitle="Aggregate reported portfolio value over time" action={<SegmentedControl items={["Value", "Positions", "8Q"]} active="Value" />}>
            <PortfolioSnapshot />
          </Panel>
          <div className="grid gap-3 sm:grid-cols-2">
            {snapshotTiles.map((tile) => <SnapshotTile key={tile.label} {...tile} />)}
          </div>
        </section>

        <section className="grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
          {metricCards.map((metric) => <MetricTile key={metric.label} metric={metric} />)}
        </section>

        <section className="grid gap-3 xl:grid-cols-[1fr_1fr]">
          <Panel title="Top Institutions by Portfolio Value">
            <DataTable
              headers={["Rank", "Institution", "Portfolio Value", "Previous Quarter", "QoQ Change", "Positions", "Largest Holding"]}
              rows={topInstitutions.map((row) => [
                row[0],
                <Link key={row[1]} href={`/profiles/institutions/${slugify(row[1])}`} prefetch={false} className="font-semibold text-emerald-300 hover:text-emerald-100">{row[1]}</Link>,
                row[2],
                row[3],
                <span key={row[4]} className="text-emerald-300">{row[4]}</span>,
                row[5],
                row[6],
              ])}
              alignRight={[2, 3, 4, 5]}
            />
          </Panel>
          <Panel title="Top Increased Positions">
            <DataTable
              headers={["Rank", "Ticker", "Company", "Current Value", "Previous Value", "Increase", "Institutions"]}
              rows={increasedPositions.map((row) => [
                row[0],
                <Link key={row[1]} href={`/ticker/${row[1]}`} prefetch={false} className="font-semibold text-emerald-300 hover:text-emerald-100">{row[1]}</Link>,
                row[2],
                row[3],
                row[4],
                <span key={row[5]} className="text-emerald-300">{row[5]}</span>,
                row[6],
              ])}
              alignRight={[3, 4, 5, 6]}
            />
          </Panel>
        </section>

        <section className="grid gap-3 xl:grid-cols-[1.05fr_0.95fr_1fr]">
          <Panel title="Sector Exposure Over Time" action={<span className="rounded-lg border border-slate-700/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-300">8 Quarters v</span>}>
            <SectorExposureChart />
          </Panel>
          <Panel title="Net Position Change by Sector" action={<span className="rounded-lg border border-slate-700/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-300">QoQ Change v</span>}>
            <NetSectorChart />
          </Panel>
          <Panel title="Increases vs Decreases Mix">
            <ChangeMixDonut />
          </Panel>
        </section>

        <section className="grid gap-3 xl:grid-cols-[0.95fr_0.85fr_1fr]">
          <Panel title="Most Widely Held Stocks">
            <DataTable
              headers={["Rank", "Ticker", "Company", "Total Market Value", "Institutions"]}
              rows={widelyHeld.map((row) => [
                row[0],
                <Link key={row[1]} href={`/ticker/${row[1]}`} prefetch={false} className="font-semibold text-emerald-300 hover:text-emerald-100">{row[1]}</Link>,
                row[2],
                row[3],
                row[4],
              ])}
              alignRight={[3, 4]}
            />
          </Panel>
          <Panel title="Top Moving Sectors">
            <MovingSectors />
          </Panel>
          <Panel title="Recent Notable Filings">
            <RecentFilings />
          </Panel>
        </section>

        <Panel title="Activity Over Time (Quarterly)" action={<span className="rounded-lg border border-slate-700/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-300">8 Quarters v</span>}>
          <ActivityOverTime />
        </Panel>
      </main>
    </section>
  );
}

function Panel({ title, subtitle, action, children, className = "" }: { title: string; subtitle?: string; action?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`min-w-0 rounded-xl border border-slate-700/70 bg-slate-950/58 p-4 shadow-[0_20px_70px_rgba(15,23,42,0.26)] ${className}`}>
      <div className="mb-3 flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold uppercase tracking-[0.24em] text-white">{title} <span className="text-slate-500">i</span></h2>
          {subtitle ? <p className="mt-1 text-xs text-slate-400">{subtitle}</p> : null}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function SnapshotTile({ label, title, value, detail, icon, tone, spark }: (typeof snapshotTiles)[number]) {
  return (
    <div className="relative grid min-h-[122px] grid-cols-[42px_1fr] gap-3 rounded-xl border border-slate-700/70 bg-slate-900/55 p-4">
      <IconBadge icon={icon} tone={tone} />
      <div className="min-w-0 pr-24">
        <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
        <p className="mt-1 text-base font-semibold leading-tight text-white">{title}</p>
        <p className="mt-1 text-lg font-semibold text-emerald-300">{value}</p>
        <p className="text-xs text-slate-400">{detail}</p>
      </div>
      <div className="absolute right-4 top-5">
        <MiniTrend values={[...spark]} positive={tone !== "amber"} color={tone === "purple" ? "#a855f7" : tone === "amber" ? "#fbbf24" : "#34d399"} />
      </div>
    </div>
  );
}

function PortfolioSnapshot() {
  const width = 980;
  const height = 225;
  const chartTop = 18;
  const chartHeight = 145;
  const x = (index: number) => 70 + (index / (portfolioValues.length - 1)) * (width - 120);
  const y = (value: number) => chartTop + (1 - (value - 25) / 20) * chartHeight;
  const points = portfolioValues.map((value, index) => `${x(index).toFixed(1)},${y(value).toFixed(1)}`).join(" ");
  const area = `70,${chartTop + chartHeight} ${points} ${width - 50},${chartTop + chartHeight}`;

  return (
    <div className="min-w-0">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-52 w-full" role="img" aria-label="Institutional portfolio value over time">
        <defs>
          <linearGradient id="institution-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#34d399" stopOpacity="0.58" />
            <stop offset="100%" stopColor="#34d399" stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {[25, 30, 35, 40, 45].map((tick) => {
          const ty = y(tick);
          return (
            <g key={tick}>
              <line x1="70" x2={width - 50} y1={ty} y2={ty} stroke="#243044" strokeWidth="1" />
              <text x="18" y={ty + 4} fill="#94a3b8" fontSize="12">{`$${tick}T`}</text>
            </g>
          );
        })}
        <polygon points={area} fill="url(#institution-area)" />
        <polyline points={points} fill="none" stroke="#6ee7b7" strokeWidth="4" />
        {portfolioValues.map((value, index) => <circle key={`${value}-${index}`} cx={x(index)} cy={y(value)} r="4.5" fill="#bbf7d0" stroke="#34d399" strokeWidth="2" />)}
        {quarters.map((label, index) => <text key={label} x={x(index)} y={height - 22} fill="#94a3b8" fontSize="12" textAnchor="middle">{label}</text>)}
      </svg>
      <div className="mt-1 flex items-center gap-3 text-sm font-semibold text-emerald-300">
        <span>+ $10.2T</span>
        <span>+33.4%</span>
        <span className="font-medium text-slate-400">over 8 quarters</span>
      </div>
    </div>
  );
}

function MetricTile({ metric }: { metric: MetricCard }) {
  return (
    <div className="grid min-h-[72px] grid-cols-[1fr_128px] gap-3 rounded-xl border border-slate-700/70 bg-slate-950/58 p-3">
      <div className="min-w-0">
        <p className="text-[10px] font-semibold uppercase tracking-[0.23em] text-slate-500">{metric.label} <span className="text-slate-600">i</span></p>
        <p className="mt-2 text-2xl font-semibold text-white">{metric.value}</p>
        <p className={`mt-1 text-xs font-semibold ${metric.positive ? "text-emerald-300" : "text-rose-300"}`}>
          <span className="font-medium text-slate-400">{metric.comparison}</span> <span>{metric.delta}</span>
        </p>
      </div>
      <Sparkline values={metric.spark} color={metric.tone === "red" ? "#fb7185" : "#34d399"} />
    </div>
  );
}

function SectorExposureChart() {
  return (
    <div className="min-w-0">
      <Legend items={sectorSeries.map(({ label, color }) => ({ label, color }))} />
      <div className="mt-4 grid grid-cols-[44px_1fr] gap-x-2 gap-y-2 text-xs">
        {[100, 75, 50, 25, 0].map((tick) => <span key={tick} className="text-right text-slate-500">{tick}%</span>)}
        <div className="row-span-5 grid grid-cols-8 items-end gap-5 border-l border-slate-800/80 bg-[linear-gradient(to_bottom,rgba(148,163,184,0.13)_1px,transparent_1px)] bg-[length:100%_25%] px-5">
          {quarters.map((label, quarterIndex) => (
            <div key={label} className="flex min-w-0 flex-col items-center gap-2">
              <div className="flex h-36 w-full max-w-11 flex-col-reverse overflow-hidden rounded-t-sm border border-slate-700/60 bg-slate-900">
                {sectorSeries.map((sector) => (
                  <span key={sector.label} style={{ height: `${sector.values[quarterIndex]}%`, backgroundColor: sector.color }} />
                ))}
              </div>
              <span className="whitespace-nowrap text-slate-400">{label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function NetSectorChart() {
  const max = 600;
  return (
    <div className="space-y-1.5">
      {netSectorChange.map(([label, value]) => (
        <div key={label} className="grid grid-cols-[150px_1fr_58px] items-center gap-3 text-xs">
          <span className="truncate text-slate-300">{label}</span>
          <div className="relative h-4 rounded bg-slate-800/75">
            <span className="absolute left-1/2 top-0 h-full w-px bg-slate-600/70" />
            <span
              className={`absolute top-0 h-full rounded ${value >= 0 ? "left-1/2 bg-emerald-400/80" : "right-1/2 bg-rose-400/80"}`}
              style={{ width: `${Math.min(50, (Math.abs(value) / max) * 50)}%` }}
            />
          </div>
          <span className={`text-right font-semibold tabular-nums ${value >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{value >= 0 ? `+$${value}B` : `-$${Math.abs(value)}B`}</span>
        </div>
      ))}
      <div className="grid grid-cols-[150px_1fr_58px] gap-3 pt-1 text-[10px] text-slate-500">
        <span />
        <div className="flex justify-between"><span>-300</span><span>0</span><span>150</span><span>300</span><span>450</span><span>600</span></div>
      </div>
    </div>
  );
}

function ChangeMixDonut() {
  return (
    <div className="grid min-h-52 items-center gap-5 md:grid-cols-[1fr_0.95fr]">
      <Donut segments={[90.9, 9.1]} colors={["#4ade80", "#f0526e"]} value="160,867" detail="Total Changes" />
      <div className="space-y-4 text-sm">
        <LegendRow color="#4ade80" label="Increases" value="146,319 (90.9%)" />
        <LegendRow color="#f0526e" label="Decreases" value="14,548 (9.1%)" />
        <div className="border-t border-slate-800 pt-4">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Net Value Change</p>
          <p className="mt-1 text-2xl font-semibold text-emerald-300">+$10.0T</p>
        </div>
      </div>
    </div>
  );
}

function MovingSectors() {
  return (
    <div className="min-w-0">
      <div className="grid grid-cols-[1fr_86px_108px_68px] gap-3 border-b border-slate-700/70 pb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
        <span>Sector</span>
        <span className="text-right">QoQ Change</span>
        <span>Trend (8Q)</span>
        <span className="text-right">Exposure</span>
      </div>
      <div className="divide-y divide-slate-800/80 text-xs">
        {movingSectors.map(([label, change, exposure, positive, spark]) => (
          <div key={label} className="grid grid-cols-[1fr_86px_108px_68px] items-center gap-3 py-2">
            <span className="truncate text-slate-300">{label}</span>
            <span className={`text-right font-semibold ${positive ? "text-emerald-300" : "text-rose-300"}`}>{change}</span>
            <MiniTrend values={[...spark]} positive={positive} color={positive ? "#34d399" : "#fb7185"} />
            <span className="text-right text-slate-300">{exposure}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function RecentFilings() {
  return (
    <div className="min-w-0">
      <div className="grid grid-cols-[minmax(0,1.2fr)_64px_70px_82px_104px] gap-3 border-b border-slate-700/70 pb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
        <span>Institution</span>
        <span>Ticker</span>
        <span>Action</span>
        <span className="text-right">Value</span>
        <span className="text-right">Date</span>
      </div>
      <div className="divide-y divide-slate-800/80 text-xs">
        {notableFilings.map((row) => (
          <div key={`${row[0]}-${row[1]}`} className="grid grid-cols-[minmax(0,1.2fr)_64px_70px_82px_104px] gap-3 py-2">
            <span className="truncate font-semibold text-slate-100">{row[0]}</span>
            <Link href={`/ticker/${row[1]}`} prefetch={false} className="font-semibold text-emerald-300 hover:text-emerald-100">{row[1]}</Link>
            <span className="font-semibold text-emerald-300">{row[2]}</span>
            <span className="text-right font-semibold tabular-nums text-slate-100">{row[3]}</span>
            <span className="text-right text-slate-300">{row[4]}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActivityOverTime() {
  const width = 1400;
  const height = 260;
  const baseY = 137;
  const step = (width - 120) / activityBars.length;
  const linePoints = activityBars.map((item, index) => {
    const x = 80 + index * step + step / 2;
    const y = 28 + (1 - item.positions / 200) * 178;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");

  return (
    <div className="min-w-0">
      <div className="mb-3 flex flex-wrap gap-5 text-xs text-slate-300">
        <LegendRow color="#34d399" label="Position Increases" compact />
        <LegendRow color="#f0526e" label="Position Decreases" compact />
        <LegendRow color="#60a5fa" label="Total Positions (Right Axis)" compact />
      </div>
      <svg viewBox={`0 0 ${width + 120} ${height + 50}`} className="h-72 w-full" role="img" aria-label="Institutional activity over time">
        {[-2, -1, 0, 1, 2].map((tick) => {
          const y = baseY - (tick / 2) * 100;
          return (
            <g key={tick}>
              <line x1="70" x2={width + 10} y1={y} y2={y} stroke="#243044" strokeWidth="1" />
              <text x="20" y={y + 4} fill="#94a3b8" fontSize="12">{tick === 0 ? "$0" : `${tick < 0 ? "-" : ""}$${Math.abs(tick)}T`}</text>
            </g>
          );
        })}
        {activityBars.map((item, index) => {
          const x = 80 + index * step + step / 2 - 30;
          const incH = (item.inc / 2) * 100;
          const decH = (item.dec / 2) * 100;
          return (
            <g key={item.label}>
              <rect x={x} y={baseY - incH} width="60" height={incH} rx="3" fill="#34d399" opacity="0.78" />
              <rect x={x} y={baseY} width="60" height={decH} rx="3" fill="#f0526e" opacity="0.8" />
              <text x={x + 30} y={height + 26} fill="#94a3b8" fontSize="12" textAnchor="middle">{item.label}</text>
            </g>
          );
        })}
        <polyline points={linePoints} fill="none" stroke="#60a5fa" strokeWidth="3" />
        {linePoints.split(" ").map((point) => {
          const [cx, cy] = point.split(",");
          return <circle key={point} cx={cx} cy={cy} r="4.5" fill="#bfdbfe" stroke="#60a5fa" strokeWidth="2" />;
        })}
        {[0, 40, 80, 120, 160, 200].map((tick) => {
          const y = 28 + (1 - tick / 200) * 178;
          return <text key={tick} x={width + 30} y={y + 4} fill="#94a3b8" fontSize="12">{tick === 0 ? "0" : `${tick}K`}</text>;
        })}
        <text x="12" y="180" fill="#94a3b8" fontSize="11" transform="rotate(-90 12 180)">Value Change</text>
        <text x={width + 88} y="176" fill="#94a3b8" fontSize="11" transform={`rotate(90 ${width + 88} 176)`}>Total Positions</text>
      </svg>
    </div>
  );
}

function DataTable({ headers, rows, alignRight = [] }: { headers: string[]; rows: ReactNode[][]; alignRight?: number[] }) {
  return (
    <div className="min-w-0 overflow-hidden">
      <table className="w-full table-fixed border-collapse text-xs">
        <thead>
          <tr className="border-b border-slate-700/70">
            {headers.map((header, index) => (
              <th key={header} className={`px-2 pb-2 text-left font-semibold uppercase tracking-[0.18em] text-slate-500 ${alignRight.includes(index) ? "text-right" : ""}`}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/80">
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {row.map((cell, index) => (
                <td key={index} className={`truncate px-2 py-1.5 align-middle text-slate-200 ${alignRight.includes(index) ? "text-right font-semibold tabular-nums" : ""}`}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Donut({ segments, colors, value, detail }: { segments: number[]; colors: string[]; value: string; detail: string }) {
  let offset = 25;
  return (
    <div className="relative mx-auto h-48 w-48">
      <svg viewBox="0 0 120 120" className="h-full w-full -rotate-90">
        <circle cx="60" cy="60" r="43" fill="none" stroke="#1f2937" strokeWidth="16" />
        {segments.map((segment, index) => {
          const length = (segment / 100) * 270;
          const current = offset;
          offset += length;
          return <circle key={index} cx="60" cy="60" r="43" fill="none" stroke={colors[index]} strokeWidth="16" strokeDasharray={`${length} 270`} strokeDashoffset={-current} />;
        })}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <p className="text-3xl font-semibold text-white">{value}</p>
        <p className="mt-1 text-xs text-slate-300">{detail}</p>
      </div>
    </div>
  );
}

function Sparkline({ values, color }: { values: number[]; color: string }) {
  const path = linePath(values, 126, 42, 4);
  const area = `${path.replace(/^M/, "M")} L126 48 L0 48 Z`;
  return (
    <svg viewBox="0 0 128 50" className="h-12 w-32 self-end" aria-hidden="true">
      <path d={area} fill={color} opacity="0.12" />
      <path d={path} fill="none" stroke={color} strokeWidth="2.3" />
    </svg>
  );
}

function MiniTrend({ values, positive, color }: { values: number[]; positive: boolean; color?: string }) {
  const stroke = color ?? (positive ? "#34d399" : "#fb7185");
  return (
    <svg viewBox="0 0 112 32" className="h-8 w-28" aria-hidden="true">
      <path d={linePath(values, 108, 24, 3)} fill="none" stroke={stroke} strokeWidth="2" />
    </svg>
  );
}

function SegmentedControl({ items, active }: { items: string[]; active: string }) {
  return (
    <div className="flex shrink-0 rounded-lg border border-slate-700/70 bg-slate-950/70 p-1">
      {items.map((item) => <span key={item} className={`rounded-md px-4 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] ${item === active ? "bg-emerald-400/18 text-emerald-100" : "text-slate-400"}`}>{item}</span>)}
    </div>
  );
}

function IconBadge({ icon, tone }: { icon: string; tone: string }) {
  const color = tone === "purple" ? "text-purple-300 border-purple-300/40 bg-purple-300/10" : tone === "amber" ? "text-amber-300 border-amber-300/40 bg-amber-300/10" : "text-emerald-300 border-emerald-300/40 bg-emerald-300/10";
  return (
    <span className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border ${color}`}>
      <svg viewBox="0 0 24 24" className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="1.8">
        {icon === "bank" ? <><path d="M4 10h16" /><path d="M6 10v8" /><path d="M10 10v8" /><path d="M14 10v8" /><path d="M18 10v8" /><path d="M3 18h18" /><path d="m12 4 8 4H4z" /></> : null}
        {icon === "chart" ? <><path d="M5 18V6" /><path d="M5 18h14" /><path d="m8 15 3-4 3 2 4-6" /></> : null}
        {icon === "arrow" ? <><path d="M7 17 17 7" /><path d="M9 7h8v8" /></> : null}
        {icon === "pie" ? <><path d="M12 3v9h9" /><path d="M21 12a9 9 0 1 1-9-9" /></> : null}
      </svg>
    </span>
  );
}

function Legend({ items }: { items: Array<{ label: string; color: string }> }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-300">
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

function HeroGlow() {
  return (
    <div className="pointer-events-none absolute inset-x-[-2rem] top-0 h-80 bg-[radial-gradient(circle_at_20%_12%,rgba(34,211,238,0.13),transparent_30%),radial-gradient(circle_at_76%_6%,rgba(16,185,129,0.14),transparent_28%)]" />
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

function slugify(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}
