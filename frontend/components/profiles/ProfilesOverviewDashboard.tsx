import Link from "next/link";
import type { ReactNode } from "react";

type PlayerCard = {
  kind: "congress" | "insiders" | "institutions" | "departments";
  title: string;
  description: string;
  href: string;
  metrics: Array<{ label: string; value: string; change: string }>;
  spark: number[];
};

type ActivityRow = {
  time: string;
  profile: string;
  type: string;
  ticker: string;
  activity: string;
  value: string;
  score: number;
};

const playerCards: PlayerCard[] = [
  {
    kind: "congress",
    title: "Congress",
    description: "Disclosed trades and portfolio activity from U.S. lawmakers.",
    href: "/members",
    metrics: [
      { label: "Trades", value: "39,442", change: "+18.6%" },
      { label: "Active Members", value: "360", change: "+4.9%" },
    ],
    spark: [24, 34, 29, 42, 39, 48, 43, 51, 46, 58, 44, 62, 53, 55, 49, 68],
  },
  {
    kind: "insiders",
    title: "Insiders",
    description: "Track buying and selling by executives and major shareholders.",
    href: "/insiders",
    metrics: [
      { label: "Trades", value: "160,026", change: "+21.3%" },
      { label: "Active Insiders", value: "7,392", change: "+6.7%" },
    ],
    spark: [18, 31, 26, 37, 34, 42, 30, 48, 45, 52, 39, 57, 49, 63, 58, 76],
  },
  {
    kind: "institutions",
    title: "Institutions",
    description: "Institutional portfolios and quarterly position changes.",
    href: "/institutions",
    metrics: [
      { label: "Institutions", value: "708", change: "+3.2%" },
      { label: "Portfolio Value", value: "$381B", change: "+8.7%" },
    ],
    spark: [16, 23, 21, 28, 24, 31, 30, 37, 35, 43, 41, 49, 47, 58, 54, 72],
  },
  {
    kind: "departments",
    title: "Departments",
    description: "Government contract awards and agency spending activity.",
    href: "/departments",
    metrics: [
      { label: "Departments / Agencies", value: "38", change: "+2.6%" },
      { label: "Contract Value", value: "$148B", change: "+11.4%" },
    ],
    spark: [26, 29, 21, 35, 27, 31, 24, 43, 38, 45, 32, 50, 47, 59, 43, 72],
  },
];

const activitySeries = {
  labels: ["Jun '24", "Jul '24", "Aug '24", "Sep '24", "Oct '24", "Nov '24", "Dec '24", "Jan '25", "Feb '25", "Mar '25", "Apr '25", "May '25"],
  congress: [15000, 16000, 17000, 15500, 19000, 18000, 16500, 17000, 19500, 18500, 17600, 18800],
  insiders: [35000, 38000, 40000, 39000, 44000, 41000, 36000, 43000, 48000, 42000, 41000, 43000],
  institutions: [58000, 62000, 69000, 72000, 79000, 74000, 65000, 71000, 84000, 69000, 76000, 74000],
  departments: [2800, 3100, 3300, 3000, 3600, 3500, 3900, 4100, 4300, 4500, 4800, 5000],
};

const profileMix = [
  { label: "Insiders", value: 45.2, color: "#2f81f7" },
  { label: "Institutions", value: 29.3, color: "#9333ea" },
  { label: "Congress", value: 14.1, color: "#31c48d" },
  { label: "Departments", value: 11.4, color: "#d99a1b" },
];

const sectorRows = [
  ["Technology", 28642, [18, 22, 36, 24]],
  ["Health Care", 18793, [24, 31, 27, 18]],
  ["Industrials", 14892, [28, 20, 25, 27]],
  ["Financials", 12331, [19, 31, 34, 16]],
  ["Energy", 7894, [32, 22, 30, 16]],
  ["Consumer Discretionary", 6201, [21, 25, 36, 18]],
  ["Defense", 5612, [16, 20, 22, 42]],
  ["Utilities", 2431, [22, 28, 27, 23]],
] as const;

const latestActivity: ActivityRow[] = [
  { time: "2m ago", profile: "Mark Warner", type: "Congress", ticker: "NVDA", activity: "Purchase", value: "$245,600", score: 92 },
  { time: "7m ago", profile: "Satya Nadella", type: "Insider", ticker: "MSFT", activity: "Purchase", value: "$1,245,300", score: 88 },
  { time: "12m ago", profile: "BlackRock", type: "Institution", ticker: "AAPL", activity: "Increased", value: "$152,430,000", score: 87 },
  { time: "18m ago", profile: "Dept. of Defense", type: "Department", ticker: "RTX", activity: "Contract Award", value: "$2,340,000,000", score: 85 },
  { time: "24m ago", profile: "Nancy Pelosi", type: "Congress", ticker: "NVDA", activity: "Purchase", value: "$512,300", score: 83 },
];

const movers = [
  { title: "Top Congress by Trading Value", href: "/members", cta: "View Congress", rows: [["Nancy Pelosi", "$8.42M"], ["Mark Warner", "$6.31M"], ["Josh Gottheimer", "$4.27M"], ["Dan Crenshaw", "$3.15M"], ["Suzan DelBene", "$2.91M"]] },
  { title: "Top Insiders by Net Buying", href: "/insiders", cta: "View Insiders", rows: [["Satya Nadella", "$45.7M"], ["Tim Cook", "$32.4M"], ["Nikesh Arora", "$18.9M"], ["Lisa Su", "$16.2M"], ["Jensen Huang", "$15.3M"]] },
  { title: "Top Institutions by Portfolio Value", href: "/institutions", cta: "View Institutions", rows: [["BlackRock", "$1.24T"], ["Vanguard", "$812B"], ["State Street", "$498B"], ["Fidelity", "$441B"], ["Capital Group", "$348B"]] },
  { title: "Top Departments by Contract Value", href: "/departments", cta: "View Departments", rows: [["Dept. of Defense", "$92.4B"], ["Dept. of Health", "$18.7B"], ["Dept. of Energy", "$9.6B"], ["NASA", "$7.2B"], ["Dept. of Homeland", "$5.1B"]] },
];

const heatmapRows = [
  ["Congress", [1.24, 0.76, -0.18, 0.32, -0.65, -0.12, 1.05, 0.21, 0.08, -0.09]],
  ["Insiders", [1.85, 1.34, 0.42, 0.77, 0.15, 0.63, 1.28, 0.33, 0.19, 0.04]],
  ["Institutions", [2.13, 1.62, 0.88, 1.01, 0.27, 0.95, 1.67, 0.46, 0.34, 0.11]],
  ["Departments", [-0.32, -0.05, -0.12, 0.21, 0.78, -0.18, 0.92, 0.15, -0.06, 0.03]],
] as const;

const heatmapColumns = ["Tech", "Health Care", "Financials", "Industrials", "Energy", "Consumer Disc.", "Defense", "Utilities", "Real Estate", "Materials"];

const stackedActivity = [
  { period: "Q3 '23", values: [42000, 46000, 76000, 12000] },
  { period: "Q4 '23", values: [44000, 50000, 83000, 13000] },
  { period: "Q1 '24", values: [41000, 78000, 91000, 14000] },
  { period: "Q2 '24", values: [47000, 84000, 108000, 15000] },
  { period: "Q3 '24", values: [52000, 96000, 124000, 16000] },
  { period: "Q4 '24", values: [54000, 101000, 129000, 17000] },
  { period: "Q1 '25", values: [59000, 108000, 136000, 18000] },
  { period: "Q2 '25", values: [68000, 121000, 152000, 20000] },
];

const lineColors = {
  Congress: "#31c48d",
  Insiders: "#2f81f7",
  Institutions: "#9333ea",
  Departments: "#d99a1b",
};

export function ProfilesOverviewDashboard() {
  return (
    <div className="relative min-w-0 overflow-hidden pb-3">
      <HeroBackground />
      <section className="relative z-10 min-w-0 space-y-3">
        <header className="pt-2">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-emerald-300">Profiles</p>
          <h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">Follow the market's major players</h1>
          <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">Track activity across Congress, corporate insiders, institutions, and government departments.</p>
        </header>

        <section className="grid gap-3 xl:grid-cols-4">
          {playerCards.map((card) => (
            <ProfileCard key={card.kind} card={card} />
          ))}
        </section>

        <section className="grid gap-3 xl:grid-cols-[1.45fr_0.82fr_1.18fr]">
          <ActivityLinePanel />
          <ActivityDonutPanel />
          <SectorMoversPanel />
        </section>

        <section className="grid gap-3 xl:grid-cols-[1.35fr_1.85fr]">
          <LatestActivityPanel />
          <FastestMovingPanel />
        </section>

        <section className="grid gap-3 xl:grid-cols-[1fr_1fr]">
          <HeatmapPanel />
          <StackedActivityPanel />
        </section>
      </section>
    </div>
  );
}

function ProfileCard({ card }: { card: PlayerCard }) {
  return (
    <Link href={card.href} prefetch={false} className="group relative min-h-[174px] overflow-hidden rounded-lg border border-slate-700/70 bg-[#101827]/88 p-4 shadow-[0_18px_50px_rgba(0,0,0,0.22)] transition hover:border-emerald-300/45 hover:bg-[#121d2f]">
      <div className="flex min-w-0 items-start gap-3">
        <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 text-emerald-200 shadow-[0_0_24px_rgba(52,211,153,0.14)]">
          <ProfileGlyph kind={card.kind} />
        </div>
        <div className="min-w-0 flex-1">
          <h2 className="text-base font-semibold leading-5 text-white">{card.title}</h2>
          <p className="mt-1 max-w-[15rem] text-xs leading-5 text-slate-300/80">{card.description}</p>
        </div>
        <Sparkline values={card.spark} className="mt-3 hidden w-28 shrink-0 sm:block" />
      </div>
      <div className="mt-4 grid grid-cols-2 gap-2">
        {card.metrics.map((metric) => (
          <div key={metric.label} className="rounded-md border border-slate-700/80 bg-[#0c1423]/80 p-3">
            <div className="text-xl font-semibold tabular-nums text-white">{metric.value}</div>
            <div className="mt-1 flex min-w-0 items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">
              <span className="truncate">{metric.label}</span>
              <span className="whitespace-nowrap text-emerald-300">{metric.change}</span>
            </div>
          </div>
        ))}
      </div>
      <span className="mt-3 inline-flex items-center text-sm font-semibold text-emerald-200 group-hover:text-emerald-100">View {card.title} -&gt;</span>
    </Link>
  );
}

function ActivityLinePanel() {
  return (
    <Panel title="Activity by Profile Type" action="12M">
      <Legend items={[["Congress", lineColors.Congress], ["Insiders", lineColors.Insiders], ["Institutions", lineColors.Institutions], ["Departments", lineColors.Departments]]} />
      <div className="mt-3 h-40">
        <MultiLineChart />
      </div>
    </Panel>
  );
}

function ActivityDonutPanel() {
  return (
    <Panel title="Where the activity is">
      <div className="grid min-h-[184px] grid-cols-[1fr_0.9fr] items-center gap-2">
        <DonutChart />
        <div className="space-y-3">
          {profileMix.map((item) => (
            <div key={item.label} className="flex items-center justify-between gap-3 text-xs text-slate-300">
              <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: item.color }} />{item.label}</span>
              <span className="font-semibold tabular-nums text-slate-100">{item.value.toFixed(1)}%</span>
            </div>
          ))}
        </div>
      </div>
    </Panel>
  );
}

function SectorMoversPanel() {
  const max = Math.max(...sectorRows.map((row) => row[1]));
  return (
    <Panel title="Top moving sectors" action="By Net Activity">
      <div className="mt-3 space-y-2.5">
        {sectorRows.map(([sector, net, mix]) => (
          <div key={sector} className="grid grid-cols-[7.2rem_minmax(0,1fr)_4rem] items-center gap-3 text-xs">
            <span className="truncate text-slate-300">{sector}</span>
            <div className="h-3 overflow-hidden rounded-sm bg-slate-800/80">
              <div className="flex h-full" style={{ width: `${Math.max(9, (net / max) * 100)}%` }}>
                {mix.map((part, index) => (
                  <span key={index} style={{ width: `${part}%`, backgroundColor: chartColor(index) }} />
                ))}
              </div>
            </div>
            <span className="text-right font-semibold tabular-nums text-emerald-300">+{net.toLocaleString()}</span>
          </div>
        ))}
      </div>
      <Legend compact items={[["Congress", lineColors.Congress], ["Insiders", lineColors.Insiders], ["Institutions", lineColors.Institutions], ["Departments", lineColors.Departments]]} />
    </Panel>
  );
}

function LatestActivityPanel() {
  return (
    <Panel title="Latest Profile Activity" action="View All Activity ->">
      <div className="mt-2 flex flex-wrap gap-2">
        {["All", "Congress", "Insiders", "Institutions", "Departments"].map((filter, index) => (
          <span key={filter} className={`rounded-full border px-3 py-1 text-xs font-semibold ${index === 0 ? "border-emerald-300/40 bg-emerald-400/15 text-emerald-100" : "border-slate-700 text-slate-300"}`}>{filter}</span>
        ))}
      </div>
      <div className="mt-3 overflow-hidden">
        <table className="w-full text-left text-xs">
          <thead className="text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <tr>
              <th className="py-1.5 font-semibold">Time</th>
              <th className="py-1.5 font-semibold">Profile</th>
              <th className="py-1.5 font-semibold">Type</th>
              <th className="py-1.5 font-semibold">Ticker</th>
              <th className="py-1.5 font-semibold">Activity</th>
              <th className="py-1.5 text-right font-semibold">Value</th>
              <th className="py-1.5 text-right font-semibold">Score</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/90">
            {latestActivity.map((row) => (
              <tr key={`${row.time}-${row.profile}`} className="text-slate-300">
                <td className="py-1.5 text-slate-400">{row.time}</td>
                <td className="py-1.5 font-medium text-slate-200">{row.profile}</td>
                <td className="py-1.5"><TypeBadge type={row.type} /></td>
                <td className="py-1.5 font-mono text-slate-200">{row.ticker}</td>
                <td className="py-1.5 text-emerald-300">{row.activity}</td>
                <td className="py-1.5 text-right tabular-nums text-slate-100">{row.value}</td>
                <td className="py-1.5 text-right"><span className="rounded-full border border-emerald-400/40 px-2 py-0.5 text-[10px] font-semibold text-emerald-300">{row.score}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Link href="/feed" prefetch={false} className="mt-2 block text-center text-xs font-semibold text-emerald-200 hover:text-emerald-100">View All Activity -&gt;</Link>
    </Panel>
  );
}

function FastestMovingPanel() {
  return (
    <Panel title="Fastest-Moving Profiles">
      <div className="mt-3 grid gap-4 md:grid-cols-4">
        {movers.map((section) => (
          <div key={section.title} className="min-w-0 border-slate-800/90 md:border-r md:pr-4 last:border-r-0">
            <h3 className="truncate text-xs font-semibold text-white">{section.title}</h3>
            <div className="mt-2 space-y-1.5">
              {section.rows.map(([name, value], index) => (
                <div key={name} className="grid grid-cols-[1rem_minmax(0,1fr)_3.7rem] items-center gap-2 text-xs">
                  <span className="text-slate-500">{index + 1}</span>
                  <span className="truncate text-slate-300">{name}</span>
                  <span className="text-right tabular-nums text-slate-100">{value}</span>
                </div>
              ))}
            </div>
            <Link href={section.href} prefetch={false} className="mt-4 inline-flex text-xs font-semibold text-emerald-200 hover:text-emerald-100">{section.cta} -&gt;</Link>
          </div>
        ))}
      </div>
    </Panel>
  );
}

function HeatmapPanel() {
  return (
    <Panel title="Cross-profile signal heatmap">
      <div className="mt-3 overflow-hidden">
        <div className="grid grid-cols-[5.5rem_repeat(10,minmax(0,1fr))] gap-1 text-[10px] uppercase tracking-[0.08em] text-slate-400">
          <span>Profile Type</span>
          {heatmapColumns.map((column) => <span key={column} className="truncate text-center">{column}</span>)}
        </div>
        <div className="mt-2 space-y-1">
          {heatmapRows.map(([label, values]) => (
            <div key={label} className="grid grid-cols-[5.5rem_repeat(10,minmax(0,1fr))] gap-1 text-xs">
              <span className="self-center truncate text-slate-300">{label}</span>
              {values.map((value, index) => (
                <span key={`${label}-${index}`} className="rounded-sm py-2 text-center font-semibold tabular-nums text-white" style={{ background: heatColor(value) }}>
                  {value > 0 ? "+" : ""}{value.toFixed(2)}
                </span>
              ))}
            </div>
          ))}
        </div>
        <div className="mt-3 grid grid-cols-[1fr_1fr_1fr_1fr_1fr] items-center gap-0 text-[10px] text-slate-400">
          <span className="text-right">-2.0</span>
          <span className="h-1.5 bg-red-500" />
          <span className="h-1.5 bg-amber-400" />
          <span className="h-1.5 bg-emerald-400" />
          <span>+2.0</span>
        </div>
      </div>
    </Panel>
  );
}

function StackedActivityPanel() {
  return (
    <Panel title="Activity over time (Quarterly)" action="8 Quarters">
      <Legend items={[["Congress", lineColors.Congress], ["Insiders", lineColors.Insiders], ["Institutions", lineColors.Institutions], ["Departments", lineColors.Departments]]} />
      <div className="mt-3 h-40">
        <StackedAreaChart />
      </div>
    </Panel>
  );
}

function Panel({ title, action, children }: { title: string; action?: string; children: ReactNode }) {
  return (
    <section className="min-w-0 rounded-lg border border-slate-700/70 bg-[#101827]/88 p-4 shadow-[0_18px_50px_rgba(0,0,0,0.2)]">
      <div className="flex min-w-0 items-center justify-between gap-3">
        <h2 className="truncate text-sm font-semibold text-white">{title} <span className="text-slate-500">i</span></h2>
        {action ? <span className="rounded-md border border-slate-700 bg-slate-950/40 px-2 py-1 text-xs font-medium text-slate-300">{action}</span> : null}
      </div>
      {children}
    </section>
  );
}

function MultiLineChart() {
  const series = [
    { values: activitySeries.congress, color: lineColors.Congress },
    { values: activitySeries.insiders, color: lineColors.Insiders },
    { values: activitySeries.institutions, color: lineColors.Institutions },
    { values: activitySeries.departments, color: lineColors.Departments },
  ];
  return (
    <svg viewBox="0 0 720 180" className="h-full w-full" role="img" aria-label="Activity by profile type">
      {[0, 1, 2, 3].map((line) => <line key={line} x1="0" x2="720" y1={30 + line * 36} y2={30 + line * 36} stroke="#233044" strokeWidth="1" />)}
      {series.map((item) => <path key={item.color} d={linePath(item.values, 720, 150, 15)} fill="none" stroke={item.color} strokeWidth="2" />)}
      {activitySeries.labels.map((label, index) => <text key={label} x={index * 62 + 4} y="174" fill="#94a3b8" fontSize="11">{label}</text>)}
    </svg>
  );
}

function DonutChart() {
  let offset = 0;
  const circumference = 100;
  return (
    <div className="relative mx-auto h-40 w-40">
      <svg viewBox="0 0 42 42" className="h-full w-full -rotate-90">
        <circle cx="21" cy="21" r="15.9" fill="transparent" stroke="#1f2a3c" strokeWidth="7" />
        {profileMix.map((item) => {
          const strokeDasharray = `${item.value} ${circumference - item.value}`;
          const strokeDashoffset = -offset;
          offset += item.value;
          return <circle key={item.label} cx="21" cy="21" r="15.9" fill="transparent" stroke={item.color} strokeWidth="7" strokeDasharray={strokeDasharray} strokeDashoffset={strokeDashoffset} />;
        })}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <div className="text-lg font-semibold text-white">199,876</div>
        <div className="text-[10px] uppercase tracking-[0.13em] text-slate-400">Total Activity<br />(12M)</div>
      </div>
    </div>
  );
}

function StackedAreaChart() {
  const max = Math.max(...stackedActivity.map((row) => row.values.reduce((sum, value) => sum + value, 0)));
  const width = 720;
  const height = 150;
  const colors = [lineColors.Departments, lineColors.Congress, lineColors.Insiders, lineColors.Institutions];
  const layers = [3, 0, 1, 2];
  let previousTop = stackedActivity.map(() => height);
  return (
    <svg viewBox="0 0 720 180" className="h-full w-full" role="img" aria-label="Quarterly activity over time">
      {[0, 1, 2].map((line) => <line key={line} x1="0" x2="720" y1={25 + line * 45} y2={25 + line * 45} stroke="#233044" strokeWidth="1" />)}
      {layers.map((layer, layerIndex) => {
        const top = stackedActivity.map((row, index) => previousTop[index] - (row.values[layer] / max) * height);
        const area = areaPath(previousTop, top, width);
        previousTop = top;
        return <path key={layer} d={area} fill={colors[layerIndex]} opacity={layerIndex === 3 ? 0.85 : 0.72} stroke={colors[layerIndex]} strokeWidth="1.5" />;
      })}
      {stackedActivity.map((row, index) => <text key={row.period} x={index * 96 + 2} y="174" fill="#94a3b8" fontSize="11">{row.period}</text>)}
    </svg>
  );
}

function Sparkline({ values, className }: { values: number[]; className?: string }) {
  return (
    <svg viewBox="0 0 120 54" className={className} aria-hidden="true">
      <defs>
        <linearGradient id={`spark-fill-${values.length}-${values[0]}`} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#34d399" stopOpacity="0.35" />
          <stop offset="100%" stopColor="#34d399" stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={`${linePath(values, 120, 42, 6)} L120 54 L0 54 Z`} fill={`url(#spark-fill-${values.length}-${values[0]})`} />
      <path d={linePath(values, 120, 42, 6)} fill="none" stroke="#34d399" strokeWidth="2" />
    </svg>
  );
}

function Legend({ items, compact = false }: { items: Array<[string, string]>; compact?: boolean }) {
  return (
    <div className={`mt-3 flex flex-wrap ${compact ? "gap-x-4 gap-y-1" : "gap-x-6 gap-y-2"}`}>
      {items.map(([label, color]) => (
        <span key={label} className="inline-flex items-center gap-2 text-xs text-slate-300">
          <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color }} />
          {label}
        </span>
      ))}
    </div>
  );
}

function TypeBadge({ type }: { type: string }) {
  const color = type === "Congress" ? "emerald" : type === "Insider" ? "blue" : type === "Institution" ? "purple" : "amber";
  const className =
    color === "emerald"
      ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-300"
      : color === "blue"
        ? "border-blue-400/40 bg-blue-400/10 text-blue-300"
        : color === "purple"
          ? "border-purple-400/40 bg-purple-400/10 text-purple-300"
          : "border-amber-400/40 bg-amber-400/10 text-amber-300";
  return <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${className}`}>{type}</span>;
}

function HeroBackground() {
  return (
    <div className="pointer-events-none absolute inset-x-0 top-0 h-80 overflow-hidden border-b border-slate-800/80 bg-[radial-gradient(circle_at_30%_10%,rgba(35,78,120,0.22),transparent_28%),linear-gradient(180deg,rgba(6,13,25,0.78),rgba(6,13,25,0))]">
      <svg viewBox="0 0 900 240" className="absolute right-4 top-5 h-56 w-[48rem] max-w-[58vw] opacity-80" aria-hidden="true">
        <defs>
          <radialGradient id="map-dot" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#6ee7b7" stopOpacity="0.95" />
            <stop offset="100%" stopColor="#10b981" stopOpacity="0" />
          </radialGradient>
        </defs>
        {[...Array(75)].map((_, index) => (
          <circle key={index} cx={120 + (index * 53) % 650} cy={70 + ((index * 37) % 88)} r="1.2" fill="#2dd4bf" opacity={0.1 + (index % 4) * 0.05} />
        ))}
        {[
          "M110 132 C220 12 332 12 444 116",
          "M280 132 C374 30 470 28 560 116",
          "M430 126 C514 14 630 12 746 122",
          "M188 136 C318 58 514 48 676 136",
          "M540 124 C620 45 730 42 810 130",
        ].map((path, index) => (
          <path key={path} d={path} fill="none" stroke="#34d399" strokeWidth={index === 3 ? "1" : "1.4"} opacity={0.45} />
        ))}
        {[110, 280, 430, 746, 810].map((x, index) => <circle key={x} cx={x} cy={index === 1 ? 132 : 124} r="8" fill="url(#map-dot)" />)}
      </svg>
    </div>
  );
}

function ProfileGlyph({ kind }: { kind: PlayerCard["kind"] }) {
  if (kind === "insiders") {
    return (
      <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="1.8">
        <circle cx="10" cy="8" r="3" />
        <path d="M4.5 19c.8-3.4 2.6-5.1 5.5-5.1s4.7 1.7 5.5 5.1" />
        <path d="M17 6h4M19 4v4" />
      </svg>
    );
  }
  if (kind === "departments") {
    return (
      <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="1.8">
        <path d="M12 3 20 6v5c0 5-3.2 8.3-8 10-4.8-1.7-8-5-8-10V6l8-3Z" />
        <path d="m9 12 2 2 4-5" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="1.8">
      <path d="M4 10h16" />
      <path d="M6 10v8M10 10v8M14 10v8M18 10v8" />
      <path d="M3 18h18M5 21h14M12 3l8 5H4l8-5Z" />
    </svg>
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

function areaPath(bottom: number[], top: number[], width: number) {
  const step = width / Math.max(1, top.length - 1);
  const topPath = top.map((y, index) => `${index === 0 ? "M" : "L"}${(index * step).toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const bottomPath = bottom.map((y, index) => `L${((bottom.length - 1 - index) * step).toFixed(1)} ${bottom[bottom.length - 1 - index].toFixed(1)}`).join(" ");
  return `${topPath} ${bottomPath} Z`;
}

function chartColor(index: number) {
  return [lineColors.Congress, lineColors.Insiders, lineColors.Institutions, lineColors.Departments][index % 4];
}

function heatColor(value: number) {
  const alpha = Math.min(0.78, 0.22 + Math.abs(value) / 2.5);
  return value >= 0 ? `rgba(34, 197, 94, ${alpha})` : `rgba(239, 68, 68, ${alpha})`;
}
