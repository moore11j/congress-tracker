import type { Metadata } from "next";
import Link from "next/link";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { getResearchBriefBySlug } from "@/lib/researchBriefs";

export const dynamic = "force-static";

const brief = getResearchBriefBySlug("ai-earnings-dd");
const insightsHref = "/insights?utm_source=reddit&utm_medium=paid_social&utm_campaign=ai_earnings_dd&utm_content=research_page_insights";
const signupHref = "/login?mode=register&return_to=%2Finsights";
const alphabetSourceHref = "https://www.sec.gov/Archives/edgar/data/1652044/000165204426000043/googexhibit991q12026.htm";
const teslaQ1SourceHref = "https://www.sec.gov/Archives/edgar/data/1318605/000162828026026673/tsla-20260331.htm";
const teslaDeliverySourceHref = "https://ir.tesla.com/press-release/tesla-second-quarter-2026-production-deliveries-and-deployments";
const snowflakeSourceHref = "https://www.sec.gov/Archives/edgar/data/1640147/000164014726000027/fy2027q1earnings.htm";
const ibmSourceHref = "https://newsroom.ibm.com/2026-04-22-IBM-RELEASES-FIRST-QUARTER-RESULTS?lnk=hpln1id";
const txnSourceHref = "https://ti.gcs-web.com/news-releases/news-release-details/ti-reports-first-quarter-2026-financial-results-and-shareholder";

export const metadata: Metadata = {
  title: `${brief?.title ?? "AI Earnings DD"} | Walnut Markets Research`,
  description: brief?.description ?? "Walnut Markets research brief. Not investment advice.",
  alternates: {
    canonical: "/research/ai-earnings-dd",
  },
};

const headlineChecks = [
  { label: "Alphabet Q1 revenue", value: "$109.9B", detail: "Up 22% year over year; Google Cloud revenue grew 63% to $20.0B." },
  { label: "Tesla Q2 deliveries", value: "480,126", detail: "Versus 451,758 vehicles produced; energy storage deployments were 13.5 GWh." },
  { label: "Snowflake product revenue", value: "$1.33B", detail: "Fiscal Q1 product revenue grew 34% with RPO up 38% to $9.21B." },
  { label: "TXN Q1 revenue", value: "$4.83B", detail: "Up 19% year over year, with Q2 revenue guidance of $5.0B to $5.4B." },
] as const;

const watchItems = [
  {
    title: "AI capex vs actual monetization",
    body: "Alphabet spent $35.7B on property and equipment in Q1 and still generated $10.1B of free cash flow. That is the kind of bridge the market wants: AI infrastructure spend funded by core operating cash.",
  },
  {
    title: "Guidance quality",
    body: "Snowflake raised full-year product revenue guidance to $5.84B, while TXN guided Q2 revenue to $5.0B-$5.4B. The guide matters because the market is punishing AI stories that cannot raise the forward bar.",
  },
  {
    title: "Margins and free cash flow",
    body: "IBM posted 56.2% GAAP gross margin and $2.2B of free cash flow in Q1. Tesla posted a 21.1% automotive gross margin in Q1, but its Q2 setup still needs proof that higher deliveries convert into cash.",
  },
  {
    title: "Second-order market impact",
    body: "GOOGL tests hyperscale cloud demand, TSLA tests physical AI funding capacity, SNOW tests enterprise data consumption, IBM tests services/software durability, and TXN tests industrial, data-center, and analog demand.",
  },
] as const;

const tickerRows = [
  ["GOOGL", "Q1 revenue $109.9B, +22% YoY; Google Search & other +19%; Google Cloud $20.0B, +63%; operating margin 36.1%."],
  ["TSLA", "Q1 revenue $22.4B, +16% YoY; total automotive revenue $16.2B; Q2 deliveries 480,126 and storage deployments 13.5 GWh."],
  ["SNOW", "Fiscal Q1 revenue $1.39B, +33% YoY; product revenue $1.33B, +34%; net revenue retention 126%; RPO $9.21B, +38%."],
  ["IBM", "Q1 revenue $15.9B, +9%; Software +11%; Infrastructure +15%; free cash flow $2.2B; 2026 constant-currency revenue guide still >5%."],
  ["TXN", "Q1 revenue $4.83B, +19%; operating profit $1.81B, +37%; Analog revenue +22%; Q2 revenue guide $5.0B-$5.4B."],
] as const;

const dataRows = [
  {
    ticker: "GOOGL",
    lastPrint: "Q1 2026",
    revenue: "$109.9B, +22%",
    marginCash: "36.1% operating margin; $10.1B FCF",
    forwardBar: "Cloud growth was the clean AI read-through: $20.0B revenue, +63%, with Cloud operating income of $6.6B.",
  },
  {
    ticker: "TSLA",
    lastPrint: "Q1 financials / Q2 deliveries",
    revenue: "$22.4B Q1 revenue, +16%",
    marginCash: "21.1% auto gross margin; $1.44B Q1 FCF",
    forwardBar: "Q2 deliveries jumped to 480,126. The question is whether volume, price, and capex still leave cash for AI, robotaxi, and Optimus spend.",
  },
  {
    ticker: "SNOW",
    lastPrint: "Fiscal Q1 2027",
    revenue: "$1.39B, +33%",
    marginCash: "75.1% non-GAAP product gross margin; 16.7% FCF margin",
    forwardBar: "RPO grew 38% to $9.21B and product revenue guide moved to $5.84B for FY2027.",
  },
  {
    ticker: "IBM",
    lastPrint: "Q1 2026",
    revenue: "$15.9B, +9%",
    marginCash: "56.2% GAAP gross margin; $2.2B FCF",
    forwardBar: "Software revenue grew 11% and Infrastructure grew 15%; the AI services story needs those segments to keep compounding.",
  },
  {
    ticker: "TXN",
    lastPrint: "Q1 2026",
    revenue: "$4.83B, +19%",
    marginCash: "$1.81B operating profit; $1.40B FCF",
    forwardBar: "Industrial and data center led growth; Q2 revenue guide of $5.0B-$5.4B is the analog-cycle read-through.",
  },
] as const;

const riskChecks = [
  "Alphabet raises capex faster than Cloud, Search, or YouTube can monetize it.",
  "Tesla delivery strength fails to translate into gross profit, operating income, or free cash flow.",
  "Snowflake usage growth slows after a strong Q1, weakening the enterprise AI data-demand signal.",
  "IBM consulting demand or software mix softens, breaking the margin and cash-flow argument.",
  "TXN's industrial/data-center strength proves cyclical instead of durable.",
] as const;

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/65 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-white">{value}</p>
      <p className="mt-2 text-sm leading-6 text-slate-400">{detail}</p>
    </article>
  );
}

export default function AiEarningsDdPage() {
  return (
    <main className="-mx-4 -my-1.5 min-h-screen bg-[#06111f] text-slate-100 sm:-mx-6 lg:-mx-8 2xl:-mx-10">
      <section className="border-b border-white/10 bg-[linear-gradient(180deg,rgba(8,20,35,0.98),rgba(6,17,31,0.94))]">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1.05fr_0.95fr] lg:px-8 lg:py-14">
          <div className="flex min-w-0 flex-col justify-center">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">
              <WalnutBrandMark className="flex h-7 w-7 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950" svgClassName="h-5 w-5 overflow-visible" />
              Walnut DD Brief
            </div>
            <h1 className="mt-5 max-w-3xl text-4xl font-semibold leading-tight text-white sm:text-5xl">
              AI earnings week: the market wants numbers, not hype.
            </h1>
            <p className="mt-5 max-w-2xl text-lg leading-8 text-slate-300">
              The AI story is already priced into a lot of large-cap tech. The question now is whether the prior-quarter data and the next guide prove real demand: revenue growth, margin durability, cash generation, deliveries, and backlog.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link href={signupHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                Create a free account
              </Link>
              <Link href={insightsHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100">
                Open Walnut Insights
              </Link>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">Research only. Not investment advice. No buy or sell recommendation.</p>
          </div>

          <div className="grid content-start gap-3 sm:grid-cols-2">
            {headlineChecks.map((metric) => (
              <MetricCard key={metric.label} {...metric} />
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">The Setup</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">The last quarter already raised the bar.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            AI remains the dominant market narrative, but the tape is less forgiving. Alphabet already printed 22% revenue growth and 63% Google Cloud growth. Snowflake printed 34% product revenue growth. IBM and TXN both showed double-digit segment strength. Tesla delivered 480,126 vehicles in Q2 before its financial report.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            That is why the setup is not simply "did management mention AI?" The market wants to see the story convert into operating leverage, cash flow, better guidance, and demand indicators that can survive a higher spending cycle.
          </p>
          <Link href={insightsHref} className="mt-6 inline-flex min-h-10 items-center justify-center rounded-lg border border-cyan-300/30 bg-cyan-300/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-300/15">
            Track the read-throughs
          </Link>
        </div>

        <div className="grid gap-3">
          {watchItems.map((item) => (
            <article key={item.title} className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
              <h3 className="text-base font-semibold text-white">{item.title}</h3>
              <p className="mt-2 text-sm leading-6 text-slate-400">{item.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">Prior-Quarter Data</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">The earnings scorecard starts with the numbers already on the board.</h2>
            <p className="mt-4 text-sm leading-7 text-slate-400">
              These are the baseline figures to compare against as the next prints land. The bar is not just growth; it is whether growth is strong enough to fund AI investment without compressing margins or cash flow.
            </p>
          </div>

          <div className="mt-6 overflow-hidden rounded-lg border border-white/10">
            <div className="hidden grid-cols-[0.7fr_1fr_1fr_1fr_1.6fr] border-b border-white/10 bg-white/[0.04] px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:grid">
              <span>Ticker</span>
              <span>Last print</span>
              <span>Revenue</span>
              <span>Margin / cash</span>
              <span>What has to prove out</span>
            </div>
            <div className="divide-y divide-white/10 bg-slate-950/45">
              {dataRows.map((row) => (
                <article key={row.ticker} className="grid gap-3 px-4 py-4 text-sm leading-6 text-slate-300 md:grid-cols-[0.7fr_1fr_1fr_1fr_1.6fr] md:items-start">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Ticker</p>
                    <p className="font-semibold text-emerald-200">${row.ticker}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Last print</p>
                    <p>{row.lastPrint}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Revenue</p>
                    <p>{row.revenue}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Margin / cash</p>
                    <p>{row.marginCash}</p>
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">What has to prove out</p>
                    <p className="text-slate-400">{row.forwardBar}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.8fr_1.2fr] lg:px-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">Names To Watch</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">Each ticker is a different data test for the AI trade.</h2>
          </div>
          <div className="grid gap-2">
            {tickerRows.map(([ticker, detail]) => (
              <div key={ticker} className="grid gap-2 rounded-lg border border-white/10 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300 sm:grid-cols-[5.5rem_1fr]">
                <span className="font-semibold text-emerald-200">${ticker}</span>
                <span>{detail}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-2 lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-rose-300">What Would Break The View</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">The setup fails if the data stops funding the story.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            AI spending is not automatically bullish. It is bullish only when the core business can fund it and customers are paying enough to protect margins and free cash flow.
          </p>
        </div>
        <ul className="grid gap-2">
          {riskChecks.map((risk) => (
            <li key={risk} className="rounded-lg border border-white/10 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300">
              {risk}
            </li>
          ))}
        </ul>
      </section>

      <section className="border-t border-white/10 bg-slate-950/30">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Follow The Evidence</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">Use Walnut Insights to keep the earnings read-through current.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            A Reddit post is a starting point. Walnut Insights gives you the broader market context: news, macro positioning, market snapshot data, and research briefs you can revisit as earnings reports land.
          </p>
          <div className="mt-6 flex flex-wrap gap-3">
            <Link href={signupHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
              Sign up free
            </Link>
            <Link href={insightsHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-white/25 hover:text-white">
              Preview Insights
            </Link>
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <p className="text-sm font-semibold text-white">What to re-check after reports land</p>
          <div className="mt-4 grid gap-2">
            {["Revenue acceleration", "Gross and operating margin", "Capex guidance", "Free cash flow", "Management demand commentary"].map((hook) => (
              <div key={hook} className="rounded-md border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-300">
                {hook}
              </div>
            ))}
          </div>
        </div>
        </div>
      </section>

      <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
          Sources:{" "}
          <Link href={alphabetSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Alphabet Q1 2026 results
          </Link>
          ,{" "}
          <Link href={teslaQ1SourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Tesla Q1 2026 10-Q
          </Link>
          ,{" "}
          <Link href={teslaDeliverySourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Tesla Q2 2026 deliveries
          </Link>
          ,{" "}
          <Link href={snowflakeSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Snowflake fiscal Q1 2027 results
          </Link>
          ,{" "}
          <Link href={ibmSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            IBM Q1 2026 results
          </Link>
          , and{" "}
          <Link href={txnSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Texas Instruments Q1 2026 results
          </Link>
          . Data as of July 22, 2026 before after-hours Q2 earnings reports. Walnut is a market intelligence platform for research and informational purposes only. Nothing on this page is financial, investment, tax, accounting, or legal advice.
        </div>
      </section>
    </main>
  );
}
