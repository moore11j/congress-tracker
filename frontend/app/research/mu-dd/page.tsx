import type { Metadata } from "next";
import Link from "next/link";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

export const dynamic = "force-static";

const signupHref = "/login?mode=register&return_to=%2Fticker%2FMU";
const muTickerHref = "/ticker/MU?utm_source=reddit&utm_medium=paid_social&utm_campaign=mu_dd_research_test&utm_content=mu_dd_landing_terminal";
const compareHref = "/compare/MU/NVDA?utm_source=reddit&utm_medium=paid_social&utm_campaign=mu_dd_research_test&utm_content=mu_dd_landing_compare";
const micronSourceHref = "https://investors.micron.com/news-releases/news-release-details/micron-technology-inc-reports-record-results-third-quarter";

export const metadata: Metadata = {
  title: "Is the MU Momentum Trade Dead? | Walnut Markets Research",
  description:
    "A research-only Micron DD landing page reviewing the memory-cycle data behind the MU momentum question. Not investment advice.",
  alternates: {
    canonical: "/research/mu-dd",
  },
};

const headlineMetrics = [
  { label: "Fiscal Q3 revenue", value: "$41.46B", detail: "Up from $23.86B in fiscal Q2 and $9.30B a year earlier." },
  { label: "GAAP EPS", value: "$24.67", detail: "Versus $12.07 in fiscal Q2 and $1.68 a year earlier." },
  { label: "GAAP gross margin", value: "84.6%", detail: "A sharp expansion from 74.4% in fiscal Q2." },
  { label: "Fiscal Q4 revenue guide", value: "$50.0B +/- $1.0B", detail: "Management guided gross margin to roughly 86%." },
] as const;

const thesisChecks = [
  {
    title: "Revenue is still accelerating",
    body: "The bear case needs the memory cycle to roll over. Micron's latest reported revenue and next-quarter guide point the other direction for now.",
  },
  {
    title: "Margins are carrying the argument",
    body: "The setup is less about top-line growth alone and more about pricing power showing up in gross margin and operating income.",
  },
  {
    title: "AI infrastructure is the demand bridge",
    body: "HBM and data-center memory demand are what make this cycle different from a generic PC or handset recovery story.",
  },
] as const;

const riskChecks = [
  "DRAM or NAND pricing weakens faster than expected.",
  "AI infrastructure spending pauses, stretches, or gets absorbed by inventory.",
  "Supply additions arrive before demand can absorb them.",
  "Margins peak before investors are positioned for a downcycle.",
  "The stock prices in too much cycle durability before the next data point.",
] as const;

const productHooks = [
  "Live MU ticker context",
  "Congress and insider activity",
  "Fundamental and technical checkpoints",
  "Peer compare against NVDA",
  "Research workflow built for follow-up",
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

export default function MuDdLandingPage() {
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
              Is the $MU momentum trade dead?
            </h1>
            <p className="mt-5 max-w-2xl text-lg leading-8 text-slate-300">
              Not yet. The bearish version needs the memory cycle to roll over, but Micron's latest revenue, margin, and guidance data still argue that the cycle is expanding.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <Link href={signupHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
                Create a free account
              </Link>
              <Link href={muTickerHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100">
                Open the MU terminal
              </Link>
            </div>
            <p className="mt-4 text-xs leading-5 text-slate-500">
              Research only. Not investment advice. No buy or sell recommendation.
            </p>
          </div>

          <div className="grid content-start gap-3 sm:grid-cols-2">
            {headlineMetrics.map((metric) => (
              <MetricCard key={metric.label} {...metric} />
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">The Answer</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">Cyclical? Yes. Broken? Not on the current data.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            Micron is still a memory-cycle stock, so the setup can change quickly. The current question is whether investors are seeing the end of the cycle or a pause inside a stronger AI-driven demand environment.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            The latest reported numbers support the second interpretation for now: record revenue, record profitability, and a higher fiscal Q4 outlook.
          </p>
          <Link href={compareHref} className="mt-6 inline-flex min-h-10 items-center justify-center rounded-lg border border-cyan-300/30 bg-cyan-300/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-300/15">
            Compare MU vs NVDA
          </Link>
        </div>

        <div className="grid gap-3">
          {thesisChecks.map((item) => (
            <article key={item.title} className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
              <h3 className="text-base font-semibold text-white">{item.title}</h3>
              <p className="mt-2 text-sm leading-6 text-slate-400">{item.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="border-y border-white/10 bg-slate-950/40">
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-2 lg:px-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-rose-300">What Would Change The View</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">The setup breaks if the cycle data turns first.</h2>
          </div>
          <ul className="grid gap-2">
            {riskChecks.map((risk) => (
              <li key={risk} className="rounded-lg border border-white/10 bg-slate-950/55 px-4 py-3 text-sm leading-6 text-slate-300">
                {risk}
              </li>
            ))}
          </ul>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Follow The Evidence</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">Use Walnut to keep the MU thesis updated.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            A Reddit post is a starting point. Walnut gives you the ongoing terminal view: the ticker page, peer comparison, market context, and public-data signals you can re-check as the cycle evolves.
          </p>
          <div className="mt-6 flex flex-wrap gap-3">
            <Link href={signupHref} className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
              Sign up to track MU
            </Link>
            <Link href={muTickerHref} className="inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-white/25 hover:text-white">
              Preview MU data
            </Link>
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <p className="text-sm font-semibold text-white">What users get after signup</p>
          <div className="mt-4 grid gap-2">
            {productHooks.map((hook) => (
              <div key={hook} className="rounded-md border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-300">
                {hook}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
          Source: Micron fiscal Q3 2026 results, reported June 24, 2026.{" "}
          <Link href={micronSourceHref} className="text-slate-300 underline decoration-white/20 underline-offset-4 hover:text-white">
            Micron investor release
          </Link>
          . Walnut is a market intelligence platform for research and informational purposes only. Nothing on this page is financial, investment, tax, accounting, or legal advice.
        </div>
      </section>
    </main>
  );
}
