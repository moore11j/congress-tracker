import type { Metadata } from "next";
import Link from "next/link";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { getResearchBriefBySlug } from "@/lib/researchBriefs";

export const dynamic = "force-static";

const brief = getResearchBriefBySlug("ai-earnings-dd");
const insightsHref = "/insights?utm_source=reddit&utm_medium=paid_social&utm_campaign=ai_earnings_dd&utm_content=research_page_insights";
const signupHref = "/login?mode=register&return_to=%2Finsights";

export const metadata: Metadata = {
  title: `${brief?.title ?? "AI Earnings DD"} | Walnut Markets Research`,
  description: brief?.description ?? "Walnut Markets research brief. Not investment advice.",
  alternates: {
    canonical: "/research/ai-earnings-dd",
  },
};

const headlineChecks = [
  { label: "Core question", value: "Numbers", detail: "Revenue, margin, cash flow, and guidance now matter more than AI mentions." },
  { label: "Market bar", value: "Clean guide", detail: "Strong results with messy forward commentary can still get sold." },
  { label: "AI spend test", value: "Capex return", detail: "Investors need evidence that infrastructure spending is converting into durable demand." },
  { label: "Read-through", value: "Broad chain", detail: "Cloud, semis, software, and enterprise IT can all reprice off the same earnings tape." },
] as const;

const watchItems = [
  {
    title: "AI capex vs actual monetization",
    body: "The strongest AI setups need a visible bridge from infrastructure spending to cloud, software, ads, or enterprise revenue.",
  },
  {
    title: "Guidance quality",
    body: "A good quarter is not enough if management points to weaker demand, longer sales cycles, or less pricing power ahead.",
  },
  {
    title: "Margins and free cash flow",
    body: "AI spending only works if the core business can support it. Capex-heavy stories need cash-flow evidence.",
  },
  {
    title: "Second-order market impact",
    body: "One report can reset expectations across suppliers, customers, peers, and the broader AI infrastructure trade.",
  },
] as const;

const tickerRows = [
  ["GOOGL", "AI capex vs search, ads, and cloud monetization"],
  ["TSLA", "AI narrative vs auto margins, demand, and free cash flow"],
  ["SNOW", "Enterprise AI data demand vs software growth durability"],
  ["IBM", "Enterprise AI services and infrastructure demand quality"],
  ["TXN", "Industrial and analog cycle read-through"],
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
              The AI story is already priced into a lot of large-cap tech. This earnings week is about whether that story is showing up in revenue, margins, guidance, free cash flow, and demand durability.
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
          <h2 className="mt-3 text-2xl font-semibold text-white">Good story plus weak numbers is not enough anymore.</h2>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            AI remains the dominant market narrative, but this tape is less forgiving. Investors are looking for proof that AI demand is moving from story to operating results.
          </p>
          <p className="mt-4 text-sm leading-7 text-slate-400">
            The cleaner setup is simple: good story, strong numbers, and credible guidance. Without that, the market can punish even high-quality AI names.
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
        <div className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[0.8fr_1.2fr] lg:px-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-amber-300">Names To Watch</p>
            <h2 className="mt-3 text-2xl font-semibold text-white">The headline tickers each test a different part of the AI trade.</h2>
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

      <section className="mx-auto grid max-w-7xl gap-8 px-4 py-10 sm:px-6 lg:grid-cols-[1fr_0.9fr] lg:px-8">
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
      </section>

      <section className="border-t border-white/10 px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl text-xs leading-6 text-slate-500">
          Walnut is a market intelligence platform for research and informational purposes only. Nothing on this page is financial, investment, tax, accounting, or legal advice.
        </div>
      </section>
    </main>
  );
}
