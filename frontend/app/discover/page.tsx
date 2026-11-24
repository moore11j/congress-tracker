import type { Metadata } from "next";
import Link from "next/link";
import { DiscoveryStocks } from "@/components/discover/DiscoveryStocks";
import { ResearchWorkspaceNav } from "@/components/discover/ResearchWorkspaceNav";

export const metadata: Metadata = { title: "Discover | Walnut Markets", robots: { index: false, follow: true } };
const toolGroups = [
  { label: "Explore the market", links: [
    ["Activity Feed", "/?mode=all"], ["Screener", "/screener"], ["Compare companies", "/compare"],
    ["Signals", "/signals"], ["Leaderboards", "/leaderboards"], ["Market Maps", "/market-pressure"],
    ["Insights", "/insights"], ["Research briefs", "/research"],
  ] },
  { label: "Follow the activity", links: [
    ["Congress", "/members"], ["Insiders", "/insiders"], ["Institutions", "/institutions"],
    ["Government contracts", "/government-contracts"], ["Departments", "/departments"],
  ] },
  { label: "Test an idea", links: [
    ["Strategies", "/strategies"], ["Backtesting", "/backtesting"], ["Signal Mixer", "/signal-mixer"],
    ["Outcomes", "/outcomes"], ["Retirement calculator", "/retirement-calculator"], ["Options calculator", "/options-calculator"],
  ] },
] as const;

export default function DiscoverPage() {
  return <div className="mx-auto max-w-7xl py-4 sm:py-6">
    <ResearchWorkspaceNav active="discover" />
    <header className="grid gap-5 py-6 sm:py-8 lg:grid-cols-[1fr_minmax(320px,480px)] lg:items-center lg:gap-10">
      <div>
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.22em] text-emerald-300">Your research workspace</p>
        <h1 className="text-3xl font-semibold tracking-tight text-white sm:text-4xl">Discover</h1>
        <p className="mt-2 text-sm text-slate-400">Find a company. Understand the evidence.</p>
      </div>
      <form action="/search" method="get" className="flex min-w-0 items-center gap-2 rounded-lg border border-white/15 bg-white/[0.035] p-1.5 focus-within:border-emerald-300/60">
        <label htmlFor="discover-search" className="sr-only">Search companies or tickers</label>
        <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" className="ml-2 h-5 w-5 shrink-0 text-slate-500"><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 4.5 4.5" /></svg>
        <input id="discover-search" name="q" type="search" required maxLength={120} placeholder="Company or ticker" className="min-w-0 flex-1 bg-transparent py-2.5 text-base text-white placeholder:text-slate-500 focus:outline-none" />
        <button type="submit" className="shrink-0 rounded-md bg-emerald-300 px-4 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">Search</button>
      </form>
    </header>
    <div className="grid items-start gap-6 lg:grid-cols-[minmax(0,1fr)_280px] xl:gap-8">
      <section aria-labelledby="discover-ideas" className="min-w-0">
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/15 py-5">
          <div className="flex items-center gap-2.5"><span aria-hidden="true" className="h-4 w-0.5 bg-emerald-300" /><h2 id="discover-ideas" className="text-lg font-semibold tracking-tight text-white">Stocks to investigate</h2></div>
          <Link href="/leaderboards#top-stocks" prefetch={false} className="text-xs font-medium text-slate-400 transition hover:text-emerald-200">All rankings <span aria-hidden="true">↗</span></Link>
        </div>
        <DiscoveryStocks />
      </section>
      <aside className="min-w-0 border-t border-white/15 lg:border-l lg:border-t-0 lg:pl-6 xl:pl-8">
        <section aria-labelledby="your-workspace" className="py-5 lg:pt-0">
          <h2 id="your-workspace" className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Your workspace</h2>
          <Link href="/my-stocks" prefetch={false} className="group mt-4 block rounded-lg border border-emerald-300/20 bg-emerald-300/[0.045] p-4 transition hover:border-emerald-300/40">
            <span className="flex items-center justify-between text-sm font-semibold text-white">My Stocks <span aria-hidden="true" className="text-emerald-300 transition group-hover:translate-x-1">→</span></span>
            <span className="mt-2 block text-xs leading-5 text-slate-400">Return to your watchlists and saved activity.</span>
          </Link>
          <div className="mt-2 divide-y divide-white/5">
            <Link href="/monitoring" prefetch={false} className="flex items-center justify-between py-3 text-sm text-slate-300 hover:text-emerald-200">Research inbox <span aria-hidden="true" className="text-slate-600">↗</span></Link>
            <Link href="/monitoring/research" prefetch={false} className="flex items-center justify-between gap-2 py-3 text-sm text-slate-300 hover:text-emerald-200">Research Memory <span className="text-[10px] text-slate-500">Premium / Pro</span></Link>
          </div>
        </section>
        <section id="advanced-research" aria-labelledby="advanced-research-title" className="scroll-mt-6 border-t border-white/10 pt-5">
          <h2 id="advanced-research-title" className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Advanced Research</h2>
          <div className="mt-5 grid gap-6 sm:grid-cols-3 lg:grid-cols-1">
            {toolGroups.map(group => <div key={group.label}>
              <h3 className="mb-2 text-xs font-semibold text-slate-200">{group.label}</h3>
              <div className="flex flex-col items-start gap-0.5">{group.links.filter(([, href]) => !(href === "/strategies" && process.env.NEXT_PUBLIC_STRATEGIES_ENABLED === "0") && !(href === "/outcomes" && process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0")).map(([label, href]) => <Link key={href} href={href} prefetch={false} className="flex min-h-11 items-center py-2 text-xs text-slate-400 transition hover:text-emerald-200 lg:min-h-0 lg:py-1.5">{label}</Link>)}</div>
            </div>)}
          </div>
        </section>
      </aside>
    </div>
  </div>;
}
