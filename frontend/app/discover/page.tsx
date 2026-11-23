import type { Metadata } from "next";
import Link from "next/link";
import { DiscoveryStocks } from "@/components/discover/DiscoveryStocks";
import { ResearchWorkspaceNav } from "@/components/discover/ResearchWorkspaceNav";

export const metadata: Metadata = { title: "Discover | Walnut Markets", robots: { index: false, follow: true } };
const researchTools = [
  ["Activity Feed", "/?mode=all"], ["Screener", "/screener"], ["Compare companies", "/compare"],
  ["Signals", "/signals"], ["Leaderboards", "/leaderboards"], ["Research briefs", "/research"],
  ["Congress", "/members"], ["Insiders", "/insiders"], ["Institutions", "/institutions"],
  ["Government contracts", "/government-contracts"], ["Departments", "/departments"],
  ["Strategies", "/strategies"], ["Backtesting", "/backtesting"], ["Signal Mixer", "/signal-mixer"],
  ["Outcomes", "/outcomes"], ["Market Maps", "/market-pressure"], ["Insights", "/insights"],
  ["Retirement calculator", "/retirement-calculator"], ["Options calculator", "/options-calculator"],
] as const;

export default function DiscoverPage() {
  return <div className="mx-auto max-w-6xl space-y-8 py-5 sm:py-8">
    <ResearchWorkspaceNav active="discover" />
    <header className="rounded-3xl border border-emerald-200/15 bg-gradient-to-br from-emerald-300/[0.08] via-slate-900/60 to-slate-950 p-6 sm:p-9">
      <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">Discover</p>
      <h1 className="mt-3 max-w-2xl text-3xl font-semibold tracking-tight text-white sm:text-4xl">Find your next research idea.</h1>
      <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300">Start with a company or explore Walnut’s ranked stocks. Read the evidence, consider what disagrees, then follow the companies you want to understand.</p>
      <form action="/search" method="get" className="mt-6 flex max-w-2xl flex-col gap-2 sm:flex-row">
        <label htmlFor="discover-search" className="sr-only">Search companies or tickers</label>
        <input id="discover-search" name="q" type="search" required maxLength={120} placeholder="Search a company or ticker" className="min-w-0 flex-1 rounded-xl border border-white/15 bg-slate-950/70 px-4 py-3 text-base text-white placeholder:text-slate-500 focus:outline-none focus:ring-2 focus:ring-emerald-300/50" />
        <button type="submit" className="rounded-xl bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 hover:bg-emerald-200">Explore company</button>
      </form>
    </header>
    <Link href="/my-stocks" prefetch={false} className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-white/10 bg-white/[0.025] p-5">
      <div><h2 className="font-semibold text-white">Pick up where you left off</h2><p className="mt-1 text-sm text-slate-400">Open your watchlists and review saved activity.</p></div><span className="text-sm font-semibold text-emerald-200">My Stocks →</span>
    </Link>
    <section aria-labelledby="discover-ideas"><div className="mb-5 flex flex-wrap items-end justify-between gap-3"><div><h2 id="discover-ideas" className="text-2xl font-semibold text-white">Stocks to investigate</h2><p className="mt-2 text-sm text-slate-400">The existing Walnut ranking, with evidence one step away.</p></div><Link href="/leaderboards#top-stocks" prefetch={false} className="text-sm text-emerald-200">View rankings →</Link></div><DiscoveryStocks /></section>
    <section id="advanced-research" aria-labelledby="advanced-research-title" className="scroll-mt-6 rounded-2xl border border-white/10 p-5 sm:p-6"><h2 id="advanced-research-title" className="text-xl font-semibold text-white">Advanced Research</h2><p className="mt-2 text-sm text-slate-400">Go deeper with Walnut’s existing research tools.</p><div className="mt-5 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">{researchTools.filter(([,href]) => !(href === "/strategies" && process.env.NEXT_PUBLIC_STRATEGIES_ENABLED === "0") && !(href === "/outcomes" && process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0")).map(([label,href]) => <Link key={href} href={href} prefetch={false} className="rounded-xl px-3 py-3 text-sm text-slate-300 hover:bg-white/5 hover:text-white">{label} <span aria-hidden="true" className="text-slate-500">↗</span></Link>)}</div></section>
  </div>;
}
