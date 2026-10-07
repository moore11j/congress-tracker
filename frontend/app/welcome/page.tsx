import Link from "next/link";
import type { Metadata } from "next";
import { TopIdeasOptIn } from "@/components/auth/TopIdeasOptIn";
import { safeAppReturnPath } from "@/lib/returnPaths";

export const metadata: Metadata = {
  title: "Start your research | Walnut Markets",
  robots: { index: false, follow: false },
};

export default async function WelcomePage({searchParams}: {searchParams: Promise<{return_to?: string}>}) {
  const params = await searchParams;
  const returnTo = safeAppReturnPath(params.return_to, "/leaderboards#top-stocks");
  const destination = new URL(returnTo, "https://app.walnutmarkets.com");
  const ticker = /^\/ticker\/([A-Z^][A-Z0-9.-]{0,14})\/?$/i.exec(destination.pathname)?.[1]?.toUpperCase();
  const followsStock = Boolean(ticker && destination.searchParams.get("follow") === "1");
  const stockSearch = (
    <form action="/search" method="get" className="mt-6 space-y-3">
      <label htmlFor="welcome-stock" className="block text-sm font-medium text-slate-200">Company or ticker</label>
      <input id="welcome-stock" name="q" required maxLength={120} placeholder="e.g. Apple or AAPL" className="w-full rounded-lg border border-white/15 bg-slate-900 px-4 py-3 text-white focus:border-emerald-300" />
      <button type="submit" className="w-full rounded-lg bg-emerald-300 px-4 py-3 font-semibold text-slate-950">Find a stock</button>
    </form>
  );
  return (
    <section className="mx-auto my-12 max-w-xl rounded-xl border border-white/10 bg-slate-950/70 p-6 sm:p-8">
      <p className="text-sm font-semibold text-emerald-300">Welcome to Walnut</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">{ticker ? `Continue your ${ticker} research` : "Which stock are you considering?"}</h1>
      <p className="mt-3 text-sm leading-6 text-slate-300">
        {followsStock ? `Continue to ${ticker} to finish saving it to your watchlist and return to its research.` : ticker ? `Pick up where you left off, then use Follow to save ${ticker} to your watchlist.` : "Open a stock, check what supports it and what deserves a second look, then use Follow to save it to your watchlist."}
      </p>
      {params.return_to && <Link href={returnTo} className="mt-5 inline-flex min-h-12 w-full items-center justify-center rounded-lg bg-emerald-300 px-4 py-3 text-center font-semibold text-slate-950 hover:bg-emerald-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-emerald-300">{ticker ? `Continue to ${ticker} →` : "Continue to your research →"}</Link>}
      {followsStock && <p className="mt-3 text-xs leading-5 text-slate-400">Watchlist emails may be sent when enabled in your notification settings, subject to your plan and delivery preferences. You can manage delivery from your watchlist.</p>}
      {ticker ? <details className="mt-5 text-sm text-slate-300"><summary className="cursor-pointer font-medium text-emerald-200">Research a different stock</summary>{stockSearch}</details> : stockSearch}
      <Link href="/leaderboards#top-stocks" className="mt-5 inline-flex text-sm font-semibold text-emerald-200 underline underline-offset-4">Need an idea? Explore your free Top 5 stocks →</Link>
      <TopIdeasOptIn />
      <Link href="/?mode=all" className="mt-5 inline-block text-sm text-emerald-200">Or explore the market feed</Link>
      <p className="mt-6 text-xs leading-5 text-slate-400">If you signed up with email, check your inbox for the verification link. Some features require a paid plan.</p>
    </section>
  );
}
