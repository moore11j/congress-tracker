import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Start your research | Walnut Markets",
  robots: { index: false, follow: false },
};

export default function WelcomePage() {
  return (
    <section className="mx-auto my-12 max-w-xl rounded-xl border border-white/10 bg-slate-950/70 p-6 sm:p-8">
      <p className="text-sm font-semibold text-emerald-300">Welcome to Walnut</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">Start with a stock you follow.</h1>
      <p className="mt-3 text-sm leading-6 text-slate-300">
        Search for a company, open its stock page, then select Follow to save it to your watchlist.
      </p>
      <form action="/search" method="get" className="mt-6 space-y-3">
        <label htmlFor="welcome-stock" className="block text-sm font-medium text-slate-200">Company or ticker</label>
        <input id="welcome-stock" name="q" required maxLength={120} placeholder="e.g. Apple or AAPL" className="w-full rounded-lg border border-white/15 bg-slate-900 px-4 py-3 text-white focus:border-emerald-300" />
        <button type="submit" className="w-full rounded-lg bg-emerald-300 px-4 py-3 font-semibold text-slate-950">Find a stock</button>
      </form>
      <Link href="/?mode=all" className="mt-5 inline-block text-sm text-emerald-200">Or explore the market feed</Link>
      <p className="mt-6 text-xs leading-5 text-slate-400">If you signed up with email, check your inbox for the verification link. Some features require a paid plan.</p>
    </section>
  );
}
