import type { Metadata } from "next";
import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { ResearchWorkspaceNav } from "@/components/discover/ResearchWorkspaceNav";
import { WatchlistsDashboard } from "@/components/watchlists/WatchlistsDashboard";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";

export const metadata: Metadata = { title: "My Stocks | Walnut Markets", robots: { index: false, follow: false } };

export default async function MyStocksPage({ searchParams }: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const returnTo = buildReturnTo("/my-stocks", await searchParams);
  const authToken = await requirePageAuth(returnTo);
  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <div className="mx-auto max-w-6xl space-y-7 py-5 sm:py-8">
        <ResearchWorkspaceNav active="my-stocks" />
        <header>
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">My Stocks</p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-white sm:text-4xl">Your research, in one place.</h1>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">Keep companies together, review their recent activity, and return to the evidence behind your ideas.</p>
        </header>
        <div className="grid gap-3 sm:grid-cols-3">
          {([
            ["/monitoring", "Review your inbox", "Read saved updates and manage notification preferences."],
            ["/monitoring/research", "Review your theses", "Open Research Memory to revisit your reasoning. Premium and Pro."],
            ["/search", "Find a company", "Open its research page and add it to a watchlist."],
          ] as const).map(([href, title, description]) => (
            <Link key={href} href={href} prefetch={false} className="rounded-2xl border border-white/10 bg-white/[0.025] p-5 transition hover:border-emerald-300/30">
              <h2 className="font-semibold text-white">{title} <span aria-hidden="true">↗</span></h2>
              <p className="mt-2 text-sm leading-6 text-slate-400">{description}</p>
            </Link>
          ))}
        </div>
        <WatchlistsDashboard initialWatchlists={[]} initialAuthPending={!authToken} />
        <p className="text-xs leading-5 text-slate-500">Unread counts describe saved activity. A quiet list does not establish that every source is current or that a thesis has been checked.</p>
      </div>
    </VerifiedSessionGuard>
  );
}
