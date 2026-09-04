import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { MonitoringDashboard } from "@/components/monitoring/MonitoringDashboard";
import { requirePageAuth } from "@/lib/serverAuth";

export default async function MonitoringPage() {
  const authToken = await requirePageAuth("/monitoring");

  return (
    <VerifiedSessionGuard returnTo="/monitoring" initiallyAuthorized={Boolean(authToken)}>
      <div className="space-y-8">
        <section className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Monitoring</p>
            <h1 className="mt-2 text-4xl font-semibold text-white">Inbox &amp; Monitoring</h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-400">
              Watchlists, saved screens, strategies, and monitoring alerts in one place.
            </p>
          </div>
          <Link
            href="/?mode=all"
            className="inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
          >
            Back to feed
          </Link>
        </section>

        <nav aria-label="Inbox and Monitoring" className="flex w-full gap-1 overflow-x-auto rounded-xl border border-white/10 bg-slate-950/45 p-1">
          <Link href="/monitoring" className="shrink-0 rounded-lg bg-emerald-400/15 px-4 py-2 text-sm font-semibold text-emerald-100">Inbox</Link>
          <Link href="/monitoring" className="shrink-0 rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 hover:bg-white/5 hover:text-white">Monitoring</Link>
          {process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED !== "false" ? <Link href="/monitoring/research" className="shrink-0 rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 hover:bg-white/5 hover:text-white">Research Memory</Link> : null}
        </nav>

        <MonitoringDashboard initialWatchlists={[]} initialAuthPending={!authToken} />
      </div>
    </VerifiedSessionGuard>
  );
}
