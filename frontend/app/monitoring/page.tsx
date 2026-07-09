import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { MonitoringDashboard } from "@/components/monitoring/MonitoringDashboard";

export const dynamic = "force-static";
export const revalidate = false;

export default async function MonitoringPage() {
  return (
    <VerifiedSessionGuard returnTo="/monitoring" initiallyAuthorized={false}>
      <div className="space-y-8">
        <section className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Monitoring</p>
            <h1 className="mt-2 text-4xl font-semibold text-white">Inbox</h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-400">
              Watchlists and saved screens in one scan. Open a source when something needs a closer look.
            </p>
          </div>
          <Link
            href="/?mode=all"
            className="inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
          >
            Back to feed
          </Link>
        </section>

        <MonitoringDashboard initialWatchlists={[]} initialAuthPending />
      </div>
    </VerifiedSessionGuard>
  );
}
