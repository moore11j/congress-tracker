import { ResearchMemoryIndex } from "@/components/research-memory/ResearchMemoryWorkspace";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { requirePageAuth } from "@/lib/serverAuth";
import Link from "next/link";

export default async function ResearchMemoryPage({ searchParams }: { searchParams: Promise<{ ticker?: string; create?: string }> }) {
  const authToken = await requirePageAuth("/monitoring/research");
  const params = await searchParams;
  return <VerifiedSessionGuard returnTo="/monitoring/research" initiallyAuthorized={Boolean(authToken)}><div className="space-y-6"><nav aria-label="Inbox and Monitoring" className="flex w-full gap-1 overflow-x-auto rounded-xl border border-white/10 bg-slate-950/45 p-1"><Link href="/monitoring" className="shrink-0 rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 hover:bg-white/5 hover:text-white">Inbox</Link><Link href="/monitoring" className="shrink-0 rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 hover:bg-white/5 hover:text-white">Monitoring</Link><Link href="/monitoring/research" className="shrink-0 rounded-lg bg-emerald-400/15 px-4 py-2 text-sm font-semibold text-emerald-100">Research Memory</Link></nav><ResearchMemoryIndex initialTicker={params.ticker?.trim().toUpperCase() || (params.create ? "__choose__" : null)} /></div></VerifiedSessionGuard>;
}
