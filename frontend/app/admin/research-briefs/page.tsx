import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AdminSettingsPanel } from "@/components/admin/AdminSettingsPanel";

export const dynamic = "force-dynamic";

export default function AdminResearchBriefsPage() {
  return (
    <VerifiedSessionGuard returnTo="/admin/research-briefs" requireAdmin>
      <div className="space-y-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Operations</p>
            <h1 className="text-3xl font-semibold text-white">Research Briefs</h1>
          </div>
          <Link href="/admin/settings" prefetch={false} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Admin panel
          </Link>
        </div>
        <AdminSettingsPanel initialTab="research_briefs" />
      </div>
    </VerifiedSessionGuard>
  );
}
