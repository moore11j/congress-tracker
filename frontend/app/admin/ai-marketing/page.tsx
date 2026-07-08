import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AdminSettingsPanel } from "@/components/admin/AdminSettingsPanel";

export const dynamic = "force-dynamic";

export default function AdminAiMarketingPage() {
  return (
    <VerifiedSessionGuard returnTo="/admin/ai-marketing" requireAdmin>
      <div className="space-y-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Operations</p>
            <h1 className="text-3xl font-semibold text-white">AI Growth Engine</h1>
          </div>
          <Link href="/admin/settings" prefetch={false} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Admin panel
          </Link>
        </div>
        <AdminSettingsPanel initialTab="ai_marketing" />
      </div>
    </VerifiedSessionGuard>
  );
}
