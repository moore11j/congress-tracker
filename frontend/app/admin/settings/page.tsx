import Link from "next/link";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AdminSettingsPanel } from "@/components/admin/AdminSettingsPanel";
import type { Metadata } from "next";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Admin Settings | Walnut Markets",
  robots: {
    index: false,
    follow: true,
  },
};

export default function AdminPanelPage() {
  return (
    <VerifiedSessionGuard returnTo="/admin/settings" requireAdmin>
      <div className="space-y-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Operations</p>
            <h1 className="text-3xl font-semibold text-white">Admin panel</h1>
          </div>
          <Link href="/account/billing" prefetch={false} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Account
          </Link>
        </div>
        <AdminSettingsPanel />
      </div>
    </VerifiedSessionGuard>
  );
}
