import Link from "next/link";
import type { Metadata } from "next";
import { AdminOutcomesDiagnostics } from "@/components/admin/AdminOutcomesDiagnostics";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Admin Outcomes | Walnut Markets",
  robots: {
    index: false,
    follow: true,
  },
};

export default function AdminOutcomesPage() {
  return (
    <VerifiedSessionGuard returnTo="/admin/outcomes" requireAdmin>
      <div className="space-y-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Operations</p>
            <h1 className="text-3xl font-semibold text-white">Admin Outcomes</h1>
          </div>
          <Link href="/admin/settings" prefetch={false} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Admin panel
          </Link>
        </div>
        <AdminOutcomesDiagnostics />
      </div>
    </VerifiedSessionGuard>
  );
}
