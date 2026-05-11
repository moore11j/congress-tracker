import type { Metadata } from "next";
import Link from "next/link";
import { PricingPlanner } from "@/components/billing/PricingPlanner";
import { getPlanConfig } from "@/lib/api";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Pricing | Walnut Market Terminal",
  description:
    "Compare Free, Premium, and Pro plans for Walnut Market Terminal, with disclosure intelligence, market signals, leaderboards, and watchlists.",
  openGraph: {
    title: "Walnut Market Terminal Pricing",
    description:
      "Disclosure intelligence, market signals, alerts, and flexible watchlists.",
  },
};

export default async function PricingPage() {
  const config = await getPlanConfig();

  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <PricingPlanner config={config} />
      <div className="flex flex-wrap gap-3 text-sm">
        <Link href="/login" className="rounded-lg border border-white/10 px-4 py-2 font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
          Login / Register
        </Link>
        <Link href="/account/billing" className="rounded-lg border border-white/10 px-4 py-2 font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
          Manage billing
        </Link>
      </div>
    </div>
  );
}
