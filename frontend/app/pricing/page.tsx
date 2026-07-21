import type { Metadata } from "next";
import { PricingPlannerDeferred } from "@/components/billing/PricingPlannerDeferred";
import { marketingPageMetadata } from "@/lib/marketingMetadata";

export const dynamic = "force-static";
export const revalidate = false;

export const metadata: Metadata = marketingPageMetadata("/pricing", {
  title: "Pricing | Walnut Market Terminal",
  description:
    "Compare Free, Premium, and Pro plans for Walnut Market Terminal, with disclosure intelligence, market signals, leaderboards, and watchlists.",
  openGraph: {
    title: "Walnut Market Terminal Pricing",
    description:
      "Disclosure intelligence, market signals, alerts, and flexible watchlists.",
  },
});

export default async function PricingPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <PricingPlannerDeferred />
    </div>
  );
}
