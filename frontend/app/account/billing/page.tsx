import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AccountAccessPanel } from "@/components/billing/AccountAccessPanel";
import { BillingAccountPanel } from "@/components/billing/BillingAccountPanel";
import { requirePageAuth } from "@/lib/serverAuth";
import type { Metadata } from "next";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Billing | Walnut Markets",
  robots: {
    index: false,
    follow: true,
  },
};

export default async function BillingPage() {
  const authToken = await requirePageAuth("/account/billing");

  return (
    <VerifiedSessionGuard returnTo="/account/billing" initiallyAuthorized={Boolean(authToken)}>
      <div className="space-y-8">
        <AccountAccessPanel />
        <BillingAccountPanel />
      </div>
    </VerifiedSessionGuard>
  );
}
