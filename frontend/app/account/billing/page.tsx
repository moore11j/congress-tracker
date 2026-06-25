import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AccountAccessPanel } from "@/components/billing/AccountAccessPanel";
import { BillingAccountPanel } from "@/components/billing/BillingAccountPanel";
import { requirePageAuth } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

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
