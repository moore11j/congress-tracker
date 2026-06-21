import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { AccountAccessPanel } from "@/components/billing/AccountAccessPanel";
import { BillingAccountPanel } from "@/components/billing/BillingAccountPanel";

export const dynamic = "force-dynamic";

export default function BillingPage() {
  return (
    <VerifiedSessionGuard returnTo="/account/billing">
      <div className="space-y-8">
        <AccountAccessPanel />
        <BillingAccountPanel />
      </div>
    </VerifiedSessionGuard>
  );
}
