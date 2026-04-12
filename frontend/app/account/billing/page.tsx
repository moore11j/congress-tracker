import { AccountAccessPanel } from "@/components/billing/AccountAccessPanel";
import { BillingAccountPanel } from "@/components/billing/BillingAccountPanel";

export const dynamic = "force-dynamic";

export default function BillingPage() {
  return (
    <div className="space-y-8">
      <AccountAccessPanel />
      <BillingAccountPanel />
    </div>
  );
}
