import { AccountSettingsPanel } from "@/components/auth/AccountSettingsPanel";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";

export const dynamic = "force-dynamic";

export default function AccountSettingsPage() {
  return (
    <VerifiedSessionGuard returnTo="/account/settings">
      <AccountSettingsPanel />
    </VerifiedSessionGuard>
  );
}
