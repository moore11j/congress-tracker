import { AccountSettingsPanel } from "@/components/auth/AccountSettingsPanel";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { requirePageAuth } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

export default async function AccountSettingsPage() {
  const authToken = await requirePageAuth("/account/settings");

  return (
    <VerifiedSessionGuard returnTo="/account/settings" initiallyAuthorized={Boolean(authToken)}>
      <AccountSettingsPanel />
    </VerifiedSessionGuard>
  );
}
