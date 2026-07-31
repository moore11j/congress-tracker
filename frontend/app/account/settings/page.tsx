import { AccountSettingsPanel } from "@/components/auth/AccountSettingsPanel";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { requirePageAuth } from "@/lib/serverAuth";
import type { Metadata } from "next";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Account Settings | Walnut Markets",
  robots: {
    index: false,
    follow: true,
  },
};

export default async function AccountSettingsPage() {
  const authToken = await requirePageAuth("/account/settings");

  return (
    <VerifiedSessionGuard returnTo="/account/settings" initiallyAuthorized={Boolean(authToken)}>
      <AccountSettingsPanel />
    </VerifiedSessionGuard>
  );
}
