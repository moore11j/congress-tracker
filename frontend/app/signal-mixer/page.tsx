import type { Metadata } from "next";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { SignalMixerTool } from "@/components/tools/SignalMixerTool";
import { getEntitlements } from "@/lib/api";
import { defaultEntitlements, entitlementsFromTierHint } from "@/lib/entitlements";
import { requirePageAuthState } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "Signal Mixer | Walnut Markets", description: "Explore what followed combinations of company disclosures. Study historical stock returns against SPY." };

export default async function SignalMixerPage() {
  const auth = await requirePageAuthState("/signal-mixer");
  const entitlements = auth.token ? await getEntitlements(auth.token).catch(() => defaultEntitlements) : entitlementsFromTierHint(auth.entitlementHint);
  return <VerifiedSessionGuard returnTo="/signal-mixer" initiallyAuthorized={Boolean(auth.token)}><SignalMixerTool initialEntitlements={entitlements} today={new Date().toISOString().slice(0, 10)} /></VerifiedSessionGuard>;
}
