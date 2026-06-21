"use client";

import { useEffect } from "react";
import { getMe } from "@/lib/api";
import type { EntitlementTier } from "@/lib/entitlements";

type Props = {
  enabled: boolean;
  renderedTier?: EntitlementTier | null;
};

export function EntitlementHintRefresh({ enabled, renderedTier }: Props) {
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    getMe({ force: true, source: "EntitlementHintRefresh" })
      .then((response) => {
        if (!alive || !response.user) return;
        const actualTier = response.entitlements.effective_tier ?? response.entitlements.tier;
        const adminMismatch = response.user.is_admin && renderedTier !== "admin";
        if (adminMismatch || (actualTier && renderedTier && actualTier !== renderedTier)) {
          window.location.reload();
        }
      })
      .catch(() => undefined);
    return () => {
      alive = false;
    };
  }, [enabled, renderedTier]);

  return null;
}
