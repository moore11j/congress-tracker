"use client";

import { useEffect } from "react";
import { getEntitlements } from "@/lib/api";
import { hasEntitlement } from "@/lib/entitlements";

export function ScreenerEntitlementRefresh({ enabled }: { enabled: boolean }) {
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    getEntitlements()
      .then((entitlements) => {
        if (!alive) return;
        if (hasEntitlement(entitlements, "screener")) {
          window.location.reload();
        }
      })
      .catch((error) => {
        console.error("[screener] entitlement refresh failed", error);
      });
    return () => {
      alive = false;
    };
  }, [enabled]);

  return null;
}
