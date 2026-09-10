"use client";

import { analytics } from "@heycatch/sdk";
import type { AccountUser } from "@/lib/api";
import { isProductionAnalyticsHost } from "./analyticsEnvironment";
import { acquisitionProperties, analyticsConsent, type AnalyticsProperties } from "./analyticsContext";

let initialized = false;
let identified: string | null = null;

export function ensureHeyCatch(): boolean {
  if (!isProductionAnalyticsHost() || !analyticsConsent()) return false;
  if (initialized) return true;
  const projectKey = process.env.NEXT_PUBLIC_HEYCATCH_PROJECT_KEY;
  if (!projectKey?.startsWith("hck_pk_")) return false;
  try {
    analytics.init({ projectKey, install: { framework: "nextjs", frameworkVersion: "15", agent: "codex" } });
    initialized = true;
    return true;
  } catch { return false; }
}

export function identifyHeyCatchUser(user: AccountUser) {
  try {
    if (!ensureHeyCatch() || identified === String(user.id)) return;
    const plan = user.current_plan || user.subscription_plan || user.entitlement_tier || user.plan;
    analytics.setIdentity(
      String(user.id),
      {
        ...(plan ? { plan } : {}),
      },
      acquisitionProperties(),
    );
    identified = String(user.id);
  } catch {
    // Analytics must never affect authentication or navigation.
  }
}

export function resetHeyCatchIdentity() {
  try {
    identified = null;
    if (!initialized) return;
    analytics.resetIdentity();
  } catch {
    // Analytics must never affect sign-out.
  }
}

export function trackHeyCatchEvent(event: string, properties: AnalyticsProperties = {}) {
  try {
    if (!ensureHeyCatch()) return;
    analytics.trackEvent(event, properties);
  } catch {
    // Analytics must never affect the completed action.
  }
}
