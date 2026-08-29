"use client";

import { analytics } from "@heycatch/sdk";
import type { AccountUser } from "@/lib/api";

export function identifyHeyCatchUser(user: AccountUser) {
  try {
    const name = user.name?.trim() || [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
    const plan = user.current_plan || user.subscription_plan || user.entitlement_tier || user.plan;
    analytics.setIdentity(
      String(user.id),
      {
        email: user.email,
        ...(name ? { name } : {}),
        ...(plan ? { plan } : {}),
      },
      user.created_at ? { signup_date: user.created_at } : undefined,
    );
  } catch {
    // Analytics must never affect authentication or navigation.
  }
}

export function resetHeyCatchIdentity() {
  try {
    analytics.resetIdentity();
  } catch {
    // Analytics must never affect sign-out.
  }
}

export function trackHeyCatchEvent(event: string) {
  try {
    analytics.trackEvent(event);
  } catch {
    // Analytics must never affect the completed action.
  }
}
