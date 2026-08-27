"use client";

import { useEffect } from "react";
import { analytics } from "@heycatch/sdk";
import { getMe, type AccountUser } from "@/lib/api";

function identityProperties(user: AccountUser) {
  const name = user.name?.trim() || [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
  const plan = user.current_plan || user.subscription_plan || user.entitlement_tier || user.plan;
  return {
    email: user.email,
    ...(name ? { name } : {}),
    ...(plan ? { plan } : {}),
  };
}

function identify(user: AccountUser) {
  analytics.setIdentity(
    String(user.id),
    identityProperties(user),
    user.created_at ? { signup_date: user.created_at } : undefined,
  );
}

export function HeyCatchIdentity() {
  useEffect(() => {
    const refreshIdentity = () => {
      void getMe({ force: true, source: "HeyCatchIdentity" })
        .then((response) => {
          if (response.user) identify(response.user);
        })
        .catch(() => undefined);
    };

    refreshIdentity();
    window.addEventListener("ct:auth-updated", refreshIdentity);
    return () => window.removeEventListener("ct:auth-updated", refreshIdentity);
  }, []);

  return null;
}
