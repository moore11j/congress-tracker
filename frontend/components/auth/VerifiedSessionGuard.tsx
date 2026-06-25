"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import { clearLegacyAuthStorage, getMe, hasClientAuthHint, type AccountUser, type MeResponse } from "@/lib/api";

type GuardState = "checking" | "authorized" | "unauthenticated" | "forbidden";

type Props = {
  children: ReactNode;
  returnTo: string;
  requireAdmin?: boolean;
  initiallyAuthorized?: boolean;
};

function loginHref(returnTo: string) {
  return `/login?return_to=${encodeURIComponent(returnTo || "/")}`;
}

function isAdminUser(user: AccountUser | null | undefined) {
  return Boolean(user && (user.is_admin || user.role === "admin" || user.entitlement_tier === "admin"));
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function GuardStatePanel({
  state,
  title,
  body,
}: {
  state: GuardState;
  title: string;
  body: string;
}) {
  return (
    <section
      data-auth-guard-state={state}
      aria-busy={state === "checking"}
      className="rounded-2xl border border-white/10 bg-slate-950/60 p-6 shadow-card"
    >
      <p className="text-xs font-semibold uppercase tracking-[0.28em] text-emerald-300">Session</p>
      <h1 className="mt-2 text-2xl font-semibold text-white">{title}</h1>
      <p className="mt-2 max-w-xl text-sm text-slate-400">{body}</p>
    </section>
  );
}

export function VerifiedSessionGuard({ children, returnTo, requireAdmin = false, initiallyAuthorized = false }: Props) {
  const router = useRouter();
  const [state, setState] = useState<GuardState>(initiallyAuthorized ? "authorized" : "checking");
  const signInHref = useMemo(() => loginHref(returnTo), [returnTo]);

  useEffect(() => {
    let alive = true;
    const source = requireAdmin ? "VerifiedSessionGuardAdmin" : "VerifiedSessionGuard";
    const verifySession = () => getMe({ force: true, source });
    const applySession = (response: MeResponse) => {
      if (!alive) return;
      if (!response.user) {
        setState("unauthenticated");
        router.replace(signInHref);
        return;
      }
      if (requireAdmin && !isAdminUser(response.user)) {
        setState("forbidden");
        return;
      }
      setState("authorized");
    };

    clearLegacyAuthStorage();
    if (!initiallyAuthorized) setState("checking");

    const runVerification = async () => {
      try {
        applySession(await verifySession());
      } catch {
        if (!alive) return;
        if (initiallyAuthorized && hasClientAuthHint()) {
          await delay(350);
          if (!alive) return;
          try {
            applySession(await verifySession());
            return;
          } catch {
            // Fall through to the normal unauthorized path.
          }
        }
        if (alive) {
          setState("unauthenticated");
          router.replace(signInHref);
        }
      }
    };

    void runVerification();

    return () => {
      alive = false;
    };
  }, [initiallyAuthorized, requireAdmin, router, signInHref]);

  if (state === "authorized") return <>{children}</>;

  if (state === "forbidden") {
    return (
      <GuardStatePanel
        state="forbidden"
        title="Not authorized"
        body="This area requires an admin account."
      />
    );
  }

  if (state === "unauthenticated") {
    return (
      <GuardStatePanel
        state="unauthenticated"
        title="Sign in required"
        body="Checking finished without a valid Walnut session."
      />
    );
  }

  return (
    <GuardStatePanel
      state="checking"
      title="Checking session"
      body="Verifying your Walnut session before loading this workspace."
    />
  );
}
