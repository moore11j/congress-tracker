"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import { clearLegacyAuthStorage, getMe, type AccountUser } from "@/lib/api";

type GuardState = "checking" | "authorized" | "unauthenticated" | "forbidden";

type Props = {
  children: ReactNode;
  returnTo: string;
  requireAdmin?: boolean;
};

function loginHref(returnTo: string) {
  return `/login?return_to=${encodeURIComponent(returnTo || "/")}`;
}

function isAdminUser(user: AccountUser | null | undefined) {
  return Boolean(user && (user.is_admin || user.role === "admin" || user.entitlement_tier === "admin"));
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

export function VerifiedSessionGuard({ children, returnTo, requireAdmin = false }: Props) {
  const router = useRouter();
  const [state, setState] = useState<GuardState>("checking");
  const signInHref = useMemo(() => loginHref(returnTo), [returnTo]);

  useEffect(() => {
    let alive = true;
    clearLegacyAuthStorage();
    setState("checking");
    getMe({ force: true, source: requireAdmin ? "VerifiedSessionGuardAdmin" : "VerifiedSessionGuard" })
      .then((response) => {
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
      })
      .catch(() => {
        if (!alive) return;
        setState("unauthenticated");
        router.replace(signInHref);
      });

    return () => {
      alive = false;
    };
  }, [requireAdmin, router, signInHref]);

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
