"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { reactivateAccount } from "@/lib/api";

type ReactivationState = "loading" | "success" | "already_active" | "expired";

export function ReactivateAccountPanel({ token }: { token: string }) {
  const [state, setState] = useState<ReactivationState>(token ? "loading" : "expired");
  const [message, setMessage] = useState("Checking your reactivation link.");

  useEffect(() => {
    if (!token) {
      setState("expired");
      setMessage("This reactivation link has expired. Please create a new account or contact support.");
      return;
    }
    let cancelled = false;
    reactivateAccount(token)
      .then((response) => {
        if (cancelled) return;
        if (response.status === "already_active") {
          setState("already_active");
          setMessage("This Walnut account is already active. Please sign in to continue.");
          return;
        }
        setState("success");
        setMessage("Your Walnut account has been reactivated. Please sign in to continue.");
      })
      .catch((error) => {
        if (cancelled) return;
        setState("expired");
        setMessage(error instanceof Error ? error.message : "This reactivation link has expired. Please create a new account or contact support.");
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  const title =
    state === "loading"
      ? "Checking reactivation link"
      : state === "success"
        ? "Account reactivated"
        : state === "already_active"
          ? "Account already active"
          : "Link unavailable";

  return (
    <section className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Account Access</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">{title}</h1>
      <p className="mt-3 text-sm leading-6 text-slate-300">{message}</p>
      {state === "loading" ? <p className="mt-5 text-sm text-slate-500">Verifying token.</p> : null}
      {state !== "loading" ? (
        <div className="mt-6 flex flex-wrap gap-3">
          <Link
            href={state === "expired" ? "/login?mode=register" : "/login?reactivated=1"}
            className="inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
          >
            {state === "expired" ? "Create account" : "Sign in"}
          </Link>
          {state === "expired" ? (
            <a
              href="mailto:support@walnut-intel.com"
              className="inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            >
              Contact support
            </a>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
