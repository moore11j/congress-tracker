"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { verifyEmail } from "@/lib/api";

type VerifyState = "checking" | "verified" | "failed";

export function VerifyEmailPanel({ token }: { token?: string }) {
  const [state, setState] = useState<VerifyState>(token ? "checking" : "failed");
  const [status, setStatus] = useState(token ? "Verifying your email..." : "Verification token is missing.");

  useEffect(() => {
    if (!token) return;
    let cancelled = false;
    verifyEmail(token)
      .then((response) => {
        if (cancelled) return;
        setState("verified");
        setStatus(response.status === "already_verified" ? "Email already verified. Opening account settings..." : "Email verified. Opening account settings...");
        window.setTimeout(() => {
          window.location.replace("/account/settings?verified=1");
        }, 900);
      })
      .catch((error) => {
        if (cancelled) return;
        setState("failed");
        setStatus(error instanceof Error ? error.message : "Unable to verify this email link.");
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  return (
    <section className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Email Verification</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">
        {state === "verified" ? "Email verified." : state === "failed" ? "Verification needs a new link." : "Checking your link."}
      </h1>
      <p className="mt-3 text-sm leading-6 text-slate-300">{status}</p>
      {state === "failed" ? (
        <div className="mt-5 flex flex-wrap gap-3">
          <Link href="/account/settings" className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100">
            Account Settings
          </Link>
          <Link href="/login" className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Sign in
          </Link>
        </div>
      ) : null}
    </section>
  );
}
