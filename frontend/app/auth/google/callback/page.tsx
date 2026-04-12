"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { completeGoogleSignIn } from "@/lib/api";

export default function GoogleCallbackPage() {
  const [status, setStatus] = useState("Finishing Google sign-in...");
  const [returnTo, setReturnTo] = useState("/account/billing");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const state = params.get("state");
    if (!code || !state) {
      setStatus("Google did not return a complete sign-in response.");
      return;
    }

    completeGoogleSignIn({
      code,
      state,
      redirect_uri: `${window.location.origin}/auth/google/callback`,
    })
      .then((response) => {
        const next = response.return_to || "/account/billing";
        setReturnTo(next);
        window.location.replace(next);
      })
      .catch((error) => {
        setStatus(error instanceof Error ? error.message : "Unable to finish Google sign-in.");
      });
  }, []);

  return (
    <div className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/70 p-6">
      <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Google sign-in</p>
      <h1 className="mt-2 text-2xl font-semibold text-white">{status}</h1>
      <Link href={returnTo} className="mt-5 inline-flex rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
        Continue
      </Link>
    </div>
  );
}
