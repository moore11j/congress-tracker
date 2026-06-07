"use client";

import { useState } from "react";
import { resendVerificationEmail, type AccountUser } from "@/lib/api";

export function EmailVerificationBadge({ user }: { user: AccountUser | null }) {
  if (!user) return null;
  const verified = user.email_verified === true || Boolean(user.email_verified_at);
  return (
    <span
      className={`inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${
        verified
          ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
          : "border-amber-300/30 bg-amber-300/10 text-amber-100"
      }`}
    >
      {verified ? "Verified" : "Unverified"}
    </span>
  );
}

export function EmailVerificationBanner({ user }: { user: AccountUser | null }) {
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const verified = user?.email_verified === true || Boolean(user?.email_verified_at);
  if (!user || verified) return null;

  const resend = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const response = await resendVerificationEmail();
      setStatus(response.message);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to resend verification email.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="rounded-lg border border-amber-300/25 bg-amber-300/10 p-4 text-sm text-amber-50">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p>Please verify your email to unlock account features, billing, and saved settings.</p>
        <button
          type="button"
          onClick={resend}
          disabled={busy}
          className="rounded-lg border border-amber-200/30 px-3 py-2 text-sm font-semibold text-amber-50 disabled:cursor-wait disabled:opacity-60"
        >
          {busy ? "Sending..." : "Resend verification"}
        </button>
      </div>
      {status ? <p className="mt-2 text-amber-100/80">{status}</p> : null}
    </div>
  );
}
