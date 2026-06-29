"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ApiError, getMe, resendVerificationEmail, verifyEmail } from "@/lib/api";

type VerifyState = "checking" | "verified" | "failed";

function verificationErrorCode(error: unknown) {
  if (!(error instanceof ApiError)) return "";
  const detail = error.detail;
  if (!detail || typeof detail !== "object" || !("code" in detail)) return "";
  const code = (detail as { code?: unknown }).code;
  return typeof code === "string" ? code : "";
}

function verificationFailureMessage(error: unknown) {
  const code = verificationErrorCode(error);
  if (code === "expired_verification_link") return "This verification link has expired. Request a new link.";
  if (code === "invalid_verification_link") return "This verification link is invalid. Request a new link.";
  if (error instanceof ApiError && error.status === 401) return "Sign in to request a new verification link.";
  return "This verification link could not be verified. Request a new link.";
}

function resendFailureMessage(error: unknown) {
  if (error instanceof ApiError && error.status === 401) return "Sign in to request a new verification link from Account Settings.";
  return "We could not send a new verification link. Please try again from Account Settings.";
}

export function VerifyEmailPanel({ token }: { token?: string }) {
  const [state, setState] = useState<VerifyState>(token ? "checking" : "failed");
  const [status, setStatus] = useState(token ? "Verifying your email..." : "Verification token is missing.");
  const [resending, setResending] = useState(false);

  useEffect(() => {
    if (!token) return;
    let cancelled = false;
    verifyEmail(token)
      .then(async (response) => {
        if (cancelled) return;
        setState("verified");
        setStatus(response.status === "already_verified" ? "Your email is already verified. Opening account settings..." : "Email verified. Opening account settings...");
        await getMe({ force: true, source: "VerifyEmailPanel" }).catch(() => undefined);
        if (cancelled) return;
        window.setTimeout(() => {
          window.location.replace("/account/settings?verified=1");
        }, 900);
      })
      .catch((error) => {
        if (cancelled) return;
        setState("failed");
        setStatus(verificationFailureMessage(error));
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  const resend = async () => {
    setResending(true);
    try {
      await resendVerificationEmail();
      setStatus("We sent you a new verification link.");
    } catch (error) {
      setStatus(resendFailureMessage(error));
    } finally {
      setResending(false);
    }
  };

  return (
    <section className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Email Verification</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">
        {state === "verified" ? "Email verified." : state === "failed" ? "Verification needs a new link." : "Checking your link."}
      </h1>
      <p className="mt-3 text-sm leading-6 text-slate-300">{status}</p>
      {state === "failed" ? (
        <div className="mt-5 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={resend}
            disabled={resending}
            className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:border-emerald-200 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
          >
            {resending ? "Sending..." : "Resend link"}
          </button>
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
