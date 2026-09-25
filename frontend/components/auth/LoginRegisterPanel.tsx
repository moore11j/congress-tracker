"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { ApiError, getGoogleAuthUrl, getMe, login, recordProductEvent, register, requestPasswordReset, verifyAuthenticatedSession } from "@/lib/api";
import { defaultPostLoginPath, reactivatedBillingPath, safeAppReturnPath } from "@/lib/returnPaths";
import { campaignParamKeys } from "@/lib/campaignAttribution";
import { trackEvent } from "@/lib/productAnalytics";

type Mode = "login" | "register";

function campaignPropertiesFromReturnTo(returnTo: string, extra: Record<string, string | number | boolean | null> = {}) {
  const url = new URL(returnTo || "/", window.location.origin);
  const properties: Record<string, string | number | boolean | null> = {
    page_path: `${url.pathname}${url.search}`,
    auth_state: "authenticated",
    plan: "free",
    referrer: document.referrer || null,
    ...extra,
  };
  for (const key of campaignParamKeys) properties[key] = url.searchParams.get(key);
  return properties;
}

function recordSignupCompleteEvents(returnTo: string) {
  const url = new URL(returnTo || "/", window.location.origin);
  const path = `${url.pathname}${url.search}`;
  const baseProperties = campaignPropertiesFromReturnTo(path);
  const referringLandingPage = url.searchParams.get("referring_landing_page");

  if (referringLandingPage === "/reddit/stock-research" || (url.searchParams.get("utm_source") === "reddit" && url.searchParams.get("utm_campaign") === "stock_research_intent_aug_2026")) {
    recordProductEvent({
      event_name: "reddit_signup_complete",
      path,
      properties: {
        ...baseProperties,
        referring_landing_page: referringLandingPage,
      },
    });
  }

  const compareMatch = url.pathname.match(/^\/compare\/([^/]+)\/([^/]+)\/?$/);
  if (compareMatch) {
    const tickerA = decodeURIComponent(compareMatch[1] || "").toUpperCase();
    const tickerB = decodeURIComponent(compareMatch[2] || "").toUpperCase();
    recordProductEvent({
      event_name: "compare_signup_complete",
      path,
      properties: {
        ...baseProperties,
        ticker_a: tickerA,
        ticker_b: tickerB,
      },
    });
  }

  const researchMatch = url.pathname.match(/^\/research\/([^/]+)\/?$/);
  if (researchMatch) {
    recordProductEvent({
      event_name: "research_brief_signup_complete",
      path,
      properties: {
        ...baseProperties,
        ticker: url.searchParams.get("cta_ticker"),
        research_slug: url.searchParams.get("research_slug") || decodeURIComponent(researchMatch[1] || ""),
      },
    });
  }
}

export function LoginRegisterPanel({
  resetStatus,
  returnTo,
  accountDeleted,
  reactivated,
}: {
  resetStatus?: string;
  returnTo?: string;
  accountDeleted?: boolean;
  reactivated?: boolean;
}) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const resolvedResetStatus = resetStatus ?? searchParams.get("reset") ?? undefined;
  const resolvedReturnTo = returnTo ?? searchParams.get("return_to") ?? undefined;
  const resolvedAccountDeleted = accountDeleted ?? searchParams.get("account_deleted") === "1";
  const resolvedReactivated = reactivated ?? searchParams.get("reactivated") === "1";
  const requestedMode: Mode = searchParams.get("mode") === "register" ? "register" : "login";
  const nextPath = safeAppReturnPath(resolvedReturnTo, resolvedReactivated ? reactivatedBillingPath : defaultPostLoginPath);
  const startedModes = useRef(new Set<Mode>());
  const [mode, setMode] = useState<Mode>(requestedMode);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [resetOpen, setResetOpen] = useState(false);
  const signupPath = safeAppReturnPath(resolvedReturnTo, "/welcome");
  const [resetEmail, setResetEmail] = useState("");
  const [status, setStatus] = useState<string | null>(
    resolvedAccountDeleted
      ? "Your account has been deleted."
      : resolvedReactivated
        ? "Your account has been reactivated. Please sign in to continue."
        : resolvedResetStatus === "success"
          ? "Password reset successful. Please sign in with your new password."
          : null,
  );
  const [duplicateAccount, setDuplicateAccount] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingLabel, setLoadingLabel] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled && response.user) router.replace(nextPath);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [nextPath, router]);

  useEffect(() => {
    setMode(requestedMode);
  }, [requestedMode]);

  useEffect(() => {
    let cancelled = false;
    void getMe().then(({ user }) => {
      if (cancelled || user || startedModes.current.has(mode)) return;
      if (trackEvent(mode === "register" ? "signup_started" : "signin_started", { destination_page: nextPath })) startedModes.current.add(mode);
    }).catch(() => undefined);
    return () => { cancelled = true; };
  }, [mode, nextPath]);

  const headline = useMemo(
    () => (mode === "register" ? "Create your free Walnut account." : "Welcome back."),
    [mode],
  );
  const validateSubmit = () => {
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !normalizedEmail.includes("@")) return "Enter a valid email address.";
    if (!password || password.length < 8) return "Password must be at least 8 characters.";
    if (mode !== "register") return null;
    const passwordChecks = [
      password.length >= 8,
      /[A-Za-z]/.test(password),
      /\d/.test(password),
      /[^A-Za-z0-9]/.test(password),
    ];
    if (passwordChecks.filter(Boolean).length < 3) {
      return "Password must satisfy at least 3 of 4 requirements.";
    }

    return null;
  };

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const validationError = validateSubmit();
    if (validationError) {
      if (mode === "register") trackEvent("signup_validation_failed", { method: "password" });
      setStatus(validationError);
      return;
    }
    setLoading(true);
    setLoadingLabel(mode === "register" ? "Creating account..." : "Authenticating...");
    setStatus(null);
    setDuplicateAccount(false);
    try {
      let destination = nextPath;
      if (mode === "register") {
        trackEvent("signup_submitted", { method: "password" });
        await register({ email: email.trim(), password });
        destination = signupPath;
        recordSignupCompleteEvents(destination);
      } else {
        await login({ email, password });
      }
      const destinationLabel = destination === "/welcome" ? "getting-started page" : destination === defaultPostLoginPath ? "feed" : "requested page";
      setLoadingLabel("Verifying session...");
      setStatus("Verifying your session...");
      await verifyAuthenticatedSession(mode === "register" ? "RegisterPanel" : "LoginPanel");
      setLoadingLabel(`Opening ${destinationLabel}...`);
      setStatus(`You're in. Opening the ${destinationLabel}...`);
      router.replace(destination);
      router.refresh();
    } catch (error) {
      if (mode === "register") trackEvent("signup_failed", { method: "password" });
      if (mode === "register" && error instanceof ApiError && error.status === 409) {
        setDuplicateAccount(true);
        setStatus("An account already exists for this email. Please sign in or reset your password.");
      } else {
        setStatus(error instanceof Error ? error.message : "Unable to continue.");
      }
      setLoadingLabel(null);
      setLoading(false);
    }
  };

  const google = async () => {
    setLoading(true);
    setLoadingLabel("Starting Google sign-in...");
    setStatus(null);
    try {
      const response = await getGoogleAuthUrl(mode === "register" ? signupPath : nextPath);
      setLoadingLabel("Opening Google...");
      window.location.href = response.authorization_url;
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start Google sign-in.");
      setLoadingLabel(null);
      setLoading(false);
    }
  };

  const reset = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoading(true);
    setLoadingLabel("Sending reset link...");
    setStatus(null);
    try {
      const response = await requestPasswordReset(resetEmail || email);
      setStatus(response.message);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start password reset.");
    } finally {
      setLoadingLabel(null);
      setLoading(false);
    }
  };

  return (
    <div className="mx-auto grid max-w-5xl gap-6 lg:grid-cols-[1fr_0.85fr]">
      <section className="rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Account Access</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">{headline}</h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">
          Save stocks to your watchlist and pick up your research where you left off.
        </p>

        <div className="mt-6 grid grid-cols-2 gap-2 rounded-lg border border-white/10 bg-slate-950/60 p-1">
          <button
            type="button"
            onClick={() => setMode("login")}
            className={`rounded-md px-4 py-2 text-sm font-semibold transition ${
              mode === "login" ? "bg-emerald-300/15 text-emerald-100" : "text-slate-300 hover:text-white"
            }`}
          >
            Login
          </button>
          <button
            type="button"
            onClick={() => setMode("register")}
            className={`rounded-md px-4 py-2 text-sm font-semibold transition ${
              mode === "register" ? "bg-emerald-300/15 text-emerald-100" : "text-slate-300 hover:text-white"
            }`}
          >
            Register
          </button>
        </div>

        <button
          type="button"
          onClick={google}
          disabled={loading}
          className="mt-5 inline-flex w-full items-center justify-center gap-2 rounded-lg border border-white/15 bg-white px-4 py-3 text-sm font-semibold text-slate-950 transition hover:bg-slate-100 disabled:cursor-wait disabled:opacity-80"
          aria-busy={loading}
        >
          {loading && loadingLabel?.includes("Google") ? <LoadingDot /> : null}
          {loading && loadingLabel?.includes("Google") ? loadingLabel : "Continue with Google"}
        </button>

        <div className="my-5 flex items-center gap-3 text-xs uppercase tracking-wide text-slate-500">
          <span className="h-px flex-1 bg-white/10" />
          or use email
          <span className="h-px flex-1 bg-white/10" />
        </div>

        <form onSubmit={submit} noValidate className="space-y-3">
          <label className="block text-sm font-medium text-slate-200">
            <RequiredLabel>Email</RequiredLabel>
            <input
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              type="email"
              autoComplete="email"
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
          </label>
          <label className="block text-sm font-medium text-slate-200">
            <RequiredLabel>Password</RequiredLabel>
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              minLength={8}
              type={showPassword ? "text" : "password"}
              aria-describedby={mode === "register" ? "password-rules" : undefined}
              autoComplete={mode === "register" ? "new-password" : "current-password"}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
          </label>
          <button type="button" onClick={() => setShowPassword(!showPassword)} aria-pressed={showPassword} className="text-sm text-emerald-200">
            {showPassword ? "Hide password" : "Show password"}
          </button>
          {mode === "register" ? (
            <p id="password-rules" className="text-xs leading-5 text-slate-400">
              At least 8 characters. Include at least two of: letters, numbers, special characters.
              No payment details needed to create your free account.
            </p>
          ) : null}
          <button
            type="submit"
            disabled={loading}
            className="inline-flex w-full items-center justify-center gap-2 rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20 disabled:cursor-wait disabled:opacity-80"
            aria-busy={loading}
          >
            {loading && !loadingLabel?.includes("Google") && !loadingLabel?.includes("reset") ? <LoadingDot /> : null}
            {loading && !loadingLabel?.includes("Google") && !loadingLabel?.includes("reset")
              ? loadingLabel
              : mode === "register"
                ? "Create free account"
                : "Login"}
          </button>
        </form>

        <details className="mt-5" open={resetOpen} onToggle={(event) => setResetOpen(event.currentTarget.open)}>
          <summary className="cursor-pointer text-sm text-emerald-200">Forgot password?</summary>
          <form onSubmit={reset} noValidate className="mt-5 rounded-lg border border-white/10 bg-white/[0.03] p-4">
            <div className="flex flex-col gap-3 sm:flex-row">
              <input
                value={resetEmail}
                onChange={(event) => setResetEmail(event.target.value)}
                type="email"
                placeholder="Email for password reset"
                aria-label="Email for password reset"
                className="min-w-0 flex-1 rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-emerald-300/50"
              />
              <button
                type="submit"
                disabled={loading}
                className="inline-flex items-center justify-center gap-2 rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:cursor-wait disabled:opacity-70"
                aria-busy={loading}
              >
                {loading && loadingLabel?.includes("reset") ? <LoadingDot /> : null}
                {loading && loadingLabel?.includes("reset") ? loadingLabel : "Reset password"}
              </button>
            </div>
          </form>

        </details>

        {status ? <p role="status" className="mt-4 text-sm text-slate-300">{status}</p> : null}
        {duplicateAccount ? (
          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => setMode("login")}
              className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100"
            >
              Sign in
            </button>
            <button
              type="button"
              onClick={() => {
                setResetEmail(email);
                setResetOpen(true);
                setStatus("Enter the email below and send a reset link.");
              }}
              className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200"
            >
              Reset password
            </button>
          </div>
        ) : null}
      </section>

      <aside className="rounded-lg border border-white/10 bg-slate-950/60 p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Your Walnut account</p>
        <h2 className="mt-3 text-2xl font-semibold text-white">One account for every research surface.</h2>
        <div className="mt-5 space-y-3 text-sm leading-6 text-slate-300">
          <p>Watchlists stay tied to your account.</p>
          <p>Signing in returns you to the page you requested. Feature availability depends on your plan.</p>
          <p>Billing remains separate from authentication, with plan details on a dedicated pricing page.</p>
        </div>
        <Link
          href="/pricing"
          className="mt-6 inline-flex rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
        >
          View Plans & Pricing
        </Link>
      </aside>
    </div>
  );
}

function RequiredLabel({ children }: { children: ReactNode }) {
  return (
    <>
      {children} <span className="text-emerald-300">*</span>
    </>
  );
}

function LoadingDot() {
  return (
    <span
      aria-hidden="true"
      className="h-2 w-2 animate-pulse rounded-full bg-current shadow-[0_0_12px_currentColor]"
    />
  );
}
