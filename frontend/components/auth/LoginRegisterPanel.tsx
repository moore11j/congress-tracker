"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { countryOptions, normalizeCountryInput, normalizeRegionInput, regionOptionsForCountry } from "@/lib/billingLocation";
import { getGoogleAuthUrl, getMe, login, register, requestPasswordReset } from "@/lib/api";
import { selectClassName } from "@/lib/styles";

type Mode = "login" | "register";

export function LoginRegisterPanel({ returnTo }: { returnTo?: string }) {
  const nextPath = returnTo && returnTo.startsWith("/") ? returnTo : "/account/billing";
  const [mode, setMode] = useState<Mode>("login");
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [country, setCountry] = useState("");
  const [stateProvince, setStateProvince] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const [city, setCity] = useState("");
  const [addressLine1, setAddressLine1] = useState("");
  const [addressLine2, setAddressLine2] = useState("");
  const [resetEmail, setResetEmail] = useState("");
  const [resetPath, setResetPath] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled && response.user) window.location.replace(nextPath);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [nextPath]);

  const headline = useMemo(
    () => (mode === "register" ? "Create your Capitol Ledger account." : "Welcome back."),
    [mode],
  );
  const normalizedCountry = normalizeCountryInput(country);
  const regionOptions = regionOptionsForCountry(normalizedCountry);
  const stateProvinceLabel =
    normalizedCountry === "US"
      ? "State"
      : normalizedCountry === "CA"
        ? "Province / territory"
        : "State / province / region";

  const validateSubmit = () => {
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !normalizedEmail.includes("@")) return "Enter a valid email address.";
    if (!password || password.length < 8) return "Password must be at least 8 characters.";
    if (mode !== "register") return null;

    const requiredFields = [
      { label: "First name", value: firstName },
      { label: "Last name", value: lastName },
      { label: "Country", value: country },
      { label: "Postal code", value: postalCode },
      { label: "City", value: city },
      { label: "Address line 1", value: addressLine1 },
    ];
    const missing = requiredFields.find((field) => !field.value.trim());
    if (missing) return `${missing.label} is required.`;
    if (normalizedCountry.length !== 2) return "Country must be a two-letter ISO code, like US or CA.";
    if (regionOptions.length && !stateProvince.trim()) return `${stateProvinceLabel} is required.`;
    return null;
  };

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const validationError = validateSubmit();
    if (validationError) {
      setStatus(validationError);
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      if (mode === "register") {
        await register({
          first_name: firstName,
          last_name: lastName,
          email,
          password,
          country: normalizedCountry,
          state_province: normalizeRegionInput(normalizedCountry, stateProvince),
          postal_code: postalCode,
          city,
          address_line1: addressLine1,
          address_line2: addressLine2,
        });
      } else {
        await login({ email, password });
      }
      window.location.replace(nextPath);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to continue.");
    } finally {
      setLoading(false);
    }
  };

  const google = async () => {
    setLoading(true);
    setStatus(null);
    try {
      const response = await getGoogleAuthUrl(nextPath);
      window.location.href = response.authorization_url;
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start Google sign-in.");
      setLoading(false);
    }
  };

  const reset = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoading(true);
    setStatus(null);
    setResetPath(null);
    try {
      const response = await requestPasswordReset(resetEmail || email);
      setStatus(response.message);
      setResetPath(response.reset_path ?? null);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start password reset.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mx-auto grid max-w-5xl gap-6 lg:grid-cols-[1fr_0.85fr]">
      <section className="rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Account Access</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">{headline}</h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">
          Keep watchlists, signals, inbox monitoring, and billing attached to one secure account.
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
          className="mt-5 inline-flex w-full items-center justify-center rounded-lg border border-white/15 bg-white px-4 py-3 text-sm font-semibold text-slate-950 transition hover:bg-slate-100"
        >
          Continue with Google
        </button>

        <div className="my-5 flex items-center gap-3 text-xs uppercase tracking-wide text-slate-500">
          <span className="h-px flex-1 bg-white/10" />
          or use email
          <span className="h-px flex-1 bg-white/10" />
        </div>

        <form onSubmit={submit} noValidate className="space-y-3">
          {mode === "register" ? (
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="block text-sm font-medium text-slate-200">
                <RequiredLabel>First name</RequiredLabel>
                <input
                  value={firstName}
                  onChange={(event) => setFirstName(event.target.value)}
                  autoComplete="given-name"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
              <label className="block text-sm font-medium text-slate-200">
                <RequiredLabel>Last name</RequiredLabel>
                <input
                  value={lastName}
                  onChange={(event) => setLastName(event.target.value)}
                  autoComplete="family-name"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
            </div>
          ) : null}
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
              type="password"
              autoComplete={mode === "register" ? "new-password" : "current-password"}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
          </label>
          {mode === "register" ? (
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="block text-sm font-medium text-slate-200">
                <RequiredLabel>Country</RequiredLabel>
                <select
                  value={country}
                  onChange={(event) => setCountry(event.target.value)}
                  autoComplete="country"
                  className={`mt-1 ${selectClassName}`}
                >
                  <option value="">Select country</option>
                  {countryOptions.map((option) => (
                    <option key={option.code} value={option.code}>
                      {option.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="block text-sm font-medium text-slate-200">
                {regionOptions.length ? <RequiredLabel>{stateProvinceLabel}</RequiredLabel> : stateProvinceLabel}
                {regionOptions.length ? (
                  <select
                    value={stateProvince}
                    onChange={(event) => setStateProvince(event.target.value)}
                    autoComplete="address-level1"
                    className={`mt-1 ${selectClassName}`}
                  >
                    <option value="">Select {stateProvinceLabel.toLowerCase()}</option>
                    {regionOptions.map((option) => (
                      <option key={option.code} value={option.code}>
                        {option.name}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    value={stateProvince}
                    onChange={(event) => setStateProvince(event.target.value)}
                    placeholder="Region"
                    autoComplete="address-level1"
                    className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                  />
                )}
              </label>
              <label className="block text-sm font-medium text-slate-200">
                <RequiredLabel>Postal code</RequiredLabel>
                <input
                  value={postalCode}
                  onChange={(event) => setPostalCode(event.target.value)}
                  autoComplete="postal-code"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
              <label className="block text-sm font-medium text-slate-200">
                <RequiredLabel>City</RequiredLabel>
                <input
                  value={city}
                  onChange={(event) => setCity(event.target.value)}
                  autoComplete="address-level2"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
              <label className="block text-sm font-medium text-slate-200 sm:col-span-2">
                <RequiredLabel>Address line 1</RequiredLabel>
                <input
                  value={addressLine1}
                  onChange={(event) => setAddressLine1(event.target.value)}
                  autoComplete="address-line1"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
              <label className="block text-sm font-medium text-slate-200 sm:col-span-2">
                Address line 2 <span className="text-slate-500">(optional)</span>
                <input
                  value={addressLine2}
                  onChange={(event) => setAddressLine2(event.target.value)}
                  autoComplete="address-line2"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
                />
              </label>
            </div>
          ) : null}
          <button
            type="submit"
            disabled={loading}
            className="inline-flex w-full items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
          >
            {mode === "register" ? "Create account" : "Login"}
          </button>
        </form>

        <form onSubmit={reset} noValidate className="mt-5 rounded-lg border border-white/10 bg-white/[0.03] p-4">
          <div className="flex flex-col gap-3 sm:flex-row">
            <input
              value={resetEmail}
              onChange={(event) => setResetEmail(event.target.value)}
              type="email"
              placeholder="Email for password reset"
              className="min-w-0 flex-1 rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
            <button
              type="submit"
              disabled={loading}
              className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            >
              Reset password
            </button>
          </div>
          {resetPath ? (
            <Link href={resetPath} className="mt-3 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">
              Open secure reset link
            </Link>
          ) : null}
        </form>

        {status ? <p className="mt-4 text-sm text-slate-300">{status}</p> : null}
      </section>

      <aside className="rounded-lg border border-white/10 bg-slate-950/60 p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Premium Workflow</p>
        <h2 className="mt-3 text-2xl font-semibold text-white">One account for every research surface.</h2>
        <div className="mt-5 space-y-3 text-sm leading-6 text-slate-300">
          <p>Watchlists stay tied to your account.</p>
          <p>Signals and leaderboards open after sign-in and return you to the page you requested.</p>
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
