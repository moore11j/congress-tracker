"use client";

import Link from "next/link";
import { useState } from "react";
import { confirmPasswordReset, requestPasswordReset } from "@/lib/api";

export function ResetPasswordPanel({ token }: { token?: string }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [resetPath, setResetPath] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const requestReset = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoading(true);
    setStatus(null);
    setResetPath(null);
    try {
      const response = await requestPasswordReset(email);
      setStatus(response.message);
      setResetPath(response.reset_path ?? null);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start password reset.");
    } finally {
      setLoading(false);
    }
  };

  const confirmReset = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!token) return;
    setLoading(true);
    setStatus(null);
    try {
      await confirmPasswordReset({ token, password });
      window.location.replace("/account/billing");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to reset password.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Password Reset</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">{token ? "Choose a new password." : "Reset your password."}</h1>
      <p className="mt-2 text-sm leading-6 text-slate-300">
        {token ? "Set a fresh password and return to your account." : "Enter your account email to create a reset link."}
      </p>

      {token ? (
        <form onSubmit={confirmReset} className="mt-6 space-y-3">
          <label className="block text-sm font-medium text-slate-200">
            New password
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              minLength={8}
              required
              type="password"
              autoComplete="new-password"
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
          </label>
          <button
            type="submit"
            disabled={loading}
            className="inline-flex w-full items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
          >
            Save new password
          </button>
        </form>
      ) : (
        <form onSubmit={requestReset} className="mt-6 space-y-3">
          <label className="block text-sm font-medium text-slate-200">
            Email
            <input
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              required
              type="email"
              autoComplete="email"
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-emerald-300/50"
            />
          </label>
          <button
            type="submit"
            disabled={loading}
            className="inline-flex w-full items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
          >
            Create reset link
          </button>
        </form>
      )}

      {resetPath ? (
        <Link href={resetPath} className="mt-4 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">
          Open secure reset link
        </Link>
      ) : null}
      {status ? <p className="mt-4 text-sm text-slate-300">{status}</p> : null}
    </section>
  );
}
