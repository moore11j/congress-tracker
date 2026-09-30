"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { completeGoogleSignIn, completeSearchConsole, completeKeywordPlanner, verifyAuthenticatedSession } from "@/lib/api";
import { identifyHeyCatchUser } from "@/lib/heycatch";
import { defaultPostLoginPath, safeAppReturnPath } from "@/lib/returnPaths";
import { clearGoogleReturnPath, googleReturnPath, withAuthTimeout } from "@/lib/authRecovery";

export default function GoogleCallbackPage() {
  const [status, setStatus] = useState("Finishing Google sign-in...");
  const [returnTo, setReturnTo] = useState(defaultPostLoginPath);
  const [failed, setFailed] = useState(false);
  const [adminConnection, setAdminConnection] = useState(false);

  const started = useRef(false);
  useEffect(() => {
    if (started.current) return;
    started.current = true;
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const state = params.get("state");
    if (state?.startsWith("gads_")) {
      setAdminConnection(true);
      window.history.replaceState(null, "", window.location.pathname);
      setReturnTo("/admin/research-briefs");
      if (!code || params.has("error")) {
        setStatus("Keyword Planner was not connected. Return to Daily SEO and try Connect again.");
        return;
      }
      setStatus("Connecting Google Keyword Planner...");
      completeKeywordPlanner(code, state).then(result => {
        setStatus(result.error ? `Keyword Planner connected, but initial lookup needs attention: ${result.error}`
          : "Keyword Planner connected. Google search-volume estimates are now available for daily topic ranking. Open Daily SEO to review the data.");
      }).catch(error => setStatus(error instanceof Error ? error.message : "Keyword Planner could not be connected."));
      return;
    }
    if (state?.startsWith("gsc_")) {
      setAdminConnection(true);
      // Reuse the registered callback, but never turn this admin connection into
      // a public login or expose its code in the visible URL after handling.
      window.history.replaceState(null, "", window.location.pathname);
      setReturnTo("/admin/research-briefs");
      if (!code || params.has("error")) {
        setStatus("Search Console was not connected. Return to Daily SEO and try Connect again.");
        return;
      }
      setStatus("Connecting Search Console...");
      completeSearchConsole(code, state).then(() => {
        setStatus("Search Console connected. Daily sync is enabled. Open Daily SEO to sync now or review performance.");
      }).catch(error => setStatus(error instanceof Error ? error.message : "Search Console could not be connected."));
      return;
    }
    setReturnTo(googleReturnPath());
    window.history.replaceState(window.history.state, "", window.location.pathname);
    if (params.has("error") || !code || !state) {
      setFailed(true);
      setStatus(params.get("error") === "access_denied"
        ? "Google sign-in was cancelled. You can try again or use email and password."
        : "Google did not return a complete sign-in response. Please try again.");
      return;
    }

    withAuthTimeout(completeGoogleSignIn({
      code,
      state,
      redirect_uri: `${window.location.origin}/auth/google/callback`,
    }))
      .then((response) => {
        const next = safeAppReturnPath(response.return_to);
        setReturnTo(next);
        setStatus("Verifying your session...");
        return withAuthTimeout(verifyAuthenticatedSession("GoogleCallbackPage"), 15_000).then((session) => {
          if (session.user) identifyHeyCatchUser(session.user);
          clearGoogleReturnPath();
          window.location.replace(next);
        });
      })
      .catch((error) => {
        setFailed(true);
        setStatus(error instanceof Error ? error.message : "Unable to finish Google sign-in.");
      });
  }, []);

  return (
    <div className="mx-auto max-w-xl rounded-lg border border-white/10 bg-slate-900/70 p-6">
      <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Google sign-in</p>
      <h1 role={failed ? "alert" : "status"} className="mt-2 text-2xl font-semibold text-white">{status}</h1>
      {failed || adminConnection ? (
        <Link href={failed ? `/login?return_to=${encodeURIComponent(returnTo)}` : returnTo} className="mt-5 inline-flex rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
          {failed ? "Try signing in again" : "Continue"}
        </Link>
      ) : null}
    </div>
  );
}
