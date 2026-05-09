"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { ApiError, getEntitlements, getSignalsAll, type SignalItem } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, type Entitlements } from "@/lib/entitlements";
import { cardClassName, tickerMonoLinkClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";

type ClientSearchParams = Record<string, string | undefined>;

const PARAM_KEYS = [
  "mode",
  "side",
  "limit",
  "sort",
  "debug",
  "symbol",
  "confirmation_band",
  "confirmation_direction",
  "min_confirmation_sources",
  "multi_source_only",
] as const;

function param(searchParams: ClientSearchParams, key: string, fallback = "") {
  return (searchParams[key] ?? fallback).trim();
}

function cleanProtectedError(error: unknown, fallback: string) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402) return "Premium access required.";
    return fallback;
  }
  return error instanceof Error ? error.message : fallback;
}

export function SignalsClientPage({ initialSearchParams }: { initialSearchParams: ClientSearchParams }) {
  const [entitlements, setEntitlements] = useState<Entitlements | null>(null);
  const [items, setItems] = useState<SignalItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const mode = param(initialSearchParams, "mode", "all") || "all";
  const side = param(initialSearchParams, "side", "all") || "all";
  const sort = param(initialSearchParams, "sort", "smart") || "smart";
  const limit = Number(param(initialSearchParams, "limit", "50")) || 50;
  const confirmationBand = param(initialSearchParams, "confirmation_band", "all") || "all";
  const confirmationDirection = param(initialSearchParams, "confirmation_direction", "all") || "all";
  const minConfirmationSources = Number(param(initialSearchParams, "min_confirmation_sources", "0")) || 0;
  const multiSourceOnly = param(initialSearchParams, "multi_source_only") === "1";

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErrorMessage(null);
    getEntitlements()
      .then(async (nextEntitlements) => {
        if (cancelled) return;
        setEntitlements(nextEntitlements);
        if (!nextEntitlements.user) {
          setItems([]);
          setErrorMessage("Sign in required.");
          return;
        }
        if (!hasEntitlement(nextEntitlements, "signals")) {
          setItems([]);
          setErrorMessage("Premium access required.");
          return;
        }
        const response = await getSignalsAll({
          mode: mode as any,
          side: side as any,
          sort: sort as any,
          limit,
          confirmation_band: confirmationBand === "all" ? undefined : (confirmationBand as any),
          confirmation_direction: confirmationDirection === "all" ? undefined : (confirmationDirection as any),
          min_confirmation_sources: minConfirmationSources,
          multi_source_only: multiSourceOnly,
        });
        if (!cancelled) setItems(response.items);
      })
      .catch((error) => {
        console.error("[signals] protected client fetch failed", error);
        if (!cancelled) {
          setEntitlements(defaultEntitlements);
          setItems([]);
          setErrorMessage(cleanProtectedError(error, "Unable to load signals."));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [confirmationBand, confirmationDirection, limit, minConfirmationSources, mode, multiSourceOnly, side, sort]);

  const defaultParams = useMemo(
    () => ({
      mode,
      side,
      limit: String(limit),
      sort,
      confirmation_band: confirmationBand,
      confirmation_direction: confirmationDirection,
      min_confirmation_sources: String(multiSourceOnly && minConfirmationSources < 2 ? 2 : minConfirmationSources),
      multi_source_only: multiSourceOnly ? "1" : "",
    }),
    [confirmationBand, confirmationDirection, limit, minConfirmationSources, mode, multiSourceOnly, side, sort],
  );

  const isSignedIn = Boolean(entitlements?.user);
  const isPremiumError = errorMessage === "Premium access required.";
  const isSignInError = errorMessage === "Sign in required.";

  return (
    <div className="space-y-8">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">SIGNALS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Unusual trade radar</h1>
        <p className="mt-2 max-w-2xl text-sm text-slate-300/80">
          Protected signals load in your browser while session cookies finish the cross-site transition.
        </p>
      </div>

      <div className={`${cardClassName} space-y-3`}>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          {(["all", "congress", "insider"] as const).map((nextMode) => (
            <Link
              key={nextMode}
              href={`/signals?mode=${nextMode}&side=${side}&limit=${limit}&sort=${sort}`}
              className={`rounded-full border px-3 py-1 font-semibold ${mode === nextMode ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-300"}`}
              prefetch={false}
            >
              {nextMode.toUpperCase()}
            </Link>
          ))}
        </div>
        <SavedViewsBar surface="signals" defaultParams={defaultParams} paramKeys={PARAM_KEYS} />
      </div>

      {loading ? (
        <div className={`${cardClassName} min-h-[18rem] p-8 text-sm text-slate-300`}>Loading your Signals access...</div>
      ) : isSignInError ? (
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Sign in required</h2>
          <p className="mt-2 text-sm text-slate-400">Log in to use Signals.</p>
          <Link href="/login?return_to=%2Fsignals" className="mt-4 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100">
            Sign in
          </Link>
        </div>
      ) : isPremiumError && isSignedIn ? (
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Premium required</h2>
          <p className="mt-2 text-sm text-slate-400">Signals are included with Premium.</p>
          <Link href={entitlements?.upgrade_url ?? "/pricing"} className="mt-4 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100">
            View plans
          </Link>
        </div>
      ) : (
        <div className={`${cardClassName} min-h-[28rem] overflow-hidden p-0`}>
          <div className="border-b border-slate-800 px-4 py-3 text-sm text-slate-400">
            {errorMessage ? "Signals unavailable" : `${items.length} visible signals`}
          </div>
          {errorMessage ? (
            <div className="p-8 text-center text-sm text-slate-400">{errorMessage}</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="bg-slate-950/50 text-xs uppercase tracking-wide text-slate-400">
                  <tr>
                    <th className="px-4 py-3">Time</th>
                    <th className="px-4 py-3">Ticker</th>
                    <th className="px-4 py-3">Who</th>
                    <th className="px-4 py-3">Side</th>
                    <th className="px-4 py-3 text-right">Smart</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {items.length === 0 ? (
                    <tr><td colSpan={5} className="px-4 py-10 text-center text-slate-400">No unusual signals returned.</td></tr>
                  ) : (
                    items.map((item) => {
                      const href = tickerHref(item.symbol);
                      return (
                        <tr key={item.event_id} className="text-slate-200">
                          <td className="px-4 py-3 font-mono text-xs text-slate-400">{item.ts?.slice(0, 10) ?? "--"}</td>
                          <td className="px-4 py-3">{href ? <Link href={href} className={tickerMonoLinkClassName}>{item.symbol}</Link> : <span className="font-mono">{item.symbol}</span>}</td>
                          <td className="px-4 py-3">{item.who ?? "--"}</td>
                          <td className="px-4 py-3">{item.trade_type ?? "--"}</td>
                          <td className="px-4 py-3 text-right">{item.smart_score ?? "--"}</td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
