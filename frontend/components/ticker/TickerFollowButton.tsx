"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { WalnutModal } from "@/components/ui/WalnutModal";
import { followTicker, getEntitlements, listWatchlists } from "@/lib/api";
import { defaultEntitlements, type Entitlements } from "@/lib/entitlements";
import { recordGoogleAnalyticsEvent } from "@/lib/googleAnalytics";
import { hasPrivacyConsent } from "@/lib/privacyConsent";
import { normalizeTickerSymbol } from "@/lib/ticker";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  symbol: string;
};

function watchlistHasSymbol(watchlist: WatchlistSummary, symbol: string) {
  const normalized = normalizeTickerSymbol(symbol) ?? "";
  return (watchlist.symbols ?? []).some((item) => normalizeTickerSymbol(item) === normalized);
}

function planForAnalytics(entitlements: Entitlements) {
  return entitlements.effective_tier ?? entitlements.plan ?? entitlements.tier ?? "free";
}

export function TickerFollowButton({ symbol }: Props) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const normalizedSymbol = normalizeTickerSymbol(symbol) ?? symbol.trim().toUpperCase();
  const searchParamsString = searchParams.toString();
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [followingWatchlist, setFollowingWatchlist] = useState<WatchlistSummary | null>(null);
  const [authPromptOpen, setAuthPromptOpen] = useState(false);
  const [isBusy, setIsBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const authResolvedRef = useRef(false);
  const impressionTrackedRef = useRef(false);
  const continuationHandledRef = useRef(false);
  const completionTrackedRef = useRef(false);

  const followIntent = searchParams.get("follow") === "1";
  const authReturnTo = `${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`;

  const track = useCallback(
    (eventName: string, source: Entitlements) => {
      if (!hasPrivacyConsent("analytics")) return;
      const params = new URLSearchParams(searchParamsString);
      recordGoogleAnalyticsEvent(eventName, {
        ticker: normalizedSymbol,
        pathname,
        source_page_type: "ticker",
        auth_state: source.user ? "authenticated" : "anonymous",
        plan: planForAnalytics(source),
        acquisition_source: params.get("utm_source") ?? undefined,
        acquisition_medium: params.get("utm_medium") ?? undefined,
        follow_method: eventName === "ticker_follow_complete" ? "ticker_header" : undefined,
      });
    },
    [normalizedSymbol, pathname, searchParamsString],
  );

  const removeFollowIntent = useCallback(() => {
    const params = new URLSearchParams(searchParamsString);
    params.delete("follow");
    const next = params.toString();
    router.replace(`${pathname}${next ? `?${next}` : ""}`);
  }, [pathname, router, searchParamsString]);

  const loadFollowingState = useCallback(async (source: Entitlements) => {
    if (!source.user) {
      setFollowingWatchlist(null);
      return [] as WatchlistSummary[];
    }
    const watchlists = await listWatchlists();
    setFollowingWatchlist(watchlists.find((watchlist) => watchlistHasSymbol(watchlist, normalizedSymbol)) ?? null);
    return watchlists;
  }, [normalizedSymbol]);

  const completeFollow = useCallback(async (source: Entitlements, fromContinuation = false) => {
    if (!source.user || isBusy) return;
    setIsBusy(true);
    setStatus(null);
    try {
      const result = await followTicker(normalizedSymbol);
      setFollowingWatchlist(result.watchlist);
      if (!completionTrackedRef.current) {
        completionTrackedRef.current = true;
        track("ticker_follow_complete", source);
      }
      if (fromContinuation) removeFollowIntent();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to follow this ticker right now.";
      setStatus(message.includes("premium_required") ? "Your current plan cannot add more followed tickers." : message);
    } finally {
      setIsBusy(false);
    }
  }, [isBusy, normalizedSymbol, removeFollowIntent, track]);

  useEffect(() => {
    let cancelled = false;
    void getEntitlements()
      .then(async (nextEntitlements) => {
        if (cancelled) return;
        authResolvedRef.current = true;
        setEntitlements(nextEntitlements);
        if (!impressionTrackedRef.current) {
          impressionTrackedRef.current = true;
          track("ticker_follow_impression", nextEntitlements);
        }
        await loadFollowingState(nextEntitlements);
        if (!cancelled && followIntent && nextEntitlements.user && !continuationHandledRef.current) {
          continuationHandledRef.current = true;
          await completeFollow(nextEntitlements, true);
        }
      })
      .catch(() => {
        if (!cancelled) authResolvedRef.current = true;
      });
    return () => {
      cancelled = true;
    };
  }, [completeFollow, followIntent, loadFollowingState, track]);

  const handleClick = async () => {
    if (!normalizedSymbol || isBusy) return;
    setStatus(null);
    let source = entitlements;
    if (!authResolvedRef.current) {
      try {
        source = await getEntitlements();
        authResolvedRef.current = true;
        setEntitlements(source);
      } catch {
        setStatus("Unable to check account access right now. Please try again.");
        return;
      }
    }
    track("ticker_follow_click", source);
    if (!source.user) {
      setAuthPromptOpen(true);
      track("ticker_follow_auth_prompt_view", source);
      return;
    }
    if (followingWatchlist) {
      router.push(`/watchlists/${followingWatchlist.id}`);
      return;
    }
    await completeFollow(source);
  };

  const signupHref = `/login?mode=register&return_to=${encodeURIComponent(authReturnTo)}`;
  const loginHref = `/login?return_to=${encodeURIComponent(authReturnTo)}`;
  const following = Boolean(followingWatchlist);

  return (
    <div className="relative inline-flex min-w-0 flex-col items-stretch gap-1 sm:items-end">
      <button
        type="button"
        onClick={() => void handleClick()}
        disabled={isBusy}
        className={`inline-flex min-h-10 items-center justify-center rounded-xl border px-4 py-2 text-sm font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 disabled:cursor-wait disabled:opacity-70 ${
          following
            ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100 hover:bg-emerald-300/15"
            : "border-emerald-300/60 bg-emerald-400/90 text-slate-950 shadow-lg shadow-emerald-400/20 hover:bg-emerald-300"
        }`}
        aria-label={following ? `Following ${normalizedSymbol}. Manage follow.` : `Follow ${normalizedSymbol}`}
        aria-busy={isBusy}
      >
        {isBusy ? "Following..." : following ? `✓ Following ${normalizedSymbol}` : `★ Follow ${normalizedSymbol}`}
      </button>
      {!following ? <p className="hidden text-right text-[11px] font-medium text-slate-400 xl:block">Get notified when the evidence changes.</p> : null}
      {status ? <p className="max-w-64 text-right text-xs text-rose-300" role="alert">{status}</p> : null}
      <span className="sr-only" aria-live="polite">{following ? `${normalizedSymbol} is now followed.` : ""}</span>

      <WalnutModal
        open={authPromptOpen}
        title={`Follow ${normalizedSymbol} with Walnut`}
        description={`We’ll monitor ${normalizedSymbol} and alert you when something important changes.`}
        eyebrow="Ongoing monitoring"
        tone="success"
        onClose={() => setAuthPromptOpen(false)}
        closeLabel="Close follow prompt"
        className="items-end p-0 sm:items-center sm:px-4 sm:py-8"
        panelClassName="w-full rounded-b-none border-x-0 border-b-0 px-5 pb-[max(1.25rem,env(safe-area-inset-bottom))] pt-5 sm:w-[min(calc(100vw_-_24px),520px)] sm:rounded-2xl sm:border sm:p-5"
        footer={
          <>
            <Link
              href={loginHref}
              onClick={() => track("ticker_follow_login_click", entitlements)}
              className="inline-flex min-h-10 items-center justify-center rounded-xl border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            >
              Already have an account? Log in
            </Link>
            <Link
              href={signupHref}
              onClick={() => track("ticker_follow_signup_click", entitlements)}
              className="inline-flex min-h-10 items-center justify-center rounded-xl border border-emerald-300/60 bg-emerald-400/90 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300"
            >
              Create free account to follow
            </Link>
          </>
        }
      >
        <ul className="space-y-2 text-sm leading-5 text-slate-200">
          <li>✓ Confirmation Score changes</li>
          <li>✓ Insider activity</li>
          <li>✓ Congress trades</li>
          <li>✓ Institutional activity</li>
          <li>✓ Major fundamental changes</li>
        </ul>
      </WalnutModal>
    </div>
  );
}
