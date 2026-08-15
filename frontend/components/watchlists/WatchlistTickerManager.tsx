"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { addToWatchlist, getEntitlements, removeFromWatchlist, removeWatchlistTarget } from "@/lib/api";
import { WatchlistTickerAutocomplete } from "@/components/watchlists/WatchlistTickerAutocomplete";
import { formatInteger } from "@/lib/accountDisplay";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { formatCompanyName } from "@/lib/companyName";
import { departmentHref } from "@/lib/departments";
import { insiderHref } from "@/lib/insider";
import { memberHref } from "@/lib/memberSlug";
import { ghostButtonClassName, subtlePrimaryButtonClassName, tickerLinkClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";
import type { WatchlistTarget } from "@/lib/types";

type Ticker = { symbol: string; name: string };

type TargetSection = {
  title: string;
  empty: string;
  items: WatchlistTarget[];
};

function watchlistTargetHref(target: WatchlistTarget): string | null {
  const value = target.value?.trim() || null;
  const label = target.label?.trim() || value;
  if (!label && !value) return null;

  if (target.type === "member") return memberHref({ name: label, memberId: value });
  if (target.type === "insider") return insiderHref(label, value);
  if (target.type === "department") return departmentHref(label ?? value);
  return null;
}

export function WatchlistTickerManager({
  watchlistId,
  tickers,
  members = [],
  insiders = [],
  departments = [],
  institutions = [],
}: {
  watchlistId: number;
  tickers: Ticker[];
  members?: WatchlistTarget[];
  insiders?: WatchlistTarget[];
  departments?: WatchlistTarget[];
  institutions?: WatchlistTarget[];
}) {
  const [rows, setRows] = useState(tickers);
  const [targetSections, setTargetSections] = useState<TargetSection[]>([
    { title: "Members in this watchlist", empty: "No members followed yet.", items: members },
    { title: "Insiders in this watchlist", empty: "No insiders followed yet.", items: insiders },
    { title: "Departments in this watchlist", empty: "No departments followed yet.", items: departments },
    { title: "Institutions in this watchlist", empty: "No institutions followed yet.", items: institutions },
  ]);
  const [symbol, setSymbol] = useState("");
  const [showAllTickers, setShowAllTickers] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(false);
  const [isPending, startTransition] = useTransition();
  const router = useRouter();
  const tickerLimit = limitFor(entitlements, "watchlist_tickers");
  const canAddTickers = entitlementsLoaded && hasEntitlement(entitlements, "watchlist_tickers");
  const atTickerLimit = entitlementsLoaded && rows.length >= tickerLimit;

  useEffect(() => {
    setRows(tickers);
  }, [tickers]);

  useEffect(() => {
    setTargetSections([
      { title: "Members in this watchlist", empty: "No members followed yet.", items: members },
      { title: "Insiders in this watchlist", empty: "No insiders followed yet.", items: insiders },
      { title: "Departments in this watchlist", empty: "No departments followed yet.", items: departments },
      { title: "Institutions in this watchlist", empty: "No institutions followed yet.", items: institutions },
    ]);
  }, [members, insiders, departments, institutions]);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) {
          setEntitlements(next);
          setEntitlementsLoaded(true);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setEntitlements(defaultEntitlements);
          setEntitlementsLoaded(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const onTickerRemoved = (event: Event) => {
      const detail = (event as CustomEvent<{ watchlistId?: number; symbol?: string }>).detail;
      if (detail?.watchlistId !== watchlistId || !detail.symbol) return;
      setRows((current) => current.filter((ticker) => ticker.symbol.toUpperCase() !== detail.symbol?.toUpperCase()));
    };
    window.addEventListener("watchlist:ticker-removed", onTickerRemoved);
    return () => window.removeEventListener("watchlist:ticker-removed", onTickerRemoved);
  }, [watchlistId]);

  const cleanAddError = (err: unknown) => {
    const message = err instanceof Error ? err.message : "";
    if (message.includes("Ticker not found") || message.includes("HTTP 404")) {
      return "We couldn't find that ticker. Check the symbol and try again.";
    }
    if (message.includes("HTTP 422")) {
      return "Enter a valid ticker symbol.";
    }
    if (message.includes("premium_required") || message.includes("Free watchlists")) {
      return `Free watchlists can track ${formatInteger(tickerLimit)} tickers. Upgrade to add more symbols.`;
    }
    return "Unable to add ticker right now.";
  };

  const addSymbol = (rawSymbol: string) => {
    const trimmed = rawSymbol.trim().toUpperCase();
    if (!trimmed) {
      setError("Enter a ticker symbol.");
      return;
    }
    if (!canAddTickers) {
      setError("Adding tickers to watchlists is currently a Premium feature.");
      return;
    }
    if (atTickerLimit) {
      setError(`Free watchlists can track ${formatInteger(tickerLimit)} tickers. Upgrade to add more symbols.`);
      return;
    }

    setError(null);
    startTransition(async () => {
      try {
        await addToWatchlist(watchlistId, trimmed);
        setSymbol("");
        router.refresh();
      } catch (err) {
        setError(cleanAddError(err));
      }
    });
  };

  const handleAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    addSymbol(symbol);
  };

  const handleRemove = (tickerSymbol: string) => {
    setError(null);
    startTransition(async () => {
      try {
        await removeFromWatchlist(watchlistId, tickerSymbol);
        router.refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to remove ticker.");
      }
    });
  };

  const handleRemoveTarget = (target: WatchlistTarget) => {
    if (!target.type || !target.value) return;
    setError(null);
    startTransition(async () => {
      try {
        await removeWatchlistTarget(watchlistId, { type: String(target.type), value: String(target.value) });
        setTargetSections((current) =>
          current.map((section) => ({
            ...section,
            items: section.items.filter((item) => !(item.type === target.type && item.value === target.value)),
          })),
        );
        router.refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to remove watchlist item.");
      }
    });
  };

  const visibleRows = showAllTickers ? rows : rows.slice(0, 12);

  return (
    <div className="w-full min-w-0 rounded-2xl border border-white/10 bg-slate-900/70 p-4 shadow-card">
      <div className="flex flex-col gap-3 border-b border-white/10 pb-4">
        <h2 className="text-lg font-semibold text-white">Tickers in this watchlist</h2>
        <p className="text-sm text-slate-400">Add a ticker to shape the monitoring feed for this watchlist.</p>
        <form onSubmit={handleAdd} className="flex gap-2">
          <WatchlistTickerAutocomplete
            value={symbol}
            onChange={setSymbol}
            onSelect={addSymbol}
            disabled={isPending || atTickerLimit || !canAddTickers}
          />
          <button type="submit" className={`${subtlePrimaryButtonClassName} shrink-0`} disabled={isPending || atTickerLimit || !canAddTickers}>
            {isPending ? "Updating..." : "Add"}
          </button>
        </form>
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
        {!entitlementsLoaded ? (
          <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3" aria-busy="true" aria-live="polite">
            <div className="h-4 w-40 animate-pulse rounded bg-white/10" />
            <div className="mt-2 h-3 w-full max-w-xs animate-pulse rounded bg-white/10" />
          </div>
        ) : !canAddTickers || atTickerLimit ? (
          <UpgradePrompt
            title="Track more tickers with Premium"
            body={
              canAddTickers
                ? `Free watchlists include ${formatInteger(tickerLimit)} tickers per list. Keep this list focused or upgrade for deeper coverage.`
                : "Adding tickers to watchlists is currently a Premium feature."
            }
            compact={true}
          />
        ) : null}
      </div>
      <div className="mt-3 divide-y divide-white/10 overflow-hidden rounded-xl border border-white/10">
        {rows.length === 0 ? (
          <p className="text-sm text-slate-400">No tickers yet. Add a symbol to start tracking filings, insider trades, and signals.</p>
        ) : (
          visibleRows.map((ticker) => (
            <div key={ticker.symbol} className="flex items-center justify-between gap-3 bg-white/[0.025] px-3 py-2.5">
              <div className="min-w-0">
                {tickerHref(ticker.symbol) ? (
                  <Link href={tickerHref(ticker.symbol)!} prefetch={false} className={tickerLinkClassName}>
                    {ticker.symbol}
                  </Link>
                ) : (
                  <span className="text-sm font-semibold text-slate-200">{ticker.symbol}</span>
                )}
                <div className="text-xs text-slate-400">{formatCompanyName(ticker.name)}</div>
              </div>
              <button
                type="button"
                className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-lg leading-none text-slate-300 transition hover:bg-rose-400/10 hover:text-rose-200 disabled:opacity-60"
                onClick={() => handleRemove(ticker.symbol)}
                disabled={isPending}
                aria-label={`Remove ${ticker.symbol}`}
                title={`Remove ${ticker.symbol}`}
              >
                ×
              </button>
            </div>
          ))
        )}
      </div>
      {rows.length > 12 ? (
        <button type="button" onClick={() => setShowAllTickers((value) => !value)} className="mt-3 w-full rounded-lg border border-white/10 px-3 py-2 text-sm font-medium text-sky-300 hover:border-sky-300/30 hover:text-sky-200">
          {showAllTickers ? "Show fewer tickers" : `View all ${rows.length} tickers →`}
        </button>
      ) : null}
      <div className="mt-6 space-y-5 border-t border-white/10 pt-5">
        {targetSections.map((section) => (
          <div key={section.title} className="space-y-3">
            <h3 className="text-sm font-semibold text-white">{section.title}</h3>
            {section.items.length === 0 ? (
              <p className="text-sm text-slate-500">{section.empty}</p>
            ) : (
              <div className="space-y-2">
                {section.items.map((target) => {
                  const href = watchlistTargetHref(target);
                  const label = target.label ?? target.value;
                  return (
                    <div key={`${target.type}-${target.value}`} className="flex flex-col items-start gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                      <div className="min-w-0">
                        {href ? (
                          <Link href={href} prefetch={false} className={`${tickerLinkClassName} block truncate`}>
                            {label}
                          </Link>
                        ) : (
                          <p className="truncate text-sm font-semibold text-slate-100">{label}</p>
                        )}
                        <p className="text-xs uppercase tracking-[0.14em] text-slate-500">{target.type}</p>
                      </div>
                      <button
                        type="button"
                        className={ghostButtonClassName}
                        onClick={() => handleRemoveTarget(target)}
                        disabled={isPending}
                      >
                        Remove
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
