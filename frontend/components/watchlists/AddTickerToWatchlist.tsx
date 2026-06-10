"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useLayoutEffect, useRef, useState, useTransition } from "react";
import type { CSSProperties } from "react";
import { addToWatchlist, createWatchlist, getEntitlements, listWatchlists } from "@/lib/api";
import { formatInteger } from "@/lib/accountDisplay";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import type { WatchlistSummary } from "@/lib/types";
import { ghostButtonClassName, inputClassName, primaryButtonClassName } from "@/lib/styles";

type Props = {
  symbol: string;
  variant?: "default" | "compact";
  align?: "left" | "right";
};

type WatchlistToast = {
  message: string;
  tone: "success" | "error" | "info";
};

function cleanWatchlistError(err: unknown) {
  const message = err instanceof Error ? err.message : "";
  if (message.includes("premium_required") || message.includes("Free accounts") || message.includes("Free watchlists")) {
    return "That Premium limit is active. Open Account to compare plans.";
  }
  if (message.includes("Ticker not found") || message.includes("HTTP 404")) {
    return "We couldn't find that ticker. Check the symbol and try again.";
  }
  if (message.includes("Watchlist name already exists") || message.includes("HTTP 409")) {
    return "That watchlist name already exists.";
  }
  if (message.includes("HTTP 422")) {
    return "Enter a valid watchlist name.";
  }
  return "Unable to update watchlists right now.";
}

let watchlistsCache: WatchlistSummary[] | null = null;
let watchlistsCacheAt = 0;
let watchlistsRequest: Promise<WatchlistSummary[]> | null = null;
const watchlistsCacheTtlMs = 5000;

function loadWatchlistsOnce() {
  if (watchlistsCache && Date.now() - watchlistsCacheAt < watchlistsCacheTtlMs) return Promise.resolve(watchlistsCache);
  if (!watchlistsRequest) {
    watchlistsRequest = listWatchlists()
      .then((items) => {
        watchlistsCache = items;
        watchlistsCacheAt = Date.now();
        return items;
      })
      .finally(() => {
        watchlistsRequest = null;
      });
  }
  return watchlistsRequest;
}

function rememberWatchlist(watchlist: WatchlistSummary) {
  const current = watchlistsCache ?? [];
  watchlistsCache = current.some((item) => item.id === watchlist.id) ? current : [...current, watchlist];
  watchlistsCacheAt = Date.now();
}

function normalizedSymbolValue(symbol: string | null | undefined) {
  return (symbol ?? "").trim().toUpperCase();
}

function watchlistHasSymbol(watchlist: WatchlistSummary, symbol: string) {
  const normalized = normalizedSymbolValue(symbol);
  return (watchlist.symbols ?? []).some((item) => normalizedSymbolValue(item) === normalized);
}

function withSymbolInWatchlist(watchlist: WatchlistSummary, symbol: string): WatchlistSummary {
  const normalized = normalizedSymbolValue(symbol);
  if (!normalized || watchlistHasSymbol(watchlist, normalized)) return watchlist;
  return { ...watchlist, symbols: [...(watchlist.symbols ?? []), normalized] };
}

function rememberWatchlistSymbol(watchlistId: number, symbol: string) {
  if (!watchlistsCache) return;
  watchlistsCache = watchlistsCache.map((watchlist) =>
    watchlist.id === watchlistId ? withSymbolInWatchlist(watchlist, symbol) : watchlist,
  );
  watchlistsCacheAt = Date.now();
}

function isAuthError(err: unknown) {
  const message = err instanceof Error ? err.message : "";
  return message.includes("HTTP 401") || message.includes("HTTP 403");
}

function watchlistToastTone(message: string): WatchlistToast["tone"] {
  const lower = message.toLowerCase();
  if (lower.includes("already")) return "info";
  if (lower.includes("unable") || lower.includes("couldn't") || lower.includes("premium") || lower.includes("valid")) return "error";
  return "success";
}

function computePanelStyle(root: HTMLDivElement | null, align: "left" | "right"): CSSProperties {
  const fallback: CSSProperties = {
    left: 16,
    maxHeight: "min(420px, calc(100vh - 2rem))",
    top: 64,
    width: "calc(100vw - 2rem)",
  };
  if (typeof window === "undefined" || !root) return fallback;

  const rect = root.getBoundingClientRect();
  const margin = 16;
  const width = Math.min(352, window.innerWidth - margin * 2);
  const preferredLeft = align === "left" ? rect.left : rect.right - width;
  const left = Math.min(Math.max(margin, preferredLeft), window.innerWidth - width - margin);
  const belowSpace = window.innerHeight - rect.bottom - margin - 8;
  const aboveSpace = rect.top - margin - 8;

  if (belowSpace < 280 && aboveSpace > belowSpace) {
    return {
      bottom: window.innerHeight - rect.top + 8,
      left,
      maxHeight: Math.max(120, Math.min(420, aboveSpace)),
      width,
    };
  }

  return {
    left,
    maxHeight: Math.max(120, Math.min(420, belowSpace)),
    top: rect.bottom + 8,
    width,
  };
}

function CompactWatchlistGlyph({ added }: { added: boolean }) {
  if (added) {
    return (
      <span aria-hidden="true" className="relative block h-3.5 w-3.5">
        <span className="absolute left-[0.12rem] top-[0.44rem] h-0.5 w-1.5 rotate-45 rounded-full bg-current" />
        <span className="absolute left-[0.38rem] top-[0.32rem] h-0.5 w-2.5 -rotate-45 rounded-full bg-current" />
      </span>
    );
  }

  return (
    <span aria-hidden="true" className="relative block h-3.5 w-3.5">
      <span className="absolute left-1/2 top-0 h-full w-0.5 -translate-x-1/2 rounded-full bg-current" />
      <span className="absolute left-0 top-1/2 h-0.5 w-full -translate-y-1/2 rounded-full bg-current" />
    </span>
  );
}

export function AddTickerToWatchlist({ symbol, variant = "default", align = "right" }: Props) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [watchlists, setWatchlists] = useState<WatchlistSummary[]>([]);
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [toast, setToast] = useState<WatchlistToast | null>(null);
  const [open, setOpen] = useState(false);
  const [authGateOpen, setAuthGateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(false);
  const [added, setAdded] = useState(false);
  const [addingWatchlistId, setAddingWatchlistId] = useState<number | null>(null);
  const [panelStyle, setPanelStyle] = useState<CSSProperties>({});
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [isPending, startTransition] = useTransition();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const normalizedSymbol = symbol.trim().toUpperCase();
  const searchParamsString = searchParams.toString();
  const returnTo = `${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`;

  const showToast = useCallback((message: string, tone: WatchlistToast["tone"] = watchlistToastTone(message)) => {
    setToast({ message, tone });
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timeoutId = window.setTimeout(() => setToast(null), 4200);
    return () => window.clearTimeout(timeoutId);
  }, [toast]);

  useEffect(() => {
    if (!open) return;

    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (!rootRef.current?.contains(target)) setOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  useLayoutEffect(() => {
    if (!open) return;

    const updatePanelPosition = () => {
      setPanelStyle(computePanelStyle(rootRef.current, align));
    };

    updatePanelPosition();
    window.addEventListener("resize", updatePanelPosition);
    window.addEventListener("scroll", updatePanelPosition, true);
    return () => {
      window.removeEventListener("resize", updatePanelPosition);
      window.removeEventListener("scroll", updatePanelPosition, true);
    };
  }, [align, open]);

  const addSymbolToWatchlist = useCallback((watchlistId: number, watchlistName?: string, options?: { closeOnSuccess?: boolean; entitlementsOverride?: Entitlements }) => {
    if (!Number.isFinite(watchlistId)) return;
    if (!normalizedSymbol) {
      showToast("No ticker symbol available for this disclosure.", "error");
      return;
    }
    const entitlementSource = options?.entitlementsOverride ?? entitlements;
    if (!hasEntitlement(entitlementSource, "watchlist_tickers")) {
      const message = "Adding tickers to watchlists is currently a Premium feature.";
      setStatus(message);
      showToast(message, "error");
      return;
    }

    setStatus(null);
    setAddingWatchlistId(watchlistId);
    startTransition(async () => {
      try {
        const result = await addToWatchlist(watchlistId, normalizedSymbol);
        const addedSymbol = normalizedSymbolValue(result.symbol) || normalizedSymbol;
        setWatchlists((current) =>
          current.map((watchlist) => (watchlist.id === watchlistId ? withSymbolInWatchlist(watchlist, addedSymbol) : watchlist)),
        );
        rememberWatchlistSymbol(watchlistId, addedSymbol);
        setAdded(true);
        const message =
          result.status === "exists"
            ? `${normalizedSymbol} is already in ${watchlistName ?? "that watchlist"}.`
            : `Added ${normalizedSymbol} to ${watchlistName ?? "watchlist"}.`;
        setStatus(message);
        showToast(message);
        if (options?.closeOnSuccess) setOpen(false);
      } catch (err) {
        const message = cleanWatchlistError(err);
        setStatus(message);
        showToast(message, "error");
      } finally {
        setAddingWatchlistId(null);
      }
    });
  }, [entitlements, normalizedSymbol, showToast]);

  const handleWatchlistRowClick = (watchlist: WatchlistSummary) => {
    if (addingWatchlistId !== null) return;
    if (watchlistHasSymbol(watchlist, normalizedSymbol)) {
      showToast(`${normalizedSymbol} is already in ${watchlist.name}.`, "info");
      return;
    }
    addSymbolToWatchlist(watchlist.id, watchlist.name);
  };

  const handleCreateAndAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = newWatchlistName.trim();
    if (!name) {
      const message = "Name the new watchlist first.";
      setStatus(message);
      showToast(message, "error");
      return;
    }
    if (!hasEntitlement(entitlements, "watchlists")) {
      const message = "Watchlist creation is currently a Premium feature.";
      setStatus(message);
      showToast(message, "error");
      return;
    }
    const limit = limitFor(entitlements, "watchlists");
    if (watchlists.length >= limit) {
      const message = `Free accounts can keep ${formatInteger(limit)} watchlists. Upgrade to create more.`;
      setStatus(message);
      showToast(message, "error");
      return;
    }
    setStatus(null);
    startTransition(async () => {
      try {
        const created = await createWatchlist(name);
        const result = await addToWatchlist(created.id, normalizedSymbol);
        const addedSymbol = normalizedSymbolValue(result.symbol) || normalizedSymbol;
        const nextWatchlist = withSymbolInWatchlist(created, addedSymbol);
        rememberWatchlist(nextWatchlist);
        setWatchlists((current) => [...current, nextWatchlist]);
        setNewWatchlistName("");
        setCreating(false);
        setAdded(true);
        const message = `Added ${normalizedSymbol} to ${created.name}.`;
        setStatus(message);
        showToast(message);
      } catch (err) {
        const message = cleanWatchlistError(err);
        setStatus(message);
        showToast(message, "error");
      }
    });
  };

  const openPickerWithWatchlists = useCallback((items: WatchlistSummary[]) => {
    setWatchlists(items);
    setAdded(items.some((watchlist) => watchlistHasSymbol(watchlist, normalizedSymbol)));
    setLoaded(true);
    setEntitlementsLoaded(true);
    setPanelStyle(computePanelStyle(rootRef.current, align));
    setOpen(true);
  }, [align, normalizedSymbol]);

  const handleTriggerClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!normalizedSymbol) {
      showToast("No ticker symbol available for this disclosure.", "error");
      return;
    }
    if (open) {
      setOpen(false);
      return;
    }

    setPanelStyle(computePanelStyle(rootRef.current, align));
    setLoaded(false);
    setEntitlementsLoaded(false);
    setAddingWatchlistId(null);
    setAuthGateOpen(false);
    setStatus(null);
    setCreating(false);

    startTransition(async () => {
      try {
        const nextEntitlements = await getEntitlements();
        setEntitlements(nextEntitlements);
        setEntitlementsLoaded(true);
        if (!nextEntitlements.user) {
          setAuthGateOpen(true);
          return;
        }

        const items = await loadWatchlistsOnce();
        setWatchlists(items);
        setLoaded(true);
        const matchingWatchlist = items.find((watchlist) => watchlistHasSymbol(watchlist, normalizedSymbol));
        setAdded(Boolean(matchingWatchlist));

        if (matchingWatchlist) {
          showToast(`${normalizedSymbol} is already in ${matchingWatchlist.name}.`, "info");
          return;
        }
        if (items.length === 1) {
          setEntitlements(nextEntitlements);
          addSymbolToWatchlist(items[0].id, items[0].name, { closeOnSuccess: true, entitlementsOverride: nextEntitlements });
          return;
        }
        if (items.length === 0) {
          showToast("No watchlist found. Create one first.", "info");
        }
        openPickerWithWatchlists(items);
      } catch (err) {
        if (isAuthError(err)) {
          setAuthGateOpen(true);
          return;
        }
        const message = "Could not add to watchlist. Please try again.";
        setStatus(message);
        showToast(message, "error");
      }
    });
  };

  const triggerClassName =
    variant === "compact"
      ? `inline-flex h-8 w-8 items-center justify-center rounded-full border text-sm font-semibold shadow-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30 ${
          added
            ? "border-emerald-300/35 bg-emerald-300/10 text-emerald-100"
            : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-emerald-300/40 hover:bg-emerald-300/10 hover:text-emerald-100"
        }`
      : `${ghostButtonClassName} rounded-xl py-1.5`;

  const tooltipLabel = "Add to watchlist";
  const buttonLabel = added ? `${normalizedSymbol} saved to watchlist` : `${tooltipLabel}${normalizedSymbol ? `: ${normalizedSymbol}` : ""}`;
  const toastToneClassName =
    toast?.tone === "error"
      ? "border-rose-300/45 text-rose-100 shadow-[0_0_28px_rgba(244,63,94,0.18)]"
      : toast?.tone === "info"
        ? "border-slate-300/30 text-slate-100 shadow-[0_0_28px_rgba(148,163,184,0.14)]"
        : "border-emerald-300/40 text-emerald-100 shadow-[0_0_28px_rgba(16,185,129,0.18)]";
  const toastDotClassName = toast?.tone === "error" ? "bg-rose-300" : toast?.tone === "info" ? "bg-slate-300" : "bg-emerald-300";

  return (
    <div
      ref={rootRef}
      className="group/watchlist relative inline-flex shrink-0"
      data-row-action="true"
      onClick={(event) => event.stopPropagation()}
      onPointerDown={(event) => event.stopPropagation()}
    >
      <button
        type="button"
        onPointerDown={(event) => {
          event.stopPropagation();
        }}
        onMouseDown={(event) => {
          event.preventDefault();
          event.stopPropagation();
        }}
        onClick={handleTriggerClick}
        className={triggerClassName}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-label={buttonLabel}
        title={tooltipLabel}
      >
        {variant === "compact" ? <CompactWatchlistGlyph added={added} /> : added ? "Saved" : "Add to watchlist"}
      </button>
      {open ? (
        <div
          role="dialog"
          aria-label={`Add ${normalizedSymbol} to watchlist`}
          style={panelStyle}
          className="fixed z-40 max-w-[calc(100vw-2rem)] overflow-y-auto rounded-3xl border border-white/10 bg-slate-950/95 p-4 text-left shadow-2xl shadow-black/40 backdrop-blur"
        >
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Watchlist</p>
              <h3 className="mt-1 text-sm font-semibold text-white">Save {normalizedSymbol}</h3>
            </div>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded-full border border-white/10 px-2 py-1 text-xs font-semibold text-slate-400 transition hover:border-white/20 hover:text-white"
              aria-label="Close watchlist picker"
            >
              Esc
            </button>
          </div>

          {!entitlementsLoaded || (entitlements.user && !loaded) ? (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">
              Loading watchlists...
            </p>
          ) : watchlists.length > 0 ? (
            <div className="mt-4 space-y-2">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Existing lists</p>
              <div className="max-h-44 space-y-1 overflow-y-auto pr-1">
                {watchlists.map((watchlist) => {
                  const isInWatchlist = watchlistHasSymbol(watchlist, normalizedSymbol);
                  const isAdding = addingWatchlistId === watchlist.id;
                  return (
                    <button
                      key={watchlist.id}
                      type="button"
                      onClick={() => handleWatchlistRowClick(watchlist)}
                      disabled={isInWatchlist || addingWatchlistId !== null}
                      className={`flex w-full items-center justify-between rounded-2xl border px-3 py-2 text-left text-sm transition ${
                        isInWatchlist
                          ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                          : "border-white/10 bg-white/[0.03] text-slate-200 hover:border-white/20 hover:bg-white/[0.06] disabled:cursor-wait disabled:opacity-70"
                      }`}
                    >
                      <span className="min-w-0 truncate">{watchlist.name}</span>
                      <span className={`ml-3 shrink-0 text-xs font-semibold ${isInWatchlist ? "text-emerald-200" : "text-slate-300"}`}>
                        {isAdding ? "Adding..." : isInWatchlist ? "Added" : "Add"}
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="mt-4 rounded-2xl border border-dashed border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">
              <p>No watchlist found. Create one first.</p>
              <Link
                href="/watchlists"
                className="mt-3 inline-flex rounded-xl border border-emerald-300/30 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
              >
                Create watchlist
              </Link>
            </div>
          )}

          <div className="mt-4 border-t border-white/10 pt-4">
            {creating || watchlists.length === 0 ? (
              <form onSubmit={handleCreateAndAdd} className="space-y-2">
                <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  New watchlist
                  <input
                    value={newWatchlistName}
                    onChange={(event) => setNewWatchlistName(event.target.value)}
                    className={`${inputClassName} rounded-xl`}
                    placeholder="e.g. AI infrastructure"
                    disabled={isPending}
                  />
                </label>
                <div className="flex gap-2">
                  <button type="submit" disabled={isPending} className={`${primaryButtonClassName} flex-1 rounded-xl py-2`}>
                    {isPending ? "Creating..." : "Create and add"}
                  </button>
                  {watchlists.length > 0 ? (
                    <button
                      type="button"
                      onClick={() => setCreating(false)}
                      className={`${ghostButtonClassName} rounded-xl py-2`}
                      disabled={isPending}
                    >
                      Cancel
                    </button>
                  ) : null}
                </div>
              </form>
            ) : (
              <button
                type="button"
                onClick={() => setCreating(true)}
                className="text-sm font-semibold text-emerald-200 transition hover:text-emerald-100"
              >
                Create a new watchlist
              </button>
            )}
          </div>

          {status ? <p className="mt-3 text-xs text-slate-400">{status}</p> : null}
        </div>
      ) : null}
      {toast ? (
        <div className="pointer-events-none fixed inset-x-3 top-4 z-[100] flex justify-center sm:inset-x-auto sm:right-4 sm:justify-end">
          <div
            role={toast.tone === "error" ? "alert" : "status"}
            aria-live={toast.tone === "error" ? "assertive" : "polite"}
            className={`pointer-events-auto flex w-full max-w-md items-start gap-3 rounded-lg border bg-slate-950 px-4 py-3 font-mono text-sm leading-5 ${toastToneClassName}`}
          >
            <span className={`mt-2 h-2 w-2 shrink-0 rounded-full ${toastDotClassName}`} aria-hidden="true" />
            <span className="min-w-0 flex-1 break-words">{toast.message}</span>
            <button
              type="button"
              onClick={() => setToast(null)}
              className="shrink-0 rounded-md px-1.5 py-0.5 text-xs font-semibold text-current opacity-70 transition hover:bg-white/10 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-white/30"
              aria-label="Close notification"
            >
              X
            </button>
          </div>
        </div>
      ) : null}
      {authGateOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Watchlists</p>
                <h2 className="mt-2 text-lg font-semibold">Create a free account</h2>
              </div>
              <button
                type="button"
                className="rounded-lg border border-white/10 px-2 py-1 text-sm text-slate-300 hover:text-white"
                onClick={() => setAuthGateOpen(false)}
              >
                Close
              </button>
            </div>
            <p className="mt-3 text-sm leading-6 text-slate-300">
              Create a free account or log in to save tickers to watchlists and keep your monitoring workflow synced.
            </p>
            <div className="mt-5 flex flex-wrap justify-end gap-3">
              <Link
                href={`/login?return_to=${encodeURIComponent(returnTo)}`}
                className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
              >
                Login
              </Link>
              <Link
                href={`/login?return_to=${encodeURIComponent(returnTo)}`}
                className="rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
              >
                Create account
              </Link>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
