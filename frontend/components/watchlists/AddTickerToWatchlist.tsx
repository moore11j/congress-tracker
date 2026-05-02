"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useEffect, useLayoutEffect, useMemo, useRef, useState, useTransition } from "react";
import type { CSSProperties } from "react";
import { addToWatchlist, createWatchlist, getEntitlements, listWatchlists } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import type { WatchlistSummary } from "@/lib/types";
import { ghostButtonClassName, inputClassName, primaryButtonClassName } from "@/lib/styles";

type Props = {
  symbol: string;
  variant?: "default" | "compact";
  align?: "left" | "right";
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
  const [selectedId, setSelectedId] = useState("");
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [authGateOpen, setAuthGateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(false);
  const [added, setAdded] = useState(false);
  const [panelStyle, setPanelStyle] = useState<CSSProperties>({});
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [isPending, startTransition] = useTransition();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const normalizedSymbol = symbol.trim().toUpperCase();
  const searchParamsString = searchParams.toString();
  const returnTo = `${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`;

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setEntitlementsLoaded(false);
    getEntitlements()
      .then((next) => {
        if (cancelled) return;
        setEntitlements(next);
        setEntitlementsLoaded(true);
        if (!next.user) {
          setOpen(false);
          setAuthGateOpen(true);
        }
      })
      .catch(() => {
        if (cancelled) return;
        setEntitlements(defaultEntitlements);
        setEntitlementsLoaded(true);
        setOpen(false);
        setAuthGateOpen(true);
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  useEffect(() => {
    if (!open || loaded || !entitlementsLoaded || !entitlements.user) return;
    let cancelled = false;
    loadWatchlistsOnce()
      .then((items) => {
        if (cancelled) return;
        setWatchlists(items);
        setSelectedId((current) => current || (items[0] ? String(items[0].id) : ""));
        setLoaded(true);
      })
      .catch((err) => {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : "";
        if (message.includes("HTTP 401") || message.includes("HTTP 403")) {
          setOpen(false);
          setAuthGateOpen(true);
          return;
        }
        setStatus("Unable to load watchlists.");
      });
    return () => {
      cancelled = true;
    };
  }, [entitlements.user, entitlementsLoaded, loaded, open]);

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
      const rect = rootRef.current?.getBoundingClientRect();
      if (!rect) return;

      const margin = 16;
      const width = Math.min(352, window.innerWidth - margin * 2);
      const preferredLeft = align === "left" ? rect.left : rect.right - width;
      const left = Math.min(Math.max(margin, preferredLeft), window.innerWidth - width - margin);
      const belowSpace = window.innerHeight - rect.bottom - margin - 8;
      const aboveSpace = rect.top - margin - 8;

      if (belowSpace < 280 && aboveSpace > belowSpace) {
        setPanelStyle({
          bottom: window.innerHeight - rect.top + 8,
          left,
          maxHeight: Math.max(120, Math.min(420, aboveSpace)),
          width,
        });
        return;
      }

      setPanelStyle({
        left,
        maxHeight: Math.max(120, Math.min(420, belowSpace)),
        top: rect.bottom + 8,
        width,
      });
    };

    updatePanelPosition();
    window.addEventListener("resize", updatePanelPosition);
    window.addEventListener("scroll", updatePanelPosition, true);
    return () => {
      window.removeEventListener("resize", updatePanelPosition);
      window.removeEventListener("scroll", updatePanelPosition, true);
    };
  }, [align, open]);

  const selectedName = useMemo(
    () => watchlists.find((watchlist) => String(watchlist.id) === selectedId)?.name,
    [selectedId, watchlists],
  );

  const addSymbolToSelected = (watchlistId: number, watchlistName?: string) => {
    if (!Number.isFinite(watchlistId)) return;
    if (!hasEntitlement(entitlements, "watchlist_tickers")) {
      setStatus("Adding tickers to watchlists is currently a Premium feature.");
      return;
    }

    setStatus(null);
    startTransition(async () => {
      try {
        const result = await addToWatchlist(watchlistId, normalizedSymbol);
        setAdded(true);
        setStatus(
          result.status === "exists"
            ? `${normalizedSymbol} is already in ${watchlistName ?? "that watchlist"}.`
            : `${normalizedSymbol} added to ${watchlistName ?? "watchlist"}.`,
        );
      } catch (err) {
        setStatus(cleanWatchlistError(err));
      }
    });
  };

  const handleAddToSelected = () => {
    const id = Number(selectedId);
    addSymbolToSelected(id, selectedName);
  };

  const handleCreateAndAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = newWatchlistName.trim();
    if (!name) {
      setStatus("Name the new watchlist first.");
      return;
    }
    if (!hasEntitlement(entitlements, "watchlists")) {
      setStatus("Watchlist creation is currently a Premium feature.");
      return;
    }
    const limit = limitFor(entitlements, "watchlists");
    if (watchlists.length >= limit) {
      setStatus(`Free accounts can keep ${limit} watchlists. Upgrade to create more.`);
      return;
    }
    setStatus(null);
    startTransition(async () => {
      try {
        const created = await createWatchlist(name);
        await addToWatchlist(created.id, normalizedSymbol);
        rememberWatchlist(created);
        setWatchlists((current) => [...current, created]);
        setSelectedId(String(created.id));
        setNewWatchlistName("");
        setCreating(false);
        setAdded(true);
        setStatus(`${normalizedSymbol} added to ${created.name}.`);
      } catch (err) {
        setStatus(cleanWatchlistError(err));
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

  return (
    <div
      ref={rootRef}
      className="relative inline-flex shrink-0"
      data-row-action="true"
      onClick={(event) => event.stopPropagation()}
      onPointerDown={(event) => event.stopPropagation()}
    >
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          setOpen((current) => !current);
          setAuthGateOpen(false);
          setStatus(null);
        }}
        className={triggerClassName}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-label={`${added ? "Saved" : "Add"} ${normalizedSymbol} to watchlist`}
        title={`${added ? "Saved" : "Add"} ${normalizedSymbol} to watchlist`}
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

          {!entitlementsLoaded ? (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">
              Loading watchlists...
            </p>
          ) : watchlists.length > 0 ? (
            <div className="mt-4 space-y-2">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Existing lists</p>
              <div className="max-h-44 space-y-1 overflow-y-auto pr-1">
                {watchlists.map((watchlist) => {
                  const active = String(watchlist.id) === selectedId;
                  return (
                    <button
                      key={watchlist.id}
                      type="button"
                      onClick={() => setSelectedId(String(watchlist.id))}
                      className={`flex w-full items-center justify-between rounded-2xl border px-3 py-2 text-left text-sm transition ${
                        active
                          ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                          : "border-white/10 bg-white/[0.03] text-slate-200 hover:border-white/20 hover:bg-white/[0.06]"
                      }`}
                    >
                      <span className="min-w-0 truncate">{watchlist.name}</span>
                      {active ? <span className="text-xs text-emerald-200">Selected</span> : null}
                    </button>
                  );
                })}
              </div>
              <button
                type="button"
                onClick={handleAddToSelected}
                disabled={isPending || !selectedId}
                className={`${primaryButtonClassName} w-full rounded-xl py-2`}
              >
                {isPending ? "Adding..." : "Add to selected watchlist"}
              </button>
            </div>
          ) : (
            <p className="mt-4 rounded-2xl border border-dashed border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">
              No watchlists yet. Create one here and we&apos;ll add {normalizedSymbol} immediately.
            </p>
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
                    autoFocus={watchlists.length === 0}
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
