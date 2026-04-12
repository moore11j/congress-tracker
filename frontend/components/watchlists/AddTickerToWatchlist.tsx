"use client";

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

export function AddTickerToWatchlist({ symbol, variant = "default", align = "right" }: Props) {
  const [watchlists, setWatchlists] = useState<WatchlistSummary[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [panelStyle, setPanelStyle] = useState<CSSProperties>({});
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [isPending, startTransition] = useTransition();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const normalizedSymbol = symbol.trim().toUpperCase();

  useEffect(() => {
    if (!open || loaded) return;
    let cancelled = false;
    loadWatchlistsOnce()
      .then((items) => {
        if (cancelled) return;
        setWatchlists(items);
        setSelectedId((current) => current || (items[0] ? String(items[0].id) : ""));
        setLoaded(true);
      })
      .catch(() => {
        if (!cancelled) setStatus("Unable to load watchlists.");
      });
    return () => {
      cancelled = true;
    };
  }, [loaded, open]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

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
        await addToWatchlist(watchlistId, normalizedSymbol);
        setStatus(`${normalizedSymbol} added to ${watchlistName ?? "watchlist"}.`);
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
        setStatus(`${normalizedSymbol} added to ${created.name}.`);
      } catch (err) {
        setStatus(cleanWatchlistError(err));
      }
    });
  };

  const triggerClassName =
    variant === "compact"
      ? "inline-flex h-8 w-8 items-center justify-center rounded-full border border-white/10 bg-slate-950/50 text-sm font-semibold text-slate-300 shadow-sm transition hover:border-emerald-300/40 hover:bg-emerald-300/10 hover:text-emerald-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30"
      : `${ghostButtonClassName} rounded-xl py-1.5`;

  return (
    <div ref={rootRef} className="relative inline-flex">
      <button
        type="button"
        onClick={() => {
          setOpen((current) => !current);
          setStatus(null);
        }}
        className={triggerClassName}
        aria-haspopup="dialog"
        aria-expanded={open}
        title={`Add ${normalizedSymbol} to watchlist`}
      >
        {variant === "compact" ? "+" : "Add to watchlist"}
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

          {watchlists.length > 0 ? (
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
    </div>
  );
}
