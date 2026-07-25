"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState, useTransition } from "react";
import { WalnutModal } from "@/components/ui/WalnutModal";
import { addWatchlistTarget, createWatchlist, getEntitlements, listWatchlists } from "@/lib/api";
import { formatInteger } from "@/lib/accountDisplay";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { ghostButtonClassName, inputClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { WatchlistSummary, WatchlistTargetType } from "@/lib/types";

type Props = {
  targetType: WatchlistTargetType;
  targetValue: string;
  targetLabel: string;
  buttonLabel: string;
  className?: string;
};

type WatchlistToast = {
  message: string;
  tone: "success" | "error" | "info";
};

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

function normalizeTargetValue(value: string) {
  return value.trim().toUpperCase();
}

function watchlistHasTarget(watchlist: WatchlistSummary, targetType: WatchlistTargetType, targetValue: string) {
  const normalized = normalizeTargetValue(targetValue);
  return (watchlist.targets ?? []).some(
    (target) => target.type === targetType && normalizeTargetValue(String(target.value ?? "")) === normalized,
  );
}

function withTargetInWatchlist(watchlist: WatchlistSummary, targetType: WatchlistTargetType, targetValue: string, targetLabel: string): WatchlistSummary {
  if (watchlistHasTarget(watchlist, targetType, targetValue)) return watchlist;
  return {
    ...watchlist,
    targets: [...(watchlist.targets ?? []), { type: targetType, value: targetValue, label: targetLabel }],
  };
}

function rememberTarget(watchlistId: number, targetType: WatchlistTargetType, targetValue: string, targetLabel: string) {
  if (!watchlistsCache) return;
  watchlistsCache = watchlistsCache.map((watchlist) =>
    watchlist.id === watchlistId ? withTargetInWatchlist(watchlist, targetType, targetValue, targetLabel) : watchlist,
  );
  watchlistsCacheAt = Date.now();
}

function cleanWatchlistError(err: unknown) {
  const message = err instanceof Error ? err.message : "";
  if (message.includes("premium_required") || message.includes("Free accounts") || message.includes("Free watchlists")) {
    return "That Premium limit is active. Open Account to compare plans.";
  }
  if (message.includes("Watchlist name already exists") || message.includes("HTTP 409")) {
    return "That watchlist name already exists.";
  }
  if (message.includes("HTTP 422")) {
    return "Enter a valid watchlist name.";
  }
  return "Unable to update watchlists right now.";
}

function isAuthError(err: unknown) {
  const message = err instanceof Error ? err.message : "";
  return message.includes("HTTP 401") || message.includes("HTTP 403");
}

function toastTone(message: string): WatchlistToast["tone"] {
  const lower = message.toLowerCase();
  if (lower.includes("already")) return "info";
  if (lower.includes("unable") || lower.includes("premium") || lower.includes("valid")) return "error";
  return "success";
}

function targetNoun(type: WatchlistTargetType) {
  if (type === "member") return "member";
  if (type === "insider") return "insider";
  if (type === "department") return "department";
  return "institution";
}

function targetLimitFeature(type: WatchlistTargetType): "watchlist_institutions" | "watchlist_people_departments" {
  return type === "institution" ? "watchlist_institutions" : "watchlist_people_departments";
}

function targetGateMessage(type: WatchlistTargetType) {
  if (type === "institution") return "Following institutions in watchlists is currently a Pro feature.";
  return "Following members, insiders, and departments in watchlists is currently a Premium feature.";
}

export function AddWatchlistTarget({ targetType, targetValue, targetLabel, buttonLabel, className }: Props) {
  const router = useRouter();
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
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [added, setAdded] = useState(false);
  const [addingWatchlistId, setAddingWatchlistId] = useState<number | null>(null);
  const [isPending, startTransition] = useTransition();
  const inputRef = useRef<HTMLInputElement | null>(null);
  const searchParamsString = searchParams.toString();
  const returnTo = `${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`;
  const cleanValue = targetValue.trim();
  const cleanLabel = targetLabel.trim() || cleanValue;
  const noun = targetNoun(targetType);

  const showToast = useCallback((message: string, tone: WatchlistToast["tone"] = toastTone(message)) => {
    setToast({ message, tone });
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timeoutId = window.setTimeout(() => setToast(null), 4200);
    return () => window.clearTimeout(timeoutId);
  }, [toast]);

  const addTargetToWatchlist = useCallback((watchlistId: number, watchlistName?: string) => {
    if (!cleanValue) {
      showToast(`No ${noun} available to follow.`, "error");
      return;
    }
    if (!hasEntitlement(entitlements, targetLimitFeature(targetType))) {
      const message = targetGateMessage(targetType);
      setStatus(message);
      showToast(message, "error");
      return;
    }

    setStatus(null);
    setAddingWatchlistId(watchlistId);
    startTransition(async () => {
      try {
        const result = await addWatchlistTarget(watchlistId, { type: targetType, value: cleanValue, label: cleanLabel });
        const savedValue = result.target?.value ?? cleanValue;
        const savedLabel = result.target?.label ?? cleanLabel;
        setWatchlists((current) =>
          current.map((watchlist) => (watchlist.id === watchlistId ? withTargetInWatchlist(watchlist, targetType, savedValue, savedLabel) : watchlist)),
        );
        rememberTarget(watchlistId, targetType, savedValue, savedLabel);
        setAdded(true);
        const message =
          result.status === "exists"
            ? `${cleanLabel} is already in ${watchlistName ?? "that watchlist"}.`
            : `Added ${cleanLabel} to ${watchlistName ?? "watchlist"}.`;
        setStatus(message);
        showToast(message);
        router.push(`/watchlists/${watchlistId}`);
      } catch (err) {
        const message = cleanWatchlistError(err);
        setStatus(message);
        showToast(message, "error");
      } finally {
        setAddingWatchlistId(null);
      }
    });
  }, [cleanLabel, cleanValue, entitlements, noun, router, showToast, targetType]);

  const handleTriggerClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (open) {
      setOpen(false);
      return;
    }
    setLoaded(false);
    setAddingWatchlistId(null);
    setAuthGateOpen(false);
    setStatus(null);
    setCreating(false);
    startTransition(async () => {
      try {
        const nextEntitlements = await getEntitlements();
        setEntitlements(nextEntitlements);
        if (!nextEntitlements.user) {
          setAuthGateOpen(true);
          return;
        }
        const items = await loadWatchlistsOnce();
        setWatchlists(items);
        setLoaded(true);
        setAdded(items.some((watchlist) => watchlistHasTarget(watchlist, targetType, cleanValue)));
        setOpen(true);
      } catch (err) {
        if (isAuthError(err)) {
          setAuthGateOpen(true);
          return;
        }
        const message = "Could not open watchlists. Please try again.";
        setStatus(message);
        showToast(message, "error");
      }
    });
  };

  const handleCreateAndAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = newWatchlistName.trim();
    if (!name) {
      showToast("Name the new watchlist first.", "error");
      return;
    }
    if (!hasEntitlement(entitlements, "watchlists")) {
      showToast("Watchlist creation is currently a Premium feature.", "error");
      return;
    }
    const limit = limitFor(entitlements, "watchlists");
    if (watchlists.length >= limit) {
      showToast(`Free accounts can keep ${formatInteger(limit)} watchlists. Upgrade to create more.`, "error");
      return;
    }
    startTransition(async () => {
      try {
        const created = await createWatchlist(name);
        const result = await addWatchlistTarget(created.id, { type: targetType, value: cleanValue, label: cleanLabel });
        const nextWatchlist = withTargetInWatchlist(created, targetType, result.target?.value ?? cleanValue, result.target?.label ?? cleanLabel);
        watchlistsCache = [...(watchlistsCache ?? []), nextWatchlist];
        watchlistsCacheAt = Date.now();
        setWatchlists((current) => [...current, nextWatchlist]);
        setNewWatchlistName("");
        setCreating(false);
        setAdded(true);
        showToast(`Added ${cleanLabel} to ${created.name}.`);
        router.push(`/watchlists/${created.id}`);
      } catch (err) {
        showToast(cleanWatchlistError(err), "error");
      }
    });
  };

  const toastToneClassName =
    toast?.tone === "error"
      ? "border-rose-300/45 text-rose-100"
      : toast?.tone === "info"
        ? "border-slate-300/30 text-slate-100"
        : "border-emerald-300/40 text-emerald-100";

  return (
    <span className="relative inline-flex shrink-0" onClick={(event) => event.stopPropagation()}>
      <button type="button" onClick={handleTriggerClick} className={className ?? ghostButtonClassName} aria-haspopup="dialog" aria-expanded={open}>
        {added ? "Following" : buttonLabel}
      </button>
      <WalnutModal
        open={open}
        title={`Follow ${cleanLabel}`}
        eyebrow="Watchlists"
        tone="success"
        onClose={() => setOpen(false)}
        closeLabel="Close watchlist picker"
        isBusy={addingWatchlistId !== null || isPending}
        initialFocusRef={creating ? inputRef : undefined}
      >
        {!loaded ? (
          <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">Loading watchlists...</p>
        ) : watchlists.length > 0 ? (
          <div className="mt-4 space-y-2">
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Existing lists</p>
            <div className="max-h-44 space-y-1 overflow-y-auto pr-1">
              {watchlists.map((watchlist) => {
                const isInWatchlist = watchlistHasTarget(watchlist, targetType, cleanValue);
                const isAdding = addingWatchlistId === watchlist.id;
                return (
                  <button
                    key={watchlist.id}
                    type="button"
                    onClick={() => (isInWatchlist ? router.push(`/watchlists/${watchlist.id}`) : addTargetToWatchlist(watchlist.id, watchlist.name))}
                    disabled={addingWatchlistId !== null}
                    className={`flex w-full items-center justify-between rounded-2xl border px-3 py-2 text-left text-sm transition ${
                      isInWatchlist
                        ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                        : "border-white/10 bg-white/[0.03] text-slate-200 hover:border-white/20 hover:bg-white/[0.06]"
                    }`}
                  >
                    <span className="min-w-0 truncate">{watchlist.name}</span>
                    <span className={`ml-3 shrink-0 text-xs font-semibold ${isInWatchlist ? "text-emerald-200" : "text-slate-300"}`}>
                      {isAdding ? "Adding..." : isInWatchlist ? "View" : "Add"}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        ) : (
          <p className="mt-4 rounded-2xl border border-dashed border-white/10 bg-white/[0.03] p-3 text-sm text-slate-400">
            No watchlist found. Create one below.
          </p>
        )}

        <div className="mt-4 border-t border-white/10 pt-4">
          {creating || watchlists.length === 0 ? (
            <form onSubmit={handleCreateAndAdd} className="space-y-2">
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
                New watchlist
                <input
                  ref={inputRef}
                  value={newWatchlistName}
                  onChange={(event) => setNewWatchlistName(event.target.value)}
                  className={`${inputClassName} rounded-xl`}
                  placeholder="e.g. Active insiders"
                  disabled={isPending}
                />
              </label>
              <div className="flex gap-2">
                <button type="submit" disabled={isPending} className={`${subtlePrimaryButtonClassName} flex-1`}>
                  {isPending ? "Creating..." : "Create and add"}
                </button>
                {watchlists.length > 0 ? (
                  <button type="button" onClick={() => setCreating(false)} className={`${ghostButtonClassName} rounded-xl py-2`} disabled={isPending}>
                    Cancel
                  </button>
                ) : null}
              </div>
            </form>
          ) : (
            <button type="button" onClick={() => setCreating(true)} className="text-sm font-semibold text-emerald-200 transition hover:text-emerald-100">
              Create a new watchlist
            </button>
          )}
        </div>
        {status ? <p className="mt-3 text-xs text-slate-400">{status}</p> : null}
      </WalnutModal>
      {toast ? (
        <div className="pointer-events-none fixed inset-x-3 top-4 z-[100] flex justify-center sm:inset-x-auto sm:right-4 sm:justify-end">
          <div role={toast.tone === "error" ? "alert" : "status"} className={`pointer-events-auto w-full max-w-md rounded-lg border bg-slate-950 px-4 py-3 font-mono text-sm ${toastToneClassName}`}>
            {toast.message}
          </div>
        </div>
      ) : null}
      <WalnutModal
        open={authGateOpen}
        title="Create a free account"
        eyebrow="Watchlists"
        tone="success"
        onClose={() => setAuthGateOpen(false)}
        closeLabel="Close account prompt"
        description={`Create a free account or log in to follow this ${noun} in watchlists.`}
        footer={
          <>
            <Link href={`/login?return_to=${encodeURIComponent(returnTo)}`} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
              Login
            </Link>
            <Link href={`/login?return_to=${encodeURIComponent(returnTo)}`} className="rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20">
              Create account
            </Link>
          </>
        }
      />
    </span>
  );
}
