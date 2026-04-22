"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { NotificationPreferences } from "@/components/notifications/NotificationPreferences";
import { getEntitlements } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import {
  emptySavedViewsStore,
  markSavedViewSeen,
  parseSavedViewsStore,
  saveSavedViewsStore,
  scopedSavedViewSurfaceKey,
  savedViewsStorageKey,
  type SavedView,
  type SavedViewsStore,
  type SavedViewSurface,
} from "@/lib/savedViews";

type SavedViewsBarProps = {
  surface: SavedViewSurface;
  scopeKey?: string;
  paramKeys: readonly string[];
  defaultParams?: Record<string, string>;
  restoreOnLoad?: boolean;
  rightSlot?: ReactNode;
};

function viewId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `view-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function compactParams(searchParams: URLSearchParams, keys: readonly string[], defaults: Record<string, string>) {
  const params: Record<string, string> = {};

  keys.forEach((key) => {
    const value = (searchParams.get(key) ?? defaults[key] ?? "").trim();
    if (value) params[key] = value;
  });

  return params;
}

function paramsSignature(params: Record<string, string>) {
  return Object.entries(params)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

function defaultName(surface: SavedViewSurface, params: Record<string, string>) {
  if (surface === "feed") {
    const mode = params.mode || "all";
    const symbol = params.symbol ? `:${params.symbol.toUpperCase()}` : "";
    return `feed/${mode}${symbol}`;
  }

  if (surface === "watchlist") {
    const mode = params.mode || "all";
    const window = params.recent_days ? `${params.recent_days}d` : "30d";
    return `watchlist/${mode}/${window}`;
  }

  if (surface === "screener") {
    const sector = params.sector ? params.sector.toLowerCase().replace(/\s+/g, "-") : "all";
    const sort = params.sort || "relevance";
    return `screen/${sector}/${sort}`;
  }

  const mode = params.mode || "all";
  const side = params.side && params.side !== "all" ? `:${params.side}` : "";
  return `signals/${mode}${side}`;
}

function hasExplicitNonDefaultParams(searchParams: URLSearchParams, keys: readonly string[], defaults: Record<string, string>) {
  return keys.some((key) => {
    const value = (searchParams.get(key) ?? "").trim();
    if (!value) return false;
    return value !== (defaults[key] ?? "").trim();
  });
}

export function SavedViewsBar({
  surface,
  scopeKey,
  paramKeys,
  defaultParams = {},
  restoreOnLoad = false,
  rightSlot,
}: SavedViewsBarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const [store, setStore] = useState<SavedViewsStore>(emptySavedViewsStore);
  const [views, setViews] = useState<SavedView[]>([]);
  const [switcherOpen, setSwitcherOpen] = useState(false);
  const [actionsOpen, setActionsOpen] = useState(false);
  const [nameModalMode, setNameModalMode] = useState<"save" | "save-as" | "rename" | null>(null);
  const [nameValue, setNameValue] = useState("");
  const [nameError, setNameError] = useState<string | null>(null);
  const [renameTarget, setRenameTarget] = useState<SavedView | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<SavedView | null>(null);
  const [notifyTarget, setNotifyTarget] = useState<SavedView | null>(null);
  const [upgradeReason, setUpgradeReason] = useState<string | null>(null);
  const [authGateOpen, setAuthGateOpen] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [authResolved, setAuthResolved] = useState(false);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const restoreAttemptedRef = useRef(false);
  const surfaceKey = scopedSavedViewSurfaceKey(surface, scopeKey);
  const isLoggedIn = Boolean(entitlements.user);
  const viewNoun = surface === "screener" ? "screen" : "view";
  const viewNounPlural = surface === "screener" ? "screens" : "views";
  const savedLabel = surface === "screener" ? "Saved screens" : "Saved views";

  const currentParams = useMemo(() => {
    return compactParams(new URLSearchParams(searchParamsString), paramKeys, defaultParams);
  }, [defaultParams, paramKeys, searchParamsString]);

  const currentSignature = useMemo(() => paramsSignature(currentParams), [currentParams]);
  const surfaceViews = useMemo(() => {
    return views.filter((view) => view.surface === surface && scopedSavedViewSurfaceKey(view.surface, view.scopeKey) === surfaceKey);
  }, [surface, surfaceKey, views]);
  const defaultViewId = store.defaultViewIds[surfaceKey] ?? null;
  const exactMatchViewId = useMemo(() => {
    return surfaceViews.find((view) => paramsSignature(view.params) === currentSignature)?.id ?? null;
  }, [currentSignature, surfaceViews]);
  const selectedViewId = store.selectedViewIds[surfaceKey] ?? null;
  const activeView = useMemo(() => {
    return surfaceViews.find((view) => view.id === exactMatchViewId) ?? surfaceViews.find((view) => view.id === selectedViewId) ?? null;
  }, [exactMatchViewId, selectedViewId, surfaceViews]);
  const activeViewIsDirty = Boolean(activeView && paramsSignature(activeView.params) !== currentSignature);
  const sortedSurfaceViews = useMemo(() => {
    return [...surfaceViews].sort((a, b) => {
      const aTime = Date.parse(a.updatedAt || a.createdAt);
      const bTime = Date.parse(b.updatedAt || b.createdAt);
      return (Number.isFinite(bTime) ? bTime : 0) - (Number.isFinite(aTime) ? aTime : 0);
    });
  }, [surfaceViews]);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((nextEntitlements) => {
        if (cancelled) return;
        setEntitlements(nextEntitlements);
        setAuthResolved(true);
        if (nextEntitlements.user) {
          const nextStore = parseSavedViewsStore(window.localStorage.getItem(savedViewsStorageKey));
          setStore(nextStore);
          setViews(nextStore.views);
          return;
        }
        setStore(emptySavedViewsStore);
        setViews([]);
      })
      .catch(() => {
        if (cancelled) return;
        setEntitlements(defaultEntitlements);
        setAuthResolved(true);
        setStore(emptySavedViewsStore);
        setViews([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    setSwitcherOpen(false);
    setActionsOpen(false);
  }, [pathname, searchParamsString]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(null), 2600);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  useEffect(() => {
    if (!switcherOpen && !actionsOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSwitcherOpen(false);
        setActionsOpen(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [actionsOpen, switcherOpen]);

  const persistStore = (nextStore: SavedViewsStore) => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    setStore(nextStore);
    setViews(nextStore.views);
    saveSavedViewsStore(nextStore);
  };

  const persistViews = (nextViews: SavedView[]) => {
    const validIds = new Set(nextViews.map((view) => view.id));
    const nextDefaultViewIds = { ...store.defaultViewIds };
    const nextSelectedViewIds = { ...store.selectedViewIds };
    Object.keys(nextDefaultViewIds).forEach((key) => {
      if (nextDefaultViewIds[key] && !validIds.has(nextDefaultViewIds[key]!)) delete nextDefaultViewIds[key];
    });
    Object.keys(nextSelectedViewIds).forEach((key) => {
      if (nextSelectedViewIds[key] && !validIds.has(nextSelectedViewIds[key]!)) delete nextSelectedViewIds[key];
    });

    persistStore({
      version: 2,
      views: nextViews,
      defaultViewIds: nextDefaultViewIds,
      selectedViewIds: nextSelectedViewIds,
    });
  };

  const applyView = (view: SavedView, options?: { replace?: boolean }) => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    const params = new URLSearchParams(searchParamsString);
    paramKeys.forEach((key) => params.delete(key));
    params.delete("cursor");
    params.delete("cursor_stack");
    params.delete("page");
    params.delete("offset");
    if (surface === "signals") params.delete("preset");

    Object.entries(view.params).forEach(([key, value]) => {
      if (paramKeys.includes(key) && value.trim()) params.set(key, value.trim());
    });

    const nextSearch = params.toString();
    const nextStore = markSavedViewSeen(
      {
        ...store,
        selectedViewIds: { ...store.selectedViewIds, [surfaceKey]: view.id },
      },
      view.id,
    );
    persistStore(nextStore);

    const nextHref = `${pathname}${nextSearch ? `?${nextSearch}` : ""}`;
    if (options?.replace) {
      router.replace(nextHref, { scroll: false });
      return;
    }
    router.push(nextHref, { scroll: false });
    setToast(`${view.name} loaded.`);
  };

  useEffect(() => {
    if (!isLoggedIn || !restoreOnLoad || restoreAttemptedRef.current || views.length === 0) return;
    restoreAttemptedRef.current = true;

    const params = new URLSearchParams(searchParamsString);
    if (hasExplicitNonDefaultParams(params, paramKeys, defaultParams)) return;

    const targetId = store.defaultViewIds[surfaceKey] ?? store.selectedViewIds[surfaceKey];
    if (!targetId) return;

    const target = views.find(
      (view) => view.id === targetId && view.surface === surface && scopedSavedViewSurfaceKey(view.surface, view.scopeKey) === surfaceKey,
    );
    if (!target || paramsSignature(target.params) === currentSignature) return;
    applyView(target, { replace: true });
  }, [currentSignature, defaultParams, isLoggedIn, paramKeys, restoreOnLoad, searchParamsString, store, surface, surfaceKey, views]);

  const canCreateSavedView = () => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return false;
    }
    const savedViewLimit = limitFor(entitlements, "saved_views");
    if (!hasEntitlement(entitlements, "saved_views")) {
      setUpgradeReason("Saved views are currently a Premium feature.");
      return false;
    }
    if (views.length >= savedViewLimit) {
      setUpgradeReason(`Free accounts can keep ${savedViewLimit} saved views. Upgrade to save more research paths.`);
      return false;
    }
    return true;
  };

  const replaceUrlWithParams = (nextParams: Record<string, string>) => {
    const params = new URLSearchParams(searchParamsString);
    paramKeys.forEach((key) => params.delete(key));
    params.delete("cursor");
    params.delete("cursor_stack");
    params.delete("page");
    params.delete("offset");
    if (surface === "signals") params.delete("preset");
    Object.entries(nextParams).forEach(([key, value]) => {
      if (paramKeys.includes(key) && value.trim()) params.set(key, value.trim());
    });
    const nextSearch = params.toString();
    router.replace(`${pathname}${nextSearch ? `?${nextSearch}` : ""}`, { scroll: false });
  };

  const openSaveModal = (mode: "save" | "save-as" = "save") => {
    if (!canCreateSavedView()) return;

    const fallback = mode === "save-as" && activeView ? `${activeView.name} copy` : defaultName(surface, currentParams);
    setNameValue(fallback);
    setNameError(null);
    setRenameTarget(null);
    setSwitcherOpen(false);
    setActionsOpen(false);
    setNameModalMode(mode);
  };

  const saveCurrentView = (name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      setNameError("Enter a view name.");
      return;
    }

    const now = new Date().toISOString();
    const nextView: SavedView = {
      id: viewId(),
      surface,
      scopeKey,
      name: trimmed,
      params: currentParams,
      createdAt: now,
      updatedAt: now,
      lastSeenAt: now,
    };
    persistStore({
      version: 2,
      views: [...views, nextView],
      defaultViewIds: store.defaultViewIds,
      selectedViewIds: { ...store.selectedViewIds, [surfaceKey]: nextView.id },
    });
    setNameModalMode(null);
    setNameValue("");
    setToast(`${trimmed} saved.`);
    replaceUrlWithParams(nextView.params);
  };

  const updateView = (view: SavedView) => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    const now = new Date().toISOString();
    persistStore({
      ...store,
      views: views.map((item) =>
        item.id === view.id ? { ...item, params: currentParams, updatedAt: now, lastSeenAt: now } : item,
      ),
      selectedViewIds: { ...store.selectedViewIds, [surfaceKey]: view.id },
    });
    setActionsOpen(false);
    setToast(`${view.name} updated.`);
    replaceUrlWithParams(currentParams);
  };

  const openRenameModal = (view: SavedView) => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    setRenameTarget(view);
    setNameValue(view.name);
    setNameError(null);
    setSwitcherOpen(false);
    setActionsOpen(false);
    setNameModalMode("rename");
  };

  const renameView = (view: SavedView, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      setNameError("Enter a view name.");
      return;
    }
    const now = new Date().toISOString();
    persistViews(views.map((item) => (item.id === view.id ? { ...item, name: trimmed, updatedAt: now } : item)));
    setNameModalMode(null);
    setRenameTarget(null);
    setNameValue("");
    setToast(`${viewNoun === "screen" ? "Screen" : "View"} renamed.`);
  };

  const deleteView = (view: SavedView) => {
    persistViews(views.filter((item) => item.id !== view.id));
    setDeleteTarget(null);
    setActionsOpen(false);
    setToast(`${view.name} deleted.`);
  };

  const setDefaultView = (view: SavedView) => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    persistStore({
      ...store,
      defaultViewIds: { ...store.defaultViewIds, [surfaceKey]: view.id },
      selectedViewIds: { ...store.selectedViewIds, [surfaceKey]: view.id },
    });
    setSwitcherOpen(false);
    setActionsOpen(false);
    setToast(`${view.name} set as default.`);
  };

  const clearDefaultView = () => {
    if (!isLoggedIn) {
      setAuthGateOpen(true);
      return;
    }
    const nextDefaultViewIds = { ...store.defaultViewIds };
    delete nextDefaultViewIds[surfaceKey];
    persistStore({ ...store, defaultViewIds: nextDefaultViewIds });
    setSwitcherOpen(false);
    setActionsOpen(false);
    setToast("Default cleared.");
  };

  const closeNameModal = () => {
    setNameModalMode(null);
    setRenameTarget(null);
    setNameValue("");
    setNameError(null);
  };

  const onNameSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!isLoggedIn) {
      setNameModalMode(null);
      setAuthGateOpen(true);
      return;
    }
    if (nameModalMode === "save" || nameModalMode === "save-as") {
      saveCurrentView(nameValue);
      return;
    }
    if (nameModalMode === "rename" && renameTarget) {
      renameView(renameTarget, nameValue);
    }
  };

  return (
    <div className="border-t border-slate-800 pt-3 text-xs">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
          <span className="uppercase tracking-wide text-slate-500">{viewNounPlural}</span>
          {!authResolved ? (
            <span className="inline-flex h-8 items-center rounded-lg border border-slate-800 bg-slate-950/30 px-3 text-slate-500">
              loading {viewNounPlural}
            </span>
          ) : !isLoggedIn ? (
            <>
              <button
                type="button"
                onClick={() => setAuthGateOpen(true)}
                className="inline-flex h-8 items-center rounded-lg border border-white/10 bg-white/[0.03] px-3 font-medium text-slate-300 transition hover:border-emerald-400/40 hover:text-emerald-100"
              >
                Save {viewNoun}
              </button>
              <button
                type="button"
                onClick={() => setAuthGateOpen(true)}
                className="inline-flex h-8 items-center rounded-lg border border-slate-800 bg-slate-950/30 px-3 text-slate-400 transition hover:border-white/20 hover:text-slate-200"
              >
                Sign in to sync
              </button>
            </>
          ) : (
            <>
              <span className="relative inline-flex">
                <button
                  type="button"
                  onClick={() => {
                    setSwitcherOpen((current) => !current);
                    setActionsOpen(false);
                  }}
                  className="inline-flex h-8 min-w-[11rem] max-w-[18rem] items-center justify-between gap-3 rounded-lg border border-slate-700 bg-slate-950/50 px-3 font-medium text-slate-100 shadow-sm transition hover:border-emerald-400/40 hover:text-white"
                  aria-expanded={switcherOpen}
                  aria-haspopup="menu"
                >
                  <span className="truncate" title={activeView?.name ?? savedLabel}>
                    {activeView ? activeView.name : savedLabel}
                  </span>
                  <span className="text-slate-500">v</span>
                </button>
                {switcherOpen ? (
                  <div
                    className="absolute left-0 top-full z-30 mt-2 w-72 overflow-hidden rounded-xl border border-slate-700 bg-slate-950 shadow-2xl shadow-slate-950/60"
                    role="menu"
                  >
                    <div className="border-b border-slate-800 px-3 py-2">
                      <p className="font-semibold text-slate-200">{savedLabel}</p>
                      <p className="mt-0.5 text-[11px] text-slate-500">
                        {sortedSurfaceViews.length === 0
                          ? `No saved ${viewNounPlural} yet.`
                          : `${sortedSurfaceViews.length} saved ${sortedSurfaceViews.length === 1 ? viewNoun : viewNounPlural}.`}
                      </p>
                    </div>
                    {sortedSurfaceViews.length === 0 ? (
                      <div className="px-3 py-4 text-sm text-slate-400">Save this setup to reuse it later.</div>
                    ) : (
                      <div className="max-h-72 overflow-y-auto py-1">
                        {sortedSurfaceViews.map((view) => {
                          const isActive = activeView?.id === view.id;
                          const isExact = exactMatchViewId === view.id;
                          return (
                            <button
                              key={view.id}
                              type="button"
                              onClick={() => applyView(view)}
                              className={`flex w-full items-center justify-between gap-3 px-3 py-2 text-left transition ${
                                isActive ? "bg-emerald-400/10 text-emerald-100" : "text-slate-200 hover:bg-slate-900"
                              }`}
                              role="menuitem"
                            >
                              <span className="min-w-0">
                                <span className="block truncate font-medium">{view.name}</span>
                                <span className="mt-0.5 flex flex-wrap items-center gap-1.5 text-[11px] text-slate-500">
                                  {defaultViewId === view.id ? <span className="text-emerald-300/80">default</span> : null}
                                  {isActive && !isExact ? <span className="text-amber-200/90">edited</span> : null}
                                  {isExact ? <span className="text-emerald-300/80">current</span> : null}
                                </span>
                              </span>
                              {isActive ? <span className="text-emerald-300">*</span> : null}
                            </button>
                          );
                        })}
                      </div>
                    )}
                    <div className="border-t border-slate-800 p-2">
                      <button
                        type="button"
                        onClick={() => openSaveModal()}
                        className="flex w-full items-center justify-center rounded-lg border border-emerald-400/30 bg-emerald-400/10 px-3 py-2 font-semibold text-emerald-100 transition hover:bg-emerald-400/15"
                      >
                        Save {viewNoun}
                      </button>
                    </div>
                  </div>
                ) : null}
              </span>

              {activeView ? (
                <>
                  <span
                    className={`inline-flex h-8 max-w-[14rem] items-center gap-2 rounded-lg border px-3 font-medium ${
                      activeViewIsDirty
                        ? "border-amber-300/30 bg-amber-400/10 text-amber-100"
                        : "border-emerald-300/30 bg-emerald-400/10 text-emerald-100"
                    }`}
                    title={activeView.name}
                  >
                    <span className="truncate">{activeView.name}</span>
                    <span className="shrink-0 text-[10px] uppercase tracking-wide">{activeViewIsDirty ? "edited" : "saved"}</span>
                  </span>
                  <button
                    type="button"
                    onClick={() => updateView(activeView)}
                    className={`inline-flex h-8 items-center rounded-lg border px-3 font-semibold transition ${
                      activeViewIsDirty
                        ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100 hover:bg-emerald-300/20"
                        : "border-white/10 bg-white/[0.03] text-slate-300 hover:border-white/20 hover:text-white"
                    }`}
                  >
                    Update
                  </button>
                  <span className="relative inline-flex">
                    <button
                      type="button"
                      onClick={() => {
                        setActionsOpen((current) => !current);
                        setSwitcherOpen(false);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-slate-700 bg-slate-950/50 text-slate-300 transition hover:border-white/20 hover:text-white"
                      aria-label={`Manage ${activeView.name}`}
                      aria-expanded={actionsOpen}
                      aria-haspopup="menu"
                    >
                      ...
                    </button>
                    {actionsOpen ? (
                      <div
                        className="absolute right-0 top-full z-30 mt-2 w-48 overflow-hidden rounded-xl border border-slate-700 bg-slate-950 py-1 shadow-2xl shadow-slate-950/60"
                        role="menu"
                      >
                        <button
                          type="button"
                          onClick={() => openSaveModal("save-as")}
                          className="block w-full px-3 py-2 text-left text-slate-200 hover:bg-slate-900"
                          role="menuitem"
                        >
                          Save as new
                        </button>
                        <button
                          type="button"
                          onClick={() => openRenameModal(activeView)}
                          className="block w-full px-3 py-2 text-left text-slate-200 hover:bg-slate-900"
                          role="menuitem"
                        >
                          Rename
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setNotifyTarget(activeView);
                            setActionsOpen(false);
                          }}
                          className="block w-full px-3 py-2 text-left text-slate-200 hover:bg-slate-900"
                          role="menuitem"
                        >
                          Notify
                        </button>
                        {defaultViewId === activeView.id ? (
                          <button
                            type="button"
                            onClick={clearDefaultView}
                            className="block w-full px-3 py-2 text-left text-slate-200 hover:bg-slate-900"
                            role="menuitem"
                          >
                            Unset default
                          </button>
                        ) : (
                          <button
                            type="button"
                            onClick={() => setDefaultView(activeView)}
                            className="block w-full px-3 py-2 text-left text-slate-200 hover:bg-slate-900"
                            role="menuitem"
                          >
                            Make default
                          </button>
                        )}
                        <button
                          type="button"
                          onClick={() => {
                            setDeleteTarget(activeView);
                            setActionsOpen(false);
                          }}
                          className="block w-full border-t border-slate-800 px-3 py-2 text-left text-red-200 hover:bg-slate-900"
                          role="menuitem"
                        >
                          Delete
                        </button>
                      </div>
                    ) : null}
                  </span>
                </>
              ) : (
                <button
                  type="button"
                  onClick={() => openSaveModal()}
                  className="inline-flex h-8 items-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-3 font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
                >
                  Save {viewNoun}
                </button>
              )}
            </>
          )}
          {toast ? (
            <span className="inline-flex h-8 items-center rounded-lg border border-emerald-300/25 bg-emerald-400/10 px-3 text-emerald-100" role="status">
              {toast}
            </span>
          ) : null}
        </div>
        {rightSlot ? <div className="flex flex-wrap items-center justify-end gap-2">{rightSlot}</div> : null}
      </div>

      {nameModalMode ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <form onSubmit={onNameSubmit} className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 className="text-lg font-semibold">
              {nameModalMode === "rename" ? `Rename ${viewNoun}` : nameModalMode === "save-as" ? `Save as new ${viewNoun}` : `Save ${viewNoun}`}
            </h2>
            <label htmlFor={`saved-view-name-${surface}`} className="mt-3 block text-xs font-semibold uppercase tracking-wide text-slate-400">
              {viewNoun === "screen" ? "Screen" : "View"} name
            </label>
            <input
              id={`saved-view-name-${surface}`}
              value={nameValue}
              onChange={(event) => {
                setNameValue(event.target.value);
                setNameError(null);
              }}
              autoFocus
              className="mt-2 w-full rounded-full border border-white/10 bg-slate-950 px-4 py-2 text-sm text-slate-100"
            />
            {nameError ? <p className="mt-3 text-sm text-rose-300">{nameError}</p> : null}
            <div className="mt-6 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                className="rounded-full border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/30"
                onClick={closeNameModal}
              >
                Cancel
              </button>
              <button type="submit" className="rounded-full bg-emerald-500 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-400">
                {nameModalMode === "rename" ? `Rename ${viewNoun}` : nameModalMode === "save-as" ? `Save new ${viewNoun}` : `Save ${viewNoun}`}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {deleteTarget ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 className="text-lg font-semibold">Delete {viewNoun}?</h2>
            <p className="mt-2 text-sm text-slate-300">
              This will remove <span className="font-medium text-white">{deleteTarget.name}</span> from your saved {viewNounPlural}.
            </p>
            <div className="mt-6 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                className="rounded-full border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/30"
                onClick={() => setDeleteTarget(null)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-full bg-rose-500 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-400"
                onClick={() => deleteView(deleteTarget)}
              >
                Delete {viewNoun}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {notifyTarget ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold">Notify me</h2>
                <p className="mt-1 text-sm text-slate-400">{notifyTarget.name}</p>
              </div>
              <button
                type="button"
                className="rounded-lg border border-white/10 px-2 py-1 text-sm text-slate-300 hover:text-white"
                onClick={() => setNotifyTarget(null)}
              >
                Close
              </button>
            </div>
            <div className="mt-4">
              <NotificationPreferences
                sourceType="saved_view"
                sourceId={notifyTarget.id}
                sourceName={notifyTarget.name}
                sourcePayload={{
                  id: notifyTarget.id,
                  surface: notifyTarget.surface,
                  scopeKey: notifyTarget.scopeKey,
                  params: notifyTarget.params,
                  lastSeenAt: notifyTarget.lastSeenAt ?? null,
                }}
                compact={true}
              />
            </div>
          </div>
        </div>
      ) : null}

      {upgradeReason ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <h2 className="text-lg font-semibold">Saved view limit reached</h2>
              <button
                type="button"
                className="rounded-lg border border-white/10 px-2 py-1 text-sm text-slate-300 hover:text-white"
                onClick={() => setUpgradeReason(null)}
              >
                Close
              </button>
            </div>
            <div className="mt-4">
              <UpgradePrompt title="Save more views with Premium" body={upgradeReason} compact={true} />
            </div>
          </div>
        </div>
      ) : null}

      {authGateOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">{savedLabel}</p>
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
              Create a free account to save {viewNounPlural} and sync your research setup.
            </p>
            <div className="mt-5 flex flex-wrap justify-end gap-3">
              <Link
                href={`/login?return_to=${encodeURIComponent(`${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`)}`}
                className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
              >
                Login
              </Link>
              <Link
                href={`/login?return_to=${encodeURIComponent(`${pathname}${searchParamsString ? `?${searchParamsString}` : ""}`)}`}
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
