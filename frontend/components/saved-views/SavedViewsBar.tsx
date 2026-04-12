"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

type SavedViewSurface = "feed" | "signals";

type SavedView = {
  id: string;
  surface: SavedViewSurface;
  name: string;
  params: Record<string, string>;
  createdAt: string;
  updatedAt: string;
};

type SavedViewsStore = {
  version: 2;
  views: SavedView[];
  defaultViewIds: Partial<Record<SavedViewSurface, string>>;
  selectedViewIds: Partial<Record<SavedViewSurface, string>>;
};

type SavedViewsBarProps = {
  surface: SavedViewSurface;
  paramKeys: readonly string[];
  defaultParams?: Record<string, string>;
  restoreOnLoad?: boolean;
  rightSlot?: ReactNode;
};

const storageKey = "ct:savedViews:v1";

function emptyStore(): SavedViewsStore {
  return { version: 2, views: [], defaultViewIds: {}, selectedViewIds: {} };
}

function parseStore(rawValue: string | null): SavedViewsStore {
  if (!rawValue) return emptyStore();

  try {
    const parsed = JSON.parse(rawValue) as Partial<SavedViewsStore> & { version?: 1 | 2 };
    if (!parsed || !Array.isArray(parsed.views)) return emptyStore();

    return {
      version: 2,
      views: parsed.views.filter((view): view is SavedView => {
        return (
          !!view &&
          typeof view.id === "string" &&
          (view.surface === "feed" || view.surface === "signals") &&
          typeof view.name === "string" &&
          !!view.params &&
          typeof view.params === "object"
        );
      }),
      defaultViewIds: parsed.version === 2 && parsed.defaultViewIds ? parsed.defaultViewIds : {},
      selectedViewIds: parsed.version === 2 && parsed.selectedViewIds ? parsed.selectedViewIds : {},
    };
  } catch {
    return emptyStore();
  }
}

function saveStore(store: SavedViewsStore) {
  window.localStorage.setItem(storageKey, JSON.stringify(store));
}

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
  paramKeys,
  defaultParams = {},
  restoreOnLoad = false,
  rightSlot,
}: SavedViewsBarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const [store, setStore] = useState<SavedViewsStore>(emptyStore);
  const [views, setViews] = useState<SavedView[]>([]);
  const [activeMenuId, setActiveMenuId] = useState<string | null>(null);
  const [nameModalMode, setNameModalMode] = useState<"save" | "rename" | null>(null);
  const [nameValue, setNameValue] = useState("");
  const [nameError, setNameError] = useState<string | null>(null);
  const [renameTarget, setRenameTarget] = useState<SavedView | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<SavedView | null>(null);
  const restoreAttemptedRef = useRef(false);

  const currentParams = useMemo(() => {
    return compactParams(new URLSearchParams(searchParamsString), paramKeys, defaultParams);
  }, [defaultParams, paramKeys, searchParamsString]);

  const currentSignature = useMemo(() => paramsSignature(currentParams), [currentParams]);
  const surfaceViews = useMemo(() => views.filter((view) => view.surface === surface), [surface, views]);
  const defaultViewId = store.defaultViewIds[surface] ?? null;
  const activeViewId = useMemo(() => {
    return surfaceViews.find((view) => paramsSignature(view.params) === currentSignature)?.id ?? null;
  }, [currentSignature, surfaceViews]);

  useEffect(() => {
    const nextStore = parseStore(window.localStorage.getItem(storageKey));
    setStore(nextStore);
    setViews(nextStore.views);
  }, []);

  useEffect(() => {
    setActiveMenuId(null);
  }, [pathname, searchParamsString]);

  const persistStore = (nextStore: SavedViewsStore) => {
    setStore(nextStore);
    setViews(nextStore.views);
    saveStore(nextStore);
  };

  const persistViews = (nextViews: SavedView[]) => {
    const validIds = new Set(nextViews.map((view) => view.id));
    const nextDefaultViewIds = { ...store.defaultViewIds };
    const nextSelectedViewIds = { ...store.selectedViewIds };
    (["feed", "signals"] as const).forEach((key) => {
      if (nextDefaultViewIds[key] && !validIds.has(nextDefaultViewIds[key]!)) delete nextDefaultViewIds[key];
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
    const nextStore = {
      ...store,
      selectedViewIds: { ...store.selectedViewIds, [surface]: view.id },
    };
    persistStore(nextStore);

    const nextHref = `${pathname}${nextSearch ? `?${nextSearch}` : ""}`;
    if (options?.replace) {
      router.replace(nextHref, { scroll: false });
      return;
    }
    router.push(nextHref, { scroll: false });
  };

  useEffect(() => {
    if (!restoreOnLoad || restoreAttemptedRef.current || views.length === 0) return;
    restoreAttemptedRef.current = true;

    const params = new URLSearchParams(searchParamsString);
    if (hasExplicitNonDefaultParams(params, paramKeys, defaultParams)) return;

    const targetId = store.defaultViewIds[surface] ?? store.selectedViewIds[surface];
    if (!targetId) return;

    const target = views.find((view) => view.id === targetId && view.surface === surface);
    if (!target || paramsSignature(target.params) === currentSignature) return;
    applyView(target, { replace: true });
  }, [currentSignature, defaultParams, paramKeys, restoreOnLoad, searchParamsString, store, surface, views]);

  const openSaveModal = () => {
    const fallback = defaultName(surface, currentParams);
    setNameValue(fallback);
    setNameError(null);
    setRenameTarget(null);
    setNameModalMode("save");
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
      name: trimmed,
      params: currentParams,
      createdAt: now,
      updatedAt: now,
    };
    persistStore({
      version: 2,
      views: [...views, nextView],
      defaultViewIds: store.defaultViewIds,
      selectedViewIds: { ...store.selectedViewIds, [surface]: nextView.id },
    });
    setNameModalMode(null);
    setNameValue("");

    const params = new URLSearchParams(searchParamsString);
    paramKeys.forEach((key) => params.delete(key));
    params.delete("cursor");
    params.delete("cursor_stack");
    params.delete("page");
    params.delete("offset");
    if (surface === "signals") params.delete("preset");
    Object.entries(nextView.params).forEach(([key, value]) => {
      if (paramKeys.includes(key) && value.trim()) params.set(key, value.trim());
    });
    const nextSearch = params.toString();
    router.replace(`${pathname}${nextSearch ? `?${nextSearch}` : ""}`, { scroll: false });
  };

  const openRenameModal = (view: SavedView) => {
    setRenameTarget(view);
    setNameValue(view.name);
    setNameError(null);
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
  };

  const deleteView = (view: SavedView) => {
    persistViews(views.filter((item) => item.id !== view.id));
    setDeleteTarget(null);
  };

  const setDefaultView = (view: SavedView) => {
    persistStore({
      ...store,
      defaultViewIds: { ...store.defaultViewIds, [surface]: view.id },
      selectedViewIds: { ...store.selectedViewIds, [surface]: view.id },
    });
    setActiveMenuId(null);
  };

  const clearDefaultView = () => {
    const nextDefaultViewIds = { ...store.defaultViewIds };
    delete nextDefaultViewIds[surface];
    persistStore({ ...store, defaultViewIds: nextDefaultViewIds });
    setActiveMenuId(null);
  };

  const closeNameModal = () => {
    setNameModalMode(null);
    setRenameTarget(null);
    setNameValue("");
    setNameError(null);
  };

  const onNameSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (nameModalMode === "save") {
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
        <div className="flex flex-wrap items-center gap-2">
          <span className="uppercase tracking-wide text-slate-500">views</span>
          <button
            type="button"
            onClick={openSaveModal}
            className="inline-flex h-7 items-center rounded border border-slate-700 bg-slate-950/40 px-2 text-slate-200 transition hover:border-emerald-500/40 hover:text-emerald-100"
          >
            save
          </button>
          {surfaceViews.length === 0 ? (
            <span className="text-slate-500">none saved</span>
          ) : (
            surfaceViews.map((view) => (
              <span key={view.id} className="relative inline-flex items-center">
                <button
                  type="button"
                  onClick={() => applyView(view)}
                  className={`inline-flex h-7 max-w-[12rem] items-center truncate rounded-l border px-2 transition ${
                    activeViewId === view.id
                      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-100"
                      : "border-slate-700 bg-slate-950/40 text-slate-200 hover:border-slate-600 hover:text-white"
                  }`}
                  title={view.name}
                >
                  {view.name}
                  {defaultViewId === view.id ? <span className="ml-1 text-emerald-300/70">default</span> : null}
                </button>
                <button
                  type="button"
                  onClick={() => setActiveMenuId((current) => (current === view.id ? null : view.id))}
                  className={`inline-flex h-7 items-center rounded-r border border-l-0 px-2 transition ${
                    activeViewId === view.id
                      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-100"
                      : "border-slate-700 bg-slate-950/40 text-slate-400 hover:text-white"
                  }`}
                  aria-label={`Manage saved view ${view.name}`}
                >
                  ...
                </button>
                {activeMenuId === view.id ? (
                  <span className="absolute left-0 top-full z-30 mt-1 inline-flex overflow-hidden rounded border border-slate-700 bg-slate-950 shadow-xl">
                    <button
                      type="button"
                      onClick={() => openRenameModal(view)}
                      className="px-3 py-2 text-slate-200 hover:bg-slate-900"
                    >
                      rename
                    </button>
                    {defaultViewId === view.id ? (
                      <button
                        type="button"
                        onClick={clearDefaultView}
                        className="px-3 py-2 text-slate-200 hover:bg-slate-900"
                      >
                        unset default
                      </button>
                    ) : (
                      <button
                        type="button"
                        onClick={() => setDefaultView(view)}
                        className="px-3 py-2 text-slate-200 hover:bg-slate-900"
                      >
                        make default
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={() => setDeleteTarget(view)}
                      className="px-3 py-2 text-red-200 hover:bg-slate-900"
                    >
                      delete
                    </button>
                  </span>
                ) : null}
              </span>
            ))
          )}
        </div>
        {rightSlot ? <div className="flex flex-wrap items-center justify-end gap-2">{rightSlot}</div> : null}
      </div>

      {nameModalMode ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <form onSubmit={onNameSubmit} className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 className="text-lg font-semibold">{nameModalMode === "save" ? "Save view" : "Rename view"}</h2>
            <label htmlFor={`saved-view-name-${surface}`} className="mt-3 block text-xs font-semibold uppercase tracking-wide text-slate-400">
              View name
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
                {nameModalMode === "save" ? "Save view" : "Rename view"}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {deleteTarget ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 className="text-lg font-semibold">Delete view?</h2>
            <p className="mt-2 text-sm text-slate-300">
              This will remove <span className="font-medium text-white">{deleteTarget.name}</span> from your saved views.
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
                Delete view
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
