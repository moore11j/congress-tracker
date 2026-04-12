"use client";

import { useEffect, useMemo, useState } from "react";
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
  version: 1;
  views: SavedView[];
};

type SavedViewsBarProps = {
  surface: SavedViewSurface;
  paramKeys: readonly string[];
  defaultParams?: Record<string, string>;
};

const storageKey = "ct:savedViews:v1";

function emptyStore(): SavedViewsStore {
  return { version: 1, views: [] };
}

function parseStore(rawValue: string | null): SavedViewsStore {
  if (!rawValue) return emptyStore();

  try {
    const parsed = JSON.parse(rawValue) as Partial<SavedViewsStore>;
    if (!parsed || parsed.version !== 1 || !Array.isArray(parsed.views)) return emptyStore();

    return {
      version: 1,
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

export function SavedViewsBar({ surface, paramKeys, defaultParams = {} }: SavedViewsBarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const [views, setViews] = useState<SavedView[]>([]);
  const [activeMenuId, setActiveMenuId] = useState<string | null>(null);

  const currentParams = useMemo(() => {
    return compactParams(new URLSearchParams(searchParamsString), paramKeys, defaultParams);
  }, [defaultParams, paramKeys, searchParamsString]);

  const currentSignature = useMemo(() => paramsSignature(currentParams), [currentParams]);
  const surfaceViews = useMemo(() => views.filter((view) => view.surface === surface), [surface, views]);
  const activeViewId = useMemo(() => {
    return surfaceViews.find((view) => paramsSignature(view.params) === currentSignature)?.id ?? null;
  }, [currentSignature, surfaceViews]);

  useEffect(() => {
    setViews(parseStore(window.localStorage.getItem(storageKey)).views);
  }, []);

  useEffect(() => {
    setActiveMenuId(null);
  }, [pathname, searchParamsString]);

  const persist = (nextViews: SavedView[]) => {
    setViews(nextViews);
    saveStore({ version: 1, views: nextViews });
  };

  const applyView = (view: SavedView) => {
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
    router.push(`${pathname}${nextSearch ? `?${nextSearch}` : ""}`, { scroll: false });
  };

  const saveCurrentView = () => {
    const fallback = defaultName(surface, currentParams);
    const name = window.prompt("Save view as", fallback)?.trim();
    if (!name) return;

    const now = new Date().toISOString();
    const nextView: SavedView = {
      id: viewId(),
      surface,
      name,
      params: currentParams,
      createdAt: now,
      updatedAt: now,
    };
    persist([...views, nextView]);
  };

  const renameView = (view: SavedView) => {
    const name = window.prompt("Rename view", view.name)?.trim();
    if (!name) return;
    const now = new Date().toISOString();
    persist(views.map((item) => (item.id === view.id ? { ...item, name, updatedAt: now } : item)));
  };

  const deleteView = (view: SavedView) => {
    if (!window.confirm(`Delete "${view.name}"?`)) return;
    persist(views.filter((item) => item.id !== view.id));
  };

  return (
    <div className="flex flex-wrap items-center gap-2 border-t border-slate-800 pt-3 text-xs">
      <span className="font-mono uppercase tracking-wide text-slate-500">views</span>
      <button
        type="button"
        onClick={saveCurrentView}
        className="inline-flex h-7 items-center rounded border border-slate-700 bg-slate-950/40 px-2 font-mono text-slate-200 transition hover:border-emerald-500/40 hover:text-emerald-100"
      >
        save
      </button>
      {surfaceViews.length === 0 ? (
        <span className="font-mono text-slate-500">none saved</span>
      ) : (
        surfaceViews.map((view) => (
          <span key={view.id} className="relative inline-flex items-center">
            <button
              type="button"
              onClick={() => applyView(view)}
              className={`inline-flex h-7 max-w-[12rem] items-center truncate rounded-l border px-2 font-mono transition ${
                activeViewId === view.id
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-100"
                  : "border-slate-700 bg-slate-950/40 text-slate-200 hover:border-slate-600 hover:text-white"
              }`}
              title={view.name}
            >
              {view.name}
            </button>
            <button
              type="button"
              onClick={() => setActiveMenuId((current) => (current === view.id ? null : view.id))}
              className={`inline-flex h-7 items-center rounded-r border border-l-0 px-2 font-mono transition ${
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
                  onClick={() => renameView(view)}
                  className="px-3 py-2 font-mono text-slate-200 hover:bg-slate-900"
                >
                  rename
                </button>
                <button
                  type="button"
                  onClick={() => deleteView(view)}
                  className="px-3 py-2 font-mono text-red-200 hover:bg-slate-900"
                >
                  delete
                </button>
              </span>
            ) : null}
          </span>
        ))
      )}
    </div>
  );
}
