"use client";

export type SavedViewSurface = "feed" | "signals" | "watchlist";

export type SavedView = {
  id: string;
  surface: SavedViewSurface;
  scopeKey?: string;
  name: string;
  params: Record<string, string>;
  createdAt: string;
  updatedAt: string;
  lastSeenAt?: string | null;
};

export type SavedViewsStore = {
  version: 2;
  views: SavedView[];
  defaultViewIds: Partial<Record<string, string>>;
  selectedViewIds: Partial<Record<string, string>>;
};

export const savedViewsStorageKey = "ct:savedViews:v1";

export function emptySavedViewsStore(): SavedViewsStore {
  return { version: 2, views: [], defaultViewIds: {}, selectedViewIds: {} };
}

export function parseSavedViewsStore(rawValue: string | null): SavedViewsStore {
  if (!rawValue) return emptySavedViewsStore();

  try {
    const parsed = JSON.parse(rawValue) as Partial<SavedViewsStore> & { version?: 1 | 2 };
    if (!parsed || !Array.isArray(parsed.views)) return emptySavedViewsStore();

    return {
      version: 2,
      views: parsed.views.filter((view): view is SavedView => {
        return (
          !!view &&
          typeof view.id === "string" &&
          (view.surface === "feed" || view.surface === "signals" || view.surface === "watchlist") &&
          (typeof view.scopeKey === "undefined" || typeof view.scopeKey === "string") &&
          typeof view.name === "string" &&
          !!view.params &&
          typeof view.params === "object"
        );
      }),
      defaultViewIds: parsed.version === 2 && parsed.defaultViewIds ? parsed.defaultViewIds : {},
      selectedViewIds: parsed.version === 2 && parsed.selectedViewIds ? parsed.selectedViewIds : {},
    };
  } catch {
    return emptySavedViewsStore();
  }
}

export function saveSavedViewsStore(store: SavedViewsStore) {
  window.localStorage.setItem(savedViewsStorageKey, JSON.stringify(store));
}

export function scopedSavedViewSurfaceKey(surface: SavedViewSurface, scopeKey?: string) {
  return scopeKey ? `${surface}:${scopeKey}` : surface;
}

export function savedViewHref(view: SavedView): string {
  const params = new URLSearchParams();
  Object.entries(view.params).forEach(([key, value]) => {
    const trimmed = value.trim();
    if (trimmed) params.set(key, trimmed);
  });

  const qs = params.toString();
  if (view.surface === "signals") return `/signals${qs ? `?${qs}` : ""}`;
  if (view.surface === "watchlist" && view.scopeKey) return `/watchlists/${view.scopeKey}${qs ? `?${qs}` : ""}`;
  return `/${qs ? `?${qs}` : "?mode=all"}`;
}

export function markSavedViewSeen(store: SavedViewsStore, viewId: string, seenAt = new Date().toISOString()): SavedViewsStore {
  return {
    ...store,
    views: store.views.map((view) => (view.id === viewId ? { ...view, lastSeenAt: seenAt } : view)),
  };
}
