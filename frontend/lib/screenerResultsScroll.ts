"use client";

export const screenerResultsScrollStorageKey = "walnut:screener-results-scroll";

export function requestScreenerResultsScroll() {
  if (typeof window === "undefined") return;
  window.sessionStorage.setItem(screenerResultsScrollStorageKey, "1");
}
