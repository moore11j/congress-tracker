// Visible impressions wait for the route event and its bounded identity lookup.
export const analyticsVisitReadyEvent = "walnut:analytics-visit-ready";
let currentPath: string | null = null;
let ready = false;

export function beginAnalyticsVisit(path: string) {
  if (path !== currentPath) { currentPath = path; ready = false; }
}

export function isAnalyticsVisitReady(path: string) {
  return currentPath === path && ready;
}

export function finishAnalyticsVisit(path: string) {
  if (currentPath !== path) return;
  ready = true;
  window.dispatchEvent(new Event(analyticsVisitReadyEvent));
}
