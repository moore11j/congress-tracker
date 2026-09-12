"use client";

import { recordProductEvent } from "./api";
import { recordGoogleAnalyticsEvent, type GoogleAnalyticsContext } from "./googleAnalytics";
import { trackHeyCatchEvent } from "./heycatch";
import { isProductionAnalyticsHost } from "./analyticsEnvironment";
import { acquisitionProperties, analyticsConsent, analyticsIdentity, safeAnalyticsPath, type AnalyticsProperties } from "./analyticsContext";
import { funnelEvents, type FunnelEvent } from "./funnelEvents";

const allowedProperties = new Set(["route", "source_page", "destination_page", "ticker", "entity_type", "entity_id", "leaderboard_type", "strategy_id", "gated_feature", "current_plan", "authenticated", "acquisition_source", "utm_source", "utm_medium", "utm_campaign", "utm_content", "target_plan", "billing_interval", "method", "placement", "destination_type", "destination_id"]);

export function trackEvent(name: FunnelEvent, properties: AnalyticsProperties = {}, gaContext?: GoogleAnalyticsContext): boolean {
  try {
    if (!funnelEvents.includes(name) || name === "subscription_completed" || !analyticsConsent()) return false;
    const route = safeAnalyticsPath(window.location.pathname);
    const merged = { ...acquisitionProperties(), ...analyticsIdentity(), route, source_page: route, ...properties };
    const clean: AnalyticsProperties = {};
    for (const [key, value] of Object.entries(merged)) {
      if (!allowedProperties.has(key)) continue;
      if (typeof value === "string") {
        const safe = key.endsWith("page") || key === "route" ? safeAnalyticsPath(value) : value.slice(0, 120);
        if (!safe.includes("@") && !/(?:token|password|secret)=/i.test(safe)) clean[key] = safe;
      }
      else if (value === null || typeof value === "boolean" || (typeof value === "number" && Number.isFinite(value))) clean[key] = value;
    }
    // Explicit local debugging never enables network transmission.
    if (!isProductionAnalyticsHost()) {
      if (process.env.NEXT_PUBLIC_ANALYTICS_DEBUG === "1") {
        window.dispatchEvent(new CustomEvent("walnut:analytics-debug", { detail: { event_name: name, properties: clean } }));
        console.info("[Walnut analytics]", JSON.stringify({ event_name: name, properties: clean }));
        return true;
      }
      return false;
    }
    for (const send of [
      () => recordProductEvent({ event_name: name, path: route, properties: clean, ...(name === "checkout_started" && gaContext ? { ga_context: gaContext } : {}) }),
      () => trackHeyCatchEvent(name, clean),
      () => recordGoogleAnalyticsEvent(name, clean),
    ]) { try { send(); } catch { /* One blocked provider must not stop the others. */ } }
    return true;
  } catch { return false; }
}

export function trackDiscoveryClick(destination: string) {
  const path = safeAnalyticsPath(destination);
  const source = window.location.pathname;
  const ticker = path.match(/^\/ticker\/([^/]+)$/);
  if (source === "/screener" && ticker) trackEvent("screener_result_clicked", { ticker: ticker[1], destination_page: path });
  if (source.startsWith("/leaderboards") || source === "/top-stocks") {
    const entity = path.match(/^\/(ticker|member|insider|institution)\/([^/]+)$/);
    if (entity) trackEvent("leaderboard_entity_clicked", { entity_type: entity[1], entity_id: entity[2], leaderboard_type: entity[1], destination_page: path, ...(ticker ? { ticker: ticker[1] } : {}) });
  }
}
