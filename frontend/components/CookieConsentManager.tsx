"use client";

import { useEffect, useMemo, useState } from "react";
import { SpeedInsights } from "@vercel/speed-insights/next";
import {
  hasGlobalPrivacyControl,
  privacyConsentOpenEvent,
  readPrivacyConsent,
  writePrivacyConsent,
  type PrivacyConsent,
} from "@/lib/privacyConsent";
import { ensureGoogleAnalytics, updateGoogleAnalyticsConsent } from "@/lib/googleAnalytics";

const REDDIT_PIXEL_ID = process.env.NEXT_PUBLIC_REDDIT_PIXEL_ID ?? "a2_jdfg5l7gwuw1";

type WindowWithPixels = Window & {
  rdt?: ((...args: unknown[]) => void) & { callQueue?: unknown[]; sendEvent?: (...args: unknown[]) => void };
  __walnutRedditPixelLoaded?: boolean;
};

function loadScript(id: string, src: string): void {
  if (document.getElementById(id)) return;
  const script = document.createElement("script");
  script.id = id;
  script.async = true;
  script.src = src;
  document.head.appendChild(script);
}

function loadRedditPixel(): void {
  if (!REDDIT_PIXEL_ID) return;
  const win = window as WindowWithPixels;
  if (win.__walnutRedditPixelLoaded) return;
  win.__walnutRedditPixelLoaded = true;
  if (!win.rdt) {
    const rdt = ((...args: unknown[]) => {
      if (rdt.sendEvent) {
        rdt.sendEvent(...args);
        return;
      }
      rdt.callQueue?.push(args);
    }) as NonNullable<WindowWithPixels["rdt"]>;
    rdt.callQueue = [];
    win.rdt = rdt;
  }
  loadScript("walnut-reddit-pixel", `https://www.redditstatic.com/ads/pixel.js?pixel_id=${REDDIT_PIXEL_ID}`);
  win.rdt?.("init", REDDIT_PIXEL_ID);
  win.rdt?.("track", "PageVisit");
}

function applyConsent(consent: PrivacyConsent | null): void {
  const analyticsGranted = consent?.analytics ?? true;
  const marketingGranted = Boolean(consent?.marketing);
  ensureGoogleAnalytics({
    analyticsGranted,
    marketingGranted,
    sendInitialPageView: analyticsGranted,
  });
  updateGoogleAnalyticsConsent(analyticsGranted, marketingGranted);
  if (consent?.marketing) loadRedditPixel();
}

export function CookieConsentManager() {
  const [ready, setReady] = useState(false);
  const [consent, setConsent] = useState<PrivacyConsent | null>(null);
  const [preferencesOpen, setPreferencesOpen] = useState(false);
  const [analyticsEnabled, setAnalyticsEnabled] = useState(false);
  const [marketingEnabled, setMarketingEnabled] = useState(false);
  const gpcEnabled = useMemo(() => hasGlobalPrivacyControl(), []);

  useEffect(() => {
    const stored = readPrivacyConsent();
    setConsent(stored);
    setAnalyticsEnabled(stored?.analytics ?? true);
    setMarketingEnabled(stored?.marketing ?? !gpcEnabled);
    setReady(true);
    applyConsent(stored);
  }, [gpcEnabled]);

  useEffect(() => {
    const openSettings = () => {
      const stored = readPrivacyConsent();
      setAnalyticsEnabled(stored?.analytics ?? true);
      setMarketingEnabled(stored?.marketing ?? !gpcEnabled);
      setPreferencesOpen(true);
    };
    window.addEventListener(privacyConsentOpenEvent, openSettings);
    return () => window.removeEventListener(privacyConsentOpenEvent, openSettings);
  }, [gpcEnabled]);

  useEffect(() => {
    applyConsent(consent);
  }, [consent]);

  function saveConsent(next: { analytics: boolean; marketing: boolean }) {
    const stored = writePrivacyConsent(next);
    const disabledLoadedCategory = Boolean(
      ((consent?.analytics ?? true) && !stored.analytics) || (consent?.marketing && !stored.marketing),
    );
    setConsent(stored);
    setAnalyticsEnabled(stored.analytics);
    setMarketingEnabled(stored.marketing);
    setPreferencesOpen(false);
    if (disabledLoadedCategory) window.location.reload();
  }

  if (!ready) return null;

  const showBanner = !consent;

  return (
    <>
      {(consent?.analytics ?? true) ? <SpeedInsights /> : null}

      {showBanner ? (
        <div className="fixed inset-x-0 bottom-0 z-[2000] border-t border-white/10 bg-slate-950/95 px-4 pb-[calc(env(safe-area-inset-bottom)+1rem)] pt-4 text-slate-100 shadow-2xl backdrop-blur">
          <div className="mx-auto flex max-w-[1728px] flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="max-w-4xl text-sm leading-6 text-slate-200">
              <p className="font-semibold text-white">Privacy choices</p>
              <p>
                We use essential cookies to keep Walnut secure and working. Optional analytics help us improve the product, and optional campaign measurement helps us understand what brings people here.
              </p>
              {gpcEnabled ? (
                <p className="mt-1 text-xs text-emerald-200">Your browser privacy signal is on, so optional campaign measurement stays off.</p>
              ) : null}
            </div>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center lg:shrink-0">
              <button
                type="button"
                className="rounded border border-white/15 px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/30 hover:bg-white/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300"
                onClick={() => saveConsent({ analytics: false, marketing: false })}
              >
                Reject optional
              </button>
              <button
                type="button"
                className="rounded border border-white/15 px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/30 hover:bg-white/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300"
                onClick={() => setPreferencesOpen(true)}
              >
                Customize
              </button>
              <button
                type="button"
                className="rounded bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200"
                onClick={() => saveConsent({ analytics: true, marketing: true })}
              >
                Accept optional
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {preferencesOpen ? (
        <div className="fixed inset-0 z-[2100] flex items-end justify-center bg-slate-950/70 px-4 py-5 backdrop-blur-sm sm:items-center" role="dialog" aria-modal="true" aria-labelledby="cookie-preferences-title">
          <div className="w-full max-w-xl rounded-lg border border-white/10 bg-slate-950 p-5 text-slate-100 shadow-2xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 id="cookie-preferences-title" className="text-lg font-semibold text-white">Privacy choices</h2>
                <p className="mt-1 text-sm leading-6 text-slate-300">Choose which optional technologies Walnut can use on this browser.</p>
              </div>
              <button
                type="button"
                className="rounded border border-white/15 px-2 py-1 text-sm text-slate-200 hover:bg-white/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300"
                onClick={() => setPreferencesOpen(false)}
                aria-label="Close privacy choices"
              >
                Close
              </button>
            </div>

            <div className="mt-5 space-y-3">
              <div className="rounded border border-white/10 bg-white/[0.03] p-4">
                <div className="flex items-center justify-between gap-4">
                  <div>
                    <p className="font-semibold text-white">Necessary</p>
                    <p className="mt-1 text-sm text-slate-300">Required for login, security, account access, and core app behavior.</p>
                  </div>
                  <span className="rounded bg-slate-800 px-3 py-1 text-xs font-semibold text-slate-200">Always on</span>
                </div>
              </div>

              <label className="block cursor-pointer rounded border border-white/10 bg-white/[0.03] p-4 transition hover:border-white/20">
                <span className="flex items-center justify-between gap-4">
                  <span>
                    <span className="block font-semibold text-white">Analytics</span>
                    <span className="mt-1 block text-sm text-slate-300">Helps us understand page views, feature usage, performance, and product quality.</span>
                  </span>
                  <input
                    type="checkbox"
                    className="h-5 w-5 accent-emerald-400"
                    checked={analyticsEnabled}
                    onChange={(event) => setAnalyticsEnabled(event.target.checked)}
                  />
                </span>
              </label>

              <label className="block cursor-pointer rounded border border-white/10 bg-white/[0.03] p-4 transition hover:border-white/20">
                <span className="flex items-center justify-between gap-4">
                  <span>
                    <span className="block font-semibold text-white">Marketing</span>
                    <span className="mt-1 block text-sm text-slate-300">Allows campaign measurement and ad pixels, including Reddit Pixel where enabled.</span>
                    {gpcEnabled ? <span className="mt-1 block text-xs text-emerald-200">Off because your browser privacy signal is enabled.</span> : null}
                  </span>
                  <input
                    type="checkbox"
                    className="h-5 w-5 accent-emerald-400 disabled:cursor-not-allowed disabled:opacity-50"
                    checked={marketingEnabled && !gpcEnabled}
                    disabled={gpcEnabled}
                    onChange={(event) => setMarketingEnabled(event.target.checked)}
                  />
                </span>
              </label>
            </div>

            <div className="mt-5 flex flex-col gap-2 sm:flex-row sm:justify-end">
              <button
                type="button"
                className="rounded border border-white/15 px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/30 hover:bg-white/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300"
                onClick={() => saveConsent({ analytics: false, marketing: false })}
              >
                Reject optional
              </button>
              <button
                type="button"
                className="rounded bg-emerald-400 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200"
                onClick={() => saveConsent({ analytics: analyticsEnabled, marketing: marketingEnabled })}
              >
                Save choices
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
