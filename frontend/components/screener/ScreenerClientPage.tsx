"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { ScreenerExportButton } from "@/components/screener/ScreenerExportButton";
import { ScreenerUpgradeOverlay } from "@/components/screener/ScreenerUpgradeOverlay";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { ApiError, getEntitlements, getScreener } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { cardClassName, ghostButtonClassName, inputClassName, selectClassName, subtlePrimaryButtonClassName, tickerMonoLinkClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";

type ClientSearchParams = Record<string, string | undefined>;

type ScreenerRow = {
  symbol: string;
  company_name?: string | null;
  sector?: string | null;
  market_cap?: number | null;
  price?: number | null;
  volume?: number | null;
  confirmation?: { score?: number | null; band?: string | null; direction?: string | null; status?: string | null };
};

type ScreenerPayload = {
  items?: ScreenerRow[];
  total_available?: number;
  has_next?: boolean;
  result_cap?: number;
};

const PARAM_KEYS = [
  "market_cap_min",
  "market_cap_max",
  "price_min",
  "price_max",
  "volume_min",
  "sector",
  "industry",
  "country",
  "exchange",
  "congress_activity",
  "insider_activity",
  "confirmation_score_min",
  "confirmation_direction",
  "confirmation_band",
  "lookback_days",
  "sort",
  "sort_dir",
  "page_size",
] as const;

const SORTS = [
  ["relevance", "Relevance"],
  ["confirmation_score", "Confirmation"],
  ["freshness", "Freshness"],
  ["market_cap", "Market cap"],
  ["price", "Price"],
  ["volume", "Volume"],
] as const;

function param(searchParams: ClientSearchParams, key: string, fallback = "") {
  return (searchParams[key] ?? fallback).trim();
}

function currentParams(searchParams: ClientSearchParams) {
  const params: Record<string, string | number> = {
    sort: param(searchParams, "sort", "relevance") || "relevance",
    sort_dir: param(searchParams, "sort_dir", "desc") === "asc" ? "asc" : "desc",
    page: Number(param(searchParams, "page", "1")) || 1,
    page_size: Number(param(searchParams, "page_size", "50")) || 50,
    lookback_days: Number(param(searchParams, "lookback_days", "30")) || 30,
  };
  PARAM_KEYS.forEach((key) => {
    if (key in params) return;
    const value = param(searchParams, key);
    if (value) params[key] = value;
  });
  return params;
}

function compact(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function money(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: value >= 100 ? 0 : 2 }).format(value);
}

function cleanProtectedError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402) return "Premium access required.";
    return "Unable to load screener.";
  }
  return error instanceof Error ? error.message : "Unable to load screener.";
}

function pageHref(params: Record<string, string | number>, overrides: Record<string, string | number | null>) {
  const url = new URL("https://local/screener");
  Object.entries(params).forEach(([key, value]) => {
    const trimmed = String(value).trim();
    if (trimmed) url.searchParams.set(key, trimmed);
  });
  Object.entries(overrides).forEach(([key, value]) => {
    if (value === null || String(value).trim() === "") {
      url.searchParams.delete(key);
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return `${url.pathname}${url.search}`;
}

export function ScreenerClientPage({ initialSearchParams }: { initialSearchParams: ClientSearchParams }) {
  const params = useMemo(() => currentParams(initialSearchParams), [initialSearchParams]);
  const [entitlements, setEntitlements] = useState<Entitlements | null>(null);
  const [data, setData] = useState<ScreenerPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErrorMessage(null);
    getEntitlements()
      .then(async (nextEntitlements) => {
        if (cancelled) return;
        setEntitlements(nextEntitlements);
        if (!nextEntitlements.user) {
          setData(null);
          setErrorMessage("Sign in required.");
          return;
        }
        if (!hasEntitlement(nextEntitlements, "screener")) {
          setData(null);
          setErrorMessage("Premium access required.");
          return;
        }
        const response = await getScreener(params);
        if (!cancelled) setData(response as ScreenerPayload);
      })
      .catch((error) => {
        console.error("[screener] protected client fetch failed", error);
        if (!cancelled) {
          setEntitlements(defaultEntitlements);
          setData(null);
          setErrorMessage(cleanProtectedError(error));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [params]);

  const canUseScreener = entitlements ? hasEntitlement(entitlements, "screener") : false;
  const canUseMonitoring = entitlements ? hasEntitlement(entitlements, "screener_monitoring") : false;
  const canExportCsv = entitlements ? hasEntitlement(entitlements, "screener_csv_export") : false;
  const resultCap = entitlements ? limitFor(entitlements, "screener_results") : defaultEntitlements.limits.screener_results;
  const rows = data?.items ?? [];

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Idea Screener</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Stock Screener</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Browser-loaded access keeps bearer-token sessions working while cross-site cookies transition.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="rounded-full border border-slate-800 bg-slate-950/30 px-2.5 py-1 text-slate-300">plan <span className="text-white">{entitlements?.tier ?? "loading"}</span></span>
          <span className="rounded-full border border-slate-800 bg-slate-950/30 px-2.5 py-1 text-slate-300">results <span className="text-white">{resultCap}</span></span>
        </div>
      </div>

      {loading ? (
        <div className={`${cardClassName} min-h-[20rem] p-8 text-sm text-slate-300`}>Loading screener access...</div>
      ) : errorMessage === "Sign in required." ? (
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Sign in required</h2>
          <p className="mt-2 text-sm text-slate-400">Log in to use the stock screener.</p>
          <Link href="/login?return_to=%2Fscreener" className="mt-4 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100">Sign in</Link>
        </div>
      ) : errorMessage === "Premium access required." && !canUseScreener ? (
        <div className={cardClassName}>
          <UpgradePrompt title="Unlock the stock screener" body="Your current plan does not include base screener access. Upgrade to open the full discovery workflow." />
        </div>
      ) : (
        <>
          <div className={`${cardClassName} space-y-4`}>
            <SavedViewsBar
              surface="screener"
              paramKeys={PARAM_KEYS}
              formId="screener-client-filters"
              dense={true}
              clearSelectionWhenPristine={true}
              allowNotifications={false}
              allowDefaultView={false}
              defaultParams={{
                sort: String(params.sort ?? "relevance"),
                sort_dir: String(params.sort_dir ?? "desc"),
                page_size: String(params.page_size ?? 50),
                lookback_days: String(params.lookback_days ?? 30),
              }}
              rightSlot={
                <div className="flex flex-wrap items-center gap-2">
                  <ScreenerExportButton params={params} filenamePrefix="screener" locked={!canExportCsv} lockedReason="CSV export is included with Premium." />
                  {canUseMonitoring ? (
                    <Link href="/monitoring" className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs`} prefetch={false}>Monitoring inbox</Link>
                  ) : (
                    <ScreenerUpgradeOverlay
                      title="Saved screen monitoring"
                      body="Saved screen monitoring and inbox events are included with Premium."
                      className="inline-flex rounded-lg pr-20"
                      buttonClassName="rounded-lg"
                    >
                      <div className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs text-amber-100`}>Monitoring Premium</div>
                    </ScreenerUpgradeOverlay>
                  )}
                  <span className="rounded-lg border border-slate-800 bg-slate-950/30 px-3 py-2 text-xs text-slate-300">sort <span className="text-white">{String(params.sort)}</span></span>
                </div>
              }
            />

            <form id="screener-client-filters" action="/screener" className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Mkt cap min<input name="market_cap_min" defaultValue={String(params.market_cap_min ?? "")} className={inputClassName} /></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Price min<input name="price_min" defaultValue={String(params.price_min ?? "")} className={inputClassName} /></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Volume min<input name="volume_min" defaultValue={String(params.volume_min ?? "")} className={inputClassName} /></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Sort<select name="sort" defaultValue={String(params.sort)} className={selectClassName}>{SORTS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Direction<select name="sort_dir" defaultValue={String(params.sort_dir)} className={selectClassName}><option value="desc">High to Low</option><option value="asc">Low to High</option></select></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Rows<select name="page_size" defaultValue={String(params.page_size)} className={selectClassName}><option value="25">25</option><option value="50">50</option><option value="100">100</option></select></label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">Overlay<select name="lookback_days" defaultValue={String(params.lookback_days)} className={selectClassName}><option value="30">30d</option><option value="60">60d</option><option value="90">90d</option></select></label>
              <button type="submit" className={`${subtlePrimaryButtonClassName} self-end`}>Run screen</button>
            </form>
          </div>

          <div className={`${cardClassName} min-h-[32rem] overflow-hidden p-0`}>
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
              <div>
                <h2 className="text-lg font-semibold text-white">Results</h2>
                <p className="mt-1 text-sm text-slate-400">{errorMessage ? "Screener unavailable" : `${rows.length} shown from ${data?.total_available ?? 0} available results`}</p>
              </div>
              <div className="flex items-center gap-2 text-xs">
                <Link href={pageHref(params, { page: Math.max(Number(params.page ?? 1) - 1, 1) })} className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs`} prefetch={false}>Prev</Link>
                <Link href={pageHref(params, { page: Number(params.page ?? 1) + 1 })} className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${data?.has_next ? "" : "pointer-events-none opacity-40"}`} prefetch={false}>Next</Link>
              </div>
            </div>
            {errorMessage ? (
              <div className="p-8 text-center text-sm text-slate-400">{errorMessage}</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full text-left text-sm">
                  <thead className="bg-slate-950/50 text-xs uppercase tracking-wide text-slate-400">
                    <tr>
                      <th className="px-4 py-3">Symbol</th>
                      <th className="px-4 py-3">Company</th>
                      <th className="px-4 py-3">Sector</th>
                      <th className="px-4 py-3 text-right">Market cap</th>
                      <th className="px-4 py-3 text-right">Price</th>
                      <th className="px-4 py-3 text-right">Volume</th>
                      <th className="px-4 py-3 text-right">Confirm</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {rows.length === 0 ? (
                      <tr><td colSpan={7} className="px-4 py-10 text-center text-slate-400">No names matched this screen.</td></tr>
                    ) : (
                      rows.map((row) => {
                        const href = tickerHref(row.symbol) ?? `/ticker/${encodeURIComponent(row.symbol)}`;
                        return (
                          <tr key={row.symbol} className="text-slate-200">
                            <td className="px-4 py-3"><Link href={href} className={tickerMonoLinkClassName}>{row.symbol}</Link></td>
                            <td className="px-4 py-3">{row.company_name ?? "--"}</td>
                            <td className="px-4 py-3">{row.sector ?? "--"}</td>
                            <td className="px-4 py-3 text-right">{compact(row.market_cap)}</td>
                            <td className="px-4 py-3 text-right">{money(row.price)}</td>
                            <td className="px-4 py-3 text-right">{compact(row.volume)}</td>
                            <td className="px-4 py-3 text-right">{row.confirmation?.score ?? "--"}</td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
