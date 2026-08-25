"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { getInsiderSummary, type InsiderSummary } from "@/lib/api";
import { insiderSlug } from "@/lib/insider";

type Lookback = "30" | "90" | "180" | "365" | "1095";

function firstText(...values: unknown[]) {
  for (const value of values) {
    if (typeof value !== "string") continue;
    const trimmed = value.trim();
    if (trimmed) return trimmed;
  }
  return null;
}

function roleTabLabel(companyName: string | null | undefined, symbol: string, role: string | null | undefined) {
  const company = firstText(companyName)?.replace(/\s+(inc\.?|corp\.?|corporation|company)$/i, "") ?? symbol;
  const roleText = firstText(role);
  return roleText ? `${company} - ${roleText}` : company;
}

function buildRoleHref(canonicalSlug: string, lookback: Lookback, symbol: string, recentTradesPage: number) {
  const query = new URLSearchParams();
  if (lookback !== "90") query.set("lookback", lookback);
  query.set("issuer", symbol);
  if (recentTradesPage > 0) query.set("recent_trades_page", String(recentTradesPage));
  return `/insider/${encodeURIComponent(canonicalSlug)}?${query.toString()}`;
}

export function InsiderProfileHeaderClient({
  reportingCik,
  lookback,
  lookbackDays,
  issuer,
  stockSymbol,
  canonicalSlug,
  recentTradesPage,
  initialSummary,
  initialRoleText,
  initialCompanyText,
  initialOwnershipContext,
}: {
  reportingCik: string;
  lookback: Lookback;
  lookbackDays: number;
  issuer?: string;
  stockSymbol?: string;
  canonicalSlug: string;
  recentTradesPage: number;
  initialSummary: InsiderSummary;
  initialRoleText: string;
  initialCompanyText: string;
  initialOwnershipContext: string;
}) {
  const [summary, setSummary] = useState(initialSummary);

  useEffect(() => {
    const controller = new AbortController();
    getInsiderSummary(reportingCik, lookbackDays, issuer, {
      signal: controller.signal,
      source: "InsiderProfileHeaderClient",
    })
      .then(setSummary)
      .catch(() => undefined);
    return () => controller.abort();
  }, [issuer, lookbackDays, reportingCik]);

  const roleContexts = useMemo(
    () =>
      (summary.role_contexts ?? [])
        .map((context) => ({ ...context, symbol: firstText(context.symbol)?.toUpperCase() ?? "" }))
        .filter((context) => context.symbol),
    [summary.role_contexts],
  );
  const activeSymbol = (issuer || stockSymbol || summary.primary_symbol || roleContexts[0]?.symbol || "").toUpperCase();
  const liveCanonicalSlug = insiderSlug(firstText(summary.insider_name), reportingCik) ?? canonicalSlug;
  const roleText = firstText(summary.primary_role, initialRoleText) ?? "Role unavailable";
  const companyText = firstText(summary.primary_company_name, initialCompanyText) ?? "Company unavailable";
  const displaySymbol = activeSymbol || stockSymbol;
  const ownershipContext =
    summary.sell_count > summary.buy_count
      ? "Net seller"
      : summary.buy_count > summary.sell_count
        ? "Net buyer"
        : summary.total_trades > 0
          ? "Insider activity"
          : initialOwnershipContext;

  return (
    <>
      <p className="mt-2 truncate text-sm text-slate-300">
        {roleText} - {companyText}
        {displaySymbol ? ` (${displaySymbol})` : ""}
      </p>
      <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] text-slate-400">
        <span className="rounded-full border border-emerald-300/20 bg-emerald-300/10 px-2.5 py-1 font-medium text-emerald-200">{ownershipContext}</span>
        {displaySymbol ? <span className="rounded-full border border-white/10 bg-slate-950/50 px-2.5 py-1 text-slate-300">{displaySymbol}</span> : null}
        <span className="rounded-full border border-white/10 bg-slate-950/50 px-2.5 py-1 text-slate-400">CIK {reportingCik}</span>
      </div>
      {roleContexts.length > 1 ? (
        <div className="mt-3 flex gap-2 overflow-x-auto border-t border-white/10 pt-3">
          {roleContexts.map((context) => {
            const selected = context.symbol === activeSymbol;
            return (
              <Link
                key={context.symbol}
                href={buildRoleHref(liveCanonicalSlug, lookback, context.symbol, recentTradesPage)}
                prefetch={false}
                className={`shrink-0 rounded-lg border px-3 py-2 text-left text-xs transition ${
                  selected
                    ? "border-emerald-300/45 bg-emerald-400/12 text-emerald-100"
                    : "border-white/10 bg-slate-950/30 text-slate-300 hover:border-white/25 hover:text-white"
                }`}
                aria-current={selected ? "page" : undefined}
              >
                <span className="block font-semibold">{context.symbol}</span>
                <span className="mt-0.5 block max-w-[13rem] truncate text-[11px] opacity-80">
                  {roleTabLabel(context.company_name, context.symbol, context.role)}
                </span>
              </Link>
            );
          })}
        </div>
      ) : null}
    </>
  );
}
