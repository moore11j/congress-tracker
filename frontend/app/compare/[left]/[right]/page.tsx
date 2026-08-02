import Link from "next/link";
import type { Metadata } from "next";
import { ApiError, getEntitlements, getPeerCompare, type PeerCompareCategory, type PeerCompareMetric, type PeerCompareResponse } from "@/lib/api";
import { ResearchActions } from "@/components/research/ResearchActions";
import { ghostButtonClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";
import { PeerCompareSelector } from "@/components/compare/PeerCompareSelector";
import { CompareEventOnMount, CompareTrackedLink } from "@/components/compare/CompareAnalytics";
import { CompareContextualCta } from "@/components/compare/CompareContextualCta";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { isAdminEntitlement } from "@/lib/entitlements";
import { WALNUT_APP_URL, appCanonicalUrl } from "@/lib/marketingMetadata";
import { isApprovedSeoPilotPath } from "@/lib/seoQuality";

type PageProps = {
  params: Promise<{ left: string; right: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

const TICKER_COLORS = {
  left: "#22d3ee",
  right: "#a78bfa",
} as const;

function cleanSymbol(value: string) {
  return decodeURIComponent(value || "").trim().toUpperCase().replace(/\./g, "-");
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const routeParams = await params;
  const left = cleanSymbol(routeParams.left);
  const right = cleanSymbol(routeParams.right);
  const hasBoth = left !== "_" && right !== "_";
  const canonicalPath = hasBoth ? `/compare/${encodeURIComponent(left)}/${encodeURIComponent(right)}` : "/compare/_/_";
  const title = hasBoth
    ? `${left} vs ${right} Stock Comparison Tool | Walnut Markets`
    : "Stock Comparison Tool | Walnut Markets";
  const description = hasBoth
    ? `Compare ${left} and ${right} across fundamentals, price action, catalysts, risks, disclosures, and Walnut's confirmation score.`
    : "Compare stocks across fundamentals, price action, catalysts, risks, disclosures, and Walnut's confirmation score.";
  const indexablePilot = hasBoth && isApprovedSeoPilotPath(canonicalPath);

  return {
    metadataBase: new URL(WALNUT_APP_URL),
    title,
    description,
    robots: indexablePilot ? { index: true, follow: true } : { index: false, follow: true },
    alternates: { canonical: appCanonicalUrl(canonicalPath) },
    openGraph: { type: "website", title, description, url: appCanonicalUrl(canonicalPath) },
    twitter: { card: "summary", title, description },
  };
}

function firstSearchParam(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function searchParamsToString(searchParams: Record<string, string | string[] | undefined>) {
  const params = new URLSearchParams();
  Object.entries(searchParams).forEach(([key, value]) => {
    if (key === "compare_upgraded") return;
    if (value === undefined) return;
    if (Array.isArray(value)) {
      value.forEach((item) => params.append(key, item));
    } else {
      params.set(key, value);
    }
  });
  return params.toString();
}

function pricingHref(returnTo: string) {
  return `/pricing?returnTo=${encodeURIComponent(returnTo)}`;
}

function edgeLabel(edge: "left" | "right" | "even", data: PeerCompareResponse) {
  if (edge === "left") return data.left.symbol;
  if (edge === "right") return data.right.symbol;
  return "Even";
}

function edgeClass(edge: "left" | "right" | "even") {
  return "border-white/10 bg-white/[0.03] text-slate-300";
}

function alpha(hex: string, value: number) {
  const clean = hex.replace("#", "");
  const r = parseInt(clean.slice(0, 2), 16);
  const g = parseInt(clean.slice(2, 4), 16);
  const b = parseInt(clean.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${value})`;
}

function edgeStyle(edge: "left" | "right" | "even") {
  if (edge === "even") return undefined;
  const color = TICKER_COLORS[edge];
  return {
    borderColor: alpha(color, 0.45),
    backgroundColor: alpha(color, 0.12),
    color,
  };
}

function metricValueStyle(metric: PeerCompareMetric, side: "left" | "right") {
  if (metric.edge !== side) return undefined;
  const color = TICKER_COLORS[side];
  return {
    color,
    textShadow: `0 0 18px ${alpha(color, 0.28)}`,
  };
}

function formatValue(metric: PeerCompareMetric, side: "left" | "right", context?: { categoryKey?: string; metrics?: PeerCompareMetric[] }) {
  const value = side === "left" ? metric.left : metric.right;
  if (context?.categoryKey === "government_contracts" && metric.key === "total_award_amount") {
    const contracts = context.metrics?.find((item) => item.key === "contract_count");
    const contractValue = side === "left" ? contracts?.left : contracts?.right;
    if (typeof contractValue === "number" && contractValue <= 0) return "N/A";
  }
  if (value === null || value === undefined || value === "") return "Unavailable";
  if (typeof value === "string") return value.replace(/_/g, " ");
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (!Number.isFinite(value)) return "Unavailable";
  if (metric.unit === "currency") {
    if (Math.abs(value) >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
    if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
    return `$${value.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  }
  if (metric.unit === "percent" || metric.unit === "percent_yield") return `${value.toFixed(1)}%`;
  if (metric.unit === "ratio") return `${value.toFixed(2)}x`;
  if (metric.unit === "multiple") return `${value.toFixed(2)}x`;
  if (metric.unit === "integer" || metric.unit === "score") return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function SideHeader({ side, winner, tone }: { side: PeerCompareResponse["left"]; winner: boolean; tone: string }) {
  const href = tickerHref(side.symbol);
  return (
    <div
      className={`min-w-0 rounded-lg border p-4 ${winner ? "bg-white/[0.04]" : "border-white/10 bg-slate-950/45"}`}
      style={winner ? { borderColor: alpha(tone, 0.5), boxShadow: `inset 0 0 0 1px ${alpha(tone, 0.14)}` } : undefined}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="text-2xl font-semibold text-white">{side.symbol}</p>
          <p className="mt-1 truncate text-sm text-slate-400">{side.company_name || side.symbol}</p>
        </div>
        {href ? (
          <Link href={href} className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-300 hover:text-white">
            Open
          </Link>
        ) : null}
      </div>
      <div className="mt-3 flex flex-wrap gap-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
        {side.exchange ? <span>{side.exchange}</span> : null}
        {side.sector ? <span>{side.sector}</span> : null}
        {side.industry ? <span>{side.industry}</span> : null}
      </div>
    </div>
  );
}

function proLockCopy(category: PeerCompareCategory) {
  if (category.key === "institutional_activity") return "See which ticker institutions are accumulating or reducing.";
  if (category.key === "options_flow") return "See whether options positioning confirms or contradicts the comparison.";
  return "Upgrade to see this additional context.";
}

function CategoryCard({ category, data, upgradeHref }: { category: PeerCompareCategory; data: PeerCompareResponse; upgradeHref: string }) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-white">{category.label}</h2>
          {category.locked ? <p className="mt-1 text-xs text-slate-500">Locked: {category.required_plan?.toUpperCase() ?? "PLAN"}</p> : null}
        </div>
        <span className={`shrink-0 rounded-md border px-2.5 py-1 text-xs font-semibold ${edgeClass(category.edge)}`} style={edgeStyle(category.edge)}>
          {category.locked ? "Locked" : edgeLabel(category.edge, data)}
        </span>
      </div>
      {category.locked ? (
        <div className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] p-3">
          <p className="text-sm leading-6 text-slate-300">{proLockCopy(category)}</p>
          <CompareTrackedLink
            href={upgradeHref}
            eventName={category.required_plan === "pro" ? "compare_pro_upgrade_click" : "compare_premium_upgrade_click"}
            path={`/compare/${encodeURIComponent(data.left.symbol)}/${encodeURIComponent(data.right.symbol)}`}
            properties={{ ticker_pair: `${data.left.symbol}/${data.right.symbol}`, cta_location: `${category.key}_card` }}
            className="mt-3 inline-flex rounded-md border border-cyan-300/30 bg-cyan-300/10 px-3 py-2 text-xs font-semibold text-cyan-100 transition hover:bg-cyan-300/15"
          >
            {category.required_plan === "pro" ? "Upgrade to Pro" : "Upgrade to Premium"}
          </CompareTrackedLink>
        </div>
      ) : null}
      {category.metrics.length ? (
        <div className="mt-4 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full table-fixed text-left text-xs sm:text-sm">
            <thead className="bg-white/[0.03] text-xs uppercase tracking-[0.16em] text-slate-500">
              <tr>
                <th className="w-[34%] px-3 py-2 font-semibold">Metric</th>
                <th className="w-[33%] px-3 py-2 text-right font-semibold" style={{ color: TICKER_COLORS.left }}>{data.left.symbol}</th>
                <th className="w-[33%] px-3 py-2 text-right font-semibold" style={{ color: TICKER_COLORS.right }}>{data.right.symbol}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10">
              {category.metrics.map((metric) => (
                <tr key={metric.key} className={metric.edge === "even" ? "" : "bg-white/[0.012]"}>
                  <td className="break-words px-3 py-2 text-slate-300">{metric.label}</td>
                  <td className="break-words px-3 py-2 text-right font-semibold text-slate-300" style={metricValueStyle(metric, "left")}>
                    {formatValue(metric, "left", { categoryKey: category.key, metrics: category.metrics })}
                  </td>
                  <td className="break-words px-3 py-2 text-right font-semibold text-slate-300" style={metricValueStyle(metric, "right")}>
                    {formatValue(metric, "right", { categoryKey: category.key, metrics: category.metrics })}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  );
}

function CompareReport({ data, upgradeHref }: { data: PeerCompareResponse; upgradeHref: string }) {
  const winner = data.call.winner;
  const leftWinner = winner === "left";
  const rightWinner = winner === "right";
  return (
    <div className="space-y-5">
      <div className="grid gap-3 lg:grid-cols-[1fr_1.2fr_1fr]">
        <SideHeader side={data.left} winner={leftWinner} tone={TICKER_COLORS.left} />
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4 text-center">
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">Our Call</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">
            {winner === "even" ? "Too close to call" : `${data.call.symbol} leads`}
          </h2>
          <p className="mt-2 text-sm leading-6 text-slate-300">{data.call.summary}</p>
          {data.call.drivers.length ? (
            <div className="mt-4 flex flex-wrap justify-center gap-2">
              {data.call.drivers.map((driver) => (
                <span key={driver} className="rounded-md border border-emerald-300/20 bg-emerald-300/10 px-2.5 py-1 text-xs font-semibold text-emerald-100">
                  {driver}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        <SideHeader side={data.right} winner={rightWinner} tone={TICKER_COLORS.right} />
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {data.categories.map((category) => (
          <CategoryCard key={category.key} category={category} data={data} upgradeHref={upgradeHref} />
        ))}
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h2 className="text-sm font-semibold text-white">Tradeoffs</h2>
          {data.tradeoffs.length ? (
            <div className="mt-3 flex flex-wrap gap-2">
              {data.tradeoffs.map((item) => (
                <span key={item} className="rounded-md border border-amber-300/20 bg-amber-300/10 px-2.5 py-1 text-xs font-semibold text-amber-100">
                  {item}
                </span>
              ))}
            </div>
          ) : (
            <p className="mt-2 text-sm text-slate-400">No material counter-edge in the visible categories.</p>
          )}
        </section>
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h2 className="text-sm font-semibold text-white">Data Notes</h2>
          {data.notes.length ? (
            <ul className="mt-2 space-y-1 text-sm text-slate-400">
              {data.notes.map((note) => (
                <li key={note}>{note}</li>
              ))}
            </ul>
          ) : (
            <p className="mt-2 text-sm text-slate-400">Visible categories loaded for the 30D comparison window.</p>
          )}
        </section>
      </div>
    </div>
  );
}

function LockedCompareState({ data, authenticated, upgradeHref, signInHref }: { data: PeerCompareResponse; authenticated: boolean; upgradeHref: string; signInHref: string }) {
  const categories = data.categories.slice(0, 7);
  const requiredPlan = data.access?.required_plan === "pro" ? "pro" : "premium";
  const requiredPlanLabel = requiredPlan === "pro" ? "Pro" : "Premium";
  return (
    <div className="space-y-5">
      <div className="grid gap-3 lg:grid-cols-[1fr_1.2fr_1fr]">
        <SideHeader side={data.left} winner={false} tone={TICKER_COLORS.left} />
        <section className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-5 text-center">
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">{requiredPlanLabel} Feature</p>
          <h2 className="mt-3 text-2xl font-semibold text-white">One comparison answers today's question.</h2>
          <p className="mx-auto mt-3 max-w-xl text-sm leading-6 text-slate-300">
            Walnut helps you compare the rest of your portfolio, monitor what changes and see when the better setup shifts. Unlock deeper confirmation, institutional activity and options-flow context with Walnut Premium or Pro.
          </p>
          <div className="mt-5 flex flex-col justify-center gap-3 sm:flex-row">
            <CompareTrackedLink
              href={upgradeHref}
              eventName={requiredPlan === "pro" ? "compare_pro_upgrade_click" : "compare_premium_upgrade_click"}
              path={`/compare/${encodeURIComponent(data.left.symbol)}/${encodeURIComponent(data.right.symbol)}`}
              properties={{ ticker_pair: `${data.left.symbol}/${data.right.symbol}`, auth_state: authenticated ? "free" : "logged_out", cta_location: "locked_state" }}
              className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
            >
              {authenticated ? `Upgrade to ${requiredPlanLabel}` : `Unlock Compare with ${requiredPlanLabel}`}
            </CompareTrackedLink>
            {!authenticated ? (
              <a href={signInHref} className="inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
                Sign in
              </a>
            ) : null}
          </div>
        </section>
        <SideHeader side={data.right} winner={false} tone={TICKER_COLORS.right} />
      </div>
      <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
        <h2 className="text-sm font-semibold text-white">Categories Walnut evaluates</h2>
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {categories.map((category) => (
            <div key={category.key} className="rounded-md border border-white/10 bg-white/[0.03] px-3 py-2 text-sm font-medium text-slate-300">
              {category.label}
            </div>
          ))}
        </div>
        <p className="mt-4 text-sm leading-6 text-slate-400">
          Walnut&apos;s proprietary confirmation score summarizes whether the available data supports or conflicts with each stock setup.
        </p>
      </section>
    </div>
  );
}

function CompareError({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-amber-300/20 bg-amber-300/10 p-4">
      <h2 className="text-lg font-semibold text-white">Choose a valid peer</h2>
      <p className="mt-2 text-sm text-amber-100">{message}</p>
    </div>
  );
}

export default async function PeerComparePage({ params, searchParams }: PageProps) {
  const routeParams = await params;
  const sp = (await searchParams) ?? {};
  const left = cleanSymbol(routeParams.left);
  const right = cleanSymbol(routeParams.right);
  const hasLeft = left !== "_";
  const hasRight = right !== "_";
  const pageTitle = hasLeft || hasRight ? `${hasLeft ? left : "..."} vs ${hasRight ? right : "..."}` : "Compare two tickers";
  const currentQuery = searchParamsToString(sp);
  const currentPath = `/compare/${encodeURIComponent(left)}/${encodeURIComponent(right)}${currentQuery ? `?${currentQuery}` : ""}`;
  const authState = await optionalPageAuthState();
  const entitlements = authState.token
    ? await getEntitlements(authState.token, { source: "PeerCompareResearchGate" }).catch(() => null)
    : null;
  const canCreateResearch = isAdminEntitlement(entitlements);
  const plan = entitlements?.effective_tier ?? entitlements?.tier ?? (authState.token ? "free" : "logged_out");
  const upgradeHref = pricingHref(currentPath);
  const signInHref = `/login?return_to=${encodeURIComponent(currentPath)}`;
  const completedUpgrade = firstSearchParam(sp.compare_upgraded) === "1";
  let data: PeerCompareResponse | null = null;
  let errorMessage = "This comparison could not be loaded.";

  try {
    if (hasLeft && hasRight) {
      data = await getPeerCompare(left, right, { authToken: authState.token ?? undefined, source: "PeerComparePage" });
    } else if (!hasLeft && !hasRight) {
      errorMessage = "Search for two tickers to compare.";
    } else if (!hasLeft) {
      errorMessage = "Search for a first ticker to compare.";
    } else {
      errorMessage = "Search for a second ticker to compare against.";
    }
  } catch (error) {
    if (error instanceof ApiError) {
      errorMessage = typeof error.detail === "string" ? error.detail : "One of these symbols is unavailable for comparison.";
    }
  }

  return (
    <main className="min-h-screen bg-[#06111f] py-6 text-slate-100">
      <div className="mx-auto w-full max-w-none space-y-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Peer Compare</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">{pageTitle}</h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">
              Compare tickers to see which setup has stronger support across fundamentals, valuation, price action, catalysts, risks and Walnut&apos;s proprietary confirmation score.
            </p>
          </div>
          {hasLeft ? (
            <Link href={tickerHref(left) || "/"} className={ghostButtonClassName}>
              Back to ticker
            </Link>
          ) : null}
          {data && canCreateResearch ? (
            <ResearchActions canCreateResearch={canCreateResearch} subject={{ kind: "compare", data }} />
          ) : null}
        </div>
        <PeerCompareSelector leftSymbol={left} rightSymbol={right} />
        {data?.status === "locked" ? (
          <>
            <CompareEventOnMount
              eventName="compare_locked_view"
              path={`/compare/${encodeURIComponent(left)}/${encodeURIComponent(right)}`}
              properties={{ ticker_pair: `${left}/${right}`, auth_state: authState.token ? "authenticated" : "logged_out", current_plan: plan }}
            />
            <LockedCompareState data={data} authenticated={Boolean(authState.token)} upgradeHref={upgradeHref} signInHref={signInHref} />
            <CompareContextualCta
              tickerA={left}
              tickerB={right}
              currentPath={currentPath}
              searchParams={sp}
              authState={authState.token ? "authenticated" : "logged_out"}
              plan={plan}
            />
          </>
        ) : data ? (
          <>
            {completedUpgrade ? (
              <CompareEventOnMount
                eventName="compare_unlocked_after_upgrade"
                path={`/compare/${encodeURIComponent(left)}/${encodeURIComponent(right)}`}
                properties={{ ticker_pair: `${left}/${right}`, auth_state: "authenticated", current_plan: plan }}
              />
            ) : null}
            <CompareReport data={data} upgradeHref={upgradeHref} />
            <CompareContextualCta
              tickerA={left}
              tickerB={right}
              currentPath={currentPath}
              searchParams={sp}
              authState={authState.token ? "authenticated" : "logged_out"}
              plan={plan}
            />
          </>
        ) : <CompareError message={errorMessage} />}
      </div>
    </main>
  );
}
