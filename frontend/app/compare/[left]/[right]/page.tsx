import Link from "next/link";
import type { Metadata } from "next";
import { ApiError, getEntitlements, getPeerCompare, type PeerCompareCategory, type PeerCompareMetric, type PeerCompareResponse } from "@/lib/api";
import { ResearchActions } from "@/components/research/ResearchActions";
import { ghostButtonClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";
import { PeerCompareSelector } from "@/components/compare/PeerCompareSelector";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { hasEntitlement } from "@/lib/entitlements";

type PageProps = {
  params: Promise<{ left: string; right: string }>;
};

export const metadata: Metadata = {
  title: "Peer Compare | Walnut",
};

function cleanSymbol(value: string) {
  return decodeURIComponent(value || "").trim().toUpperCase().replace(/\./g, "-");
}

function edgeLabel(edge: "left" | "right" | "even", data: PeerCompareResponse) {
  if (edge === "left") return data.left.symbol;
  if (edge === "right") return data.right.symbol;
  return "Even";
}

function edgeClass(edge: "left" | "right" | "even") {
  if (edge === "left") return "border-cyan-300/40 bg-cyan-300/10 text-cyan-100";
  if (edge === "right") return "border-violet-300/40 bg-violet-300/10 text-violet-100";
  return "border-white/10 bg-white/[0.03] text-slate-300";
}

function formatValue(metric: PeerCompareMetric, side: "left" | "right") {
  const value = side === "left" ? metric.left : metric.right;
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

function SideHeader({ side, winner }: { side: PeerCompareResponse["left"]; winner: boolean }) {
  const href = tickerHref(side.symbol);
  return (
    <div className={`min-w-0 rounded-lg border p-4 ${winner ? "border-emerald-300/40 bg-emerald-300/10" : "border-white/10 bg-slate-950/45"}`}>
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

function CategoryCard({ category, data }: { category: PeerCompareCategory; data: PeerCompareResponse }) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-white">{category.label}</h2>
          {category.locked ? <p className="mt-1 text-xs text-slate-500">Locked: {category.required_plan?.toUpperCase() ?? "PLAN"}</p> : null}
        </div>
        <span className={`shrink-0 rounded-md border px-2.5 py-1 text-xs font-semibold ${edgeClass(category.edge)}`}>
          {category.locked ? "Locked" : edgeLabel(category.edge, data)}
        </span>
      </div>
      {category.metrics.length ? (
        <div className="mt-4 overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-left text-sm">
            <thead className="bg-white/[0.03] text-xs uppercase tracking-[0.16em] text-slate-500">
              <tr>
                <th className="px-3 py-2 font-semibold">Metric</th>
                <th className="px-3 py-2 text-right font-semibold">{data.left.symbol}</th>
                <th className="px-3 py-2 text-right font-semibold">{data.right.symbol}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10">
              {category.metrics.map((metric) => (
                <tr key={metric.key}>
                  <td className="px-3 py-2 text-slate-300">{metric.label}</td>
                  <td className={`px-3 py-2 text-right font-medium ${metric.edge === "left" ? "text-cyan-100" : "text-slate-300"}`}>{formatValue(metric, "left")}</td>
                  <td className={`px-3 py-2 text-right font-medium ${metric.edge === "right" ? "text-violet-100" : "text-slate-300"}`}>{formatValue(metric, "right")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  );
}

function CompareReport({ data }: { data: PeerCompareResponse }) {
  const winner = data.call.winner;
  return (
    <div className="space-y-5">
      <div className="grid gap-3 lg:grid-cols-[1fr_1.2fr_1fr]">
        <SideHeader side={data.left} winner={winner === "left"} />
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4 text-center">
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">Our Call</p>
          <h1 className="mt-2 text-2xl font-semibold text-white">
            {winner === "even" ? "Too close to call" : `${data.call.symbol} leads`}
          </h1>
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
        <SideHeader side={data.right} winner={winner === "right"} />
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {data.categories.map((category) => (
          <CategoryCard key={category.key} category={category} data={data} />
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

function CompareError({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-amber-300/20 bg-amber-300/10 p-4">
      <h1 className="text-lg font-semibold text-white">Choose a valid peer</h1>
      <p className="mt-2 text-sm text-amber-100">{message}</p>
    </div>
  );
}

export default async function PeerComparePage({ params }: PageProps) {
  const routeParams = await params;
  const left = cleanSymbol(routeParams.left);
  const right = cleanSymbol(routeParams.right);
  const authState = await optionalPageAuthState();
  const entitlements = authState.token
    ? await getEntitlements(authState.token, { source: "PeerCompareResearchGate" }).catch(() => null)
    : null;
  const canCreateResearch = entitlements ? hasEntitlement(entitlements, "institutional_feed") : false;
  let data: PeerCompareResponse | null = null;
  let errorMessage = "This comparison could not be loaded.";

  try {
    if (right !== "_") {
      data = await getPeerCompare(left, right, { authToken: authState.token ?? undefined, source: "PeerComparePage" });
    } else {
      errorMessage = "Search for a second ticker to compare against.";
    }
  } catch (error) {
    if (error instanceof ApiError) {
      errorMessage = typeof error.detail === "string" ? error.detail : "One of these symbols is unavailable for comparison.";
    }
  }

  return (
    <main className="min-h-screen bg-[#06111f] px-4 py-6 text-slate-100 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-7xl space-y-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Peer Compare</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">{left} vs {right === "_" ? "..." : right}</h1>
          </div>
          <Link href={tickerHref(left) || "/"} className={ghostButtonClassName}>
            Back to ticker
          </Link>
          {data && canCreateResearch ? (
            <ResearchActions canCreateResearch={canCreateResearch} subject={{ kind: "compare", data }} />
          ) : null}
        </div>
        <PeerCompareSelector leftSymbol={left} rightSymbol={right} />
        {data ? <CompareReport data={data} /> : <CompareError message={errorMessage} />}
      </div>
    </main>
  );
}
