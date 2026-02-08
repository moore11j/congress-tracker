import Link from "next/link";
import { Badge } from "@/components/Badge";
import { chamberBadge } from "@/lib/format";

type SearchParams = Record<string, string | string[] | undefined>;

type SignalItem = {
  event_id: number;
  ts: string;
  symbol: string;
  member_name?: string;
  member_bioguide_id?: string;
  party?: string;
  chamber?: string;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  source?: string;
};

type SignalsWrappedResponse = {
  items?: SignalItem[];
  debug?: any;
};

function getParam(sp: SearchParams, key: string): string {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function clampPreset(preset: string): "discovery" | "balanced" | "strict" {
  if (preset === "discovery" || preset === "balanced" || preset === "strict") return preset;
  return "balanced";
}

function clampLimit(limitRaw: string): 25 | 50 | 100 {
  const n = Number(limitRaw);
  if (n === 25 || n === 50 || n === 100) return n;
  return 50;
}

function isTrue(v: string): boolean {
  const s = v.toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

function buildPageHref(preset: string, limit: number, debug: boolean): string {
  const u = new URL("https://local/signals");
  u.searchParams.set("preset", preset);
  u.searchParams.set("limit", String(limit));
  if (debug) u.searchParams.set("debug", "true");
  return u.pathname + u.search;
}

function buildSignalsUrl(apiBase: string, preset: string, limit: number, debug: boolean): string {
  const u = new URL("/api/signals/unusual", apiBase);
  u.searchParams.set("preset", preset);
  u.searchParams.set("limit", String(limit));
  if (debug) u.searchParams.set("debug", "true");
  return u.toString();
}

function formatUSD(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatMultiple(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}×`;
}

function sideLabel(tradeType?: string): { label: string; klass: string } {
  const t = (tradeType ?? "").toLowerCase();
  if (t === "purchase" || t === "buy") {
    return { label: "Buy", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10" };
  }
  if (t === "sale" || t === "sell") {
    return { label: "Sell", klass: "border-red-500/30 text-red-200 bg-red-500/10" };
  }
  return { label: tradeType ? tradeType : "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function strengthLabel(m?: number): { label: string; klass: string } {
  const x = typeof m === "number" ? m : 0;
  if (x >= 8) return { label: "Whale", klass: "border-rose-500/30 text-rose-200 bg-rose-500/10" };
  if (x >= 4) return { label: "Extreme", klass: "border-orange-500/30 text-orange-200 bg-orange-500/10" };
  if (x >= 2) return { label: "Abnormal", klass: "border-amber-500/30 text-amber-200 bg-amber-500/10" };
  if (x >= 1.5) return { label: "Elevated", klass: "border-sky-500/30 text-sky-200 bg-sky-500/10" };
  return { label: "Normal", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function sourceBadge(item: SignalItem): { label: string; tone: Parameters<typeof Badge>[0]["tone"] } {
  const chamber = (item.chamber ?? "").toLowerCase();
  if (chamber.includes("house")) return chamberBadge("house");
  if (chamber.includes("senate")) return chamberBadge("senate");

  const source = (item.source ?? "").toLowerCase();
  if (source.includes("house")) return chamberBadge("house");
  if (source.includes("senate")) return chamberBadge("senate");
  return chamberBadge();
}

export default async function SignalsPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const preset = clampPreset(getParam(sp, "preset"));
  const limit = clampLimit(getParam(sp, "limit"));
  const debug = isTrue(getParam(sp, "debug"));

  const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://congress-tracker-api.fly.dev";
  const requestUrl = buildSignalsUrl(API_BASE, preset, limit, debug);

  let errorMessage: string | null = null;

  // Support BOTH API shapes:
  // A) array of items: [...]
  // B) wrapped: { items: [...], debug: {...} }
  let items: SignalItem[] = [];
  let debugObj: any = null;

  try {
    const res = await fetch(requestUrl, { cache: "no-store" });

    if (!res.ok) {
      errorMessage = `Request failed with ${res.status}`;
    } else {
      const json: unknown = await res.json();

      if (Array.isArray(json)) {
        items = json as SignalItem[];
      } else {
        const obj = json as SignalsWrappedResponse;
        items = Array.isArray(obj.items) ? obj.items : [];
        debugObj = obj.debug ?? null;
      }
    }
  } catch (e) {
    errorMessage = e instanceof Error ? e.message : "Unable to load signals.";
  }

  const card = "rounded-2xl border border-slate-800 bg-slate-950/40 shadow-sm";
  const pill = "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium";
  const btn =
    "inline-flex items-center justify-center rounded-full border px-3 py-1 text-xs font-medium transition hover:bg-slate-900/60";
  const btnActive = "border-emerald-500/40 text-emerald-200 bg-emerald-500/10";
  const btnIdle = "border-slate-800 text-slate-200 bg-slate-950/30";

  return (
    <div className="space-y-8">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">SIGNALS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Unusual trade radar</h1>
        <p className="mt-2 max-w-2xl text-sm text-slate-300/80">
          Presets for quick scanning, with optional debug transparency.
        </p>
      </div>

      {/* Controls */}
      <div className={`mt-6 p-4 ${card}`}>
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-3">
            <div className="text-xs text-slate-400">Preset</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {(["discovery", "balanced", "strict"] as const).map((p) => (
                <Link key={p} href={buildPageHref(p, limit, debug)} className={`${btn} ${preset === p ? btnActive : btnIdle}`}>
                  {p.toUpperCase()}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Limit</div>
            <div className="inline-flex items-center gap-2">
              {[25, 50, 100].map((l) => (
                <Link key={l} href={buildPageHref(preset, l, debug)} className={`${btn} ${limit === l ? btnActive : btnIdle}`}>
                  {l}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Debug</div>
            <Link href={buildPageHref(preset, limit, !debug)} className={`${btn} ${debug ? btnActive : btnIdle}`}>
              {debug ? "ON" : "OFF"}
            </Link>
          </div>

          <div className="flex items-center gap-2">
            <span className={`${pill} border-slate-800 text-slate-200 bg-slate-950/30`}>
              Showing <span className="text-white">{items.length}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              preset <span className="text-white">{preset}</span>
            </span>
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="mt-6">
        <div className="mb-3">
          <h2 className="text-xl font-semibold text-white">Signals table</h2>
          <p className="text-sm text-slate-400">Abnormal trades vs per-symbol historical median.</p>
        </div>

        <div className={`${card} overflow-hidden`}>
          <div className="overflow-x-auto">
            <table className="min-w-full border-collapse text-sm">
              <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
                <tr>
                  <th className="px-4 py-3 text-left">Time</th>
                  <th className="px-4 py-3 text-left">Ticker</th>
                  <th className="px-4 py-3 text-left">Member</th>
                  <th className="px-4 py-3 text-left">Side</th>
                  <th className="px-4 py-3 text-left">Amount</th>
                  <th className="px-4 py-3 text-left">Baseline</th>
                  <th className="px-4 py-3 text-left">Multiple</th>
                  <th className="px-4 py-3 text-left">Strength</th>
                  <th className="px-4 py-3 text-left">Source</th>
                </tr>
              </thead>

              <tbody className="divide-y divide-slate-800">
                {items.length === 0 ? (
                  <tr>
                    <td className="px-4 py-10 text-center text-slate-400" colSpan={9}>
                      {errorMessage ? "Unable to load signals." : "No unusual signals returned."}
                    </td>
                  </tr>
                ) : (
                  items.map((it) => {
                    const side = sideLabel(it.trade_type);
                    const strength = strengthLabel(it.unusual_multiple);
                    const source = sourceBadge(it);

                    return (
                      <tr key={it.event_id} className="hover:bg-slate-900/20">
                        <td className="px-4 py-3 text-slate-300">
                          <span title={it.ts}>{it.ts}</span>
                        </td>
                        <td className="px-4 py-3">
                          <Link href={`/tickers/${it.symbol}`} className="font-mono text-emerald-200 hover:underline">
                            {it.symbol}
                          </Link>
                        </td>
                        <td className="px-4 py-3 text-slate-200">
                          {it.member_bioguide_id ? (
                            <Link href={`/members/${it.member_bioguide_id}`} className="hover:underline">
                              {it.member_name ?? "—"}
                            </Link>
                          ) : (
                            it.member_name ?? "—"
                          )}
                        </td>
                        <td className="px-4 py-3">
                          <span className={`${pill} ${side.klass}`}>{side.label}</span>
                        </td>
                        <td className="px-4 py-3 text-slate-200" title={`${formatUSD(it.amount_min)} – ${formatUSD(it.amount_max)}`}>
                          {formatUSD(it.amount_max)}
                        </td>
                        <td className="px-4 py-3 text-slate-200">{formatUSD(it.baseline_median_amount_max)}</td>
                        <td className="px-4 py-3 text-slate-200">{formatMultiple(it.unusual_multiple)}</td>
                        <td className="px-4 py-3">
                          <span className={`${pill} ${strength.klass}`}>{strength.label}</span>
                        </td>
                        <td className="px-4 py-3">
                          <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">
                            {source.label}
                          </Badge>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>

          {debug && debugObj && (
            <details className="border-t border-slate-800 bg-slate-950/30 p-4">
              <summary className="cursor-pointer text-sm text-slate-200">Debug info</summary>
              <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-slate-800 bg-slate-950/50 p-3 text-xs text-slate-400">
                {JSON.stringify(debugObj, null, 2)}
              </pre>
            </details>
          )}
        </div>
      </div>
    </div>
  );
}
