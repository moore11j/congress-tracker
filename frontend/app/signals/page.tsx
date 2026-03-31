import Link from "next/link";
import { Badge } from "@/components/Badge";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import { chamberBadge } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { memberHref } from "@/lib/memberSlug";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge, resolveInsiderDisplayName } from "@/lib/insiderRole";
import { tickerHref } from "@/lib/ticker";
import { tickerMonoLinkClassName } from "@/lib/styles";
import { Suspense } from "react";

type SearchParams = Record<string, string | string[] | undefined>;

type SignalItem = {
  kind?: "congress" | "insider" | string;
  event_id: number;
  ts: string;
  symbol: string;
  who?: string;
  position?: string;
  reporting_cik?: string | null;
  reportingCik?: string | null;
  member_bioguide_id?: string;
  party?: string;
  chamber?: string;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  smart_score?: number;
  smart_band?: string;
  source?: string;
  confirmation_30d?: {
    congress_active_30d: boolean;
    insider_active_30d: boolean;
    congress_trade_count_30d: number;
    insider_trade_count_30d: number;
    insider_buy_count_30d: number;
    insider_sell_count_30d: number;
    cross_source_confirmed_30d: boolean;
    repeat_congress_30d: boolean;
    repeat_insider_30d: boolean;
  } | null;
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

function clampMode(modeRaw: string): "all" | "congress" | "insider" {
  if (modeRaw === "all" || modeRaw === "congress" || modeRaw === "insider") return modeRaw;
  return "all";
}


function isInsiderSignalKind(kind?: string): boolean {
  const normalized = (kind ?? "").trim().toLowerCase();
  return normalized === "insider" || normalized === "insider_trade";
}

function resolveSignalReportingCik(item: SignalItem): string | null {
  return item.reporting_cik ?? item.reportingCik ?? null;
}

function clampSide(sideRaw: string): "all" | "buy" | "sell" | "buy_or_sell" | "award" | "inkind" | "exempt" {
  if (
    sideRaw === "all" ||
    sideRaw === "buy" ||
    sideRaw === "sell" ||
    sideRaw === "buy_or_sell" ||
    sideRaw === "award" ||
    sideRaw === "inkind" ||
    sideRaw === "exempt"
  ) {
    return sideRaw;
  }
  return "all";
}

function clampLimit(limitRaw: string): 25 | 50 | 100 {
  const n = Number(limitRaw);
  if (n === 25 || n === 50 || n === 100) return n;
  return 50;
}

function clampSort(sortRaw: string): "multiple" | "smart" | "recent" | "amount" {
  if (sortRaw === "multiple" || sortRaw === "smart" || sortRaw === "recent" || sortRaw === "amount") return sortRaw;
  return "smart";
}

function clampConfirm(confirmRaw: string): "all" | "cross" | "single" {
  if (confirmRaw === "all" || confirmRaw === "cross" || confirmRaw === "single") return confirmRaw;
  return "all";
}

function isTrue(v: string): boolean {
  const s = v.toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

function buildPageHref(params: {
  mode: string;
  side: string;
  preset: string;
  limit: number;
  debug: boolean;
  sort: string;
  confirm: string;
}): string {
  const u = new URL("https://local/signals");
  u.searchParams.set("mode", params.mode);
  u.searchParams.set("side", params.side);
  u.searchParams.set("preset", params.preset);
  u.searchParams.set("limit", String(params.limit));
  u.searchParams.set("sort", params.sort);
  u.searchParams.set("confirm", params.confirm);
  if (params.debug) u.searchParams.set("debug", "true");
  return u.pathname + u.search;
}

function buildSignalsUrl(apiBase: string, mode: string, side: string, preset: string, limit: number, debug: boolean, sort: string, confirm: string): string {
  const u = new URL("/api/signals/all", apiBase);
  u.searchParams.set("mode", mode);
  u.searchParams.set("side", side);
  u.searchParams.set("preset", preset);
  u.searchParams.set("limit", String(limit));
  u.searchParams.set("sort", sort);
  u.searchParams.set("confirm", confirm);
  if (debug) u.searchParams.set("debug", "1");
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

function normalizeSide(tradeType?: string | null): "buy" | "sell" | "buy_or_sell" | "award" | "inkind" | "exempt" | "return" | null {
  const t = (tradeType ?? "").trim().toLowerCase();
  if (!t) return null;

  if (t === "buy_or_sell" || t === "buy-sell" || t === "buy/sell") return "buy_or_sell";
  if (t === "d-return" || t === "return") return "return";
  if (t === "purchase" || t === "buy" || t === "p-purchase" || t.includes("purchase")) return "buy";
  if (t === "sale" || t === "sell" || t === "s-sale" || t.includes("sale")) return "sell";
  if (t.startsWith("a-") || t.includes("award")) return "award";
  if (t.startsWith("f-") || t.includes("inkind")) return "inkind";
  if (t.startsWith("m-") || t.includes("exempt")) return "exempt";
  return null;
}

function sideLabel(_kind: string, tradeType?: string): { label: string; klass: string } {
  const side = normalizeSide(tradeType);

  if (side === "buy") {
    return { label: "Buy", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10" };
  }
  if (side === "sell") {
    return { label: "Sell", klass: "border-red-500/30 text-red-200 bg-red-500/10" };
  }
  if (side === "buy_or_sell") {
    return { label: "Buy/Sell", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "award") {
    return { label: "Award", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "inkind") {
    return { label: "InKind", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "exempt") {
    return { label: "Exempt", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "return") {
    return { label: "Return", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }

  const fallback = (tradeType ?? "").trim();
  return { label: fallback ? fallback.toUpperCase() : "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function smartLabel(band?: string, score?: number): { label: string; klass: string; dotClass: string } {
  const b = (band ?? "").toLowerCase();
  if (typeof score !== "number" || !Number.isFinite(score)) {
    return { label: "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30", dotClass: "bg-slate-500" };
  }
  if (b === "strong") {
    return { label: "Strong", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10", dotClass: "bg-emerald-400" };
  }
  if (b === "notable") {
    return { label: "Notable", klass: "border-amber-500/30 text-amber-200 bg-amber-500/10", dotClass: "bg-amber-400" };
  }
  if (b === "mild") {
    return { label: "Mild", klass: "border-orange-500/30 text-orange-200 bg-orange-500/10", dotClass: "bg-orange-400" };
  }
  return { label: "Noise", klass: "border-slate-700 text-slate-300 bg-slate-900/30", dotClass: "bg-slate-500" };
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
  const mode = clampMode(getParam(sp, "mode"));
  const side = clampSide(getParam(sp, "side"));
  const preset = clampPreset(getParam(sp, "preset"));
  const limit = clampLimit(getParam(sp, "limit"));
  const sort = clampSort(getParam(sp, "sort"));
  const confirm = clampConfirm(getParam(sp, "confirm"));
  const debug = isTrue(getParam(sp, "debug"));
  const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://congress-tracker-api.fly.dev";
  const requestUrl = buildSignalsUrl(API_BASE, mode, side, preset, limit, debug, sort, confirm);

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
            <div className="text-xs text-slate-400">Mode</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "ALL"],
                ["congress", "CONGRESS"],
                ["insider", "INSIDER"],
              ] as const).map(([m, label]) => (
                <Link
                  key={m}
                  href={buildPageHref({ mode: m, side, preset, limit, debug, sort, confirm })}
                  className={`${btn} ${mode === m ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="text-xs text-slate-400">Side</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "All"],
                ["buy", "Buy"],
                ["sell", "Sell"],
                ["buy_or_sell", "Buy/Sell"],
                ["award", "Award"],
                ["inkind", "InKind"],
                ["exempt", "Exempt"],
              ] as const).map(([s, label]) => (
                <Link
                  key={s}
                  href={buildPageHref({ mode, side: s, preset, limit, debug, sort, confirm })}
                  className={`${btn} ${side === s ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="text-xs text-slate-400">Preset</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {(["discovery", "balanced", "strict"] as const).map((p) => (
                <Link
                  key={p}
                  href={buildPageHref({ mode, side, preset: p, limit, debug, sort, confirm })}
                  className={`${btn} ${preset === p ? btnActive : btnIdle}`}
                >
                  {p.toUpperCase()}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Limit</div>
            <div className="inline-flex items-center gap-2">
              {[25, 50, 100].map((l) => (
                <Link
                  key={l}
                  href={buildPageHref({ mode, side, preset, limit: l, debug, sort, confirm })}
                  className={`${btn} ${limit === l ? btnActive : btnIdle}`}
                >
                  {l}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Sort</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["multiple", "MULTIPLE"],
                ["smart", "SMART"],
                ["recent", "RECENT"],
                ["amount", "AMOUNT"],
              ] as const).map(([s, label]) => (
                <Link
                  key={s}
                  href={buildPageHref({ mode, side, preset, limit, debug, sort: s, confirm })}
                  className={`${btn} ${sort === s ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Confirm</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "ALL"],
                ["cross", "CROSS-SOURCE"],
                ["single", "SINGLE-SOURCE"],
              ] as const).map(([c, label]) => (
                <Link
                  key={c}
                  href={buildPageHref({ mode, side, preset, limit, debug, sort, confirm: c })}
                  className={`${btn} ${confirm === c ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

          </div>

          <div className="flex items-center gap-2">
            <span className={`${pill} border-slate-800 text-slate-200 bg-slate-950/30`}>
              <span className="text-white">Updating results</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              mode <span className="text-white">{mode}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              preset <span className="text-white">{preset}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              side <span className="text-white">{side}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              sort <span className="text-white">{sort}</span>
            </span>
            {confirm !== "all" ? (
              <span className={`${pill} border-cyan-500/30 text-cyan-100 bg-cyan-500/10`}>
                confirm <span className="text-white">{confirm}</span>
              </span>
            ) : null}
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="mt-6">
        <div className="mb-3">
          <h2 className="text-xl font-semibold text-white">Signals table</h2>
          <p className="text-sm text-slate-400">Abnormal trades vs per-symbol historical median.</p>
        </div>
        <Suspense key={requestUrl} fallback={<SignalsResultsFallback card={card} />}>
          <SignalsResultsSection requestUrl={requestUrl} card={card} pill={pill} />
        </Suspense>
      </div>
    </div>
  );
}

function SignalsResultsFallback({ card }: { card: string }) {
  return (
    <div className={`${card} min-h-[32rem] overflow-hidden p-4`} aria-live="polite" aria-busy="true">
      <div className="mb-4 flex items-center justify-between">
        <SkeletonBlock className="h-4 w-36" />
        <SkeletonBlock className="h-4 w-28" />
      </div>
      <SkeletonTable columns={9} rows={8} />
    </div>
  );
}

async function SignalsResultsSection({ requestUrl, card, pill }: { requestUrl: string; card: string; pill: string }) {
  let errorMessage: string | null = null;
  let items: SignalItem[] = [];
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
      }
    }
  } catch (e) {
    errorMessage = e instanceof Error ? e.message : "Unable to load signals.";
  }

  return (
    <div className={`${card} min-h-[32rem] overflow-hidden`}>
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
              <th className="px-4 py-3 text-left">Smart</th>
              <th className="px-4 py-3 text-left">Source</th>
              <th className="px-4 py-3 text-left">Confirm</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {items.length === 0 ? (
              <tr>
                <td className="px-4 py-10 text-center text-slate-400" colSpan={10}>
                  {errorMessage ? "Unable to load signals." : "No unusual signals returned."}
                </td>
              </tr>
            ) : (
              items.map((it) => {
                const side = sideLabel(it.kind ?? "", it.trade_type);
                const smart = smartLabel(it.smart_band, it.smart_score);
                const source = sourceBadge(it);
                const isInsider = isInsiderSignalKind(it.kind);
                const rawPos = it.position ?? null;
                const roleCode = normalizeInsiderRoleBadge(rawPos);
                const roleTone = insiderRoleBadgeTone(roleCode);
                const insiderName = getInsiderDisplayName(resolveInsiderDisplayName(it.who, rawPos));
                const insiderProfileHref = insiderHref(insiderName, resolveSignalReportingCik(it));
                return (
                  <tr key={it.event_id} className="hover:bg-slate-900/20">
                    <td className="px-4 py-3 text-slate-300"><span title={it.ts}>{it.ts}</span></td>
                    <td className="px-4 py-3">
                      {tickerHref(it.symbol) ? (
                        <Link href={tickerHref(it.symbol)!} className={tickerMonoLinkClassName}>{it.symbol}</Link>
                      ) : (
                        <span className="font-mono text-slate-300">{it.symbol}</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-slate-200">
                      {isInsider ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span title={rawPos ?? undefined}><Badge tone={roleTone}>{roleCode}</Badge></span>
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} className="min-w-0 truncate text-slate-100 hover:underline">{insiderName ?? "—"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{insiderName ?? "—"}</span>
                          )}
                        </div>
                      ) : (
                        <>
                          <span className="mr-2 inline-flex align-middle"><Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge></span>
                          {it.member_bioguide_id ? (
                            <Link href={memberHref({ name: it.who, memberId: it.member_bioguide_id })} className="hover:underline">{it.who ?? "—"}</Link>
                          ) : (
                            it.who ?? "—"
                          )}
                        </>
                      )}
                    </td>
                    <td className="px-4 py-3"><span className={`${pill} ${side.klass}`}>{side.label}</span></td>
                    <td className="px-4 py-3 text-slate-200" title={`${formatUSD(it.amount_min)} – ${formatUSD(it.amount_max)}`}>{formatUSD(it.amount_max)}</td>
                    <td className="px-4 py-3 text-slate-200">{formatUSD(it.baseline_median_amount_max)}</td>
                    <td className="px-4 py-3 text-slate-200">{formatMultiple(it.unusual_multiple)}</td>
                    <td className="px-4 py-3">
                      <span className={`${pill} ${smart.klass}`}>
                        <span className={`h-2 w-2 rounded-full ${smart.dotClass}`} />
                        <span className="font-mono">{typeof it.smart_score === "number" && Number.isFinite(it.smart_score) ? it.smart_score : "—"}</span>
                        <span className="opacity-80">{smart.label}</span>
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      {isInsider ? (
                        <Badge tone="insider_default" className="px-2 py-0.5 text-[10px]">INSIDER</Badge>
                      ) : (
                        <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      {it.confirmation_30d?.cross_source_confirmed_30d ? (
                        <Badge tone="neutral" className="border-cyan-400/25 bg-cyan-400/10 text-[10px] text-cyan-100">Cross-source 30D</Badge>
                      ) : (
                        <span className="text-xs text-slate-500">—</span>
                      )}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
