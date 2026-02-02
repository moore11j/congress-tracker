import Link from "next/link";
import { Badge } from "@/components/Badge";
import { formatCurrency, formatCurrencyRange, formatSymbol } from "@/lib/format";
import { cardClassName, pillClassName, primaryButtonClassName } from "@/lib/styles";
import { SignalsControls } from "@/app/signals/signals-controls";

type SignalItem = {
  event_id: number;
  ts: string;
  symbol: string;
  member_name: string;
  member_bioguide_id: string;
  party?: string | null;
  chamber?: string | null;
  trade_type?: string | null;
  amount_min?: number | null;
  amount_max?: number | null;
  baseline_median_amount_max?: number | null;
  baseline_count?: number | null;
  unusual_multiple?: number | null;
  source?: string | null;
};

type SignalsResponse = {
  items: SignalItem[];
  debug?: Record<string, unknown>;
};

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

const presets = ["discovery", "balanced", "strict"] as const;
const limits = [25, 50, 100] as const;

export const dynamic = "force-dynamic";

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function resolvePreset(value: string) {
  if (presets.includes(value as (typeof presets)[number])) {
    return value as (typeof presets)[number];
  }
  return "balanced";
}

function resolveLimit(value: string) {
  const parsed = Number(value);
  if (limits.includes(parsed as (typeof limits)[number])) {
    return parsed as (typeof limits)[number];
  }
  return 50;
}

function resolveDebug(value: string) {
  return value.toLowerCase() === "true";
}

function buildSignalsUrl(preset: string, limit: number, debug: boolean) {
  const url = new URL("https://congress-tracker-api.fly.dev/api/signals/unusual");
  if (preset) url.searchParams.set("preset", preset);
  if (limit) url.searchParams.set("limit", String(limit));
  if (debug) url.searchParams.set("debug", "true");
  return url.toString();
}

function formatRelativeTime(iso: string | null) {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";

  const now = Date.now();
  const diffMs = date.getTime() - now;
  const diffSeconds = Math.round(diffMs / 1000);
  const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

  const units: Array<[Intl.RelativeTimeFormatUnit, number]> = [
    ["year", 60 * 60 * 24 * 365],
    ["month", 60 * 60 * 24 * 30],
    ["day", 60 * 60 * 24],
    ["hour", 60 * 60],
    ["minute", 60],
    ["second", 1],
  ];

  for (const [unit, secondsInUnit] of units) {
    if (Math.abs(diffSeconds) >= secondsInUnit || unit === "second") {
      return rtf.format(Math.round(diffSeconds / secondsInUnit), unit);
    }
  }
  return "—";
}

function formatDateTime(iso: string | null) {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString();
}

function formatSide(value?: string | null) {
  const cleaned = (value ?? "").toLowerCase();
  if (cleaned === "purchase" || cleaned === "buy") return { label: "Buy", tone: "pos" as const };
  if (cleaned === "sale" || cleaned === "sell") return { label: "Sell", tone: "neg" as const };
  if (!cleaned) return { label: "—", tone: "neutral" as const };
  return { label: cleaned.replace(/_/g, " "), tone: "neutral" as const };
}

function formatMultiple(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

function strengthBadge(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return { label: "—", className: "border-white/10 bg-white/5 text-slate-300" };
  }
  if (value >= 8) return { label: "Whale", className: "border-emerald-400/40 bg-emerald-400/20 text-emerald-100" };
  if (value >= 4) return { label: "Extreme", className: "border-rose-400/40 bg-rose-400/20 text-rose-100" };
  if (value >= 2) return { label: "Abnormal", className: "border-amber-400/40 bg-amber-400/20 text-amber-100" };
  if (value >= 1.5) return { label: "Elevated", className: "border-sky-400/40 bg-sky-400/20 text-sky-100" };
  return { label: "Normal", className: "border-white/10 bg-white/5 text-slate-300" };
}

function formatMemberName(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : "Unknown";
}

function formatSource(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : "—";
}

export default async function SignalsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const preset = resolvePreset(getParam(sp, "preset"));
  const limit = resolveLimit(getParam(sp, "limit"));
  const debug = resolveDebug(getParam(sp, "debug"));

  const requestUrl = buildSignalsUrl(preset, limit, debug);

  let data: SignalsResponse = { items: [] };
  let errorMessage: string | null = null;

  try {
    const response = await fetch(requestUrl, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Request failed with ${response.status}`);
    }
    data = (await response.json()) as SignalsResponse;
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "Unable to load signals.";
  }

  const retryParams = new URLSearchParams();
  if (preset) retryParams.set("preset", preset);
  if (limit) retryParams.set("limit", String(limit));
  if (debug) retryParams.set("debug", "true");
  const retryHref = retryParams.toString() ? `/signals?${retryParams.toString()}` : "/signals";

  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Unusual signals</p>
          <h1 className="text-3xl font-semibold text-white">Unusual trade radar.</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Scan anomalous congressional trades against historical baselines. Tuned for quick, terminal-like triage.
          </p>
        </div>
        <SignalsControls preset={preset} limit={limit} debug={debug} />
      </section>

      {errorMessage ? (
        <section className={cardClassName}>
          <div className="space-y-3">
            <div className="text-sm font-semibold text-rose-200">Unable to load unusual signals</div>
            <p className="text-sm text-slate-300">{errorMessage}</p>
            <Link href={retryHref} className={primaryButtonClassName}>
              Retry
            </Link>
          </div>
        </section>
      ) : (
        <section className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-white">Signals table</h2>
              <p className="text-sm text-slate-400">
                Showing {data.items.length} items · preset {preset}
              </p>
            </div>
            <span className={pillClassName}>limit {limit}</span>
          </div>

          <div className="overflow-hidden rounded-3xl border border-white/10 bg-slate-900/60">
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-left text-xs text-slate-200">
                <thead className="bg-white/5 text-[11px] uppercase tracking-[0.2em] text-slate-400">
                  <tr>
                    <th className="px-4 py-3">Time</th>
                    <th className="px-4 py-3">Ticker</th>
                    <th className="px-4 py-3">Member</th>
                    <th className="px-4 py-3">Side</th>
                    <th className="px-4 py-3">Amount</th>
                    <th className="px-4 py-3">Baseline Median</th>
                    <th className="px-4 py-3">Multiple</th>
                    <th className="px-4 py-3">Strength</th>
                    <th className="px-4 py-3">Source</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {data.items.length === 0 ? (
                    <tr>
                      <td colSpan={9} className="px-4 py-6 text-center text-sm text-slate-400">
                        No unusual signals returned.
                      </td>
                    </tr>
                  ) : (
                    data.items.map((item) => {
                      const side = formatSide(item.trade_type);
                      const multiple = formatMultiple(item.unusual_multiple);
                      const strength = strengthBadge(item.unusual_multiple);
                      const amountTooltip = formatCurrencyRange(item.amount_min ?? null, item.amount_max ?? null);
                      const symbol = formatSymbol(item.symbol);
                      const memberName = formatMemberName(item.member_name);
                      const memberId = item.member_bioguide_id?.trim();

                      return (
                        <tr key={item.event_id} className="hover:bg-white/5">
                          <td className="px-4 py-3 font-mono text-[11px] text-slate-300">
                            <time title={formatDateTime(item.ts)}>{formatRelativeTime(item.ts)}</time>
                          </td>
                          <td className="px-4 py-3">
                            {symbol !== "—" ? (
                              <Link href={`/tickers/${symbol}`} className="font-semibold text-emerald-200">
                                {symbol}
                              </Link>
                            ) : (
                              <span className="text-slate-400">—</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            {memberId ? (
                              <Link href={`/members/${memberId}`} className="text-sm font-semibold text-slate-100">
                                {memberName}
                              </Link>
                            ) : (
                              <span className="text-slate-400">{memberName}</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            <Badge tone={side.tone}>{side.label}</Badge>
                          </td>
                          <td className="px-4 py-3" title={amountTooltip}>
                            <span className="font-semibold text-white">{formatCurrency(item.amount_max ?? null)}</span>
                          </td>
                          <td className="px-4 py-3 text-slate-300">
                            {formatCurrency(item.baseline_median_amount_max ?? null)}
                          </td>
                          <td className="px-4 py-3 font-mono text-slate-200">{multiple}</td>
                          <td className="px-4 py-3">
                            <span
                              className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide ${
                                strength.className
                              }`}
                            >
                              {strength.label}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-[11px] uppercase tracking-[0.2em] text-slate-400">
                            {formatSource(item.source)}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {debug && data.debug ? (
            <details className={cardClassName}>
              <summary className="cursor-pointer text-sm font-semibold text-slate-100">Debug Info</summary>
              <pre className="mt-4 max-h-96 overflow-auto rounded-2xl border border-white/10 bg-black/40 p-4 text-[11px] text-emerald-200">
                {JSON.stringify(data.debug, null, 2)}
              </pre>
            </details>
          ) : null}
        </section>
      )}
    </div>
  );
}
