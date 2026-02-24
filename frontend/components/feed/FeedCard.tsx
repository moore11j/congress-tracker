import Link from "next/link";
import type { FeedItem } from "@/lib/types";
import { Badge } from "@/components/Badge";
import {
  chamberBadge,
  formatCurrencyRange,
  formatDateShort,
  formatSymbol,
  formatTransactionLabel,
  memberTag,
  partyBadge,
  transactionTone,
} from "@/lib/format";

type FeedCardInsiderItem = FeedItem & {
  trade_type?: string | null;
  amount_min?: number | string | null;
  amount_max?: number | string | null;
  payload?: {
    transaction_type?: string | null;
    transaction_date?: string | null;
    filing_date?: string | null;
    shares?: number | string | null;
    price?: number | string | null;
    raw?: {
      transactionType?: string | null;
      transactionDate?: string | null;
      filingDate?: string | null;
      acquisitionOrDisposition?: string | null;
      securitiesTransacted?: number | string | null;
      transactionShares?: number | string | null;
      price?: number | string | null;
      typeOfOwner?: string | null;
      officerTitle?: string | null;
      insiderRole?: string | null;
      position?: string | null;
      securityName?: string | null;
    };
  };
  insider?: FeedItem["insider"] & {
    transaction_type?: string | null;
    shares?: number | string | null;
  };
};

type WhaleMode = "off" | "500k" | "1m" | "5m";

type WhaleTier = 0 | 1 | 2 | 3;

const whaleMinTierMap: Record<WhaleMode, WhaleTier> = {
  off: 0,
  "500k": 1,
  "1m": 2,
  "5m": 3,
};

function tierClassFor(tier: WhaleTier): string {
  if (tier === 1) return "bg-white/20";
  if (tier === 2) return "bg-sky-400/50";
  if (tier === 3) return "bg-amber-400/60";
  return "";
}

function parseNum(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const cleaned = v.replace(/[$,]/g, "").trim();
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function formatShares(n: number): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: Number.isInteger(n) ? 0 : 2,
  }).format(n);
}

function formatMoney(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function netClass(net: number): string {
  if (net > 0) return "text-emerald-400";
  if (net < 0) return "text-rose-400";
  return "text-slate-400";
}

function pnlClass(p: number, highlighted: boolean) {
  const base =
    p > 0 ? "text-emerald-400" : p < 0 ? "text-rose-400" : "text-slate-400";

  // Whale highlight increases weight only — never overrides color
  return highlighted ? `${base} font-bold` : `${base} font-semibold`;
}

function formatPnl(p: number): string {
  const arrow = p > 0 ? "▲" : p < 0 ? "▼" : "•";
  return `${arrow} ${p.toFixed(1)}%`;
}

function formatYMD(ymd?: string | null): string {
  if (!ymd) return "—";
  const s = ymd.slice(0, 10);
  const [y, m, d] = s.split("-").map(Number);
  if (!y || !m || !d) return s;
  const dt = new Date(Date.UTC(y, m - 1, d));
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
    timeZone: "UTC",
  }).format(dt);
}

function daysBetweenYMD(a?: string | null, b?: string | null): number | null {
  if (!a || !b) return null;
  const aa = a.slice(0, 10);
  const bb = b.slice(0, 10);
  const [ay, am, ad] = aa.split("-").map(Number);
  const [by, bm, bd] = bb.split("-").map(Number);
  if (!ay || !am || !ad || !by || !bm || !bd) return null;
  const t1 = Date.UTC(ay, am - 1, ad);
  const t2 = Date.UTC(by, bm - 1, bd);
  const diff = Math.round((t2 - t1) / (1000 * 60 * 60 * 24));
  return Number.isFinite(diff) ? diff : null;
}

function normalizeSecurityClass(
  securityName: string | undefined,
): string | null {
  if (!securityName) return null;
  const trimmed = securityName.trim();
  if (!trimmed) return null;
  const value = trimmed.toLowerCase();
  if (value === "common stock") return "Common";
  if (value === "preferred stock") return "Preferred";
  return trimmed;
}

function getInsiderKind(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;
  const rawDirection =
    insiderItem.trade_type ??
    insiderItem.insider?.transaction_type ??
    insiderItem.payload?.transaction_type ??
    insiderItem.payload?.raw?.transactionType ??
    item.transaction_type ??
    "";
  const t = rawDirection.toUpperCase();
  const ad =
    insiderItem.payload?.raw?.acquisitionOrDisposition?.toUpperCase() ?? "";

  if (t.startsWith("P-") || t.startsWith("P") || t.includes("PURCHASE"))
    return "purchase";
  if (t.startsWith("S-") || t.startsWith("S") || t.includes("SALE"))
    return "sale";
  if (ad === "A") return "purchase";
  if (ad === "D") return "sale";
  return null;
}

function getInsiderValue(item: FeedItem) {
  const insiderItem = item as FeedCardInsiderItem;

  const totalValue = parseNum(
    item.amount_range_min ??
      insiderItem.amount_min ??
      item.amount_range_max ??
      insiderItem.amount_max,
  );
  const shares = parseNum(
    insiderItem.payload?.shares ??
      insiderItem.payload?.raw?.securitiesTransacted,
  );
  const price = parseNum(
    insiderItem.insider?.price ??
      insiderItem.payload?.price ??
      insiderItem.payload?.raw?.price,
  );

  return {
    totalValue,
    price: price && price > 0 ? price : null,
    shares: shares && shares > 0 ? shares : null,
  };
}

function getInsiderRoleBadge(item: FeedItem): string {
  const insiderItem = item as FeedCardInsiderItem;
  const raw =
    insiderItem.insider?.role ??
    insiderItem.payload?.raw?.typeOfOwner ??
    insiderItem.payload?.raw?.officerTitle ??
    insiderItem.payload?.raw?.insiderRole ??
    insiderItem.payload?.raw?.position ??
    "INSIDER";
  const s = raw.toUpperCase();

  if (/\bCHIEF EXECUTIVE OFFICER\b|\bCEO\b/.test(s)) return "CEO";
  if (/\bCHIEF FINANCIAL OFFICER\b|\bCFO\b/.test(s)) return "CFO";
  if (/\bCHIEF OPERATING OFFICER\b|\bCOO\b/.test(s)) return "COO";
  if (/\bCHIEF TECHNOLOGY OFFICER\b|\bCTO\b/.test(s)) return "CTO";
  if (/\bCHIEF COMPLIANCE OFFICER\b|\bCCO\b/.test(s)) return "CCO";
  if (/\bCHIEF LEGAL OFFICER\b|\bCLO\b/.test(s)) return "CLO";
  if (/\bCHIEF ACCOUNTING OFFICER\b|\bCAO\b/.test(s)) return "CAO";
  if (/\bEXECUTIVE VICE PRESIDENT\b|\bEXEC\s+VP\b|\bEVP\b/.test(s))
    return "EVP";
  if (/\bSENIOR VICE PRESIDENT\b|\bSR\s+VP\b|\bSVP\b/.test(s)) return "SVP";
  if (/\bPRESIDENT\b/.test(s)) return "PRES";
  if (/\bVICE PRESIDENT\b|\bVP\b/.test(s)) return "VP";
  if (/\bDIRECTOR\b/.test(s)) return "DIR";
  if (/\bOFFICER\b/.test(s)) return "OFFICER";
  return "INSIDER";
}

function toTitleCase(name?: string | null): string {
  if (!name) return "";
  if (name === name.toUpperCase()) {
    return name.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return name;
}

export function FeedCard({
  item,
  whaleMode = "off",
  density = "default",
  gridPreset = "default",
}: {
  item: FeedItem;
  whaleMode?: WhaleMode;
  density?: "default" | "compact";
  gridPreset?: "default" | "tight";
}) {
  if (!item) return null;

  const kind = item.kind ?? (item as any).event_type;
  const isCongress = kind === "congress_trade";
  const isInsider = kind === "insider_trade";
  const chamber = chamberBadge(item.member?.chamber ?? "—");
  const party = partyBadge(item.member?.party ?? null);
  const tag = memberTag(item.member?.party ?? null, item.member?.state ?? null);
  const insiderKind = isInsider ? getInsiderKind(item) : null;
  const insiderValue = isInsider ? getInsiderValue(item) : null;
  const insiderAmount = insiderValue?.totalValue ?? null;
  const insiderPrice = insiderValue?.price ?? null;
  const insiderShares = insiderValue?.shares ?? null;

  const insiderItem = item as FeedCardInsiderItem;
  const securityClass = isInsider
    ? normalizeSecurityClass(
        insiderItem.payload?.raw?.securityName ?? undefined,
      )
    : null;
  const insiderRoleBadge = isInsider ? getInsiderRoleBadge(item) : null;
  const insiderTxDate =
    insiderItem.payload?.transaction_date ??
    insiderItem.payload?.raw?.transactionDate ??
    item.trade_date;
  const insiderFilingDate =
    insiderItem.payload?.filing_date ??
    insiderItem.payload?.raw?.filingDate ??
    item.report_date;
  const lagDays = isCongress
    ? daysBetweenYMD(item.trade_date, item.report_date)
    : null;
  const congressEstimatedPrice = isCongress
    ? parseNum(item.estimated_price)
    : null;
  const pnl = parseNum((item as any).pnl_pct);
  const ownershipLabel = item.insider?.ownership ?? item.owner_type ?? "—";
  const memberNet30d = parseNum(item.member_net_30d);
  const symbolNet30d = parseNum((item as any).symbol_net_30d);
  const amountText = isInsider
    ? insiderAmount !== null
      ? formatMoney(insiderAmount)
      : "—"
    : (formatCurrencyRange(item.amount_range_min, item.amount_range_max) ??
      "—");
  const tradeValueNumber = isCongress
    ? parseNum(item.amount_range_max)
    : insiderAmount;
  const tier: WhaleTier =
    tradeValueNumber !== null && tradeValueNumber >= 5_000_000
      ? 3
      : tradeValueNumber !== null && tradeValueNumber >= 1_000_000
        ? 2
        : tradeValueNumber !== null && tradeValueNumber >= 500_000
          ? 1
          : 0;
  const highlightEnabled = whaleMode !== "off";
  const minTier = whaleMinTierMap[whaleMode];
  const isHighlighted = highlightEnabled && tier >= minTier;
  const tierClass = tierClassFor(tier);
  const badge = (
    <Badge
      tone={
        isInsider
          ? insiderKind === "purchase"
            ? "pos"
            : "neg"
          : transactionTone(item.transaction_type)
      }
    >
      {isInsider
        ? insiderKind === "purchase"
          ? "Purchase"
          : insiderKind === "sale"
            ? "Sale"
            : "—"
        : (formatTransactionLabel(item.transaction_type) ?? "—")}
    </Badge>
  );

  if (isInsider && !insiderKind) return null;

  const isCompact = density === "compact";
  const gridClassName =
    gridPreset === "tight"
      ? "lg:grid-cols-[minmax(160px,1.1fr)_minmax(180px,1.3fr)_minmax(160px,1fr)_minmax(100px,0.7fr)_80px_160px_80px]"
      : "lg:grid-cols-[minmax(220px,1.3fr)_minmax(260px,1.8fr)_minmax(200px,1.2fr)_minmax(120px,0.8fr)_92px_190px_92px]";

  return (
    <div
      className={`relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card ${isHighlighted ? "ring-1 ring-white/10 border-white/20" : ""}`}
    >
      {isHighlighted && tierClass ? (
        <span
          className={`absolute left-0 top-0 bottom-0 w-[3px] rounded-r-full ${tierClass}`}
        />
      ) : null}
      {isHighlighted ? (
        <span className="pointer-events-none absolute inset-0 bg-white/[0.03]" />
      ) : null}
      <div
        className={`grid min-w-0 gap-y-3 lg:grid lg:min-w-0 lg:items-center lg:gap-y-0 lg:gap-x-5 ${gridClassName}`}
      >
        <div className="min-w-0 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            {isInsider ? (
              <span className="text-lg font-semibold text-white">
                {toTitleCase((item as any).member_name ?? item.member?.name) ||
                  "—"}
              </span>
            ) : (
              <Link
                href={`/member/${item.member?.bioguide_id ?? "event"}`}
                className="text-lg font-semibold text-white hover:text-emerald-200"
              >
                {item.member?.name ?? "—"}
              </Link>
            )}
            {isInsider ? (
              <Badge tone="dem">{insiderRoleBadge}</Badge>
            ) : (
              <Badge tone={party.tone}>{tag}</Badge>
            )}
            {isCongress ? (
              <Badge tone={chamber.tone}>{chamber.label}</Badge>
            ) : null}
          </div>
          {memberNet30d !== null ? (
            <div className="text-xs mt-1 tabular-nums">
              <span className="text-white/40">Net 30D:</span>{" "}
              <span className={netClass(memberNet30d)}>
                {formatMoney(memberNet30d)}
              </span>
            </div>
          ) : null}
        </div>

        <div className="min-w-0 text-sm text-slate-300">
          {isCompact ? (
            <div className="min-w-0 flex items-start gap-3">
              <div className="shrink-0 flex flex-col gap-1">
                {item.security?.symbol ? (
                  <Link
                    href={`/ticker/${formatSymbol(item.security.symbol ?? "—")}`}
                    className="inline-flex items-center justify-center shrink-0 whitespace-nowrap px-2 py-0.5 text-xs font-medium rounded-full bg-white/5 border border-white/10"
                  >
                    {formatSymbol(item.security.symbol ?? "—")}
                  </Link>
                ) : (
                  <span className="inline-flex items-center justify-center shrink-0 whitespace-nowrap px-2 py-0.5 text-xs font-medium rounded-full bg-white/5 border border-white/10">
                    —
                  </span>
                )}
                <div className="max-w-[140px] truncate text-xs text-white/60">
                  {item.security?.name ?? "—"}
                </div>
              </div>
              <div className="min-w-0">
                <div className="truncate text-xs text-white/60">
                  {isInsider
                    ? (securityClass ?? "—")
                    : (item.security?.asset_class ?? "—")}
                </div>
                {isInsider && item.security?.symbol && symbolNet30d !== null ? (
                  <div className="mt-1 text-xs tabular-nums">
                    <span className="text-white/40">Net 30D:</span>{" "}
                    <span className={netClass(symbolNet30d)}>
                      {formatMoney(symbolNet30d)}
                    </span>
                  </div>
                ) : null}
              </div>
            </div>
          ) : (
            <div className="min-w-0 flex items-center gap-3">
              {item.security?.symbol ? (
                <Link
                  href={`/ticker/${formatSymbol(item.security.symbol ?? "—")}`}
                  className="inline-flex items-center justify-center shrink-0 whitespace-nowrap px-2 py-0.5 text-xs font-medium rounded-full bg-white/5 border border-white/10"
                >
                  {formatSymbol(item.security.symbol ?? "—")}
                </Link>
              ) : (
                <span className="inline-flex items-center justify-center shrink-0 whitespace-nowrap px-2 py-0.5 text-xs font-medium rounded-full bg-white/5 border border-white/10">
                  —
                </span>
              )}
              <div className="min-w-0">
                <div className="truncate font-medium text-slate-200">
                  {item.security?.name ?? "—"}
                </div>
                <div className="truncate text-xs opacity-70">
                  {isInsider
                    ? (securityClass ?? "—")
                    : (item.security?.asset_class ?? "—")}
                </div>
                {isInsider && item.security?.symbol && symbolNet30d !== null ? (
                  <div className="mt-1 text-xs tabular-nums">
                    <span className="text-white/40">Net 30D:</span>{" "}
                    <span className={netClass(symbolNet30d)}>
                      {formatMoney(symbolNet30d)}
                    </span>
                  </div>
                ) : null}
              </div>
            </div>
          )}
        </div>

        <div className="min-w-0 whitespace-nowrap text-xs leading-5 text-slate-400">
          <div>
            {isInsider ? "Transaction" : "Trade"}:{" "}
            <span className="inline-block max-w-full truncate align-bottom text-slate-200">
              {isInsider
                ? formatYMD(insiderTxDate)
                : item.trade_date
                  ? formatDateShort(item.trade_date)
                  : "—"}
            </span>
          </div>
          <div>
            {isInsider ? "Filing" : "Report"}:{" "}
            <span className="inline-block max-w-full truncate align-bottom text-slate-200">
              {isInsider
                ? formatYMD(insiderFilingDate)
                : item.report_date
                  ? formatDateShort(item.report_date)
                  : "—"}
            </span>
          </div>
        </div>

        <div className="min-w-0 whitespace-nowrap text-xs leading-5 text-slate-400">
          <div>
            {isInsider ? (
              <>
                Ownership:{" "}
                <span className="inline-block max-w-full truncate align-bottom text-slate-200">
                  {ownershipLabel}
                </span>
              </>
            ) : (
              <>
                Filed after:{" "}
                <span className="inline-block max-w-full truncate align-bottom text-slate-200">
                  {lagDays !== null && lagDays >= 0 ? `${lagDays}d` : "—"}
                </span>
              </>
            )}
          </div>
        </div>

        <div className="min-w-0 whitespace-nowrap opacity-90">{badge}</div>

        <div className="min-w-0 max-w-full justify-self-end whitespace-nowrap text-right tabular-nums">
          <div
            className={`${isCompact ? "text-base lg:text-base" : "text-lg"} tabular-nums ${isHighlighted ? "font-bold" : "font-semibold"}`}
          >
            {amountText}
          </div>

          {isCongress && congressEstimatedPrice !== null && (
            <div className="mt-1 truncate text-xs text-slate-400 tabular-nums">
              Est. Trade Price: {formatMoney(congressEstimatedPrice)}
            </div>
          )}

          {isInsider && insiderShares !== null && insiderPrice !== null && (
            <div className="mt-1 truncate text-xs text-slate-400 tabular-nums">
              {formatShares(insiderShares)} shares @ {formatMoney(insiderPrice)}
            </div>
          )}
        </div>

        <div className="min-w-0 max-w-full justify-self-end whitespace-nowrap text-right tabular-nums">
          {pnl !== null && (
            <div
              className={`whitespace-nowrap tabular-nums ${isCompact ? "text-sm lg:text-base" : "text-base lg:text-lg"} ${pnlClass(
                pnl,
                isHighlighted,
              )}`}
            >
              {formatPnl(pnl)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
