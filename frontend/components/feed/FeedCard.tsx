import Link from "next/link";
import type { FeedItem } from "@/lib/types";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
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
import { nameToSlug } from "@/lib/memberSlug";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge } from "@/lib/insiderRole";

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
type SignalOverlay = { score: number; band: string } | null;

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

function smartBadgeClasses(band?: string) {
  switch (band) {
    case "strong":
      return "border-emerald-500/30 bg-emerald-500/10 text-emerald-200";
    case "notable":
      return "border-amber-500/30 bg-amber-500/10 text-amber-200";
    case "mild":
      return "border-orange-500/30 bg-orange-500/10 text-orange-200";
    case "none":
      return "border-slate-700 bg-slate-900/30 text-slate-300";
    default:
      return "border-slate-700 bg-slate-900/30 text-slate-300";
  }
}

function smartDotClasses(band?: string) {
  switch (band) {
    case "strong":
      return "bg-emerald-400";
    case "notable":
      return "bg-amber-400";
    case "mild":
      return "bg-orange-400";
    default:
      return "bg-slate-500";
  }
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

function toTitleCase(name?: string | null): string {
  if (!name) return "";
  if (name === name.toUpperCase()) {
    return name.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return name;
}

function displaySymbol(raw?: string | null): string {
  if (!raw) return "—";
  const s = raw.trim();
  if (!s) return "—";
  if (s.includes(":")) {
    const parts = s.split(":", 2);
    const rhs = parts[1]?.trim();
    return rhs || s;
  }
  return s;
}

export function FeedCard({
  item,
  whaleMode = "off",
  signalOverlay: _signalOverlay = null,
  density = "default",
  gridPreset = "default",
  context = "feed",
}: {
  item: FeedItem;
  whaleMode?: WhaleMode;
  signalOverlay?: SignalOverlay;
  density?: "default" | "compact";
  gridPreset?: "default" | "member";
  context?: "feed" | "member";
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
  const insiderRoleBadge = isInsider
    ? normalizeInsiderRoleBadge(
        insiderItem.insider?.role ??
          insiderItem.payload?.raw?.typeOfOwner ??
          insiderItem.payload?.raw?.officerTitle ??
          insiderItem.payload?.raw?.insiderRole ??
          insiderItem.payload?.raw?.position,
      )
    : null;
  const insiderRoleTone = isInsider && insiderRoleBadge ? insiderRoleBadgeTone(insiderRoleBadge) : "insider_default";
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
  const smartScoreRaw = (item as any).smart_score;
  const smartBand = (item as any).smart_band as string | undefined;
  const smartScore = parseNum(smartScoreRaw);

  const pnlPct = (item as any).pnl_pct;
  const hasPnl = typeof pnlPct === "number" && Number.isFinite(pnlPct);
  const pnl = parseNum(pnlPct);
  const pnlSource = (item as any).pnl_source as string | undefined;
  const pnlAvailable = hasPnl && pnlSource !== "none";
  const isStale = Boolean((item as any).quote_is_stale);

  const tipParts: string[] = [];
  if (!hasPnl) {
    tipParts.push("PnL unavailable");
  } else {
    if (pnlSource === "filing") tipParts.push("PnL uses filing price");
    else if (pnlSource === "eod") tipParts.push("PnL uses EOD close");
    else tipParts.push("PnL computed");
  }

  if (hasPnl && isStale) tipParts.push("Quote may be stale (cached)");

  const tip = tipParts.join(" • ");
  const tipTitle = tip || undefined;
  const ownershipLabel = item.insider?.ownership ?? item.owner_type ?? "—";
  const memberNet30d = parseNum(item.member_net_30d);
  const symbolNet30d = parseNum((item as any).symbol_net_30d);
  const symbol = item.security?.symbol ?? (item as any).ticker ?? null;
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
  const isMember = context === "member" || gridPreset === "member";
  const isFeed = !isMember;
  const smartText = smartScore !== null ? String(smartScore) : "—";
  const badgeClass = smartBadgeClasses(smartBand);
  const dotClass = smartDotClasses(smartBand);
  const smartBadgeNode = (
    <span
      className={`inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-[11px] font-semibold ${badgeClass}`}
    >
      <span className={`h-2 w-2 rounded-full ${dotClass}`} />
      <span className="font-mono">{smartText}</span>
    </span>
  );
  const gridClassName = isMember
    ? "lg:grid-cols-[minmax(100px,0.75fr)_minmax(100px,.5fr)_minmax(100px,.4fr)_minmax(100px,.4fr)_minmax(100px,1fr)_minmax(0,0fr)]"
    : "lg:grid-cols-[minmax(200px,1fr)_minmax(250px,1fr)_minmax(100px,.5fr)_minmax(85px,.5fr)_90px_170px_170px]";

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
        className={`flex w-full min-w-0 flex-col gap-4 pr-2 md:grid md:min-w-0 md:items-center md:gap-y-3 lg:gap-y-0 lg:gap-x-5 ${gridClassName}`}
      >
        {!isMember ? (
          <div className="min-w-0 space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              {isInsider ? (
                <span className="min-w-0 truncate text-lg font-semibold text-white">
                  {toTitleCase(
                    (item as any).member_name ?? item.member?.name,
                  ) || "—"}
                </span>
              ) : (
                <Link
                  href={`/member/${nameToSlug(item.member?.name ?? "event")}`}
                  className="min-w-0 truncate text-lg font-semibold text-white hover:text-emerald-200"
                >
                  {item.member?.name ?? "—"}
                </Link>
              )}
              {isInsider ? (
                <Badge tone={insiderRoleTone}>{insiderRoleBadge}</Badge>
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
        ) : null}

        <div className="min-w-0 text-sm text-slate-300">
          {isCompact ? (
            <div className="min-w-0">
              <div className="min-w-0 flex items-center gap-2">
                {symbol ? (
                  <Link
                    href={`/ticker/${formatSymbol(item.security.symbol ?? "—")}`}
                    className="inline-flex shrink-0"
                  >
                    <TickerPill symbol={displaySymbol(symbol)} />
                  </Link>
                ) : (
                  <TickerPill symbol="—" />
                )}
                <div className="min-w-0 overflow-hidden truncate text-xs text-white/60">
                  {isInsider
                    ? (securityClass ?? "—")
                    : (item.security?.asset_class ?? "—")}
                </div>
              </div>
              <div className="mt-1 min-w-0 overflow-hidden truncate text-xs font-semibold text-white">
                {item.security?.name ?? "—"}
              </div>
              {isInsider && symbol && symbolNet30d !== null ? (
                <div className="mt-1 text-xs tabular-nums">
                  <span className="text-white/40">Net 30D:</span>{" "}
                  <span className={netClass(symbolNet30d)}>
                    {formatMoney(symbolNet30d)}
                  </span>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="min-w-0 flex items-center gap-3">
              {symbol ? (
                <Link
                  href={`/ticker/${formatSymbol(item.security.symbol ?? "—")}`}
                  className="inline-flex shrink-0"
                >
                  <TickerPill symbol={displaySymbol(symbol)} />
                </Link>
              ) : (
                <TickerPill symbol="—" />
              )}
              <div className="min-w-0">
                <div className="min-w-0 overflow-hidden truncate font-semibold text-white">
                  {item.security?.name ?? "—"}
                </div>
                <div className="min-w-0 overflow-hidden truncate text-xs opacity-70">
                  {isInsider
                    ? (securityClass ?? "—")
                    : (item.security?.asset_class ?? "—")}
                </div>
                {isInsider && symbol && symbolNet30d !== null ? (
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

        <div
          className={
            isMember
              ? "min-w-0 max-w-[120px] text-xs leading-tight text-slate-400"
              : "min-w-0 text-xs leading-5 text-slate-400 text-center space-y-1 md:space-y-0 md:text-left md:whitespace-nowrap"
          }
        >
          <div className={isMember ? "truncate" : undefined}>
            {isInsider ? "Transaction" : "Trade"}:{" "}
            <span
              className={`inline-block align-bottom text-slate-200 ${isMember ? "max-w-full truncate" : "md:max-w-full md:truncate"}`}
            >
              {isInsider
                ? formatYMD(insiderTxDate)
                : item.trade_date
                  ? formatDateShort(item.trade_date)
                  : "—"}
            </span>
          </div>
          <div className={isMember ? "truncate" : undefined}>
            {isInsider ? "Filing" : "Report"}:{" "}
            <span
              className={`inline-block align-bottom text-slate-200 ${isMember ? "max-w-full truncate" : "md:max-w-full md:truncate"}`}
            >
              {isInsider
                ? formatYMD(insiderFilingDate)
                : item.report_date
                  ? formatDateShort(item.report_date)
                  : "—"}
            </span>
          </div>
        </div>

        <div
          className={
            isMember
              ? "min-w-0 max-w-[90px] text-xs leading-tight text-slate-400"
              : "min-w-0 text-xs leading-5 text-slate-400 text-center md:text-left md:whitespace-nowrap"
          }
        >
          <div className={isMember ? "truncate" : undefined}>
            {isInsider ? (
              <>
                Ownership:{" "}
                <span
                  className={`inline-block align-bottom text-slate-200 ${isMember ? "max-w-full truncate" : "md:max-w-full md:truncate"}`}
                >
                  {ownershipLabel}
                </span>
              </>
            ) : (
              <>
                Filed after:{" "}
                <span
                  className={`inline-block align-bottom text-slate-200 ${isMember ? "max-w-full truncate" : "md:max-w-full md:truncate"}`}
                >
                  {lagDays !== null && lagDays >= 0 ? `${lagDays}d` : "—"}
                </span>
              </>
            )}
          </div>
        </div>

        <div className="min-w-0 whitespace-nowrap opacity-90">
          <div className="flex justify-center md:justify-start">{badge}</div>
        </div>

        <div
          className={`min-w-0 max-w-full justify-self-end whitespace-nowrap text-right tabular-nums ${isFeed ? "lg:col-span-2" : ""}`}
        >
          {isMember ? (
            <div className="shrink-0 w-[320px]">
              <div className="grid items-center gap-3 [grid-template-columns:185px_90px_55px]">
                <div className="min-w-0 text-right">
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

                  {isInsider &&
                    insiderShares !== null &&
                    insiderPrice !== null && (
                      <div className="mt-1 truncate text-xs text-slate-400 tabular-nums">
                        {formatShares(insiderShares)} shares @{" "}
                        {formatMoney(insiderPrice)}
                      </div>
                    )}
                </div>

                <div className="text-right">
                  {pnl !== null && (
                    <div>
                      <div
                        className={`inline-flex items-center gap-1 whitespace-nowrap tabular-nums ${isCompact ? "text-sm lg:text-base" : "text-base lg:text-lg"} ${pnlClass(
                          pnl,
                          isHighlighted,
                        )}`}
                      >
                        {isStale ? <span className="opacity-70">~ </span> : null}
                        {formatPnl(pnl)}
                      </div>
                      {pnlSource === "filing" || pnlSource === "eod" ? (
                        <div className="mt-1">
                          <span
                            title={tipTitle}
                            aria-label={tipTitle}
                            className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300"
                          >
                            {pnlSource === "filing" ? "FILING" : "EOD"}
                          </span>
                        </div>
                      ) : null}
                    </div>
                  )}
                </div>

                <div className="flex justify-end">{smartBadgeNode}</div>
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-3 text-center md:grid md:[grid-template-columns:196px_100px_60px] md:items-center md:text-right">
              <div className="min-w-0 text-center md:text-right">
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

              <div className="text-right">
                {pnl !== null && (
                  <div>
                    <div
                      className={`inline-flex items-center gap-1 whitespace-nowrap tabular-nums ${isCompact ? "text-sm lg:text-base" : "text-base lg:text-lg"} ${pnlClass(
                        pnl,
                        isHighlighted,
                      )}`}
                    >
                      {isStale ? <span className="opacity-70">~ </span> : null}
                      {formatPnl(pnl)}
                    </div>
                    {pnlSource === "filing" || pnlSource === "eod" ? (
                      <div className="mt-1">
                        <span
                          title={tipTitle}
                          aria-label={tipTitle}
                          className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300"
                        >
                          {pnlSource === "filing" ? "FILING" : "EOD"}
                        </span>
                      </div>
                    ) : null}
                  </div>
                )}
              </div>

              <div className="flex justify-center">
                {smartBadgeNode}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
