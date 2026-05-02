import Link from "next/link";
import type { FeedItem } from "@/lib/types";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import {
  chamberBadge,
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  memberTag,
  partyBadge,
  transactionTone,
} from "@/lib/format";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";
import { formatCompanyName } from "@/lib/companyName";
import { insiderRoleBadgeTone, resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  parseInsiderNumber,
  resolveInsiderDisplayPrice,
  resolveInsiderDisplayValue,
  resolveInsiderShares,
} from "@/lib/insiderTradeDisplay";

type FeedCardInsiderItem = FeedItem & {
  trade_type?: string | null;
  amount_min?: number | string | null;
  amount_max?: number | string | null;
  payload?: {
    transaction_type?: string | null;
    reporting_cik?: string | null;
    transaction_date?: string | null;
    filing_date?: string | null;
    shares?: number | string | null;
    price?: number | string | null;
    display_price?: number | string | null;
    displayPrice?: number | string | null;
    display_trade_value?: number | string | null;
    displayTradeValue?: number | string | null;
    raw?: {
      transactionType?: string | null;
      transactionDate?: string | null;
      reportingCik?: string | null;
      filingDate?: string | null;
      acquisitionOrDisposition?: string | null;
      securitiesTransacted?: number | string | null;
      transactionShares?: number | string | null;
      price?: number | string | null;
      typeOfOwner?: string | null;
      insiderName?: string | null;
      officerTitle?: string | null;
      insiderRole?: string | null;
      position?: string | null;
      securityName?: string | null;
    };
  };
  insider?: FeedItem["insider"] & {
    transaction_type?: string | null;
    shares?: number | string | null;
    display_price?: number | string | null;
  };
};

type FeedCardGovernmentContractItem = FeedItem & {
  contract_description?: string | null;
  url?: string | null;
  payload?: {
    event_subtype?: string | null;
    modification_number?: string | null;
    title?: string | null;
    description?: string | null;
    award_description?: string | null;
    contract_description?: string | null;
    action_date?: string | null;
    report_date?: string | null;
    period_start?: string | null;
    period_end?: string | null;
    end_date?: string | null;
    total_obligated_amount?: number | string | null;
    total_obligated?: number | string | null;
    total_obligation?: number | string | null;
    current_total_obligation?: number | string | null;
    award_amount?: number | string | null;
    source_url?: string | null;
    raw?: {
      total_obligated_amount?: number | string | null;
      totalObligatedAmount?: number | string | null;
      current_total_obligation?: number | string | null;
      currentTotalObligation?: number | string | null;
      period_end?: string | null;
      end_date?: string | null;
      parent_award?: {
        award_amount?: number | string | null;
        total_obligated_amount?: number | string | null;
        totalObligatedAmount?: number | string | null;
        current_total_obligation?: number | string | null;
        currentTotalObligation?: number | string | null;
        period_end?: string | null;
        end_date?: string | null;
      };
    };
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

function formatMoneyCompact(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return formatMoney(n);
}

function firstParsedNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const parsed = parseNum(value);
    if (parsed !== null) return parsed;
  }
  return null;
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

  const totalValue = resolveInsiderDisplayValue(insiderItem);
  const shares = resolveInsiderShares(insiderItem);
  const price = resolveInsiderDisplayPrice(insiderItem);

  return {
    totalValue,
    price: price && price > 0 ? price : null,
    shares: shares && shares > 0 ? shares : null,
  };
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

const titleCaseLowerWords = new Set(["a", "an", "and", "as", "at", "by", "for", "from", "in", "of", "on", "or", "the", "to", "with"]);

function titleCaseContractDescription(value?: string | null): string {
  const text = value?.trim();
  if (!text) return "Government Contract Award";

  let wordIndex = 0;
  return text
    .toLowerCase()
    .replace(/\b[A-Za-z0-9]+\b/g, (lowerWord, offset) => {
      const original = text.slice(offset, offset + lowerWord.length);
      const isAcronym = /^[A-Z]{2,4}$/.test(original);
      const hasDigit = /\d/.test(original);
      const currentIndex = wordIndex;
      wordIndex += 1;

      if (isAcronym || hasDigit) return original;
      if (currentIndex > 0 && titleCaseLowerWords.has(lowerWord)) return lowerWord;
      return `${lowerWord.charAt(0).toUpperCase()}${lowerWord.slice(1)}`;
    });
}

function resolveInsiderDisplayName(item: FeedItem): string {
  const insiderItem = item as FeedCardInsiderItem;
  return getInsiderDisplayName(
    insiderItem.insider?.name,
    insiderItem.payload?.raw?.insiderName,
    (item as any).member_name,
    item.member?.name,
  ) ?? "—";
}

function resolveInsiderReportingCik(item: FeedItem): string | null {
  const insiderItem = item as FeedCardInsiderItem;
  return insiderItem.insider?.reporting_cik ?? insiderItem.payload?.reporting_cik ?? insiderItem.payload?.raw?.reportingCik ?? null;
}

export function FeedCard({
  item,
  whaleMode = "off",
  signalOverlay = null,
  density = "default",
  gridPreset = "default",
  context = "feed",
}: {
  item: FeedItem;
  whaleMode?: WhaleMode;
  signalOverlay?: SignalOverlay;
  density?: "default" | "compact";
  gridPreset?: "default" | "member" | "watchlist";
  context?: "feed" | "member";
}) {
  if (!item) return null;

  const kind = item.kind ?? (item as any).event_type;
  const isCongress = kind === "congress_trade";
  const isInsider = kind === "insider_trade";
  const isInstitutional = kind === "institutional_buy";
  const isGovernmentContract = kind === "government_contract";
  const chamber = chamberBadge(item.member?.chamber ?? "—");
  const party = partyBadge(item.member?.party ?? null);
  const tag = memberTag(item.member?.party ?? null, item.member?.state ?? null);
  const insiderKind = isInsider ? getInsiderKind(item) : null;
  const insiderValue = isInsider ? getInsiderValue(item) : null;
  const insiderAmount = insiderValue?.totalValue ?? null;
  const insiderPrice = insiderValue?.price ?? null;
  const insiderShares = insiderValue?.shares ?? null;

  const insiderItem = item as FeedCardInsiderItem;
  const insiderProfileHref = insiderHref(resolveInsiderDisplayName(item), resolveInsiderReportingCik(item));
  const securityClass = isInsider
    ? normalizeSecurityClass(
        insiderItem.payload?.raw?.securityName ?? undefined,
      )
    : null;
  const insiderRoleBadge = isInsider
    ? resolveInsiderRoleBadge(
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
  const signalValue = resolveSmartSignalValue(item as Record<string, unknown>);
  const overlaySmartScore = signalOverlay ? parseNum(signalOverlay.score) : null;
  const smartScore = signalValue.score ?? overlaySmartScore;
  const smartBand = signalValue.band ?? signalOverlay?.band ?? null;

  const pnlPct = (item as any).pnl_pct;
  const hasPnl = typeof pnlPct === "number" && Number.isFinite(pnlPct);
  const pnl = parseInsiderNumber(pnlPct);
  const pnlSource = (item as any).pnl_source as string | undefined;
  const pnlAvailable = hasPnl && pnlSource !== "none";
  const isStale = Boolean((item as any).quote_is_stale);

  const tipParts: string[] = [];
  if (!hasPnl) {
    tipParts.push("PnL unavailable");
  } else {
    if (pnlSource === "normalized_filing") tipParts.push("PnL uses normalized filing price");
    else if (pnlSource === "filing") tipParts.push("PnL uses filing price");
    else if (pnlSource === "eod") tipParts.push("PnL uses EOD close");
    else tipParts.push("PnL computed");
  }

  if (hasPnl && isStale) tipParts.push("Quote may be stale (cached)");

  const tip = tipParts.join(" • ");
  const tipTitle = tip || undefined;
  const ownershipLabel = item.insider?.ownership ?? item.owner_type ?? "—";
  const memberNet30d = parseNum(item.member_net_30d);
  const symbolNet30d = parseNum((item as any).symbol_net_30d);
  const confirmation = (item as any).confirmation_30d as FeedItem["confirmation_30d"];
  const isCrossSourceConfirmed = Boolean(confirmation?.cross_source_confirmed_30d);
  const symbol = item.security?.symbol ?? (item as any).ticker ?? null;
  const amountText = isInsider
    ? insiderAmount !== null
      ? formatMoney(insiderAmount)
      : "—"
    : (formatCurrencyRange(item.amount_range_min, item.amount_range_max) ?? "—");
  const tradeValueNumber = isCongress || isInstitutional
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
          : isInstitutional
            ? "pos"
          : transactionTone(item.transaction_type)
      }
    >
      {isInsider
        ? insiderKind === "purchase"
          ? "Purchase"
          : insiderKind === "sale"
            ? "Sale"
            : "—"
        : isInstitutional
          ? "Filing Increase"
        : (formatTransactionLabel(item.transaction_type) ?? "—")}
    </Badge>
  );

  if (isInsider && !insiderKind) return null;

  const isCompact = density === "compact";
  const isMember = context === "member" || gridPreset === "member";
  const isWatchlist = gridPreset === "watchlist";
  const isFeed = !isMember;
  const smartBadgeNode = (
    <SmartSignalPill score={smartScore} band={smartBand} size="compact" />
  );
  const gridClassName = isMember
    ? "lg:grid-cols-[minmax(100px,0.75fr)_minmax(100px,.5fr)_minmax(100px,.4fr)_minmax(100px,.4fr)_minmax(100px,1fr)_minmax(0,0fr)]"
    : isWatchlist
      ? "xl:grid-cols-[minmax(135px,0.8fr)_minmax(180px,1fr)_minmax(130px,0.7fr)_minmax(100px,0.5fr)_minmax(180px,220px)]"
    : isCompact
      ? "lg:grid-cols-[minmax(170px,.95fr)_minmax(200px,1fr)_minmax(160px,.75fr)_minmax(135px,.6fr)_90px_130px_110px]"
      : "lg:grid-cols-[minmax(200px,1fr)_minmax(250px,1fr)_minmax(100px,.5fr)_minmax(85px,.5fr)_90px_170px_170px]";

  if (isGovernmentContract) {
    const contractItem = item as FeedCardGovernmentContractItem;
    const isFundingAction =
      contractItem.payload?.event_subtype === "funding_action" ||
      Boolean((contractItem.payload as any)?.modification_number) ||
      Boolean(contractItem.payload?.action_date);
    const agency = item.member?.name?.trim() || "Government Contract";
    const companyName = formatCompanyName(item.security?.name) || (symbol ? displaySymbol(symbol) : "Company unavailable");
    const description = titleCaseContractDescription(
      contractItem.contract_description ??
        contractItem.payload?.title ??
        contractItem.payload?.description ??
        contractItem.payload?.award_description ??
        contractItem.payload?.contract_description,
    );
    const contractValue = parseNum(item.amount_range_max);
    const sourceUrl = (contractItem.url ?? contractItem.payload?.source_url ?? "").trim();
    const reportDate = contractItem.payload?.report_date ?? contractItem.payload?.action_date ?? item.report_date;
    const endDate =
      contractItem.payload?.period_end ??
      contractItem.payload?.end_date ??
      contractItem.payload?.raw?.period_end ??
      contractItem.payload?.raw?.end_date ??
      contractItem.payload?.raw?.parent_award?.period_end ??
      contractItem.payload?.raw?.parent_award?.end_date ??
      null;
    const totalObligated = firstParsedNumber(
      contractItem.payload?.total_obligated_amount,
      contractItem.payload?.total_obligated,
      contractItem.payload?.total_obligation,
      contractItem.payload?.current_total_obligation,
      contractItem.payload?.raw?.total_obligated_amount,
      contractItem.payload?.raw?.totalObligatedAmount,
      contractItem.payload?.raw?.current_total_obligation,
      contractItem.payload?.raw?.currentTotalObligation,
      contractItem.payload?.raw?.parent_award?.total_obligated_amount,
      contractItem.payload?.raw?.parent_award?.totalObligatedAmount,
      contractItem.payload?.raw?.parent_award?.current_total_obligation,
      contractItem.payload?.raw?.parent_award?.currentTotalObligation,
      contractItem.payload?.raw?.parent_award?.award_amount,
      isFundingAction ? contractItem.payload?.award_amount : null,
    );

    return (
      <div className="relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card">
        <div className="flex w-full min-w-0 flex-col gap-4 pr-2 md:grid md:min-w-0 md:items-center md:gap-y-3 lg:grid-cols-[minmax(200px,1fr)_minmax(250px,1fr)_minmax(100px,.5fr)_minmax(85px,.5fr)_90px_170px_170px] lg:gap-x-5 lg:gap-y-0">
          <div className="min-w-0 space-y-2">
            <span className="block min-w-0 truncate text-lg font-semibold text-white">
              {agency}
            </span>
          </div>

          <div className="min-w-0 text-sm text-slate-300">
            <div className="flex min-w-0 items-start gap-3">
              {symbol ? (
                <div className="mt-0.5 inline-flex shrink-0 items-center gap-1.5">
                  <AddTickerToWatchlist symbol={displaySymbol(symbol)} variant="compact" align="left" />
                  <TickerPill symbol={displaySymbol(symbol)} href={tickerHref(symbol)} className="inline-flex" />
                </div>
              ) : null}
              <div className="min-w-0">
                <div className="max-w-full overflow-hidden break-words font-semibold leading-5 text-white [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:1]">
                  {companyName}
                </div>
                <div className="mt-1 max-w-full overflow-hidden break-words text-xs leading-5 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2]">
                  {description}
                </div>
                <div className="mt-1 min-w-0 overflow-hidden truncate text-[11px] text-slate-500">
                  {isFundingAction ? "Government Contract Funding" : "Government Contract"}
                </div>
              </div>
            </div>
          </div>

          <div className="min-w-0 overflow-hidden text-xs leading-5 text-center text-slate-400 md:text-left md:whitespace-nowrap">
            <div className="min-w-0 truncate">
              Report:{" "}
              <span className="inline-block align-bottom text-slate-200 md:max-w-full md:truncate">
                {formatYMD(reportDate)}
              </span>
            </div>
            {endDate ? (
              <div className="min-w-0 truncate text-[11px] text-slate-500">
                End: <span>{formatYMD(endDate)}</span>
              </div>
            ) : null}
          </div>

          <div className="hidden min-w-0 overflow-hidden text-xs leading-5 text-slate-400 lg:block" aria-hidden="true" />

          <div className="min-w-0">
            <Badge tone="neutral" className="text-[10px]">Contract</Badge>
          </div>

          <div className="min-w-0 whitespace-nowrap text-right tabular-nums">
            <div className="text-lg font-semibold">
              {contractValue !== null ? formatMoney(contractValue) : "Value unavailable"}
            </div>
            {totalObligated !== null ? (
              <div className="mt-1 truncate text-xs font-medium text-slate-500">
                {formatMoneyCompact(totalObligated)} Total Obligated
              </div>
            ) : null}
          </div>

          <div className="min-w-0 text-center md:text-right">
            {sourceUrl ? (
              <a
                href={sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex h-9 items-center justify-center rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-400/20"
              >
                View Contract
              </a>
            ) : (
              <span className="text-xs text-slate-500">Source unavailable</span>
            )}
          </div>
        </div>
      </div>
    );
  }

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
        className={`flex w-full min-w-0 max-w-full flex-col gap-4 md:grid md:min-w-0 md:items-center md:gap-y-3 lg:gap-y-0 ${isWatchlist ? "lg:gap-x-4" : "pr-2 lg:gap-x-5"} ${gridClassName}`}
      >
        {!isMember ? (
          <div className="min-w-0 space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              {isInsider ? (
                insiderProfileHref ? (
                  <Link href={insiderProfileHref} prefetch={false} className="min-w-0 truncate text-lg font-semibold text-white hover:text-emerald-200">
                    {resolveInsiderDisplayName(item)}
                  </Link>
                ) : (
                  <span className="min-w-0 truncate text-lg font-semibold text-white">
                    {resolveInsiderDisplayName(item)}
                  </span>
                )
              ) : (
                <Link
                  href={memberHref({ name: item.member?.name, memberId: item.member?.bioguide_id })}
                  prefetch={false}
                  className="min-w-0 truncate text-lg font-semibold text-white hover:text-emerald-200"
                >
                  {item.member?.name ?? "—"}
                </Link>
              )}
              {isInsider ? (
                <Badge tone={insiderRoleTone}>{insiderRoleBadge}</Badge>
              ) : isInstitutional ? (
                <Badge tone="neutral">Institutional Filing</Badge>
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
                {symbol ? <AddTickerToWatchlist symbol={displaySymbol(symbol)} variant="compact" align="left" /> : null}
                {symbol ? (
                  <TickerPill symbol={displaySymbol(symbol)} href={tickerHref(symbol)} className="inline-flex shrink-0" />
                ) : (
                  <TickerPill symbol="—" />
                )}
                <div className="min-w-0 overflow-hidden truncate text-xs text-white/60">
                  {isInsider
                    ? (securityClass ?? "—")
                    : isInstitutional
                      ? "Institutional filing (delayed)"
                    : (item.security?.asset_class ?? "—")}
                </div>
              </div>
              <div className="mt-1 min-w-0 overflow-hidden truncate text-xs font-semibold text-white">
                {formatCompanyName(item.security?.name) || "—"}
              </div>
              {isCrossSourceConfirmed ? (
                <div className="mt-1">
                  <Badge tone="neutral" className="border-cyan-400/20 bg-cyan-400/10 text-[10px] text-cyan-100">Cross-source confirmed (30D)</Badge>
                </div>
              ) : null}
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
              {symbol ? <AddTickerToWatchlist symbol={displaySymbol(symbol)} variant="compact" align="left" /> : null}
              {symbol ? (
                <TickerPill symbol={displaySymbol(symbol)} href={tickerHref(symbol)} className="inline-flex shrink-0" />
              ) : (
                <TickerPill symbol="—" />
              )}
              <div className="min-w-0">
                <div className="min-w-0 overflow-hidden truncate font-semibold text-white">
                  {formatCompanyName(item.security?.name) || "—"}
                </div>
                <div className="min-w-0 overflow-hidden truncate text-xs opacity-70">
                  {isInsider
                    ? (securityClass ?? "—")
                    : isInstitutional
                      ? "Institutional filing (delayed)"
                    : (item.security?.asset_class ?? "—")}
                </div>
                {isCrossSourceConfirmed ? (
                  <div className="mt-1">
                    <Badge tone="neutral" className="border-cyan-400/20 bg-cyan-400/10 text-[10px] text-cyan-100">Cross-source confirmed (30D)</Badge>
                  </div>
                ) : null}
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
              : "min-w-0 overflow-hidden text-xs leading-5 text-slate-400 text-center space-y-1 md:space-y-0 md:text-left md:whitespace-nowrap"
          }
        >
          <div className={isMember ? "truncate" : undefined}>
            {isInsider ? "Transaction" : isInstitutional ? "Position" : "Trade"}:{" "}
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
            {isInsider || isInstitutional ? "Filing" : "Report"}:{" "}
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
              : "min-w-0 overflow-hidden text-xs leading-5 text-slate-400 text-center md:text-left md:whitespace-nowrap lg:pl-3"
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
            ) : isInstitutional ? (
              <>
                Source:{" "}
                <span
                  className={`inline-block align-bottom text-slate-200 ${isMember ? "max-w-full truncate" : "md:max-w-full md:truncate"}`}
                >
                  {(item as any).source ?? "Institutional filing (delayed)"}
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
          className={`max-w-full shrink-0 whitespace-nowrap text-right tabular-nums ${
            isWatchlist
              ? "w-full min-w-[180px] justify-self-end xl:w-[220px]"
              : isFeed
                ? "min-w-0 justify-self-start lg:col-span-2"
                : "min-w-0 justify-self-end"
          }`}
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
                      <div className="mt-1 min-w-[170px] whitespace-nowrap text-right text-xs text-slate-400 tabular-nums">
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
                  {pnlSource === "filing" || pnlSource === "normalized_filing" || pnlSource === "eod" ? (
                        <div className="mt-1">
                          <span
                            title={tipTitle}
                            aria-label={tipTitle}
                            className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300"
                          >
                      {pnlSource === "normalized_filing" ? "NORMALIZED" : pnlSource === "filing" ? "FILING" : "EOD"}
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
            <div className={`${isWatchlist ? "grid grid-cols-[minmax(105px,1fr)_minmax(58px,auto)] items-center gap-x-3 gap-y-2 text-right sm:grid-cols-[minmax(105px,1fr)_minmax(62px,auto)_minmax(44px,auto)]" : "flex flex-col items-center gap-3 text-center md:grid md:[grid-template-columns:170px_90px_60px] md:items-center md:text-right"}`}>
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

                {isInsider && insiderShares !== null && insiderPrice !== null && (
                  <div className={`${isWatchlist ? "mt-1 text-[11px]" : "mt-1 min-w-[170px] text-xs"} whitespace-nowrap text-right text-slate-400 tabular-nums`}>
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
                {pnlSource === "filing" || pnlSource === "normalized_filing" || pnlSource === "eod" ? (
                      <div className="mt-1">
                        <span
                          title={tipTitle}
                          aria-label={tipTitle}
                          className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300"
                        >
                    {pnlSource === "normalized_filing" ? "NORMALIZED" : pnlSource === "filing" ? "FILING" : "EOD"}
                        </span>
                      </div>
                    ) : null}
                  </div>
                )}
              </div>

              <div className="flex justify-end">
                {smartBadgeNode}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
