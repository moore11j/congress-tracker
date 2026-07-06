"use client";

import Link from "next/link";
import { Fragment } from "react";
import type { KeyboardEvent } from "react";
import type { FeedItem } from "@/lib/types";
import { FeedCard } from "@/components/feed/FeedCard";
import { formatCurrencyRange, formatDateShort, formatTransactionLabel } from "@/lib/format";
import { formatCompanyName } from "@/lib/companyName";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { institutionHref } from "@/lib/institution";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";

type SignalOverlay = { score: number; band: string } | null;

type FeedTableProps = {
  items: FeedItem[];
  expandedItemId: number | null;
  onToggleExpanded: (id: number) => void;
  overlaySignals?: Record<string, { score: number; band: string }>;
  canViewPremiumMetrics?: boolean;
};

const sourceLabels: Record<string, string> = {
  congress_trade: "Congress",
  congress_treasury_trade: "Congress",
  congress_crypto_trade: "Congress",
  insider_trade: "Insider",
  government_contract: "Gov Contracts",
  institutional_buy: "Institutional",
  institutional_accumulation: "Institutional",
  institutional_distribution: "Institutional",
  new_institutional_position: "Institutional",
  major_holder_reduction: "Institutional",
  major_holder_exit: "Institutional",
  cluster_accumulation: "Institutional",
  cluster_distribution: "Institutional",
  smart_money_confirmation: "Institutional",
  crowded_long: "Institutional",
  contrarian_accumulation: "Institutional",
};

const institutionalKinds = new Set([
  "institutional_buy",
  "institutional_accumulation",
  "institutional_distribution",
  "new_institutional_position",
  "major_holder_reduction",
  "major_holder_exit",
  "cluster_accumulation",
  "cluster_distribution",
  "smart_money_confirmation",
  "crowded_long",
  "contrarian_accumulation",
]);

function compactMoney(value: number): string {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1_000_000_000) return `${sign}$${(abs / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
  return `${sign}$${Math.round(abs).toLocaleString("en-US")}`;
}

function signedCompactMoney(value: number): string {
  if (value > 0) return `+${compactMoney(value)}`;
  if (value < 0) return compactMoney(value);
  return "$0";
}

function parseNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value.replace(/[$,]/g, "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function displayName(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text ? text : null;
}

function payloadRecord(item: FeedItem): Record<string, any> {
  return (item.payload ?? {}) as Record<string, any>;
}

function sourceLabel(item: FeedItem): string {
  const kind = String(item.kind ?? "");
  return sourceLabels[kind] ?? formatTransactionLabel(kind) ?? "Event";
}

function isInstitutional(item: FeedItem): boolean {
  return institutionalKinds.has(String(item.kind ?? "").toLowerCase());
}

function isGovernmentContract(item: FeedItem): boolean {
  return String(item.kind ?? "") === "government_contract";
}

function isInsider(item: FeedItem): boolean {
  return String(item.kind ?? "") === "insider_trade";
}

function isCongress(item: FeedItem): boolean {
  const kind = String(item.kind ?? "");
  return kind === "congress_trade" || kind === "congress_treasury_trade" || kind === "congress_crypto_trade";
}

function entityLabel(item: FeedItem): string {
  const payload = payloadRecord(item);
  if (isInsider(item)) {
    return (
      getInsiderDisplayName(
        item.insider?.name,
        displayName(payload.raw?.insiderName),
        displayName((item as any).member_name),
        item.member?.name,
      ) ?? "Insider unavailable"
    );
  }
  if (isInstitutional(item)) {
    return item.member?.name?.trim() || displayName(payload.institution_name) || displayName(payload.holder_name) || "Institution unavailable";
  }
  if (isGovernmentContract(item)) {
    return item.member?.name?.trim() || displayName(payload.department) || displayName(payload.agency) || "Agency unavailable";
  }
  return item.member?.name?.trim() || "Member unavailable";
}

function partyStateLabel(item: FeedItem): string | null {
  const party = item.member?.party?.trim();
  const state = item.member?.state?.trim();
  const partyAbbrev = party ? party.charAt(0).toUpperCase() : null;
  if (partyAbbrev && state) return `${partyAbbrev} - ${state.toUpperCase()}`;
  if (partyAbbrev) return partyAbbrev;
  if (state) return state.toUpperCase();
  return null;
}

function roleLabel(item: FeedItem): string | null {
  const payload = payloadRecord(item);
  if (isCongress(item)) {
    const partyState = partyStateLabel(item);
    const chamber = item.member?.chamber?.trim();
    if (partyState && chamber) return `${partyState} / ${chamber}`;
    return partyState ?? chamber ?? null;
  }
  if (isInsider(item)) {
    return (
      displayName(item.insider?.role) ??
      displayName(payload.role) ??
      displayName(payload.insiderRole) ??
      displayName(payload.raw?.officerTitle) ??
      displayName(payload.raw?.typeOfOwner)
    );
  }
  if (isInstitutional(item)) {
    return displayName(item.institutional?.report_period) ?? displayName(payload.report_period) ?? "13F filing";
  }
  if (isGovernmentContract(item)) {
    return displayName(payload.awarding_agency) ?? displayName(payload.agency) ?? displayName(payload.department);
  }
  return null;
}

function entityHref(item: FeedItem): string | null {
  const payload = payloadRecord(item);
  if (isInsider(item)) {
    return insiderHref(entityLabel(item), item.insider?.reporting_cik ?? displayName(payload.reporting_cik) ?? displayName(payload.raw?.reportingCik));
  }
  if (isInstitutional(item)) {
    return institutionHref(item.member?.bioguide_id ?? displayName(payload.institution_cik) ?? displayName(payload.cik));
  }
  if (isCongress(item)) {
    return memberHref({ name: item.member?.name, memberId: item.member?.bioguide_id });
  }
  return null;
}

function companyLabel(item: FeedItem): string {
  const payload = payloadRecord(item);
  const symbol = item.security?.symbol?.trim().toUpperCase();
  if (isGovernmentContract(item)) {
    return formatCompanyName(item.security?.name) || displayName(payload.recipient_name) || displayName(payload.company_name) || symbol || "Company unavailable";
  }
  return formatCompanyName(item.security?.name) || symbol || "Security unavailable";
}

function contractDescription(item: FeedItem): string {
  const payload = payloadRecord(item);
  return (
    displayName(item.contract_description) ||
    displayName(payload.title) ||
    displayName(payload.description) ||
    displayName(payload.award_description) ||
    displayName(payload.contract_description) ||
    "Government contract"
  );
}

function tradeSide(item: FeedItem): "purchase" | "sale" | null {
  const payload = payloadRecord(item);
  const raw = String((item as any).trade_type ?? item.transaction_type ?? payload.transaction_type ?? payload.raw?.transactionType ?? "");
  const cleaned = raw.trim().toLowerCase();
  if (!cleaned) return null;
  if (["p", "a", "purchase", "buy"].includes(cleaned) || cleaned.startsWith("p-") || cleaned.startsWith("a-") || cleaned.includes("purchase")) return "purchase";
  if (["s", "d", "sale", "sell"].includes(cleaned) || cleaned.startsWith("s-") || cleaned.startsWith("d-") || cleaned.includes("sale")) return "sale";
  return null;
}

function institutionalActionLabel(item: FeedItem): string {
  const kind = String(item.kind ?? "");
  const payload = payloadRecord(item);
  const direction = String((item as any).direction ?? payload.direction ?? "").toLowerCase();
  const valueDelta = parseNumber(item.institutional?.value_delta_usd ?? payload.value_delta_usd);
  if (kind === "new_institutional_position") return "New Position";
  if (kind === "major_holder_exit") return "Reported Exit";
  if (kind === "institutional_distribution" || kind === "major_holder_reduction" || kind === "cluster_distribution" || direction === "bearish") return "Reported Reduction";
  if (kind === "institutional_accumulation" || kind === "cluster_accumulation" || kind === "contrarian_accumulation" || direction === "bullish") return "Reported Increase";
  if (kind === "institutional_buy") return "Reported Increase";
  if (valueDelta !== null) {
    if (valueDelta < 0) return "Reported Reduction";
    if (valueDelta > 0) return "Reported Increase";
  }
  return "Reported Activity";
}

function actionLabel(item: FeedItem): string {
  if (isInstitutional(item)) return institutionalActionLabel(item);
  if (isGovernmentContract(item)) {
    const subtype = displayName(payloadRecord(item).event_subtype);
    return subtype?.toLowerCase().includes("fund") ? "Funding" : "Contract";
  }
  if (isInsider(item) || isCongress(item)) {
    const side = tradeSide(item);
    if (side === "purchase") return "Purchase";
    if (side === "sale") return "Sale";
  }
  return formatTransactionLabel(item.transaction_type) ?? "Activity";
}

function actionTone(item: FeedItem): string {
  const label = actionLabel(item).toLowerCase();
  if (label.includes("reduction") || label.includes("exit") || label.includes("sale")) return "text-rose-300";
  if (label.includes("increase") || label.includes("new position") || label.includes("purchase")) return "text-emerald-300";
  return "text-slate-300";
}

function dateLabel(item: FeedItem): string {
  const payload = payloadRecord(item);
  const value = isGovernmentContract(item)
    ? displayName(payload.report_date) ?? displayName(payload.action_date) ?? item.report_date ?? item.trade_date
    : item.report_date ?? item.trade_date;
  return formatDateShort(value);
}

function amountLabel(item: FeedItem): string {
  const payload = payloadRecord(item);
  const kind = String(item.kind ?? "");
  const max = parseNumber(item.amount_range_max);
  const min = parseNumber(item.amount_range_min);
  if (isInstitutional(item)) {
    if (kind.includes("exit")) return "$0";
    if (max !== null && min !== null && Math.abs(max - min) < 0.5) return compactMoney(max);
    if (max !== null) return compactMoney(max);
    if (min !== null) return compactMoney(min);
  }
  const explicitValue = parseNumber(payload.display_trade_value) ?? parseNumber(payload.value_usd) ?? parseNumber(payload.reported_value_usd);
  if (explicitValue !== null) return compactMoney(explicitValue);
  return formatCurrencyRange(min, max);
}

function gainLossLabel(item: FeedItem, canViewPremiumMetrics: boolean): { label: string; sublabel: string | null; tone: string } {
  const status = item.gain_loss_status ?? null;
  const percent = parseNumber(item.gain_loss_percent ?? item.pnl_pct);
  const amount = parseNumber(item.gain_loss_amount);

  if (isInstitutional(item)) {
    const delta = parseNumber(item.institutional?.value_delta_usd ?? payloadRecord(item).value_delta_usd);
    return {
      label: "N/A",
      sublabel: delta !== null ? `Change ${signedCompactMoney(delta)}` : "Change N/A",
      tone: delta !== null && delta < 0 ? "text-rose-300" : delta !== null && delta > 0 ? "text-emerald-300" : "text-slate-400",
    };
  }

  if (!canViewPremiumMetrics && (percent !== null || amount !== null || status)) {
    return { label: "Locked", sublabel: "Premium", tone: "text-slate-400" };
  }

  if (percent !== null) {
    const prefix = percent > 0 ? "+" : "";
    return {
      label: `${prefix}${percent.toFixed(1)}%`,
      sublabel: amount !== null ? signedCompactMoney(amount) : status === "ok" ? "Ready" : null,
      tone: percent > 0 ? "text-emerald-300" : percent < 0 ? "text-rose-300" : "text-slate-300",
    };
  }

  if (status === "pending") return { label: "Pending", sublabel: null, tone: "text-slate-400" };
  if (status === "unavailable") return { label: "Unavailable", sublabel: null, tone: "text-slate-400" };
  if (status === "missing_trade_price") return { label: "Missing trade price", sublabel: null, tone: "text-amber-200" };
  if (status === "missing_current_price") return { label: "Missing current price", sublabel: null, tone: "text-amber-200" };
  if (status === "missing_quantity") return { label: "Missing quantity", sublabel: null, tone: "text-amber-200" };
  return { label: "Pending", sublabel: null, tone: "text-slate-400" };
}

function signalLabel(item: FeedItem, canViewPremiumMetrics: boolean): { label: string; tone: string } {
  const score = parseNumber(item.smart_score);
  const band = item.smart_band?.trim();
  if (!canViewPremiumMetrics && (score !== null || band)) return { label: "Locked", tone: "text-slate-400" };
  if (score !== null) return { label: String(Math.round(score)), tone: score >= 70 ? "text-emerald-300" : score >= 45 ? "text-sky-300" : "text-slate-300" };
  if (band) return { label: band, tone: "text-slate-300" };
  return { label: "-", tone: "text-slate-500" };
}

function disclosureHref(item: FeedItem): string | null {
  const payload = payloadRecord(item);
  const value = item.url ?? payload.document_url ?? payload.source_url ?? payload.url ?? payload.award_url ?? null;
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function disclosureLabel(item: FeedItem): string {
  if (isGovernmentContract(item)) return "Contract";
  if (isInstitutional(item)) return "13F";
  if (isInsider(item)) return "Filing";
  if (isCongress(item)) return "Disclosure";
  return "Open";
}

function onRowKeyDown(event: KeyboardEvent, id: number, onToggleExpanded: (id: number) => void) {
  if (event.key !== "Enter" && event.key !== " ") return;
  event.preventDefault();
  onToggleExpanded(id);
}

function ExpandedDetails({
  item,
  overlay,
  canViewPremiumMetrics,
}: {
  item: FeedItem;
  overlay: SignalOverlay;
  canViewPremiumMetrics: boolean;
}) {
  return (
    <div className="rounded-2xl border border-emerald-400/20 bg-slate-950/60 p-3">
      <FeedCard item={item} signalOverlay={overlay} canViewPremiumMetrics={canViewPremiumMetrics} density="compact" />
    </div>
  );
}

export function FeedTable({
  items,
  expandedItemId,
  onToggleExpanded,
  overlaySignals,
  canViewPremiumMetrics = false,
}: FeedTableProps) {
  return (
    <div className="overflow-hidden rounded-2xl border border-white/10 bg-slate-950/35">
      <div className="hidden lg:block">
        <table className="w-full table-fixed border-collapse text-left text-sm">
          <colgroup>
            <col className="w-[9%]" />
            <col className="w-[10%]" />
            <col className="w-[12%]" />
            <col className="w-[20%]" />
            <col className="w-[12%]" />
            <col className="w-[10%]" />
            <col className="w-[10%]" />
            <col className="w-[7%]" />
            <col className="w-[10%]" />
          </colgroup>
          <thead className="border-b border-white/10 bg-white/[0.03] text-xs uppercase tracking-[0.14em] text-slate-400">
            <tr>
              <th className="px-3 py-3 font-semibold">Date</th>
              <th className="px-3 py-3 font-semibold">Source</th>
              <th className="px-3 py-3 font-semibold">Ticker</th>
              <th className="px-3 py-3 font-semibold">Person / Entity</th>
              <th className="px-3 py-3 font-semibold">Action</th>
              <th className="px-3 py-3 text-right font-semibold">Amount</th>
              <th className="px-3 py-3 text-right font-semibold">G/L</th>
              <th className="px-3 py-3 text-right font-semibold">Signal</th>
              <th className="px-3 py-3 text-right font-semibold">Disclosure</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {items.map((item) => {
              const expanded = expandedItemId === item.id;
              const symbol = item.security?.symbol?.trim().toUpperCase() || null;
              const symbolHref = symbol ? tickerHref(symbol) : null;
              const gainLoss = gainLossLabel(item, canViewPremiumMetrics);
              const signal = signalLabel(item, canViewPremiumMetrics);
              const href = disclosureHref(item);
              const profileHref = entityHref(item);
              const overlay: SignalOverlay = overlaySignals ? overlaySignals[String(item.id)] ?? null : null;
              return (
                <Fragment key={item.id}>
                  <tr
                    tabIndex={0}
                    className="cursor-pointer bg-slate-950/20 align-top transition hover:bg-emerald-400/[0.06] focus:bg-emerald-400/[0.08] focus:outline-none"
                    onClick={() => onToggleExpanded(item.id)}
                    onKeyDown={(event) => onRowKeyDown(event, item.id, onToggleExpanded)}
                    aria-expanded={expanded}
                  >
                    <td className="px-3 py-3 text-slate-300">{dateLabel(item)}</td>
                    <td className="px-3 py-3 font-semibold text-slate-100">{sourceLabel(item)}</td>
                    <td className="px-3 py-3">
                      <div className="min-w-0">
                        {symbolHref ? (
                          <Link href={symbolHref} prefetch={false} className="font-mono font-semibold text-emerald-200 hover:text-emerald-100" onClick={(event) => event.stopPropagation()}>
                            {symbol}
                          </Link>
                        ) : (
                          <span className="text-slate-500">-</span>
                        )}
                        <div className="mt-0.5 break-words text-xs leading-4 text-slate-500">
                          {isGovernmentContract(item) ? contractDescription(item) : companyLabel(item)}
                        </div>
                      </div>
                    </td>
                    <td className="break-words px-3 py-3 font-medium text-slate-100">
                      {profileHref ? (
                        <Link href={profileHref} prefetch={false} className="hover:text-emerald-100" onClick={(event) => event.stopPropagation()}>
                          {entityLabel(item)}
                        </Link>
                      ) : (
                        entityLabel(item)
                      )}
                      {roleLabel(item) ? <div className="mt-0.5 text-xs font-medium leading-4 text-slate-500">{roleLabel(item)}</div> : null}
                    </td>
                    <td className={`break-words px-3 py-3 font-semibold uppercase tracking-[0.08em] ${actionTone(item)}`}>
                      {actionLabel(item)}
                      {isGovernmentContract(item) ? <div className="mt-1 normal-case tracking-normal text-xs font-medium leading-4 text-slate-500">{contractDescription(item)}</div> : null}
                    </td>
                    <td className="px-3 py-3 text-right font-semibold tabular-nums text-slate-100">{amountLabel(item)}</td>
                    <td className={`px-3 py-3 text-right font-semibold tabular-nums ${gainLoss.tone}`}>
                      {gainLoss.label}
                      {gainLoss.sublabel ? <div className="text-[11px] font-medium text-slate-500">{gainLoss.sublabel}</div> : null}
                    </td>
                    <td className={`px-3 py-3 text-right font-semibold tabular-nums ${signal.tone}`}>{signal.label}</td>
                    <td className="px-3 py-3 text-right">
                      {href ? (
                        <a href={href} target="_blank" rel="noreferrer" className="text-xs font-semibold text-emerald-200 hover:text-emerald-100" onClick={(event) => event.stopPropagation()}>
                          {disclosureLabel(item)}
                        </a>
                      ) : (
                        <span className="text-xs text-slate-500">-</span>
                      )}
                    </td>
                  </tr>
                  {expanded ? (
                    <tr>
                      <td colSpan={9} className="bg-slate-950/40 px-4 py-4">
                        <ExpandedDetails item={item} overlay={overlay} canViewPremiumMetrics={canViewPremiumMetrics} />
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="divide-y divide-white/5 lg:hidden">
        {items.map((item) => {
          const expanded = expandedItemId === item.id;
          const symbol = item.security?.symbol?.trim().toUpperCase() || "-";
          const gainLoss = gainLossLabel(item, canViewPremiumMetrics);
          const signal = signalLabel(item, canViewPremiumMetrics);
          const overlay: SignalOverlay = overlaySignals ? overlaySignals[String(item.id)] ?? null : null;
          return (
            <div key={`mobile-${item.id}`} className="bg-slate-950/20">
              <button type="button" className="grid w-full grid-cols-[1fr_auto] gap-3 px-4 py-4 text-left" onClick={() => onToggleExpanded(item.id)} aria-expanded={expanded}>
                <span className="min-w-0">
                  <span className="block text-xs uppercase tracking-[0.16em] text-slate-500">
                    {sourceLabel(item)} / {dateLabel(item)}
                  </span>
                  <span className="mt-1 flex items-center gap-2">
                    <span className="font-mono text-sm font-semibold text-emerald-200">{symbol}</span>
                    <span className="min-w-0 break-words text-sm font-semibold text-slate-100">{entityLabel(item)}</span>
                  </span>
                  <span className={`mt-1 block text-sm font-semibold ${actionTone(item)}`}>{actionLabel(item)}</span>
                  <span className="mt-1 block text-sm text-slate-300">
                    {amountLabel(item)} / {isGovernmentContract(item) ? contractDescription(item) : companyLabel(item)}
                  </span>
                </span>
                <span className="text-right">
                  <span className={`block text-sm font-semibold tabular-nums ${gainLoss.tone}`}>{gainLoss.label}</span>
                  <span className={`mt-1 block text-xs font-semibold tabular-nums ${signal.tone}`}>Sig {signal.label}</span>
                </span>
              </button>
              {expanded ? (
                <div className="px-3 pb-4">
                  <ExpandedDetails item={item} overlay={overlay} canViewPremiumMetrics={canViewPremiumMetrics} />
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
