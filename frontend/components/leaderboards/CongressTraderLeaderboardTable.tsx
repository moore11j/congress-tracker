import Link from "next/link";
import { Badge } from "@/components/Badge";
import {
  type CongressTraderLeaderboardPerformanceModel,
  type CongressTraderLeaderboardResponse,
  type CongressTraderLeaderboardSort,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { resultsTableFrameClassName, stickyResultsTableHeaderClassName } from "@/components/ui/resultsTableFrame";
import { tickerHref } from "@/lib/ticker";

function pct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(digits)}%`;
}

function pct0(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${Math.round(value * 100)}%`;
}

function ratio(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toFixed(2);
}

function lookbackLabel(days: number | null | undefined): string {
  if (days === 1095) return "3Y";
  if (days === 365) return "1Y";
  return days ? `${days}D` : "selected window";
}

function signedPctTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-400";
  if (Math.abs(value) < 0.05) return "text-slate-300";
  return value > 0 ? "text-emerald-300" : "text-rose-300";
}

function sharpeTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-400";
  if (value >= 1) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-200";
}

function drawdownTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-400";
  if (value <= 10) return "text-emerald-300";
  if (value >= 30) return "text-rose-300";
  return "text-slate-200";
}

function winRateTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-300";
  if (value >= 0.65) return "text-emerald-300";
  if (value <= 0.35) return "text-rose-300";
  return "text-slate-200";
}

function sortedColumnClass(active: boolean): string {
  return active ? "border-l border-emerald-400/15 bg-emerald-500/[0.04]" : "";
}

function sortedHeaderClass(active: boolean): string {
  return active
    ? "border-l border-emerald-400/20 bg-emerald-500/[0.07] font-semibold text-emerald-100"
    : "text-slate-400";
}

function isSortColumn(sort: CongressTraderLeaderboardSort, column: CongressTraderLeaderboardSort): boolean {
  return sort === column;
}

function sortDirectionLabel(column: CongressTraderLeaderboardSort): string {
  return column === "max_drawdown_pct" ? "asc" : "desc";
}

function SortHeaderLabel({
  label,
  active,
  sort,
}: {
  label: string;
  active: boolean;
  sort: CongressTraderLeaderboardSort;
}) {
  return (
    <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
      <span className="normal-case">{label}</span>
      <span className="text-[10px] font-semibold normal-case tracking-normal text-slate-500">
        {active ? sortDirectionLabel(sort) : ""}
      </span>
    </span>
  );
}

function SortHeader({
  label,
  column,
  activeSort,
  sortHrefs,
  className = "text-right",
}: {
  label: string;
  column: CongressTraderLeaderboardSort;
  activeSort: CongressTraderLeaderboardSort;
  sortHrefs?: Partial<Record<CongressTraderLeaderboardSort, string>>;
  className?: string;
}) {
  const active = isSortColumn(activeSort, column);
  const content = <SortHeaderLabel label={label} active={active} sort={column} />;
  return (
    <th className={`px-4 py-3 ${className} ${sortedHeaderClass(active)}`}>
      {sortHrefs?.[column] ? (
        <Link href={sortHrefs[column]} prefetch={false} className="inline-flex justify-end hover:text-white">
          {content}
        </Link>
      ) : (
        content
      )}
    </th>
  );
}

export function CongressTraderLeaderboardEmptyState({
  performanceModel,
}: {
  performanceModel: CongressTraderLeaderboardPerformanceModel;
}) {
  return (
    <div className="p-8 text-center text-sm text-slate-300">
      {performanceModel === "portfolio"
        ? "Portfolio simulations are being recomputed following a methodology update."
        : "No members matched your current filters."}
    </div>
  );
}

function LeaderboardTableHeader({
  sort,
  isInsiderMode,
  performanceModel,
  sortHrefs,
}: {
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sortHrefs?: Partial<Record<CongressTraderLeaderboardSort, string>>;
}) {
  const isPortfolioMode = performanceModel === "portfolio";

  return (
    <thead className={`${stickyResultsTableHeaderClassName} border-b border-white/10 bg-slate-950 text-xs uppercase tracking-wide`}>
      <tr>
        <th className="px-4 py-3 text-slate-400">Rank</th>
        <th className="px-4 py-3 text-slate-400">{isInsiderMode ? "Insider" : "Member"}</th>
        {isInsiderMode ? (
          <>
            <th className="px-4 py-3 text-slate-400">Ticker</th>
            <th className="px-4 py-3 text-slate-400">Role</th>
          </>
        ) : (
          <>
            <th className="px-4 py-3 text-slate-400">Chamber</th>
            <th className="px-4 py-3 text-slate-400">Party</th>
          </>
        )}
        {isPortfolioMode ? (
          <>
            <SortHeader label="Total Return" column="total_return_pct" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="CAGR" column="cagr_pct" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Alpha" column="alpha_pct" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Sharpe" column="sharpe_ratio" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Max Drawdown" column="max_drawdown_pct" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Position Win Rate" column="win_rate_pct" activeSort={sort} sortHrefs={sortHrefs} />
          </>
        ) : (
          <>
            <SortHeader label="Trades" column="trade_count" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Avg Return" column="avg_return" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Avg Alpha" column="avg_alpha" activeSort={sort} sortHrefs={sortHrefs} />
            <SortHeader label="Win Rate" column="win_rate" activeSort={sort} sortHrefs={sortHrefs} />
          </>
        )}
      </tr>
    </thead>
  );
}

export function CongressTraderLeaderboardStatusState({
  title,
  message,
  sort,
  isInsiderMode,
  performanceModel,
  sortHrefs,
  actionLabel,
  onAction,
}: {
  title: string;
  message: string;
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sortHrefs?: Partial<Record<CongressTraderLeaderboardSort, string>>;
  actionLabel?: string;
  onAction?: () => void;
}) {
  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
          <LeaderboardTableHeader
            sort={sort}
            isInsiderMode={isInsiderMode}
            performanceModel={performanceModel}
            sortHrefs={sortHrefs}
          />
        </table>
      </div>
      <div className="p-6 text-sm text-slate-300">
        <p className="font-semibold text-white">{title}</p>
        <p className="mt-2 text-slate-400">{message}</p>
        {actionLabel && onAction ? (
          <button
            type="button"
            onClick={onAction}
            className="mt-4 rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
          >
            {actionLabel}
          </button>
        ) : null}
      </div>
    </>
  );
}

export function CongressTraderLeaderboardTable({
  data,
  sort,
  isInsiderMode,
  performanceModel,
  sortHrefs,
}: {
  data: CongressTraderLeaderboardResponse;
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sortHrefs?: Partial<Record<CongressTraderLeaderboardSort, string>>;
}) {
  const isPortfolioMode = performanceModel === "portfolio";
  const excludedPoorQualityCount =
    data.excluded_poor_quality_count ?? data.metadata?.excluded_poor_quality_count ?? 0;
  const qualityFilterApplied = data.quality_filter_applied ?? data.metadata?.quality_filter_applied ?? false;

  return (
    <>
      <div className={resultsTableFrameClassName(data.rows.length)}>
        <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
          <LeaderboardTableHeader
            sort={sort}
            isInsiderMode={isInsiderMode}
            performanceModel={performanceModel}
            sortHrefs={sortHrefs}
          />
          <tbody className="divide-y divide-white/5">
            {data.rows.map((row) => {
              const chamberBadgeValue = chamberBadge(row.chamber);
              const party = partyBadge(row.party);
              const roleCode = normalizeInsiderRoleBadge(row.role);
              const roleTone = insiderRoleBadgeTone(roleCode);
              const insiderDisplayName = getInsiderDisplayName(row.member_name) ?? row.member_name;
              const insiderLink = insiderHref(insiderDisplayName, row.reporting_cik ?? row.member_id);
              const congressMemberId = row.bioguide_id ?? row.member_id;
              const memberLink = memberHref({ slug: congressMemberId, memberId: congressMemberId });
              const rowTicker = row.symbol ?? row.ticker ?? null;
              const tickerLink = tickerHref(rowTicker);
              return (
                <tr key={`${row.rank}-${row.member_id}-${rowTicker ?? ""}`} className="text-slate-200 transition-colors hover:bg-slate-900/35">
                  <td className="px-4 py-3">
                    <span className="inline-flex min-w-11 items-center justify-center rounded-md border border-white/10 bg-white/[0.03] px-2 py-1 text-center font-semibold text-white">
                      #{row.rank}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    {isInsiderMode ? (
                      <div className="min-w-[210px]">
                        {insiderLink ? (
                          <Link href={insiderLink} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200 hover:underline">
                            {insiderDisplayName}
                          </Link>
                        ) : (
                          <span className="font-semibold text-slate-100">{insiderDisplayName}</span>
                        )}
                        {row.company_name ? <div className="text-xs text-slate-400">{row.company_name}</div> : null}
                      </div>
                    ) : row.chamber ? (
                      <Link href={memberLink} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200 hover:underline">
                        {row.member_name}
                      </Link>
                    ) : (
                      <span className="font-semibold text-slate-100">{row.member_name}</span>
                    )}
                  </td>
                  {isInsiderMode ? (
                    <>
                      <td className="px-4 py-3">
                        {rowTicker ? (
                          tickerLink ? (
                            <Link href={tickerLink} prefetch={false} className="font-mono text-xs font-semibold uppercase tracking-wide text-emerald-200 hover:text-emerald-100 hover:underline">
                              {rowTicker}
                            </Link>
                          ) : (
                            <span className="font-mono text-xs uppercase tracking-wide text-slate-300">{rowTicker}</span>
                          )
                        ) : (
                          <span className="text-slate-500">--</span>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <Badge tone={roleTone} className="px-2 py-0.5 text-[10px]">
                          {roleCode}
                        </Badge>
                      </td>
                    </>
                  ) : (
                    <>
                      <td className="px-4 py-3">
                        <span title={row.chamber ?? undefined}>
                          <Badge tone={chamberBadgeValue.tone} className="px-2 py-0.5 text-[10px]">
                            {chamberBadgeValue.label}
                          </Badge>
                        </span>
                      </td>
                      <td className="px-4 py-3">
                        <span title={row.party ?? undefined}>
                          <Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">
                            {party.label}
                          </Badge>
                        </span>
                      </td>
                    </>
                  )}
                  {isPortfolioMode ? (
                    <>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.total_return_pct)} ${isSortColumn(sort, "total_return_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "total_return_pct"))}`}>
                        {pct(row.total_return_pct)}
                      </td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.cagr_pct)} ${isSortColumn(sort, "cagr_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "cagr_pct"))}`}>
                        {pct(row.cagr_pct)}
                      </td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.alpha_pct)} ${isSortColumn(sort, "alpha_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "alpha_pct"))}`}>
                        {pct(row.alpha_pct)}
                      </td>
                      <td className={`px-4 py-3 text-right ${sharpeTone(row.sharpe_ratio)} ${isSortColumn(sort, "sharpe_ratio") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "sharpe_ratio"))}`}>
                        {ratio(row.sharpe_ratio)}
                      </td>
                      <td className={`px-4 py-3 text-right ${drawdownTone(row.max_drawdown_pct)} ${isSortColumn(sort, "max_drawdown_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "max_drawdown_pct"))}`}>
                        {pct(row.max_drawdown_pct)}
                      </td>
                      <td
                        className={`px-4 py-3 text-right ${winRateTone((row.win_rate_pct ?? 0) / 100)} ${isSortColumn(sort, "win_rate_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "win_rate_pct"))}`}
                        title="Share of simulated portfolio positions with positive realized or marked returns."
                      >
                        {pct(row.win_rate_pct)}
                      </td>
                    </>
                  ) : (
                    <>
                      <td className={`px-4 py-3 text-right text-slate-300 ${sortedColumnClass(isSortColumn(sort, "trade_count"))}`}>
                        {row.trade_count_total}
                      </td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_return)} ${isSortColumn(sort, "avg_return") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_return"))}`}>
                        {pct(row.avg_return)}
                      </td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_alpha)} ${isSortColumn(sort, "avg_alpha") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_alpha"))}`}>
                        {pct(row.avg_alpha)}
                      </td>
                      <td className={`px-4 py-3 text-right ${winRateTone(row.win_rate)} ${isSortColumn(sort, "win_rate") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "win_rate"))}`}>
                        {pct0(row.win_rate)}
                      </td>
                    </>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-white/10 bg-slate-950/60 px-4 py-3 text-xs text-slate-400">
        <div>
          {isPortfolioMode ? (
            <span>
              Portfolio simulation over {lookbackLabel(data.lookback_days ?? data.metadata?.lookback_days)} with realistic disclosure lag.
              {qualityFilterApplied ? " Showing simulations that meet the public data-quality threshold." : ""}
              {excludedPoorQualityCount > 0 ? " Lower-coverage simulations are excluded from rankings." : ""}
            </span>
          ) : (
            "Historical trade performance over the selected lookback period, compared against the S&P 500."
          )}
        </div>
        <div>{data.rows.length} rows</div>
      </div>
    </>
  );
}
