import Link from "next/link";
import { Badge } from "@/components/Badge";
import {
  type CongressTraderLeaderboardPerformanceModel,
  type CongressTraderLeaderboardResponse,
  type CongressTraderLeaderboardSort,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";

function pct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(digits)}%`;
}

function pct0(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${Math.round(value * 100)}%`;
}

function signedPctTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-400";
  if (Math.abs(value) < 0.05) return "text-slate-300";
  return value > 0 ? "text-emerald-300" : "text-rose-300";
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

function SortHeaderLabel({ label, active }: { label: string; active: boolean }) {
  return (
    <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
      <span className="normal-case">{label}</span>
      <span className="text-[10px] font-semibold normal-case tracking-normal text-slate-500">
        {active ? "desc" : ""}
      </span>
    </span>
  );
}

function coverageLabel(status: string | null | undefined, coveragePct: number | null | undefined): string {
  if (coveragePct != null && Number.isFinite(coveragePct)) return `${Math.round(coveragePct)}% coverage`;
  const normalized = (status ?? "").trim().toLowerCase();
  if (normalized === "warning") return "Sufficient coverage";
  return "High coverage";
}

function coverageTitle(status: string | null | undefined): string {
  const normalized = (status ?? "").trim().toLowerCase();
  if (normalized === "warning") {
    return "Some holdings had limited pricing coverage, but this simulation meets the public quality threshold.";
  }
  return "This simulation has strong pricing coverage.";
}

function coverageBadgeClass(status: string | null | undefined): string {
  const normalized = (status ?? "").trim().toLowerCase();
  if (normalized === "warning") {
    return "border-amber-300/20 bg-amber-300/[0.08] text-amber-100";
  }
  return "border-emerald-300/20 bg-emerald-300/[0.08] text-emerald-100";
}

export function CongressTraderLeaderboardEmptyState({
  performanceModel,
}: {
  performanceModel: CongressTraderLeaderboardPerformanceModel;
}) {
  return (
    <div className="p-8 text-center text-sm text-slate-300">
      {performanceModel === "portfolio"
        ? "No portfolio simulations meet the data-quality threshold for this view yet."
        : "No members matched your current filters."}
    </div>
  );
}

function LeaderboardTableHeader({
  sort,
  isInsiderMode,
  performanceModel,
}: {
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
}) {
  const isPortfolioMode = performanceModel === "portfolio";

  return (
    <thead className="border-b border-white/10 bg-slate-950/70 text-xs uppercase tracking-wide">
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
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "total_return_pct"))}`}>
              <SortHeaderLabel label="Total Return" active={isSortColumn(sort, "total_return_pct")} />
            </th>
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "alpha_pct"))}`}>
              <SortHeaderLabel label="Alpha" active={isSortColumn(sort, "alpha_pct")} />
            </th>
            <th className="px-4 py-3 text-right text-slate-400">Benchmark Return</th>
            <th className="px-4 py-3 text-right text-slate-400">Avg Exposure</th>
            <th className="px-4 py-3 text-right text-slate-400">Positions</th>
            <th className="px-4 py-3 text-slate-400">Data Quality</th>
          </>
        ) : (
          <>
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "trade_count"))}`}>
              <SortHeaderLabel label="Trades" active={isSortColumn(sort, "trade_count")} />
            </th>
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_return"))}`}>
              <SortHeaderLabel label="Avg Return" active={isSortColumn(sort, "avg_return")} />
            </th>
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_alpha"))}`}>
              <SortHeaderLabel label="Avg Alpha" active={isSortColumn(sort, "avg_alpha")} />
            </th>
            <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "win_rate"))}`}>
              <SortHeaderLabel label="Win Rate" active={isSortColumn(sort, "win_rate")} />
            </th>
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
}: {
  title: string;
  message: string;
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
}) {
  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
          <LeaderboardTableHeader sort={sort} isInsiderMode={isInsiderMode} performanceModel={performanceModel} />
        </table>
      </div>
      <div className="p-6 text-sm text-slate-300">
        <p className="font-semibold text-white">{title}</p>
        <p className="mt-2 text-slate-400">{message}</p>
      </div>
    </>
  );
}

export function CongressTraderLeaderboardTable({
  data,
  sort,
  isInsiderMode,
  performanceModel,
}: {
  data: CongressTraderLeaderboardResponse;
  sort: CongressTraderLeaderboardSort;
  isInsiderMode: boolean;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
}) {
  const isPortfolioMode = performanceModel === "portfolio";
  const excludedPoorQualityCount =
    data.excluded_poor_quality_count ?? data.metadata?.excluded_poor_quality_count ?? 0;
  const qualityFilterApplied = data.quality_filter_applied ?? data.metadata?.quality_filter_applied ?? false;

  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
          <LeaderboardTableHeader sort={sort} isInsiderMode={isInsiderMode} performanceModel={performanceModel} />
          <tbody className="divide-y divide-white/5">
            {data.rows.map((row) => {
              const chamberBadgeValue = chamberBadge(row.chamber);
              const party = partyBadge(row.party);
              const roleCode = normalizeInsiderRoleBadge(row.role);
              const roleTone = insiderRoleBadgeTone(roleCode);
              const insiderLink = insiderHref(row.member_name, row.reporting_cik ?? row.member_id);
              const memberLink = memberHref({ slug: row.member_slug, name: row.member_name, memberId: row.member_id });
              const rowTicker = row.symbol ?? row.ticker ?? null;
              const tickerLink = tickerHref(rowTicker);
              const qualityStatus = row.curve_quality_status ?? row.data_coverage?.curve_quality_status ?? "good";
              const pricedCoveragePct = row.avg_priced_invested_value_pct ?? row.data_coverage?.avg_priced_invested_value_pct;

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
                            {row.member_name}
                          </Link>
                        ) : (
                          <span className="font-semibold text-slate-100">{row.member_name}</span>
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
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.alpha_pct)} ${isSortColumn(sort, "alpha_pct") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "alpha_pct"))}`}>
                        {pct(row.alpha_pct)}
                      </td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.benchmark_return_pct)}`}>{pct(row.benchmark_return_pct)}</td>
                      <td className="px-4 py-3 text-right text-slate-300">{pct(row.average_exposure_pct)}</td>
                      <td className="px-4 py-3 text-right text-slate-300">{row.positions_count ?? "--"}</td>
                      <td className="px-4 py-3">
                        <span
                          title={coverageTitle(qualityStatus)}
                          className={`inline-flex items-center whitespace-nowrap rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${coverageBadgeClass(qualityStatus)}`}
                        >
                          {coverageLabel(qualityStatus, pricedCoveragePct)}
                        </span>
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
              Portfolio simulation over 365D with realistic disclosure lag.
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
