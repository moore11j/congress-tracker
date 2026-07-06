import { FeedFiltersServer } from "@/components/feed/FeedFiltersServer";
import { FeedResultsClient } from "@/components/feed/FeedResultsClient";
import { FeedMountLogger } from "@/components/feed/FeedMountLogger";
import { FeedClientProbe } from "@/components/feed/FeedClientProbe";
import { getEntitlements } from "@/lib/api";
import type { Metadata } from "next";
import { entitlementsFromTierHint, hasEntitlement } from "@/lib/entitlements";
import { isCompactFeedFilterMode, isInstitutionalFeedMode, isValidFeedMode, type FeedMode } from "@/lib/feedModes";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

// PR summary: Home feed renders a fast shell first; the client hydrates the unified event tape after page load. Production
// trades require backfill/dual-write from the legacy trade store.
function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

const feedParamKeys = [
  "symbol",
  "member",
  "chamber",
  "party",
  "trade_type",
  "role",
  "ownership",
  "recent_days",
  "department",
  "sort_by",
  "sort_dir",
] as const;

type FeedParamKey = (typeof feedParamKeys)[number];
type SearchParamsInput = Record<string, string | string[] | undefined>;

function feedParamsForMode(mode: FeedMode, params: Record<FeedParamKey, string>): Record<FeedParamKey, string> {
  if (!isCompactFeedFilterMode(mode)) return params;
  return {
    ...params,
    chamber: "",
    party: "",
    member: mode === "institutional" ? params.member : "",
    trade_type: params.trade_type,
    role: "",
    ownership: "",
    department: mode === "government_contracts" ? params.department : "",
  };
}

export async function generateMetadata({
  searchParams,
}: {
  searchParams?: Promise<SearchParamsInput>;
}): Promise<Metadata> {
  const sp = (await searchParams) ?? {};
  const modeParam = getParam(sp, "mode");
  const mode = isValidFeedMode(modeParam) ? modeParam : "all";

  return {
    alternates: {
      canonical: `/?mode=${mode}`,
    },
  };
}

function DebugMountLogger({
  enabled,
  name,
  detail,
}: {
  enabled: boolean;
  name: string;
  detail?: Record<string, unknown>;
}) {
  if (!enabled) return null;
  return <FeedMountLogger name={name} enabled={true} detail={detail} />;
}

function InstitutionalFeedLockedPanel() {
  return (
    <section className="space-y-4">
      <div className="rounded-3xl border border-emerald-400/20 bg-emerald-500/[0.06] p-5 shadow-card">
        <div className="max-w-2xl">
          <div className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-200">Institutional Activity</div>
          <h2 className="mt-2 text-xl font-semibold text-white">Pro required</h2>
          <p className="mt-2 text-sm leading-6 text-slate-300">
            Institutional Activity shows material 13F filing updates using filing dates and reported quarterly holdings.
          </p>
          <a
            href="/pricing"
            className="mt-4 inline-flex h-10 items-center justify-center rounded-xl border border-emerald-300/40 bg-emerald-400/10 px-4 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-400/20"
          >
            Upgrade to Pro
          </a>
        </div>
      </div>
    </section>
  );
}

export default async function FeedPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParamsInput>;
}) {
  const sp = (await searchParams) ?? {};
  const modeParam = getParam(sp, "mode");
  const feedMode = isValidFeedMode(modeParam) ? modeParam : "all";
  const authState = await optionalPageAuthState();
  const entitlements = authState.token
    ? await getEntitlements(authState.token, { source: "FeedPage" }).catch(() => entitlementsFromTierHint(authState.entitlementHint))
    : entitlementsFromTierHint(authState.entitlementHint);
  const canViewInstitutionalFeed = Boolean(authState.token && hasEntitlement(entitlements, "institutional_feed"));
  const canViewPremiumMetrics = Boolean(authState.token && hasEntitlement(entitlements, "premium_feed_metrics"));
  const institutionalFeedLocked = isInstitutionalFeedMode(feedMode) && !canViewInstitutionalFeed;
  const queryDebug = getParam(sp, "debug") === "1";
  const debugDisableFeedFilters = getParam(sp, "debug_disable_feed_filters") === "1";
  const debugDisableFeedResults = getParam(sp, "debug_disable_feed_results") === "1";
  const debugPlainFeedShell = getParam(sp, "debug_plain_feed_shell") === "1";
  const debugMoveProbeBelowResults = getParam(sp, "debug_move_probe_below_results") === "1";
  const debugMoveProbeAboveHeader = getParam(sp, "debug_move_probe_above_header") === "1";
  const debugReplaceHeaderWithProbe = getParam(sp, "debug_replace_header_with_probe") === "1";
  const debugServerPlaceholderInFilterSlot = getParam(sp, "debug_server_placeholder_in_filter_slot") === "1";
  const debugDisableTopMountLogger = getParam(sp, "debug_disable_top_mount_logger") === "1";
  const debugDisableAllMountLoggers = getParam(sp, "debug_disable_all_mount_loggers") === "1";
  const debugClientProbeInsideOuterWrapper = getParam(sp, "debug_client_probe_inside_outer_wrapper") === "1";
  const debugClientProbeInsideHeaderWrapper = getParam(sp, "debug_client_probe_inside_header_wrapper") === "1";
  const debugClientProbeBetweenHeaderAndResults = getParam(sp, "debug_client_probe_between_header_and_results") === "1";
  const debugLifecycle =
    queryDebug ||
    debugDisableFeedFilters ||
    debugDisableFeedResults ||
    debugPlainFeedShell ||
    debugMoveProbeBelowResults ||
    debugMoveProbeAboveHeader ||
    debugReplaceHeaderWithProbe ||
    debugServerPlaceholderInFilterSlot ||
    debugDisableTopMountLogger ||
    debugDisableAllMountLoggers ||
    debugClientProbeInsideOuterWrapper ||
    debugClientProbeInsideHeaderWrapper ||
    debugClientProbeBetweenHeaderAndResults ||
    getParam(sp, "debug_lifecycle") === "1";
  const debugMountLoggersEnabled = debugLifecycle && !debugDisableAllMountLoggers;
  const debugTopMountLoggerEnabled = debugMountLoggersEnabled && !debugDisableTopMountLogger;
  const requestedPage = Number(getParam(sp, "page") || "1");
  const page = Number.isFinite(requestedPage) ? Math.max(1, Math.floor(requestedPage)) : 1;
  const requestedPageSize = Number(getParam(sp, "page_size") || getParam(sp, "limit") || "50");
  const pageSize: 25 | 50 | 100 = [25, 50, 100].includes(requestedPageSize) ? (requestedPageSize as 25 | 50 | 100) : 50;
  const activeParams = feedParamsForMode(feedMode, {
    symbol: getParam(sp, "symbol"),
    member: getParam(sp, "member"),
    chamber: getParam(sp, "chamber"),
    party: getParam(sp, "party"),
    trade_type: getParam(sp, "trade_type"),
    role: getParam(sp, "role"),
    ownership: getParam(sp, "ownership"),
    recent_days: getParam(sp, "recent_days"),
    department: getParam(sp, "department"),
    sort_by: getParam(sp, "sort_by"),
    sort_dir: getParam(sp, "sort_dir"),
  });
  const resultsBoundaryKey = JSON.stringify({
    mode: feedMode,
    page,
    pageSize,
    institutionalFeedLocked,
    canViewPremiumMetrics,
    ...activeParams,
  });
  if (debugPlainFeedShell) {
    return (
      <div className="space-y-4">
        <DebugMountLogger enabled={debugTopMountLoggerEnabled} name="FeedPage" detail={{ feedMode, debugPlainFeedShell: true }} />
        <section className="rounded-2xl border border-amber-400/30 bg-amber-500/10 p-4 text-sm text-amber-100">
          <div className="font-semibold">debug_plain_feed_shell=1</div>
          <p className="mt-2">
            Minimal shell only. FeedFilters, FeedResultsClient loading visuals, and feed cards are intentionally disabled.
          </p>
        </section>
        <div className="rounded-2xl border border-white/15 bg-white/5 p-4 text-sm text-slate-300">
          <p>Static feed shell diagnostic content.</p>
          <p className="mt-1 text-slate-400">mode={feedMode}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <DebugMountLogger
        name="FeedPage"
        enabled={debugTopMountLoggerEnabled}
        detail={{
          feedMode,
          debugDisableFeedFilters,
          debugDisableFeedResults,
          debugMoveProbeBelowResults,
          debugMoveProbeAboveHeader,
          debugReplaceHeaderWithProbe,
          debugServerPlaceholderInFilterSlot,
          debugDisableTopMountLogger,
          debugDisableAllMountLoggers,
          debugClientProbeInsideOuterWrapper,
          debugClientProbeInsideHeaderWrapper,
          debugClientProbeBetweenHeaderAndResults,
        }}
      />
      <DebugMountLogger enabled={debugTopMountLoggerEnabled} name="FeedPageOuterWrapper" detail={{ wrapper: "top-outer-page-div" }} />

      {debugClientProbeInsideOuterWrapper ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeInsideOuterWrapperSlot" />
          <FeedClientProbe label="inside-outer-wrapper" />
        </div>
      ) : null}

      {debugMoveProbeAboveHeader ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeAboveHeaderSlot" />
          <FeedClientProbe label="above-header" />
        </div>
      ) : null}

      {debugReplaceHeaderWithProbe ? (
        <section className="flex flex-col gap-6">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedHeaderWrapper" detail={{ mode: "replaced_with_client_probe" }} />
          <FeedClientProbe label="header-replacement" />
        </section>
      ) : (
        <section className="flex flex-col gap-6">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedHeaderWrapper" detail={{ mode: "normal-header" }} />
          {debugClientProbeInsideHeaderWrapper ? (
            <div className="space-y-2">
              <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeInsideHeaderWrapperSlot" />
              <FeedClientProbe label="inside-header-wrapper" />
            </div>
          ) : null}
          <div className="flex flex-col gap-2">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
            <h1 className="text-4xl font-semibold text-white sm:text-5xl">Unified disclosure and market intelligence feed.</h1>
            <p className="max-w-2xl text-sm text-slate-400">
              One intelligence workflow: switch between All, Congress, Insider, Government Contracts, and Institutional Activity with mode-aware filters.
            </p>
          </div>
          <div className="contents">
            <DebugMountLogger
              name="FeedFilterSlotWrapper"
              enabled={debugMountLoggersEnabled}
              detail={{
                slot: "header-filter-area",
                debugDisableFeedFilters,
                debugMoveProbeBelowResults,
                debugServerPlaceholderInFilterSlot,
              }}
            />
            {debugDisableFeedFilters ? (
              <div className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-3 text-xs text-amber-100">
                debug_disable_feed_filters=1 (FeedFilters disabled)
              </div>
            ) : debugMoveProbeBelowResults ? null : debugServerPlaceholderInFilterSlot ? (
              <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-3 text-xs text-slate-300">
                debug_server_placeholder_in_filter_slot=1 (server-rendered placeholder only)
              </div>
            ) : (
              <FeedFiltersServer mode={feedMode} params={activeParams} />
            )}
          </div>
        </section>
      )}

      {debugClientProbeBetweenHeaderAndResults ? (
        <div className="space-y-2">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeBetweenHeaderAndResultsSlot" />
          <FeedClientProbe label="between-header-and-results" />
        </div>
      ) : null}

      {debugDisableFeedResults ? (
        <section className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-4 text-sm text-amber-100">
          debug_disable_feed_results=1 (FeedResultsClient / cards disabled)
        </section>
      ) : (
        <div className="space-y-3">
          <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedResultsClientWrapper" detail={{ wrapper: "results-client-wrapper" }} />
          {institutionalFeedLocked ? (
            <InstitutionalFeedLockedPanel />
          ) : (
            <FeedResultsClient
              key={resultsBoundaryKey}
              feedMode={feedMode}
              queryDebug={queryDebug}
              debugLifecycle={debugMountLoggersEnabled}
              page={page}
              pageSize={pageSize}
              activeParams={activeParams}
              canViewPremiumMetrics={canViewPremiumMetrics}
            />
          )}
          {debugMoveProbeBelowResults ? (
            <div className="space-y-2">
              <DebugMountLogger enabled={debugMountLoggersEnabled} name="FeedProbeBelowResultsSlot" />
              <FeedClientProbe label="below-results" />
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
