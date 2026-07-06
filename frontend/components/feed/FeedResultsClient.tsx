"use client";

import { useEffect, useMemo, useState } from "react";
import { FeedDebugVisibility } from "@/components/feed/FeedDebugVisibility";
import { FeedList } from "@/components/feed/FeedList";
import { FeedMountLogger } from "@/components/feed/FeedMountLogger";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getEvents, getTickerProfiles, type EventsResponse } from "@/lib/api";
import {
  eventToRenderedFeedItem,
  feedProfileSymbols,
  parsePayload,
  redactPremiumFeedMetrics,
  sortFeedItems,
  type CompanyNameMap,
} from "@/lib/feedEventMapper";
import type { FeedMode } from "@/lib/feedModes";
import type { FeedItem } from "@/lib/types";

type FeedParamKey =
  | "symbol"
  | "member"
  | "chamber"
  | "party"
  | "trade_type"
  | "role"
  | "ownership"
  | "recent_days"
  | "department"
  | "sort_by"
  | "sort_dir";

type FeedResultsClientProps = {
  feedMode: FeedMode;
  queryDebug: boolean;
  debugLifecycle: boolean;
  page: number;
  pageSize: 25 | 50 | 100;
  activeParams: Record<FeedParamKey, string>;
  canViewPremiumMetrics: boolean;
};

type FeedState = {
  loading: boolean;
  events: EventsResponse;
  companyNames: CompanyNameMap;
  errorMessage: string | null;
};

function FeedResultsSectionSkeleton() {
  return (
    <section className="space-y-4" aria-live="polite" aria-busy="true">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="space-y-2">
          <SkeletonBlock className="h-6 w-40" />
          <SkeletonBlock className="h-4 w-56" />
        </div>
      </div>
      <div className="space-y-3">
        {Array.from({ length: 5 }).map((_, idx) => (
          <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-2">
                <SkeletonBlock className="h-3 w-20" />
                <SkeletonBlock className="h-5 w-48" />
              </div>
              <SkeletonBlock className="h-6 w-16 rounded-full" />
            </div>
            <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
              {Array.from({ length: 4 }).map((__, statIdx) => (
                <SkeletonBlock key={statIdx} className="h-3 w-full" />
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export function FeedResultsClient({
  feedMode,
  queryDebug,
  debugLifecycle,
  page,
  pageSize,
  activeParams,
  canViewPremiumMetrics,
}: FeedResultsClientProps) {
  const [state, setState] = useState<FeedState>({
    loading: true,
    events: { items: [], limit: null, offset: null, total: null, has_more: null },
    companyNames: {},
    errorMessage: null,
  });
  const requestParams = useMemo(
    () => ({
      ...activeParams,
      enrich_prices: 1,
      include_net_flows: 0,
      limit: pageSize,
      page_size: pageSize,
      offset: (page - 1) * pageSize,
    }),
    [activeParams, page, pageSize],
  );
  const requestKey = useMemo(() => JSON.stringify({ feedMode, requestParams }), [feedMode, requestParams]);

  useEffect(() => {
    const controller = new AbortController();
    setState((current) => ({ ...current, loading: true, errorMessage: null }));
    getEvents({
      ...requestParams,
      tape: feedMode,
      source: "FeedClient",
      requestSource: "client",
      routeFamily: "feed",
      signal: controller.signal,
    })
      .then(async (events) => {
        if (controller.signal.aborted) return;
        const profileSymbols = feedProfileSymbols(events.items);
        let companyNames: CompanyNameMap = {};
        if (profileSymbols.length > 0) {
          try {
            const profiles = await getTickerProfiles(profileSymbols, { source: "FeedClient" });
            if (controller.signal.aborted) return;
            companyNames = Object.fromEntries(
              Object.entries(profiles)
                .map(([symbol, profile]) => [symbol.toUpperCase(), typeof profile?.ticker?.name === "string" ? profile.ticker.name.trim() : ""] as const)
                .filter((entry): entry is readonly [string, string] => Boolean(entry[1])),
            );
          } catch {
            companyNames = {};
          }
        }
        setState({ loading: false, events, companyNames, errorMessage: null });
      })
      .catch((error) => {
        if (controller.signal.aborted) return;
        setState({
          loading: false,
          events: { items: [], limit: null, offset: null, total: null, has_more: null },
          companyNames: {},
          errorMessage: error instanceof Error ? error.message : "Unable to load feed.",
        });
      });
    return () => controller.abort();
  }, [feedMode, requestKey, requestParams]);

  const items = useMemo(() => {
    const mapped = state.events.items
      .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
      .map((event) => eventToRenderedFeedItem(event, state.companyNames))
      .filter(Boolean) as FeedItem[];
    return redactPremiumFeedMetrics(sortFeedItems(mapped, activeParams.sort_by, activeParams.sort_dir), canViewPremiumMetrics);
  }, [activeParams.sort_by, activeParams.sort_dir, canViewPremiumMetrics, state.companyNames, state.events.items]);

  const total = typeof state.events.total === "number" ? state.events.total : null;
  const hasMore = typeof state.events.has_more === "boolean" ? state.events.has_more : null;
  const totalPages = total ? Math.max(1, Math.ceil(total / pageSize)) : 1;

  const debug = {
    request_params: requestParams,
    events_returned: state.events.items.length,
    fetch_error: state.errorMessage,
  };

  if (state.loading) return <FeedResultsSectionSkeleton />;

  return (
    <section className="space-y-4">
      <FeedMountLogger enabled={debugLifecycle} name="FeedResultsClient" detail={{ feedMode, page, pageSize }} />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-white">Latest events</h2>
          <p className="text-sm text-slate-400">Showing {items.length} events on page {page}.</p>
        </div>
      </div>
      {queryDebug ? (
        <FeedDebugVisibility initialQueryDebug={queryDebug}>
          <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4 text-xs text-slate-300">
            <div className="font-semibold text-slate-100">Debug feed request</div>
            <div className="mt-2 text-slate-400">
              <span className="font-semibold text-slate-200">request_params:</span>{" "}
              <span className="break-all font-mono text-[11px]">{JSON.stringify(debug.request_params)}</span>
            </div>
            <div className="mt-2 text-slate-400">
              <span className="font-semibold text-slate-200">events_returned:</span> {debug.events_returned}
            </div>
            {debug.fetch_error ? (
              <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-2 text-red-300">
                <div className="font-semibold">fetch_error:</div>
                <pre className="mt-1 whitespace-pre-wrap text-xs">{debug.fetch_error}</pre>
              </div>
            ) : null}
            <div className="mt-3 space-y-2">
              {state.events.items.slice(0, 3).map((event) => {
                const payload = parsePayload(event.payload);
                const symbol = typeof payload.symbol === "string" ? payload.symbol : event.ticker ?? "-";
                const memberName =
                  typeof payload.member?.name === "string"
                    ? payload.member.name
                    : typeof payload.member_name === "string"
                      ? payload.member_name
                      : event.source ?? "-";
                const tradeType = typeof payload.transaction_type === "string" ? payload.transaction_type : event.event_type;
                return (
                  <div key={event.id} className="rounded-lg border border-slate-800/60 bg-slate-900/40 p-3">
                    <div className="text-slate-200">
                      <span className="font-semibold">Symbol:</span> {symbol}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Member:</span> {memberName}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Trade type:</span> {tradeType}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </FeedDebugVisibility>
      ) : null}
      {state.errorMessage ? (
        <div className="rounded-2xl border border-rose-400/25 bg-rose-500/10 p-4 text-sm text-rose-100">
          Feed data could not load. Try again shortly.
        </div>
      ) : null}
      <div id="feed-top" />
      <div className="min-h-[32rem]">
        <FeedList
          items={items}
          page={page}
          pageSize={pageSize}
          total={total}
          totalPages={totalPages}
          hasMore={hasMore}
          overlaySignals={{}}
          canViewPremiumMetrics={canViewPremiumMetrics}
          debugLifecycle={debugLifecycle}
        />
      </div>
    </section>
  );
}
