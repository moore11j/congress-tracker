"use client";

import { useEffect, useState } from "react";
import { getInsightsMacroSnapshot } from "@/lib/api";
import type { MacroSnapshotResponse } from "@/lib/types";
import { MarketSnapshot } from "@/components/insights/MarketSnapshot";
import { cardClassName } from "@/lib/styles";

const EMPTY_SNAPSHOT: MacroSnapshotResponse = {
  world_indexes: [],
  indexes: [],
  treasury: [],
  economics: [],
  commodities: [],
  currencies: [],
  crypto: [],
  sector_performance: [],
  status: "loading",
  generated_at: new Date().toISOString(),
};

function SnapshotSkeleton() {
  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="h-7 w-48 animate-pulse rounded bg-white/10" />
          <div className="mt-3 h-4 w-full max-w-2xl animate-pulse rounded bg-white/10" />
        </div>
        <div className="h-8 w-28 animate-pulse rounded bg-white/10" />
      </div>
      <div className="mt-6 grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 8 }).map((_, index) => (
          <div key={index} className="min-h-[18rem] rounded-2xl border border-white/10 bg-slate-950/55 p-4">
            <div className="h-4 w-28 animate-pulse rounded bg-white/10" />
            <div className="mt-2 h-3 w-20 animate-pulse rounded bg-white/10" />
            <div className="mt-5 space-y-3">
              {Array.from({ length: 5 }).map((__, row) => (
                <div key={row} className="grid grid-cols-[1fr_auto] gap-3">
                  <div className="h-4 animate-pulse rounded bg-white/10" />
                  <div className="h-4 w-16 animate-pulse rounded bg-white/10" />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export function InsightsMarketSnapshotClient() {
  const [snapshot, setSnapshot] = useState<MacroSnapshotResponse | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    getInsightsMacroSnapshot({ signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setSnapshot(payload);
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setSnapshot({ ...EMPTY_SNAPSHOT, status: "unavailable" });
          setFailed(true);
        }
      });
    return () => controller.abort();
  }, []);

  if (!snapshot) return <SnapshotSkeleton />;
  if (snapshot.status === "warming") {
    return (
      <section className={cardClassName}>
        <h2 className="text-2xl font-semibold text-white">Market Snapshot</h2>
        <p className="mt-2 text-sm text-slate-400">Market snapshot is warming. Check back shortly.</p>
      </section>
    );
  }

  return (
    <div>
      {failed ? <div className="mb-3 rounded-lg border border-rose-300/20 bg-rose-400/10 px-3 py-2 text-sm text-rose-100">Market snapshot is temporarily unavailable.</div> : null}
      <MarketSnapshot snapshot={snapshot} />
    </div>
  );
}
