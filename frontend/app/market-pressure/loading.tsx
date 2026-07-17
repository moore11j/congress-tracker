"use client";

import { useEffect, useState } from "react";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

const loadingMessages = [
  "Loading components",
  "Analyzing data sets",
  "Ranking sector pressure",
  "Building ticker weights",
  "Calibrating confirmation layers",
  "Rendering market map",
];

export default function MarketPressureLoading() {
  const [messageIndex, setMessageIndex] = useState(0);

  useEffect(() => {
    const interval = window.setInterval(() => {
      setMessageIndex((index) => (index + 1) % loadingMessages.length);
    }, 420);
    return () => window.clearInterval(interval);
  }, []);

  return (
    <div className="space-y-5" aria-busy="true" aria-live="polite">
      <section className="space-y-3">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Loading</p>
        <div className="max-w-lg overflow-hidden rounded-md border border-emerald-300/20 bg-slate-950/80 p-1 shadow-inner shadow-black/30">
          <div className="h-3 origin-left rounded-sm bg-gradient-to-r from-emerald-400 via-cyan-300 to-white shadow-[0_0_18px_rgba(52,211,153,0.45)] motion-safe:animate-[market-pressure-progress_3.2s_cubic-bezier(0.3,0,0.05,1)_infinite]" />
        </div>
        <p key={messageIndex} className="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-100/85 motion-safe:animate-[market-pressure-status_420ms_ease-out]">
          {loadingMessages[messageIndex]}
        </p>
      </section>

      <section className="rounded-md border border-white/10 bg-slate-900/45 p-3 shadow-card sm:p-4">
        <div className="mb-3 space-y-2">
          <SkeletonBlock className="h-6 w-44" />
          <SkeletonBlock className="h-4 w-56" />
        </div>
        <div className="mb-4 flex flex-wrap gap-1">
          {Array.from({ length: 7 }).map((_, index) => (
            <SkeletonBlock key={index} className="h-10 w-20 rounded-none" />
          ))}
        </div>
        <div className="min-h-[34rem] overflow-hidden rounded-md border border-slate-950 bg-slate-950 p-1 sm:min-h-[42rem]">
          <div className="grid h-[34rem] grid-cols-[1.2fr_0.8fr_1fr] grid-rows-[0.9fr_1.1fr_0.8fr] gap-1 sm:h-[42rem]">
            <SkeletonBlock className="col-span-1 row-span-2 h-full w-full rounded-none" />
            <SkeletonBlock className="h-full w-full rounded-none" />
            <SkeletonBlock className="row-span-2 h-full w-full rounded-none" />
            <SkeletonBlock className="h-full w-full rounded-none" />
            <SkeletonBlock className="col-span-2 h-full w-full rounded-none" />
          </div>
        </div>
      </section>
    </div>
  );
}
