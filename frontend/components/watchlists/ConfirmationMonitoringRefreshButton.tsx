"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { refreshWatchlistConfirmationMonitoring } from "@/lib/api";

type Props = {
  watchlistId: number;
};

export function ConfirmationMonitoringRefreshButton({ watchlistId }: Props) {
  const router = useRouter();
  const [status, setStatus] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function refresh() {
    setPending(true);
    setStatus(null);
    try {
      const result = await refreshWatchlistConfirmationMonitoring(watchlistId);
      const generated = Math.max(result.generated ?? 0, 0);
      const initialized = Math.max(result.initialized ?? 0, 0);
      setStatus(
        generated > 0
          ? `${generated} change${generated === 1 ? "" : "s"} found`
          : initialized > 0
          ? "Monitor baseline set"
          : "No material change",
      );
      router.refresh();
    } catch {
      setStatus("Refresh failed");
    } finally {
      setPending(false);
    }
  }

  return (
    <div className="flex flex-wrap items-center gap-2">
      <button
        type="button"
        onClick={refresh}
        disabled={pending}
        className="inline-flex h-9 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-300/20 disabled:cursor-not-allowed disabled:opacity-60"
      >
        {pending ? "Checking..." : "Refresh monitor"}
      </button>
      {status ? <span className="text-xs text-slate-400">{status}</span> : null}
    </div>
  );
}
