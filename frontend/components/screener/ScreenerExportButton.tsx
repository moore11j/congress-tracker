"use client";

import { useEffect, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { downloadScreenerCsv } from "@/lib/api";
import { ghostButtonClassName } from "@/lib/styles";

type Props = {
  params: Record<string, string | number>;
  filenamePrefix?: string;
  locked?: boolean;
  lockedReason?: string;
};

function saveBlob(blob: Blob, filename: string) {
  const href = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(href);
}

export function ScreenerExportButton({
  params,
  filenamePrefix = "screener",
  locked = false,
  lockedReason = "CSV export is included with Premium.",
}: Props) {
  const [exporting, setExporting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [statusTone, setStatusTone] = useState<"default" | "error">("default");
  const [upgradeOpen, setUpgradeOpen] = useState(false);

  useEffect(() => {
    if (!status) return;
    const timer = window.setTimeout(() => setStatus(null), 4000);
    return () => window.clearTimeout(timer);
  }, [status]);

  const exportCsv = async () => {
    setExporting(true);
    setStatus(null);
    setStatusTone("default");
    try {
      const { blob, filename, rowCap, exportedRows } = await downloadScreenerCsv(params, filenamePrefix);
      saveBlob(blob, filename);
      const capped = rowCap && exportedRows && exportedRows >= rowCap;
      setStatus(capped ? `CSV ready. Export capped at ${rowCap} rows.` : "CSV ready.");
    } catch (error) {
      setStatusTone("error");
      setStatus(error instanceof Error ? error.message : "Unable to export CSV.");
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="flex flex-col items-end gap-1">
      <button
        type="button"
        onClick={locked ? () => setUpgradeOpen(true) : exportCsv}
        disabled={exporting}
        className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${exporting ? "cursor-wait opacity-70" : ""}`}
      >
        {exporting ? "Exporting..." : locked ? "Export CSV · Premium" : "Export CSV"}
      </button>
      {status ? (
        <div className={`text-[11px] ${statusTone === "error" ? "text-rose-300" : "text-slate-400"}`}>{status}</div>
      ) : null}
      {upgradeOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-amber-300">Premium</p>
                <h2 className="mt-2 text-lg font-semibold">Export screener results</h2>
              </div>
              <button
                type="button"
                className="rounded-lg border border-white/10 px-2 py-1 text-sm text-slate-300 hover:text-white"
                onClick={() => setUpgradeOpen(false)}
              >
                Close
              </button>
            </div>
            <div className="mt-4">
              <UpgradePrompt title="Export screener results with Premium" body={lockedReason} compact={true} />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
