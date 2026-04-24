"use client";

import { useEffect, useState } from "react";
import { downloadScreenerCsv } from "@/lib/api";
import { ghostButtonClassName } from "@/lib/styles";

type Props = {
  params: Record<string, string | number>;
  filenamePrefix?: string;
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

export function ScreenerExportButton({ params, filenamePrefix = "screener" }: Props) {
  const [exporting, setExporting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [statusTone, setStatusTone] = useState<"default" | "error">("default");

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
        onClick={exportCsv}
        disabled={exporting}
        className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${exporting ? "cursor-wait opacity-70" : ""}`}
      >
        {exporting ? "Exporting..." : "Export CSV"}
      </button>
      {status ? (
        <div className={`text-[11px] ${statusTone === "error" ? "text-rose-300" : "text-slate-400"}`}>{status}</div>
      ) : null}
    </div>
  );
}
