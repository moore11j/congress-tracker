"use client";

import { useEffect, useMemo, useState } from "react";
import {
  downloadAdminSalesLedger,
  getAdminSalesLedger,
  type SalesLedgerPeriod,
  type SalesLedgerResponse,
  type SalesLedgerSortBy,
  type SalesLedgerSortDir,
} from "@/lib/api";

const PERIOD_OPTIONS: Array<{ value: SalesLedgerPeriod; label: string }> = [
  { value: "last_7_days", label: "Last 7 Days" },
  { value: "last_30_days", label: "Last 30 Days" },
  { value: "month_to_date", label: "Month to Date" },
  { value: "year_to_date", label: "Year to Date" },
  { value: "all_dates", label: "All Dates" },
];

const SORT_OPTIONS: Array<{ value: SalesLedgerSortBy; label: string }> = [
  { value: "date_charged", label: "Date charged" },
  { value: "customer_name", label: "Customer name" },
  { value: "gross_amount", label: "Gross amount" },
  { value: "country", label: "Country" },
];

function formatDate(value?: string | null) {
  if (!value) return "-";
  return new Date(value).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function compactStatus(value: string) {
  return value.replaceAll("_", " ");
}

export function SalesLedgerReport() {
  const [period, setPeriod] = useState<SalesLedgerPeriod>("month_to_date");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [country, setCountry] = useState("");
  const [sortBy, setSortBy] = useState<SalesLedgerSortBy>("date_charged");
  const [sortDir, setSortDir] = useState<SalesLedgerSortDir>("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [ledger, setLedger] = useState<SalesLedgerResponse | null>(null);
  const [busy, setBusy] = useState(false);
  const [exporting, setExporting] = useState<"xlsx" | "pdf" | null>(null);
  const [status, setStatus] = useState<string | null>(null);

  const query = useMemo(
    () => ({
      period,
      start_date: period === "custom" ? startDate : undefined,
      end_date: period === "custom" ? endDate : undefined,
      country,
      sort_by: sortBy,
      sort_dir: sortDir,
      page,
      page_size: pageSize,
    }),
    [country, endDate, page, pageSize, period, sortBy, sortDir, startDate],
  );

  useEffect(() => {
    let ignore = false;
    const load = async () => {
      setBusy(true);
      setStatus(null);
      try {
        const next = await getAdminSalesLedger(query);
        if (!ignore) setLedger(next);
      } catch (error) {
        if (!ignore) setStatus(error instanceof Error ? error.message : "Unable to load Sales Ledger.");
      } finally {
        if (!ignore) setBusy(false);
      }
    };
    load();
    return () => {
      ignore = true;
    };
  }, [query]);

  const resetPage = () => setPage(1);

  const exportLedger = async (format: "xlsx" | "pdf") => {
    setExporting(format);
    setStatus(null);
    try {
      const { blob, filename } = await downloadAdminSalesLedger(format, {
        ...query,
        page: undefined,
        page_size: undefined,
      });
      const href = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = href;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(href);
      setStatus(`${format.toUpperCase()} export ready.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : `Unable to export ${format.toUpperCase()}.`);
    } finally {
      setExporting(null);
    }
  };

  const totalPages = ledger?.total_pages ?? 1;

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Reports</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Sales Ledger</h2>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Ledger-ready billing activity with tax, location, refund state, and finance exports.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => exportLedger("xlsx")}
            disabled={busy || exporting !== null}
            className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-60"
          >
            {exporting === "xlsx" ? "Exporting XLSX" : "Export XLSX"}
          </button>
          <button
            type="button"
            onClick={() => exportLedger("pdf")}
            disabled={busy || exporting !== null}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-60"
          >
            {exporting === "pdf" ? "Exporting PDF" : "Export PDF"}
          </button>
        </div>
      </div>

      <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-6">
        <label className="text-sm xl:col-span-2">
          <span className="block font-medium text-slate-200">Date filter</span>
          <select
            value={period}
            onChange={(event) => {
              setPeriod(event.target.value as SalesLedgerPeriod);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            {PERIOD_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        {period === "custom" ? (
          <>
            <label className="text-sm">
              <span className="block font-medium text-slate-200">Start date</span>
              <input
                type="date"
                value={startDate}
                onChange={(event) => {
                  setStartDate(event.target.value);
                  resetPage();
                }}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
              />
            </label>
            <label className="text-sm">
              <span className="block font-medium text-slate-200">End date</span>
              <input
                type="date"
                value={endDate}
                onChange={(event) => {
                  setEndDate(event.target.value);
                  resetPage();
                }}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
              />
            </label>
          </>
        ) : null}

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Country</span>
          <input
            value={country}
            maxLength={2}
            onChange={(event) => {
              setCountry(event.target.value.toUpperCase());
              resetPage();
            }}
            placeholder="All"
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white uppercase outline-none placeholder:normal-case placeholder:text-slate-500 focus:border-emerald-300/50"
          />
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Sort by</span>
          <select
            value={sortBy}
            onChange={(event) => {
              setSortBy(event.target.value as SalesLedgerSortBy);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            {SORT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Direction</span>
          <select
            value={sortDir}
            onChange={(event) => {
              setSortDir(event.target.value as SalesLedgerSortDir);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
        </label>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-400">
        <div>
          {busy ? "Loading Sales Ledger." : `${ledger?.total ?? 0} transactions`}
          {ledger?.filters.start_date ? ` from ${ledger.filters.start_date}` : ""}
          {ledger?.filters.end_date ? ` to ${ledger.filters.end_date}` : ""}
          {ledger?.filters.country ? ` in ${ledger.filters.country}` : ""}
        </div>
        <label className="flex items-center gap-2">
          <span>Rows</span>
          <select
            value={pageSize}
            onChange={(event) => {
              setPageSize(Number(event.target.value));
              resetPage();
            }}
            className="rounded-lg border border-white/10 bg-slate-950 px-2 py-1 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </label>
      </div>

      {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}

      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        <table className="min-w-[1500px] text-left text-xs">
          <thead className="bg-slate-950/70 uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-3 py-3">Transaction id</th>
              <th className="px-3 py-3">Customer</th>
              <th className="px-3 py-3">Date charged</th>
              <th className="px-3 py-3">Description</th>
              <th className="px-3 py-3">Country</th>
              <th className="px-3 py-3">State/province</th>
              <th className="px-3 py-3 text-right">Net revenue</th>
              <th className="px-3 py-3">VAT1 label</th>
              <th className="px-3 py-3 text-right">VAT1 collected</th>
              <th className="px-3 py-3">VAT2 label</th>
              <th className="px-3 py-3 text-right">VAT2 collected</th>
              <th className="px-3 py-3 text-right">Gross amount</th>
              <th className="px-3 py-3">Status / refund</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {(ledger?.items ?? []).map((row) => (
              <tr key={row.id} className="text-slate-300">
                <td className="whitespace-nowrap px-3 py-3 font-mono text-slate-200">{row.transaction_id}</td>
                <td className="whitespace-nowrap px-3 py-3 text-white">{row.customer_name}</td>
                <td className="whitespace-nowrap px-3 py-3">{formatDate(row.date_charged)}</td>
                <td className="min-w-[220px] px-3 py-3">{row.description || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3">{row.country || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3">{row.state_province || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3 text-right">{row.net_revenue_display}</td>
                <td className="whitespace-nowrap px-3 py-3">{row.vat1_label || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3 text-right">{row.vat1_collected_display || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3">{row.vat2_label || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3 text-right">{row.vat2_collected_display || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3 text-right font-semibold text-white">{row.gross_amount_display}</td>
                <td className="whitespace-nowrap px-3 py-3">{compactStatus(row.status_refund_state)}</td>
              </tr>
            ))}
            {!busy && (ledger?.items.length ?? 0) === 0 ? (
              <tr>
                <td colSpan={13} className="px-3 py-8 text-center text-sm text-slate-400">
                  No transactions match these filters.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm text-slate-400">
          Page {ledger?.page ?? page} of {totalPages}
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setPage(1)}
            disabled={busy || page <= 1}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            First
          </button>
          <button
            type="button"
            onClick={() => setPage((current) => Math.max(1, current - 1))}
            disabled={busy || page <= 1}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Previous
          </button>
          <button
            type="button"
            onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
            disabled={busy || page >= totalPages}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Next
          </button>
          <button
            type="button"
            onClick={() => setPage(totalPages)}
            disabled={busy || page >= totalPages}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Last
          </button>
        </div>
      </div>
    </section>
  );
}
