import { getPublicActivity } from "@/lib/api";
import { formatDateShort, formatTransactionLabel } from "@/lib/format";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";

function filingUrl(value?: string | null) {
  if (!value) return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" && (url.hostname === "sec.gov" || url.hostname.endsWith(".sec.gov")) ? url.href : null;
  } catch { return null; }
}

export async function InsiderActivityPreview() {
  const response = await getPublicActivity({ tape: "insider", recent_days: 30, limit: 5 }).catch(() => null);
  const rows = response?.items ?? [];
  const dates = rows.map(row => resolveInsiderActivityDisplay(row as Record<string, unknown>).filingDate).filter((date): date is string => Boolean(date)).sort();
  return (
    <section className="mx-auto max-w-6xl px-4 pt-10 sm:px-6 lg:px-8" aria-labelledby="public-insider-preview">
      <h2 id="public-insider-preview" className="text-2xl font-semibold text-white">Recent reported insider trades</h2>
      <p className="mt-3 text-sm leading-6 text-slate-300">A public sample of recorded purchases and sales from the last 30 days, ordered by transaction date. Filings can arrive later or be amended; these are reported transactions, not live trades.</p>
      {dates.length ? <p className="mt-2 text-xs text-slate-400">Latest filing shown: {formatDateShort(dates[dates.length - 1])}. Cached preview; full research access depends on your plan.</p> : null}
      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        {rows.length ? <table className="w-full min-w-[700px] text-left text-sm">
          <thead className="bg-white/5 text-slate-400"><tr>{["Stock", "Insider / role", "Transaction", "Traded", "Filed", "Source"].map(label => <th key={label} scope="col" className="px-4 py-3 font-medium">{label}</th>)}</tr></thead>
          <tbody className="divide-y divide-white/10">{rows.map(row => {
            const display = resolveInsiderActivityDisplay(row as Record<string, unknown>);
            const source = filingUrl(row.url);
            return <tr key={row.id}>
              <td className="px-4 py-3">{row.symbol ? <a href={`https://app.walnutmarkets.com/ticker/${encodeURIComponent(row.symbol)}`} className="font-semibold text-emerald-200">{row.symbol}</a> : "Not provided"}</td>
              <td className="px-4 py-3 text-slate-200">{display.insiderName || row.member_name}<span className="block text-xs text-slate-400">{display.role || "Role not provided"}</span></td>
              <td className="px-4 py-3 text-slate-200">{formatTransactionLabel(row.trade_type)}</td>
              <td className="px-4 py-3 text-slate-300">{formatDateShort(display.transactionDate ?? row.ts)}</td>
              <td className="px-4 py-3 text-slate-300">{formatDateShort(display.filingDate)}</td>
              <td className="px-4 py-3">{source ? <a href={source} className="text-emerald-200 underline underline-offset-4">SEC filing</a> : <span className="text-slate-400">Not linked</span>}</td>
            </tr>;
          })}</tbody>
        </table> : <p className="p-5 text-sm text-slate-400">{response ? "No recorded insider purchases or sales in this preview window." : "The public preview is temporarily unavailable. You can still open the insider activity feed."}</p>}
      </div>
      <a href="https://app.walnutmarkets.com/feed?mode=insider" className="mt-4 inline-block text-sm font-semibold text-emerald-200">Explore insider activity →</a>
    </section>
  );
}
