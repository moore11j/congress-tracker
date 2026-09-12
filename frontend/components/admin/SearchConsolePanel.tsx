"use client";

import { useState } from "react";
import { connectSearchConsole, disconnectSearchConsole, syncSearchConsole, type SearchConsoleStatus } from "@/lib/api";

export default function SearchConsolePanel({ status, onChange }: { status: SearchConsoleStatus; onChange: (value: SearchConsoleStatus) => void }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  async function action(kind: "connect" | "sync" | "disconnect") {
    if (kind === "disconnect" && !window.confirm("Stop Search Console syncing and remove the stored connection and performance snapshot? This does not revoke your ordinary Google login. You can also remove access in your Google account.")) return;
    setBusy(true); setError(""); setMessage("");
    try {
      if (kind === "connect") {
        const { authorization_url } = await connectSearchConsole();
        window.location.assign(authorization_url);
        return;
      }
      const result = kind === "sync" ? await syncSearchConsole() : await disconnectSearchConsole();
      onChange(result);
      setMessage(kind === "disconnect" ? "Search Console disconnected."
        : result.sync_result === "already_attempted" ? "A sync was recently requested. Wait five minutes before requesting another."
        : result.error ? "Sync needs attention; the previous successful data has been retained." : "Search Console performance synced.");
    } catch (err) { setError(err instanceof Error ? err.message : "Search Console request failed."); }
    finally { setBusy(false); }
  }
  const button = "rounded-lg border border-white/15 px-4 py-2 text-sm font-semibold disabled:opacity-40";
  return <section className="mt-6 rounded-lg border border-emerald-300/20 p-4">
    <h4 className="font-semibold text-white">Google Search Console</h4>
    <p className="mt-2 text-sm text-slate-300">{status.connected ? `Connected: ${status.email} · ${status.property}` : "Connect Google to use measured search performance in daily topic selection."}</p>
    <p className="mt-2 text-xs leading-5 text-slate-400">Read-only access. Walnut reads only walnutmarkets.com, stores its connection encrypted, and syncs daily. Google’s permission covers the Search Console properties your account can access; Walnut limits its requests to this property. No Gmail or Drive access.</p>
    <p className="mt-2 text-xs text-slate-400">{status.note} Keyword Planner volumes are managed separately below.</p>
    <div className="mt-3 flex flex-wrap gap-3">
      <button className={button} disabled={busy || !status.configured} onClick={() => action("connect")}>{status.connected ? "Reconnect Google" : "Connect Google Search Console"}</button>
      {status.connected ? <><button className={button} disabled={busy} onClick={() => action("sync")}>Sync performance now</button><button className={button} disabled={busy} onClick={() => action("disconnect")}>Disconnect</button></> : null}
    </div>
    {!status.configured ? <p className="mt-2 text-sm text-amber-200">Server Google OAuth configuration is required.</p> : null}
    {error || status.error ? <p role="alert" className="mt-2 text-sm text-rose-200">{error || status.error}</p> : null}
    {message ? <p role="status" className="mt-2 text-sm text-emerald-200">{message}</p> : null}
    {status.connected ? <p className="mt-3 text-xs text-slate-400">Last successful sync: {status.last_sync_at ? new Date(status.last_sync_at).toLocaleString() : "Not synced yet"}.{status.stale ? " Data is missing or stale and is excluded from topic selection." : ""}</p> : null}
    {status.period ? <p className="mt-2 text-xs text-slate-400">Finalized web-search data: {status.period.start} through {status.period.end} (Pacific dates). Compared with the preceding 28 days. {status.row_cap_reached ? "The 50,000-row import cap was reached." : ""}</p> : null}
    {status.queries?.length ? <div className="mt-4 overflow-x-auto"><table className="w-full text-left text-xs"><caption className="mb-2 text-left font-semibold">Top imported queries by impressions</caption><thead><tr><th className="p-2">Query</th><th className="p-2">Impressions</th><th className="p-2">Clicks</th><th className="p-2">CTR</th><th className="p-2">Position</th></tr></thead><tbody>{status.queries.map(row => <tr className="border-t border-white/10" key={row.query}><td className="p-2">{row.query}</td><td className="p-2">{row.impressions.toLocaleString()}</td><td className="p-2">{row.clicks.toLocaleString()}</td><td className="p-2">{(row.ctr * 100).toFixed(1)}%</td><td className="p-2">{row.position.toFixed(1)}</td></tr>)}</tbody></table></div> : status.last_sync_at ? <p className="mt-3 text-sm text-slate-400">Google returned no query rows for this period.</p> : null}
    <h5 className="mt-4 text-sm font-semibold">Existing brief improvements</h5>
    <p className="mt-1 text-xs text-slate-400">Suggestions only. Existing posts are never automatically rewritten or republished.</p>
    {status.recommendations?.map(row => <article className="mt-3 rounded border border-white/10 p-3 text-sm" key={row.page}><a className="text-emerald-200 underline" href={row.page} target="_blank" rel="noreferrer">{row.page}</a><p className="mt-1">{row.recommendation}</p><p className="mt-1 text-xs text-slate-400">{row.impressions} impressions · {row.clicks} clicks · {(row.ctr * 100).toFixed(1)}% CTR · Position {row.position.toFixed(1)}</p></article>)}
    {status.last_sync_at && !status.recommendations?.length ? <p className="mt-2 text-sm text-slate-400">No imported research pages currently meet the improvement thresholds.</p> : null}
  </section>;
}
