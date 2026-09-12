"use client";

import {useState} from "react";
import {connectKeywordPlanner, disconnectKeywordPlanner, lookupKeywordMetrics, type KeywordPlannerStatus} from "@/lib/api";

export default function KeywordPlannerPanel({status, onChange}: {status: KeywordPlannerStatus; onChange: (value: KeywordPlannerStatus) => void}) {
  const [keywords, setKeywords] = useState("institutional ownership\ninsider buying\ncongress stock trades\nNvidia institutional ownership");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  async function action(kind: "connect" | "lookup" | "disconnect") {
    if (kind === "disconnect" && !window.confirm("Remove the stored Keyword Planner connection and cached metrics? This does not revoke Search Console or Google login. You may also revoke access in Google account settings.")) return;
    setBusy(true); setError(""); setMessage("");
    try {
      if (kind === "connect") {
        window.location.assign((await connectKeywordPlanner()).authorization_url);
        return;
      }
      const result = kind === "lookup" ? await lookupKeywordMetrics(keywords.split(/\n|,/).map(k => k.trim()).filter(Boolean).slice(0,50)) : await disconnectKeywordPlanner();
      onChange(result);
      setMessage(kind === "disconnect" ? "Keyword Planner disconnected." : "Results refreshed from cache or Google. New requests are limited to one per five minutes; allow up to ten words and 80 characters per phrase.");
    } catch (err) {setError(err instanceof Error ? err.message : "Keyword Planner request failed.");}
    finally {setBusy(false);}
  }
  const button = "rounded-lg border border-white/15 px-4 py-2 text-sm font-semibold disabled:opacity-40";
  return <section className="mt-6 rounded-lg border border-emerald-300/20 p-4">
    <h4 className="font-semibold text-white">Google Keyword Planner</h4>
    <p className="mt-2 text-sm">{status.connected ? `Connected: ${status.email} · Ads account ${status.customer_id}` : "Connect Google Ads for keyword-volume estimates in daily topic ranking."}</p>
    <p className="mt-2 text-xs leading-5 text-slate-400">Google requires its broad Google Ads permission, which includes viewing and managing Ads accounts. Walnut’s connector only reads account identity and historical keyword metrics for account 453-375-9595. It cannot create campaigns, change budgets or publish ads. This is separate from Search Console.</p>
    <p className="mt-2 text-xs text-slate-400">{status.note}</p>
    <p className="mt-2 text-sm">Target: {status.targeting.country} · {status.targeting.language} · {status.targeting.network}. Metrics cached for 30 days. Daily candidates are checked automatically before ranking.</p>
    <div className="mt-3 flex flex-wrap gap-3">
      <button className={button} disabled={busy || !status.configured} onClick={() => action("connect")}>{status.connected ? "Reconnect Keyword Planner" : "Connect Google Keyword Planner"}</button>
      {status.connected ? <button className={button} disabled={busy} onClick={() => action("disconnect")}>Disconnect Keyword Planner</button> : null}
    </div>
    {status.connected ? <><label className="mt-4 block text-sm">Check editorial keywords (one per line, up to 50)<textarea className="mt-2 w-full rounded-lg border border-white/15 bg-slate-950 p-3 text-white" rows={4} maxLength={4050} value={keywords} onChange={e => setKeywords(e.target.value)} /></label><button className={`${button} mt-2`} disabled={busy || !keywords.trim()} onClick={() => action("lookup")}>Check keyword volumes</button></> : null}
    {error || status.error ? <p role="alert" className="mt-3 text-sm text-rose-200">{error || status.error}</p> : null}
    {message ? <p role="status" className="mt-3 text-sm text-emerald-200">{message}</p> : null}
    {status.last_sync_at ? <p className="mt-3 text-xs text-slate-400">Last successful lookup: {new Date(status.last_sync_at).toLocaleString()}</p> : null}
    {status.metrics.length ? <div className="mt-4 overflow-x-auto"><table className="w-full text-left text-xs"><caption className="mb-2 text-left">Recent Google keyword estimates</caption><thead><tr><th className="p-2">Keyword / Google grouping</th><th className="p-2">Average monthly searches</th><th className="p-2">Monthly history</th><th className="p-2">Fetched</th></tr></thead><tbody>{status.metrics.map(row => <tr key={row.keyword} className="border-t border-white/10"><td className="p-2">{row.keyword}{row.google_keyword ? <p className="text-slate-400">Google grouping: {row.google_keyword}{row.close_variants.length ? ` + ${row.close_variants.join(", ")}` : ""}</p> : null}</td><td className="p-2">{row.avg_monthly_searches == null ? "Unavailable" : `~${row.avg_monthly_searches.toLocaleString()}`}{row.stale ? " (stale; excluded)" : ""}</td><td className="p-2"><details><summary>{row.monthly_searches.length} months</summary>{row.monthly_searches.map(m => <p key={`${m.year}-${m.month}`}>{m.month} {m.year}: {m.searches == null ? "Unavailable" : `~${m.searches.toLocaleString()}`}</p>)}</details></td><td className="p-2">{new Date(row.fetched_at).toLocaleDateString()}</td></tr>)}</tbody></table></div> : null}
  </section>;
}
