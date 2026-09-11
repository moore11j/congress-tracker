"use client";

import { useEffect, useState } from "react";
import { getDailyResearchSeo, saveDailyResearchSeo, runDailyResearchSeo, type DailySeoStatus, type DailySeoConfig } from "@/lib/api";

export default function DailyResearchSeo() {
  const [status, setStatus] = useState<DailySeoStatus | null>(null);
  const [config, setConfig] = useState<DailySeoConfig | null>(null);
  const [tickers, setTickers] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    getDailyResearchSeo().then((data) => {
      if (active) { setStatus(data); setConfig(data.config); setTickers(data.config.tickers.join(", ")); }
    }).catch((err) => { if (active) setError(err instanceof Error ? err.message : "Could not load Daily SEO."); });
    return () => { active = false; };
  }, []);
  async function action(kind: "save" | "run" | "refresh") {
    if (!config) return;
    setBusy(true); setError(""); setMessage("");
    try {
      const data = kind === "save" ? await saveDailyResearchSeo({ ...config, tickers: tickers.split(",").map(t => t.trim().toUpperCase()).filter(Boolean) })
        : kind === "run" ? await runDailyResearchSeo() : await getDailyResearchSeo();
      setStatus(data);
      if (kind === "save") { setConfig(data.config); setTickers(data.config.tickers.join(", ")); }
      setMessage(kind === "run" ? "Today's run requested. If already attempted, it will not run again or spend more on discovery. Refresh status to check progress."
        : kind === "save" ? "Daily SEO settings saved." : "Status updated.");
    } catch (err) { setError(err instanceof Error ? err.message : "Daily SEO request failed."); }
    finally { setBusy(false); }
  }
  const input = "mt-2 w-full rounded-lg border border-white/15 bg-slate-950 p-3 text-sm text-white";
  const button = "rounded-lg border border-white/15 px-4 py-2 text-sm font-semibold disabled:opacity-40";
  const dirty = Boolean(config && status && (JSON.stringify(config) !== JSON.stringify(status.config) || tickers !== status.config.tickers.join(", ")));
  return <section className="rounded-lg border border-emerald-300/20 bg-slate-950/55 p-5 text-slate-200">
    <h3 className="text-lg font-semibold text-white">Daily SEO research</h3>
    <p className="mt-2 text-sm leading-6 text-slate-400">Discover investor questions → rank opportunities → generate one sourced ticker research draft → email you for review. Nothing publishes without your approval.</p>
    {error ? <p role="alert" className="mt-4 text-rose-200">{error}</p> : null}
    {!config ? <p className="mt-4">{error ? "Reopen this tab to retry." : "Loading Daily SEO…"}</p> : <>
      <label className="mt-5 flex items-center gap-3"><input type="checkbox" checked={config.enabled} onChange={e => setConfig({ ...config, enabled: e.target.checked })} />Enable daily drafts (one per day)</label>
      <div className="mt-4 grid gap-4 md:grid-cols-2">
        <label className="text-sm">Audience topics<textarea className={input} rows={3} maxLength={1200} value={config.topics} onChange={e => setConfig({ ...config, topics: e.target.value })} /><span className="text-xs text-slate-400">Comma-separated themes, not a rigid article template.</span></label>
        <label className="text-sm">Focus tickers (optional)<input className={input} value={tickers} onChange={e => setTickers(e.target.value)} placeholder="NVDA, AVGO, NBIS" /><span className="text-xs text-slate-400">Leave empty to follow customer ticker searches and public signals. Up to 12.</span></label>
        <label className="text-sm">Start daily draft generation<input type="time" className={input} value={config.draft_time} onChange={e => setConfig({ ...config, draft_time: e.target.value })} /></label>
        <label className="text-sm">Timezone<input className={input} value={config.timezone} onChange={e => setConfig({ ...config, timezone: e.target.value })} placeholder="America/Los_Angeles" /></label>
      </div>
      <p className="mt-4 text-sm">Review email: <strong>{status?.review_email}</strong>. Email follows successful draft generation, not at the exact start time. Open the draft to preview, approve, edit directly, or ask AI for changes.</p>
      <p className="mt-2 text-xs leading-5 text-slate-400">Up to five candidates per discovery; one discovery attempt per day. No automatic daily discovery retry after failure. Generation uses the existing bounded correction flow. Topics below the editorial threshold ({config.minimum_score}/100) are skipped. Seven unreviewed drafts pause new generation.</p>
      <p className="mt-2 text-xs text-slate-400">{status?.metric_note}</p>
      <div className="mt-5 flex flex-wrap gap-3">
        <button className={`${button} bg-emerald-300 text-slate-950`} disabled={busy} onClick={() => action("save")}>Save Daily SEO</button>
        <button className={button} disabled={busy || dirty} onClick={() => action("run")}>Generate today’s draft</button>
        <button className={button} disabled={busy} onClick={() => action("refresh")}>Refresh status</button>
      </div>
      {dirty ? <p className="mt-2 text-xs text-amber-200">Save your settings before requesting a draft.</p> : null}
      {message ? <p role="status" className="mt-3 text-sm text-emerald-200">{message}</p> : null}
      <h4 className="mt-6 font-semibold">Editorial run history</h4>
      <p className="mt-1 text-xs text-slate-400">Queued runs start on the next five-minute worker check. A run stuck in planning or generating needs attention; it will not silently restart paid work.</p>
      <div className="mt-3 space-y-3">{status?.runs.map(run => <article key={run.day} className="rounded-lg border border-white/10 p-3">
        <p className="text-xs text-slate-400">{run.day} · {run.status.replaceAll("_", " ")}</p>
        {run.detail.selected ? <><p className="mt-2 font-semibold">{run.detail.selected.target_keyword} <span className="text-emerald-200">{run.detail.selected.priority_score}/100</span></p><p className="mt-1 text-sm text-slate-400">{run.detail.selected.selection_reason}</p></> : null}
        {run.detail.note || run.detail.error ? <p className="mt-2 text-sm text-amber-200">{run.detail.note || run.detail.error}</p> : null}
        {run.draft_id ? <a className="mt-2 inline-block font-semibold text-emerald-200 underline" href={`/admin/research-briefs?draft=${encodeURIComponent(run.draft_id)}`}>Preview and edit draft</a> : null}
        {run.draft_id ? <p className="mt-2 text-xs text-slate-400">Review email: {run.email_status || "not sent"}{run.email_status === "sent" ? " (accepted by email provider)" : ". Check email delivery settings if the draft is ready but the email was not sent."}</p> : null}
        {run.campaign_id ? <p className="mt-2 text-xs text-slate-400">Open Scheduled or Campaigns to review this article and its proposed publish time.</p> : null}
      </article>)}{status?.runs.length === 0 ? <p className="text-sm text-slate-400">No runs yet. Save your settings, then enable daily drafting or request today’s draft.</p> : null}</div>
    </>}
  </section>;
}
