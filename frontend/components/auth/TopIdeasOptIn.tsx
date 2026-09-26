"use client";

import { useState } from "react";
import { updateTopIdeasPreference } from "@/lib/api";

export function TopIdeasOptIn() {
  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);
  const [saved, setSaved] = useState(false);
  async function subscribe() {
    setBusy(true);
    try {
      await updateTopIdeasPreference("weekly");
      setSaved(true);
      setStatus("Weekly Top 5 enabled. Verify your email to receive ideas. Manage delivery in Account Settings.");
    } catch (error) { setStatus(error instanceof Error ? error.message : "Unable to save your preference."); }
    finally { setBusy(false); }
  }
  return <div className="mt-6 rounded-lg border border-emerald-300/25 p-5"><h2 className="text-lg font-semibold text-white">Get Walnut’s Top 5 ideas every week?</h2><p className="mt-2 text-sm leading-6 text-slate-400">Optional weekly email with ranked ideas and short reasons. Off until you choose to subscribe. You can turn it off anytime.</p><button type="button" disabled={busy || saved} onClick={subscribe} className="mt-4 rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 disabled:opacity-60">{saved ? "Weekly ideas enabled" : "Yes, email me the weekly Top 5"}</button><p role="status" className="mt-3 text-sm text-slate-300">{status}</p></div>;
}
