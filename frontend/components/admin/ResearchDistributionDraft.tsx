"use client";

import { useMemo, useState } from "react";
import type { AdminResearchBriefArticle } from "@/lib/api";
import { researchDistributionDraft } from "@/lib/researchDistribution";

export function ResearchDistributionDraft({article, published}: {article: AdminResearchBriefArticle; published: boolean}) {
  const [platform, setPlatform] = useState("reddit");
  const generated = useMemo(() => researchDistributionDraft(article, platform), [article, platform]);
  const [edit, setEdit] = useState<{base: string; text: string} | null>(null);
  const [status, setStatus] = useState("");
  const text = edit?.base === generated ? edit.text : generated;
  return <details className="rounded-lg border border-white/10 p-4">
    <summary className="cursor-pointer font-semibold text-emerald-200">Sharing draft and evidence</summary>
    <p className="my-3 text-xs leading-5 text-slate-400">One finding, one caveat, and a link back to this brief. Review before sharing. {published ? "Confirm the linked page is available before posting." : "This brief is not published; its public link may not work yet."} Copying does not publish a post. Instagram and TikTok caption links may not be clickable; use an appropriate profile link when posting.</p>
    <label className="text-sm text-slate-300">Channel <select className="ml-2 rounded bg-slate-900 p-2" value={platform} onChange={event => {setPlatform(event.target.value); setStatus("");}}>{["reddit", "instagram", "tiktok"].map(value => <option key={value}>{value}</option>)}</select></label>
    <textarea aria-label="Sharing draft" value={text} onChange={event => {setEdit({base: generated, text: event.target.value}); setStatus("");}} className="my-3 min-h-48 w-full rounded border border-white/10 bg-slate-900 p-3 text-sm" />
    <button type="button" className="rounded border border-emerald-300/30 px-3 py-2 text-sm text-emerald-200" onClick={async () => {try {await navigator.clipboard.writeText(text); setStatus("Copied for review.");} catch {setStatus("Could not copy. Select and copy the text above.");}}}>Copy draft</button>
    <p role="status" className="mt-2 text-xs text-slate-400">{status || "Edits here are temporary. Copy your draft before changing channels or leaving this brief."}</p>
    <p className="mt-4 text-xs text-slate-400">Data as of: {article.current_data_as_of || "Check the brief’s source dates"}. Use the brief’s existing chart or this evidence table when preparing a visual; do not invent chart values.</p>
    <table className="mt-3 w-full text-left text-xs text-slate-300"><tbody>
      <tr className="border-b border-white/10"><th className="p-2">Finding</th><td className="p-2">{article.preview_body || article.summary || article.title}</td></tr>
      <tr><th className="p-2">Caveat</th><td className="p-2">{article.premium_required ? "Reported data can lag current activity." : article.risks?.[0] || "Reported data can lag current activity."}</td></tr>
    </tbody></table>
    <ul className="mt-3 text-xs text-emerald-200">{article.source_links?.filter(source => /^https?:\/\//i.test(source.url)).slice(0, 5).map((source, index) => <li key={index}><a href={source.url} target="_blank" rel="noopener noreferrer">{source.label}</a></li>)}</ul>
  </details>;
}
