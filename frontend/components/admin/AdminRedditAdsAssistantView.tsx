"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  approveAdminRedditAdDraft,
  archiveAdminRedditAdDraft,
  createAdminRedditAdDraft,
  createAdminRedditAdsExtensionToken,
  duplicateAdminRedditAdDraft,
  getAdminRedditAdDrafts,
  getAdminRedditAdsOptions,
  regenerateAdminRedditAdDraft,
  rejectAdminRedditAdDraft,
  updateAdminRedditAdDraft,
  type RedditAdDraft,
  type RedditAdDraftPayload,
  type RedditAdsOptions,
} from "@/lib/api";

type Props = {
  showToast?: (message: string, tone?: "success" | "error" | "info") => void;
};

const STATUS_FILTERS = ["all", "draft", "needs_review", "approved", "rejected", "archived"] as const;

const DEFAULT_FORM: RedditAdDraftPayload = {
  campaign_objective: "Free account registrations",
  audience: "General retail investors",
  geography: "United States",
  product_angle: "Find stronger investment setups",
  plan: "Free",
  tone: "Official Walnut",
  destination: "Landing page",
  destination_url: "https://walnutmarkets.com",
  ticker_symbols: [],
  research_urls: [],
  generate: true,
};

export function AdminRedditAdsAssistantView({ showToast }: Props) {
  const [options, setOptions] = useState<RedditAdsOptions | null>(null);
  const [drafts, setDrafts] = useState<RedditAdDraft[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [status, setStatus] = useState<(typeof STATUS_FILTERS)[number]>("all");
  const [form, setForm] = useState<RedditAdDraftPayload>(DEFAULT_FORM);
  const [busy, setBusy] = useState<string | null>(null);
  const [extensionToken, setExtensionToken] = useState<string | null>(null);

  const selected = drafts.find((draft) => draft.id === selectedId) ?? drafts[0] ?? null;
  const draftText = selected?.final_draft ?? null;
  const statusCounts = useMemo(() => {
    return drafts.reduce<Record<string, number>>((counts, draft) => {
      counts[draft.status] = (counts[draft.status] ?? 0) + 1;
      return counts;
    }, {});
  }, [drafts]);

  useEffect(() => {
    void load();
  }, [status]);

  async function load() {
    try {
      const [optionsPayload, draftsPayload] = await Promise.all([
        options ? Promise.resolve(options) : getAdminRedditAdsOptions(),
        getAdminRedditAdDrafts({ status, limit: 50 }),
      ]);
      setOptions(optionsPayload);
      setDrafts(draftsPayload.items);
      if (!selectedId && draftsPayload.items[0]) setSelectedId(draftsPayload.items[0].id);
    } catch (error) {
      notifyError(error, "Unable to load Reddit ad drafts.");
    }
  }

  async function createDraft() {
    setBusy("create");
    try {
      const draft = await createAdminRedditAdDraft(normalizeForm(form));
      setDrafts((items) => [draft, ...items]);
      setSelectedId(draft.id);
      showToast?.("Reddit ad draft created.", "success");
    } catch (error) {
      notifyError(error, "Unable to create Reddit ad draft.");
    } finally {
      setBusy(null);
    }
  }

  async function saveSelected() {
    if (!selected || !draftText) return;
    setBusy("save");
    try {
      const draft = await updateAdminRedditAdDraft(selected.id, draftText);
      replaceDraft(draft);
      showToast?.("Draft updated.", "success");
    } catch (error) {
      notifyError(error, "Unable to update draft.");
    } finally {
      setBusy(null);
    }
  }

  async function approveSelected() {
    if (!selected) return;
    setBusy("approve");
    try {
      const draft = await approveAdminRedditAdDraft(selected.id);
      replaceDraft(draft);
      showToast?.("Draft approved for extension use.", "success");
    } catch (error) {
      notifyError(error, "Approval blocked.");
    } finally {
      setBusy(null);
    }
  }

  async function act(action: "duplicate" | "regenerate" | "reject" | "archive") {
    if (!selected) return;
    setBusy(action);
    try {
      const draft =
        action === "duplicate"
          ? await duplicateAdminRedditAdDraft(selected.id)
          : action === "regenerate"
            ? await regenerateAdminRedditAdDraft(selected.id)
            : action === "reject"
              ? await rejectAdminRedditAdDraft(selected.id)
              : await archiveAdminRedditAdDraft(selected.id);
      if (action === "duplicate") {
        setDrafts((items) => [draft, ...items]);
        setSelectedId(draft.id);
      } else {
        replaceDraft(draft);
      }
      showToast?.(`Draft ${action === "duplicate" ? "duplicated" : action === "reject" ? "rejected" : action + "d"}.`, "success");
    } catch (error) {
      notifyError(error, `Unable to ${action} draft.`);
    } finally {
      setBusy(null);
    }
  }

  async function issueExtensionToken() {
    setBusy("token");
    try {
      const payload = await createAdminRedditAdsExtensionToken();
      setExtensionToken(payload.token);
      await navigator.clipboard?.writeText(payload.token);
      showToast?.("Extension token copied.", "success");
    } catch (error) {
      notifyError(error, "Unable to create extension token.");
    } finally {
      setBusy(null);
    }
  }

  function replaceDraft(draft: RedditAdDraft) {
    setDrafts((items) => items.map((item) => (item.id === draft.id ? draft : item)));
  }

  function updateDraftText(key: keyof NonNullable<RedditAdDraft["final_draft"]>, value: string) {
    if (!selected) return;
    const next = { ...selected, final_draft: { ...selected.final_draft, [key]: value } };
    replaceDraft(next);
  }

  function notifyError(error: unknown, fallback: string) {
    const message = error instanceof ApiError ? error.message : fallback;
    showToast?.(message, "error");
  }

  if (!options) {
    return <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-300">Loading Reddit Ads Assistant...</div>;
  }

  return (
    <div className="space-y-5">
      <div className="rounded-lg border border-emerald-400/20 bg-slate-950/50 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300">Reddit Ads Assistant</p>
            <h2 className="text-xl font-semibold text-white">New campaign draft</h2>
          </div>
          <button
            type="button"
            onClick={issueExtensionToken}
            disabled={busy === "token"}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-100 hover:bg-white/10 disabled:opacity-50"
          >
            Open in extension
          </button>
        </div>

        <div className="mt-4 grid gap-3 md:grid-cols-3">
          <Select label="Objective" value={form.campaign_objective} options={options.campaign_objectives} onChange={(value) => setForm({ ...form, campaign_objective: value })} />
          <Select label="Audience" value={form.audience} options={options.audiences} onChange={(value) => setForm({ ...form, audience: value })} />
          <Select label="Geography" value={form.geography} options={options.geographies} onChange={(value) => setForm({ ...form, geography: value })} />
          <Select label="Product angle" value={form.product_angle} options={options.product_angles} onChange={(value) => setForm({ ...form, product_angle: value })} />
          <Select label="Plan" value={form.plan} options={options.plans} onChange={(value) => setForm({ ...form, plan: value })} />
          <Select label="Tone" value={form.tone} options={options.tones} onChange={(value) => setForm({ ...form, tone: value })} />
          <Select label="Destination" value={form.destination} options={options.destinations} onChange={(value) => setForm({ ...form, destination: value })} />
          <Field label="Destination URL" value={form.destination_url ?? ""} onChange={(value) => setForm({ ...form, destination_url: value })} />
          <Field label="Tickers" value={(form.ticker_symbols ?? []).join(", ")} onChange={(value) => setForm({ ...form, ticker_symbols: splitList(value).map((item) => item.toUpperCase()) })} />
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 text-sm text-slate-300">
            <input
              type="checkbox"
              checked={form.generate !== false}
              onChange={(event) => setForm({ ...form, generate: event.target.checked })}
              className="h-4 w-4 rounded border-white/20 bg-slate-950"
            />
            Generate with approved backend OpenAI integration
          </label>
          <button
            type="button"
            onClick={createDraft}
            disabled={busy === "create"}
            className="rounded-lg bg-emerald-400 px-4 py-2 text-sm font-bold text-slate-950 disabled:opacity-50"
          >
            New campaign draft
          </button>
          <span className="text-xs text-slate-500">Official logo: {options.official_logo.sha256.slice(0, 12)}</span>
        </div>

        {extensionToken ? (
          <div className="mt-3 break-all rounded-md border border-amber-300/30 bg-amber-300/10 p-3 text-xs text-amber-100">
            Short-lived extension token copied. Store it only in the extension session.
          </div>
        ) : null}
      </div>

      <div className="flex flex-wrap gap-2">
        {STATUS_FILTERS.map((item) => (
          <button
            key={item}
            type="button"
            onClick={() => setStatus(item)}
            className={`rounded-full border px-3 py-1 text-xs font-semibold ${
              status === item ? "border-emerald-300 bg-emerald-300 text-slate-950" : "border-white/10 text-slate-300"
            }`}
          >
            {item === "all" ? "Saved drafts" : item.replace("_", " ")} {item !== "all" && statusCounts[item] ? `(${statusCounts[item]})` : ""}
          </button>
        ))}
      </div>

      <div className="grid gap-4 xl:grid-cols-[360px_minmax(0,1fr)]">
        <div className="space-y-2">
          {drafts.length ? (
            drafts.map((draft) => (
              <button
                key={draft.id}
                type="button"
                onClick={() => setSelectedId(draft.id)}
                className={`block w-full rounded-lg border p-3 text-left ${
                  selected?.id === draft.id ? "border-emerald-300 bg-emerald-300/10" : "border-white/10 bg-slate-950/40"
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm font-semibold text-white">{draft.final_draft?.headline || draft.product_angle}</span>
                  <Badge label={draft.status.replace("_", " ")} />
                </div>
                <div className="mt-1 text-xs text-slate-400">{draft.plan} · {draft.audience}</div>
              </button>
            ))
          ) : (
            <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No drafts match this view.</div>
          )}
        </div>

        {selected && draftText ? (
          <div className="space-y-4 rounded-lg border border-white/10 bg-slate-950/50 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <h3 className="text-lg font-semibold text-white">Preview</h3>
                <p className="text-xs text-slate-400">{selected.geography} · {selected.campaign_objective}</p>
              </div>
              <div className="flex flex-wrap gap-2">
                <button className="rounded-lg border border-white/10 px-3 py-2 text-sm text-slate-100" onClick={() => void navigator.clipboard?.writeText(JSON.stringify(selected.final_draft, null, 2))}>Copy fields</button>
                <a className="rounded-lg border border-white/10 px-3 py-2 text-sm text-slate-100" href={selected.reddit_ads_manager_url ?? options.reddit_ads_manager_url} target="_blank" rel="noreferrer">Open Reddit Ads Manager</a>
                <button className="rounded-lg border border-white/10 px-3 py-2 text-sm text-slate-100" onClick={() => act("duplicate")}>Duplicate variation</button>
                <button className="rounded-lg border border-white/10 px-3 py-2 text-sm text-slate-100" onClick={() => act("regenerate")}>Regenerate</button>
              </div>
            </div>

            <div className="rounded-lg border border-white/10 bg-white p-4 text-slate-950">
              <div className="flex items-start gap-3">
                <div className="h-12 w-12 shrink-0 rounded-md bg-slate-200" />
                <div className="min-w-0 flex-1">
                  <div className="text-xs font-semibold uppercase text-orange-600">Promoted · walnutmarkets.com</div>
                  <div className="text-lg font-bold leading-tight">{draftText.headline}</div>
                  <p className="mt-1 text-sm leading-5 text-slate-700">{draftText.primary_text}</p>
                  <div className="mt-3 inline-flex rounded-full bg-orange-500 px-3 py-1 text-xs font-bold text-white">{draftText.cta}</div>
                </div>
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <Editor label="Headline" value={draftText.headline} onChange={(value) => updateDraftText("headline", value)} count={300} />
              <Editor label="CTA" value={draftText.cta} onChange={(value) => updateDraftText("cta", value)} count={80} />
              <Editor label="Primary text" value={draftText.primary_text} onChange={(value) => updateDraftText("primary_text", value)} count={1400} multiline />
              <Editor label="Destination URL" value={draftText.destination_url} onChange={(value) => updateDraftText("destination_url", value)} count={1200} />
            </div>

            <div className="flex flex-wrap gap-2">
              {selected.compliance_warnings.length ? (
                selected.compliance_warnings.map((warning, index) => <Badge key={`${warning.status}-${index}`} label={warning.status} tone={warning.severity === "error" ? "danger" : "warn"} />)
              ) : (
                <Badge label={selected.status === "approved" ? "Ready" : "Needs review"} tone={selected.status === "approved" ? "ready" : "warn"} />
              )}
              <Badge label={`${draftText.headline.length}/300 headline`} />
              <Badge label={`${draftText.primary_text.length}/1400 body`} />
            </div>

            <div className="flex flex-wrap gap-2">
              <button className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-100" onClick={saveSelected} disabled={busy === "save"}>Edit</button>
              <button className="rounded-lg bg-emerald-400 px-3 py-2 text-sm font-bold text-slate-950" onClick={approveSelected} disabled={busy === "approve"}>Approve Draft</button>
              <button className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-100" onClick={() => act("reject")}>Rejected drafts</button>
              <button className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-100" onClick={() => act("archive")}>Archive</button>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function Select({ label, value, options, onChange }: { label: string; value: string; options: string[]; onChange: (value: string) => void }) {
  return (
    <label className="block text-xs font-semibold uppercase tracking-wide text-slate-400">
      {label}
      <select value={value} onChange={(event) => onChange(event.target.value)} className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm normal-case tracking-normal text-slate-100">
        {options.map((option) => <option key={option}>{option}</option>)}
      </select>
    </label>
  );
}

function Field({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <label className="block text-xs font-semibold uppercase tracking-wide text-slate-400">
      {label}
      <input value={value} onChange={(event) => onChange(event.target.value)} className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm normal-case tracking-normal text-slate-100" />
    </label>
  );
}

function Editor({ label, value, onChange, count, multiline = false }: { label: string; value: string; onChange: (value: string) => void; count: number; multiline?: boolean }) {
  const inputClass = "mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-100";
  return (
    <label className="block text-xs font-semibold uppercase tracking-wide text-slate-400">
      <span className="flex items-center justify-between gap-2"><span>{label}</span><span>{value.length}/{count}</span></span>
      {multiline ? (
        <textarea value={value} onChange={(event) => onChange(event.target.value)} rows={5} className={inputClass} />
      ) : (
        <input value={value} onChange={(event) => onChange(event.target.value)} className={inputClass} />
      )}
    </label>
  );
}

function Badge({ label, tone = "neutral" }: { label: string; tone?: "neutral" | "ready" | "warn" | "danger" }) {
  const colors =
    tone === "ready"
      ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
      : tone === "warn"
        ? "border-amber-300/40 bg-amber-300/10 text-amber-100"
        : tone === "danger"
          ? "border-red-300/40 bg-red-300/10 text-red-100"
          : "border-white/10 bg-white/5 text-slate-300";
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold ${colors}`}>{label}</span>;
}

function splitList(value: string) {
  return value.split(/[,\s]+/).map((item) => item.trim()).filter(Boolean);
}

function normalizeForm(form: RedditAdDraftPayload): RedditAdDraftPayload {
  return {
    ...form,
    ticker_symbols: form.ticker_symbols ?? [],
    research_urls: form.research_urls ?? [],
    destination_url: form.destination_url?.trim() || undefined,
  };
}
