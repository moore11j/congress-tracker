"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { growthVideoRequest } from "@/lib/api";
import { GrowthDisclosure } from "@/components/admin/GrowthDisclosure";

type Opportunity = {
  opportunity_type?: string;
  id: string;
  topic: string;
  score: number;
  reason: string;
  tickers: string[];
  suggested_format: string;
  component_scores: Record<string, number>;
  factual_data_timestamp: string;
  destination_url: string;
  research_brief_id: string;
  search_signal: unknown;
};
type Evidence = {
  id: string;
  statement: string;
  effective_date: string;
  source_record_id: string;
  source_url: string;
  source_path: string;
  source_hash: string;
};
type Scene = {
  sequence: number;
  duration_seconds: number;
  statement_ids: string[];
  visual_type: string;
  capture_target: string;
  capture_action: string;
  transition: string;
};
type Board = {
  format: string;
  creative_angle: string;
  hook_id: string;
  alternate_hook_ids: string[];
  caption_statement_ids: string[];
  scenes: Scene[];
};
type Creative = Board & {
  hook: string;
  narration: string;
  caption: string;
  first_comment?: string;
  posting_notes?: string;
  cta: string;
  target_url: string;
  target_duration_seconds: number;
  warnings: string[];
  alternate_hooks?: string[];
  storyboard: Array<Scene & { narration: string; evidence_ids: string[] }>;
};
type VideoJob = {
  id: string;
  opportunity_id: string;
  status: string;
  revision: number;
  created_at: string;
  lease_until: string | null;
  payload: {
    campaign_id?: string;
    actual_duration?: number;
    platform: string;
    format: string;
    opportunity?: Opportunity;
    creative?: Creative;
    failure_reason?: string;
    budget_message?: string;
    retry_at?: string;
    failed_stage?: string;
    retry_count: number;
    model_metadata?: unknown;
    video?: unknown;
    thumbnail?: unknown;
    captures?: Record<string, { page_url?: string; captured_at?: string; source_text?: string }>;
    experiment: unknown;
  };
};
type Memory = {
  id: string;
  action: string;
  active: number;
  created_at: string;
  payload: {
    feedback: string;
    previous_hook?: string;
    format: string;
    duration?: number;
  };
};
type Config = {
  model: string;
  voice: string;
  narration_model: string;
  duration: number;
  opportunity_limit: number;
  render_limit: number;
  creative_limit: number;
  weights: Record<string, number>;
  template_ids: Record<string, string>;
  capture_base_url: string;
  default_cta: string;
};
type State = {
  development_override?: {expires_at: string} | null;
  automation: AutomationState;
  opportunities: Opportunity[];
  jobs: VideoJob[];
  memory: Memory[];
  brief: { version_id: string | null; sections: Record<string, string> };
  brief_fields: string[];
  brief_versions: { id: string; created_at: string }[];
  config: Config;
  readiness: Record<string, boolean | string>;
  copy_library: Record<string, string>;
  formats: string[];
};
type Publication = {job_id: string; platform: string; status: string; caption: string; post_id?: string; external_url?: string; error?: string};
type AutomationState = {
  config: {enabled: boolean; enabled_at?: string};
  buffer_key_configured: boolean;
  email_enabled: boolean;
  last_pass?: {status?: string; skipped?: Array<{brief_id: string; reason: string}>};
  runs: Array<{day: string; status: string; error?: string; job_id?: string}>;
  publications: Publication[];
};
type View = "opportunities" | "queue" | "brief" | "memory" | "settings";
const card = "rounded-xl border border-white/10 bg-slate-950/50 p-5";
const input =
  "w-full rounded-md border border-white/15 bg-slate-900 p-2 text-sm text-slate-100";
const button =
  "rounded-md border border-emerald-300/30 bg-emerald-300/10 px-3 py-2 text-sm text-emerald-100 disabled:opacity-40";
const label = (s: string) => s.replaceAll("_", " ");
const active = (s: string) =>
  ![
    "CREATIVE_READY",
    "READY_FOR_REVIEW",
    "APPROVED",
    "REJECTED",
    "FAILED",
  ].includes(s);

export function GrowthVideoView({ view = "queue" }: { view?: View }) {
  const [state, setState] = useState<State | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [selection, setSelection] = useState<string | null>(null);
  const [evidence, setEvidence] = useState<Evidence[]>([]);
  const [sections, setSections] = useState<Record<string, string>>({});
  const [cfg, setCfg] = useState<Config | null>(null);
  const [filter, setFilter] = useState("all");
  const [showOlder, setShowOlder] = useState(false);
  const [selectedVideo, setSelectedVideo] = useState<string | null>(null);
  const [briefSection, setBriefSection] = useState("");
  const [platform, setPlatform] = useState("instagram");
  const [format, setFormat] = useState("research_finding");
  const [productHook, setProductHook] = useState("navigation");
  const openedVideo = useRef(false);
  useEffect(() => {
    if (!state || openedVideo.current) return;
    const id = new URLSearchParams(window.location.search).get("video");
    if (!id) return;
    setSelectedVideo(id);
    const element = document.getElementById(`video-${id}`);
    if (element) { element.scrollIntoView({block:"start"}); openedVideo.current = true; }
  }, [state, selectedVideo]);

  const refresh = useCallback(async (initialize = false) => {
    const next = await growthVideoRequest<State>();
    setState(next);
    if (initialize) {
      setSections(next.brief.sections);
      setCfg(next.config);
    }
  }, []);
  useEffect(() => {
    void refresh(true).catch((e) => setError(String(e.message || e)));
  }, [refresh]);
  useEffect(() => {
    if (!state?.jobs.some((j) => active(j.status)) && !state?.automation.publications.some(p => ["QUEUED", "SUBMITTING", "SUBMITTED"].includes(p.status))) return;
    const timer = window.setInterval(() => {
      void refresh().catch((e) => setError(String(e.message || e)));
    }, 8000);
    return () => window.clearInterval(timer);
  }, [state?.jobs, state?.automation.publications, refresh]);
  const run = async (fn: () => Promise<unknown>, success = "Saved.") => {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await fn();
      await refresh();
      setNotice(success);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  const inspect = async (id: string) => {
    setSelection(id);
    setEvidence([]);
    try {
      const result = await growthVideoRequest<{ evidence: Evidence[] }>(
        `/opportunities/${id}`,
      );
      setEvidence(result.evidence);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  const latestIds = new Set<string>();
  const seenFamilies = new Set<string>();
  for (const job of state?.jobs || []) {
    const family = `${job.opportunity_id}:${job.payload.platform}`;
    if (!seenFamilies.has(family)) { seenFamilies.add(family); latestIds.add(job.id); }
  }
  const focusId = selectedVideo ?? state?.jobs.find(job => latestIds.has(job.id) && job.status === "READY_FOR_REVIEW")?.id ?? state?.jobs[0]?.id;
  if (!state)
    return (
      <div className={card}>{error || "Loading Walnut video workspace…"}</div>
    );
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-white">
            {view === "queue"
              ? "Video review queue"
              : label(
                  view === "brief"
                    ? "Growth Brief"
                    : view === "memory"
                      ? "Growth Memory"
                      : view === "settings"
                        ? "Video Settings"
                        : "Content Opportunities",
                )}
          </h2>
          <p className="mt-1 text-sm text-slate-400">
            {view === "queue" ? "Watch the video, check its caption, then publish to your connected Instagram and TikTok accounts."
              : view === "brief" ? "Keep the product, audience and brand guidance used in your content up to date."
              : view === "memory" ? "Review the feedback that guides future drafts."
              : view === "settings" ? "Manage daily drafts, connected accounts and generation settings."
              : "Choose a research topic or product walkthrough to turn into a video."}
          </p>
        </div>
        <button
          className={button}
          disabled={busy}
          onClick={() => void run(() => refresh())}
        >
          Refresh
        </button>
      </div>
      {error && (
        <p role="alert" className="rounded-lg bg-rose-500/10 p-3 text-rose-200">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="text-sm text-emerald-200">
          {notice}
        </p>
      )}
      {state.development_override && <p role="status" className="rounded-lg bg-emerald-300/10 p-3 text-sm text-emerald-100">Development today: video draft limit lifted until {new Date(state.development_override.expires_at).toLocaleString()}. Render limits still apply.</p>}
      {!state.readiness.worker_enabled && (
        <p className="rounded-lg bg-amber-500/10 p-3 text-sm text-amber-200">
          Video worker is not enabled. Opportunities and edits can be saved;
          queued jobs wait for worker setup in Video Settings.
        </p>
      )}

      {view === "opportunities" && (
        <>
          <section className={`${card} space-y-3 border-emerald-300/25`}>
            <h3 className="font-semibold text-emerald-100">Walnut research videos</h3>
            <p className="text-sm text-slate-300">Show the actual clicks from NVIDIA search to ownership, institutional filings and the published brief in Insights. Narration follows the navigation. Includes a caption and first-comment draft.</p>
            <div className="flex flex-wrap items-end gap-3">
              <label className="text-sm text-slate-300">Opening angle
                <select className={input} value={productHook} onChange={(e)=>setProductHook(e.target.value)}>
                  <option value="navigation">Who's buying NVIDIA? — Follow the clicks</option>
                  <option value="ownership">V3 — Research walkthrough</option>
                  <option value="opinion">Everyone has an NVIDIA opinion</option>
                  <option value="score">Show me what is behind the score</option>
                  <option value="accountability">What happened to the last call?</option>
                </select>
              </label>
              <button className={button} disabled={busy} onClick={()=>void run(()=>growthVideoRequest("/product-ad","POST",{platform,hook:productHook}),"Product ad draft ready. Review its script in Draft Queue before rendering.")}>Create product ad draft</button>
            </div>
            <p className="text-xs text-slate-400">Uses reviewed editorial copy and product capabilities. No invented returns or market forecasts. Final approval stays with you.</p>
          </section>
          <div className="flex flex-wrap gap-3">
            <button
              className={button}
              disabled={busy}
              onClick={() =>
                void run(async () => {
                  const result = await growthVideoRequest<{
                    created: string[];
                    skipped: unknown[];
                  }>("/discover", "POST");
                  if (!result.created.length)
                    throw new Error(
                      `No new supported opportunities. ${result.skipped.length} approved briefs lack supported dated quote evidence; existing opportunities are deduplicated. The daily limit may also be reached.`,
                    );
                }, "Opportunities refreshed from approved research and synced search demand.")
              }
            >
              Discover from approved research
            </button>
            <label className="text-sm text-slate-300">
              Platform
              <select
                aria-label="Video platform"
                className={input}
                value={platform}
                onChange={(e) => setPlatform(e.target.value)}
              >
                <option value="instagram">Instagram Reel</option>
                <option value="tiktok">TikTok</option>
              </select>
            </label>
            <label className="text-sm text-slate-300">
              Format
              <select
                aria-label="Video format"
                className={input}
                value={format}
                onChange={(e) => setFormat(e.target.value)}
              >
                {state.formats.map((f) => (
                  <option key={f} value={f}>
                    {label(f)}
                  </option>
                ))}
              </select>
            </label>
          </div>
          {!state.opportunities.length && (
            <p className={card}>
              No opportunities yet. Discovery uses approved research with
              retained quote values and dates. Missing evidence is skipped.
            </p>
          )}
          {state.opportunities.filter((o)=>o.opportunity_type!=="product_campaign").map((o) => (
            <section key={o.id} className={card}>
              <div className="flex justify-between gap-4">
                <h3 className="font-semibold text-white">{o.topic}</h3>
                <span className="text-xl font-semibold text-emerald-200">
                  {o.score.toFixed(0)}
                  <span className="text-xs text-slate-500"> /100</span>
                </span>
              </div>
              <p className="my-2 text-sm text-slate-300">{o.reason}</p>
              <p className="text-xs text-slate-400">
                Data as of {o.factual_data_timestamp} · Suggested:{" "}
                {label(o.suggested_format)}
              </p>
              <div className="mt-3 flex flex-wrap gap-2">
                <button className={button} onClick={() => void inspect(o.id)}>
                  Why this opportunity
                </button>
                <button
                  className={button}
                  disabled={busy}
                  onClick={() =>
                    void run(
                      () =>
                        growthVideoRequest("/jobs", "POST", {
                          opportunity_id: o.id,
                          platform,
                          format,
                        }),
                      "Storyboard queued. It will appear in the video review queue.",
                    )
                  }
                >
                  Generate storyboard
                </button>
                <a
                  href={o.destination_url}
                  className={button}
                  target="_blank"
                  rel="noreferrer"
                >
                  View research
                </a>
              </div>
              {selection === o.id && (
                <div className="mt-4 space-y-3">
                  <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                    {Object.entries(o.component_scores).map(([k, v]) => (
                      <div key={k} className="rounded bg-slate-900 p-2 text-xs">
                        <span className="text-slate-400">{label(k)}</span>
                        <p>{v.toFixed(1)}</p>
                      </div>
                    ))}
                  </div>
                  <EvidenceList evidence={evidence} />
                  <details>
                    <summary>Search signal</summary>
                    <pre className="overflow-auto text-xs">
                      {JSON.stringify(o.search_signal, null, 2)}
                    </pre>
                  </details>
                </div>
              )}
            </section>
          ))}
        </>
      )}

      {view === "queue" && (
        <>
          <select
            aria-label="Video status filter"
            className={`${input} max-w-xs`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          >
            {[
              "all",
              "Draft",
              "Rendering",
              "Ready for Review",
              "Approved",
              "Rejected",
              "Failed",
            ].map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
          <label className="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" checked={showOlder} onChange={e => setShowOlder(e.target.checked)} />Show older versions</label>
          <p className="text-xs text-slate-400">Newest versions shown first. Select a video to review it; expand history to find an earlier take.</p>
          {!state.jobs.length && (
            <p className={card}>
              Generate a storyboard from Content Opportunities to start a video
              draft.
            </p>
          )}
          {state.jobs
            .filter(j => showOlder || latestIds.has(j.id) || j.id === selectedVideo || ["Rejected", "Approved", "Failed"].includes(filter))
            .filter(
              (j) =>
                filter === "all" ||
                (
                  {
                    CREATIVE_READY: "Draft",
                    OPPORTUNITY_CREATED: "Draft",
                    CREATIVE_GENERATING: "Draft",
                    READY_FOR_REVIEW: "Ready for Review",
                    APPROVED: "Approved",
                    REJECTED: "Rejected",
                    FAILED: "Failed",
                  } as Record<string, string>
                )[j.status] === filter ||
                (filter === "Rendering" &&
                  active(j.status) &&
                  !j.status.startsWith("CREATIVE") &&
                  j.status !== "OPPORTUNITY_CREATED"),
            )
            .map((j) => (
              <VideoCard
                key={j.id}
                item={j}
                opportunity={state.opportunities.find(
                  (o) => o.id === j.opportunity_id,
                )}
                copy={state.copy_library}
                busy={busy}
                run={run}
                publications={state.automation.publications.filter(p => p.job_id === j.id)}
                bufferReady={state.automation.buffer_key_configured}
                expanded={focusId === j.id}
                onExpand={() => setSelectedVideo(focusId === j.id ? "" : j.id)}
                latest={latestIds.has(j.id)}
              />
            ))}
        </>
      )}

      {view === "brief" && (
        <section className={card}>
          <p className="mb-4 text-sm text-slate-400">
            Paste your growth plan into these sections. Each save creates a
            version. Creative prompts use selected relevant sections.
          </p>
          <label className="mb-4 block text-sm text-slate-300">Section
            <select className={`${input} mt-1 max-w-sm`} value={briefSection || state.brief_fields[0]} onChange={e => setBriefSection(e.target.value)}>{state.brief_fields.map(key => <option key={key} value={key}>{label(key)}</option>)}</select>
          </label>
          <div>
            {state.brief_fields.map((key) => (
              <label key={key} hidden={key !== (briefSection || state.brief_fields[0])} className="text-sm capitalize text-slate-300">
                {label(key)}
                <textarea
                  className={`${input} mt-1`}
                  rows={12}
                  maxLength={12000}
                  value={sections[key] || ""}
                  onChange={(e) =>
                    setSections({ ...sections, [key]: e.target.value })
                  }
                />
              </label>
            ))}
          </div>
          <button
            className={`${button} mt-4`}
            disabled={busy}
            onClick={() =>
              void run(
                () => growthVideoRequest("/brief", "PUT", { sections }),
                "Growth Brief version saved.",
              )
            }
          >
            Save Growth Brief
          </button>
          <details className="mt-4">
            <summary>Version history</summary>
            {state.brief_versions.map((v) => (
              <button
                key={v.id}
                className="my-2 block text-sm text-emerald-200"
                onClick={() =>
                  void run(async () => {
                    const old = await growthVideoRequest<{
                      sections: Record<string, string>;
                    }>(`/brief/${v.id}`);
                    setSections(old.sections);
                  }, "Previous version loaded into the editor. Save to restore it as a new version.")
                }
              >
                {v.created_at}
              </button>
            ))}
          </details>
        </section>
      )}

      {view === "memory" && (
        <>
          <p className="text-sm text-slate-400">
            Recent decisions matching the platform or format inform future
            prompts. Disable a lesson to exclude it while retaining its audit
            history.
          </p>
          {!state.memory.length && (
            <p className={card}>Review feedback will appear here.</p>
          )}
          {state.memory.map((m) => (
            <MemoryCard key={m.id} item={m} run={run} busy={busy} />
          ))}
        </>
      )}

      {view === "settings" && cfg && (
        <section className={`${card} space-y-4`}>
          <AutomationSettings value={state.automation} run={run} busy={busy} />
          <GrowthDisclosure title="Advanced generation settings · limits, voice and templates">
          <p className="text-xs text-slate-400">Daily videos use the continuous founder voice and Walnut’s native HD renderer. The model, voice and Creatomate settings below apply to the older manual generator; daily videos share the creative and render limits.</p>
          <div className="grid gap-2 sm:grid-cols-3">
            {Object.entries(state.readiness)
              .filter(([k]) => k !== "note")
              .map(([k, v]) => (
                <p
                  key={k}
                  className={`rounded p-2 text-sm ${v ? "bg-emerald-400/10 text-emerald-200" : "bg-amber-400/10 text-amber-200"}`}
                >
                  {label(k)}: {v ? "configured" : "required"}
                </p>
              ))}
          </div>
          <p className="text-xs text-slate-400">{state.readiness.note}</p>
          <div className="grid gap-4 md:grid-cols-2">
            {(
              [
                "model",
                "voice",
                "narration_model",
                "duration",
                "opportunity_limit",
                "creative_limit",
                "render_limit",
              ] as const
            ).map((k) => (
              <label key={k} className="text-sm text-slate-300">
                {label(k)}
                <input
                  className={input}
                  value={cfg[k]}
                  type={typeof cfg[k] === "number" ? "number" : "text"}
                  onChange={(e) =>
                    setCfg({
                      ...cfg,
                      [k]:
                        typeof cfg[k] === "number"
                          ? Number(e.target.value)
                          : e.target.value,
                    })
                  }
                />
              </label>
            ))}
          </div>
          <p className="text-sm text-slate-400">
            Capture host: {cfg.capture_base_url}. Public approved research pages
            only. CTA: {cfg.default_cta}
          </p>
          <fieldset className="grid gap-3 md:grid-cols-3">
            <legend className="mb-2 text-sm">
              Optional Creatomate template IDs — leave blank to use Walnut’s
              built-in templates
            </legend>
            {Object.entries(cfg.template_ids).map(([k, v]) => (
              <label key={k} className="text-sm">
                {label(k)}
                <input
                  className={input}
                  value={v}
                  onChange={(e) =>
                    setCfg({
                      ...cfg,
                      template_ids: {
                        ...cfg.template_ids,
                        [k]: e.target.value,
                      },
                    })
                  }
                />
              </label>
            ))}
          </fieldset>
          <fieldset className="grid gap-3 md:grid-cols-4">
            <legend className="mb-2 text-sm">
              Opportunity scoring weights
            </legend>
            {Object.entries(cfg.weights).map(([k, v]) => (
              <label key={k} className="text-sm">
                {label(k)}
                <input
                  type="number"
                  min="0"
                  step="0.05"
                  className={input}
                  value={v}
                  onChange={(e) =>
                    setCfg({
                      ...cfg,
                      weights: { ...cfg.weights, [k]: Number(e.target.value) },
                    })
                  }
                />
              </label>
            ))}
          </fieldset>
          <p className="text-sm text-slate-400">
            Server setup: existing OpenAI access, CREATOMATE_API_KEY,
            ELEVENLABS_API_KEY, a private GROWTH_ASSET_BUCKET with S3 access,
            and the video worker with Chromium. Credentials stay on the server.
            See docs/ai-growth-video-v1.md for deployment steps.
          </p>
          <button
            className={button}
            disabled={busy}
            onClick={() =>
              void run(() =>
                growthVideoRequest("/config", "PUT", { config: cfg }),
              )
            }
          >
            Save Video Settings
          </button>
          </GrowthDisclosure>
        </section>
      )}
    </div>
  );
}

function EvidenceList({ evidence }: { evidence: Evidence[] }) {
  return (
    <div className="space-y-2">
      {evidence.map((e) => (
        <div key={e.id} className="rounded border border-white/10 p-3 text-sm">
          <p>{e.statement}</p>
          <p className="mt-1 break-all text-xs text-slate-400">
            {e.source_record_id} · {e.source_path} · {e.effective_date}
          </p>
          <a
            href={e.source_url}
            target="_blank"
            rel="noreferrer"
            className="text-emerald-200"
          >
            Source research
          </a>
        </div>
      ))}
    </div>
  );
}

type Run = (fn: () => Promise<unknown>, success?: string) => Promise<void>;
function VideoCard({
  item,
  opportunity,
  copy,
  busy,
  run,
  publications,
  bufferReady,
  expanded,
  onExpand,
  latest,
}: {
  expanded: boolean;
  onExpand: () => void;
  latest: boolean;
  item: VideoJob;
  opportunity?: Opportunity;
  copy: Record<string, string>;
  busy: boolean;
  run: Run;
  publications: Publication[];
  bufferReady: boolean;
}) {
  opportunity = opportunity || item.payload.opportunity;
  const creative = item.payload.creative;
  const [feedback, setFeedback] = useState("");
  const [editing, setEditing] = useState(false);
  const [board, setBoard] = useState<Board | null>(null);
  const [facts, setFacts] = useState<Evidence[]>([]);
  const [media, setMedia] = useState<{
    video_url?: string;
    thumbnail_url?: string;
  }>({});
  const [retryAcknowledged, setRetryAcknowledged] = useState(false);
  const player = useRef<HTMLVideoElement>(null);
  useEffect(() => { if (!expanded) player.current?.pause(); }, [expanded]);
  const act = (action: string, extra = {}) =>
    run(
      () =>
        growthVideoRequest(`/jobs/${item.id}/decision`, "POST", {
          action,
          feedback,
          ...extra,
        }),
      action === "edit" || action === "regenerate"
        ? "New revision queued. Previous video and review history retained."
        : "Video draft updated.",
    );
  const loadEvidence = async () => {
    const result = await growthVideoRequest<{ evidence: Evidence[] }>(
      `/opportunities/${item.opportunity_id}`,
    );
    setFacts(result.evidence);
  };
  return (
    <section id={`video-${item.id}`} className={`${card} space-y-3`}>
      <button type="button" aria-expanded={expanded} aria-controls={`video-body-${item.id}`} onClick={onExpand} className="flex w-full flex-wrap items-center justify-between gap-3 text-left">
        <h3 className="font-semibold">
          {item.payload.opportunity?.topic || opportunity?.topic || creative?.hook || "Research video"}
        </h3>
        <span className="rounded bg-slate-800 px-2 py-1 text-xs">
          {label(item.status)} · Version {item.revision}{latest ? " · Latest" : " · Earlier"} · {expanded ? "Close" : "Review"}
        </span>
      </button>
      <div id={`video-body-${item.id}`} hidden={!expanded} className="space-y-4">
      <p className="text-xs text-slate-400">
        {item.payload.platform} · {label(item.payload.format)} ·{" "}
        {item.payload.actual_duration?.toFixed(1) || creative?.target_duration_seconds || "—"} seconds · {item.payload.campaign_id ? "Product campaign · Continuous voiceover" : `Score ${opportunity?.score ?? "—"}`}
      </p>
      {item.payload.failure_reason && (
        <p className="rounded bg-rose-400/10 p-3 text-sm text-rose-200">
          Could not finish this version. {item.payload.failure_reason}
        </p>
      )}
      {item.payload.budget_message && <p className="rounded bg-emerald-300/10 p-3 text-sm">{item.payload.budget_message} Next attempt: {item.payload.retry_at ? new Date(item.payload.retry_at).toLocaleString() : "next quota window"}.</p>}
      <div className="mx-auto grid max-w-6xl gap-6 lg:grid-cols-[minmax(220px,320px)_minmax(0,1fr)]">
      <div className="space-y-3">
      {Boolean(item.payload.video || item.payload.thumbnail) && (
        <button
          className={button}
          disabled={busy}
          onClick={() =>
            void run(
              async () =>
                setMedia(await growthVideoRequest(`/jobs/${item.id}/media`)),
              "Preview loaded.",
            )
          }
        >
          {media.video_url ? "Reload preview" : "Watch preview"}
        </button>
      )}
      {media.video_url ? (
        <video
          ref={player}
          className="mx-auto aspect-[9/16] max-h-[440px] max-w-full rounded-lg bg-black object-contain"
          controls
          playsInline
          preload="metadata"
          src={media.video_url}
          poster={media.thumbnail_url}
        />
      ) : media.thumbnail_url ? (
        <img
          className="max-h-80 rounded-lg"
          src={media.thumbnail_url}
          alt="Actual Walnut research capture"
        />
      ) : null}
      </div>
      <div className="min-w-0 space-y-4">
      {creative && (
        <>
          <p className="font-semibold text-emerald-100">{creative.hook}</p>
          <details>
            <summary className="cursor-pointer text-sm">
              Script, caption and evidence
            </summary>
            <div className="mt-3 space-y-3 text-sm">
              <p>{creative.narration}</p>
              <p>
                <strong>Caption:</strong> {creative.caption}
              </p>
              {creative.first_comment ? <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/5 p-3">
                <strong>First comment draft:</strong>
                <p className="mt-2 whitespace-pre-wrap break-words">{creative.first_comment}</p>
              </div> : null}
              {creative.posting_notes ? <p className="text-xs text-slate-400">{creative.posting_notes}</p> : null}
              <p>
                <strong>CTA:</strong> {creative.cta}
              </p>
              <a
                className="text-emerald-200"
                href={creative.target_url}
                target="_blank"
                rel="noreferrer"
              >
                {creative.target_url}
              </a>
              <p className="text-xs text-slate-400">
                Data as of {opportunity?.factual_data_timestamp}
              </p>
              {creative.alternate_hooks?.length ? <p>Alternate hooks: {creative.alternate_hooks.join(" / ")}</p> : null}
              {creative.warnings?.map((warning)=><p key={warning} className="text-xs text-amber-100">{warning}</p>)}
              {item.payload.campaign_id ? Object.entries(item.payload.captures ?? {}).map(([shot, capture]) => (
                <details key={shot} className="rounded border border-white/10 p-2">
                  <summary className="cursor-pointer">{shot} · captured {capture.captured_at ?? "pending"}</summary>
                  <a href={capture.page_url} target="_blank" rel="noreferrer" className="text-emerald-200">Actual product source</a>
                  <p className="mt-2 whitespace-pre-wrap text-xs text-slate-400">{capture.source_text}</p>
                </details>
              )) : <>
                <button className={button} onClick={() => void run(loadEvidence)}>Load supporting evidence</button>
                <EvidenceList evidence={facts} />
              </>}
            </div>
          </details>
          <details>
            <summary className="cursor-pointer text-sm">
              Storyboard and generation metadata
            </summary>
            <ol className="my-3 space-y-2">
              {creative.storyboard.map((s) => (
                <li
                  className="rounded bg-slate-900 p-3 text-sm"
                  key={s.sequence}
                >
                  {s.sequence}. {label(s.visual_type)} · {s.duration_seconds}s
                  <p>{s.narration}</p>
                  <p className="break-all text-xs text-slate-400">
                    {s.capture_target} ·{" "}
                    {s.evidence_ids.join(", ") || "Educational copy"}
                  </p>
                </li>
              ))}
            </ol>
            <pre className="max-h-60 overflow-auto whitespace-pre-wrap break-all text-xs">
              {JSON.stringify(
                {
                  model: item.payload.model_metadata,
                  experiment: item.payload.experiment,
                },
                null,
                2,
              )}
            </pre>
          </details>
        </>
      )}
      {creative && ["READY_FOR_REVIEW", "APPROVED"].includes(item.status) && (
        <PublishVideo item={item} caption={creative.caption} publications={publications} bufferReady={bufferReady} previewLoaded={Boolean(media.video_url)} run={run} busy={busy} />
      )}
      </div>
      </div>
      <GrowthDisclosure title="Changes, history and download" open={item.status === "CREATIVE_READY" || item.status === "FAILED"}>
      <label className="block text-sm text-slate-300">
        Review feedback
        <textarea
          className={`${input} mt-1`}
          maxLength={1500}
          rows={2}
          value={feedback}
          onChange={(e) => setFeedback(e.target.value)}
          placeholder="What should the next version improve?"
        />
      </label>
      {!publications.length && <>
      <div className="flex flex-wrap gap-2">
        {item.status === "CREATIVE_READY" && (
          <button
            className={button}
            disabled={busy}
            onClick={() => void act("render")}
          >
            Capture and render video
          </button>
        )}
        {item.status === "READY_FOR_REVIEW" && (
          <button
            className={button}
            disabled={busy}
            onClick={() => void act("approve")}
          >
            Approve for download only
          </button>
        )}
        {["CREATIVE_READY", "READY_FOR_REVIEW", "APPROVED", "FAILED"].includes(
          item.status,
        ) && (
          <button
            className={button}
            disabled={busy || !feedback.trim()}
            onClick={() => void act("reject")}
          >
            Reject with feedback
          </button>
        )}
        {!active(item.status) && (
          <button
            className={button}
            disabled={busy}
            onClick={() => void act("regenerate")}
          >
            {item.payload.campaign_id ? "Create another take" : "Regenerate"}
          </button>
        )}
        {creative && !item.payload.campaign_id && !active(item.status) && (
          <button
            className={button}
            disabled={busy}
            onClick={() =>
              void run(async () => {
                await loadEvidence();
                setBoard({
                  format: creative.format,
                  creative_angle: creative.creative_angle,
                  hook_id: creative.hook_id,
                  alternate_hook_ids: creative.alternate_hook_ids,
                  caption_statement_ids: creative.caption_statement_ids,
                  scenes: creative.scenes,
                });
                setEditing(!editing);
              }, "Edit using verified statements below.")
            }
          >
            Edit hook, script or caption
          </button>
        )}
        {item.status === "APPROVED" && (
          <button
            className={button}
            disabled={busy}
            onClick={() =>
              void run(async () => {
                const result = await growthVideoRequest<{ video_url: string }>(
                  `/jobs/${item.id}/media?download=true`,
                );
                window.location.assign(result.video_url);
              }, "Approved MP4 download started.")
            }
          >
            Download approved MP4
          </button>
        )}
      </div>
      {item.status === "FAILED" && (
        <div className="space-y-2">
          <label className="flex items-start gap-2 text-xs text-slate-400">
            <input
              type="checkbox"
              checked={retryAcknowledged}
              onChange={(e) => setRetryAcknowledged(e.target.checked)}
            />
            I checked provider activity. I understand retrying an uncertain
            request may incur another charge.
          </label>
          <button
            className={button}
            disabled={busy || !retryAcknowledged}
            onClick={() =>
              void act("retry", { acknowledge_provider_retry: true })
            }
          >
            Retry failed stage
          </button>
        </div>
      )}
      {editing && board && (
        <div className="space-y-3 rounded-lg border border-white/10 p-4">
          <p className="text-sm text-slate-400">
            V1 edits use verified statements. Reordering or changing copy
            creates a new revision and requires a new render and approval.
          </p>
          <label className="block text-sm">
            Hook
            <select
              className={input}
              value={board.hook_id}
              onChange={(e) =>
                setBoard({
                  ...board,
                  hook_id: e.target.value,
                  scenes: board.scenes.map((s, i) =>
                    i === 0 ? { ...s, statement_ids: [e.target.value] } : s,
                  ),
                })
              }
            >
              {Object.entries(copy)
                .filter(([k]) => k.startsWith("hook_"))
                .map(([k, v]) => (
                  <option key={k} value={k}>
                    {v}
                  </option>
                ))}
            </select>
          </label>
          {board.scenes.map((scene, i) => (
            <div
              key={scene.sequence}
              className="grid gap-2 md:grid-cols-[1fr_100px]"
            >
              <label className="text-sm">
                Scene {scene.sequence} statements
                <select
                  multiple
                  className={input}
                  value={scene.statement_ids}
                  disabled={i === 0 || i === board.scenes.length - 1}
                  onChange={(e) =>
                    setBoard({
                      ...board,
                      scenes: board.scenes.map((s, j) =>
                        j === i
                          ? {
                              ...s,
                              statement_ids: Array.from(
                                e.target.selectedOptions,
                                (o) => o.value,
                              ),
                            }
                          : s,
                      ),
                    })
                  }
                >
                  {Object.entries({
                    ...copy,
                    ...Object.fromEntries(
                      facts.map((f) => [f.id, f.statement]),
                    ),
                  }).map(([k, v]) => (
                    <option key={k} value={k}>
                      {v}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-sm">
                Seconds
                <input
                  className={input}
                  type="number"
                  min={3}
                  max={15}
                  value={scene.duration_seconds}
                  onChange={(e) =>
                    setBoard({
                      ...board,
                      scenes: board.scenes.map((s, j) =>
                        j === i
                          ? { ...s, duration_seconds: Number(e.target.value) }
                          : s,
                      ),
                    })
                  }
                />
              </label>
            </div>
          ))}
          <label className="block text-sm">
            Caption statements
            <select
              multiple
              className={input}
              value={board.caption_statement_ids}
              onChange={(e) =>
                setBoard({
                  ...board,
                  caption_statement_ids: Array.from(
                    e.target.selectedOptions,
                    (o) => o.value,
                  ),
                })
              }
            >
              {Object.entries({
                ...copy,
                ...Object.fromEntries(facts.map((f) => [f.id, f.statement])),
              }).map(([k, v]) => (
                <option key={k} value={k}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <button
            className={button}
            disabled={busy}
            onClick={() => void act("edit", { storyboard: board })}
          >
            Save as new revision
          </button>
        </div>
      )}
      </>}
      </GrowthDisclosure>
      </div>
    </section>
  );
}

function AutomationSettings({value, run, busy}: {value: AutomationState; run: Run; busy: boolean}) {
  const [connection, setConnection] = useState("");
  return <div className="space-y-3 rounded-lg border border-emerald-300/25 p-4">
    <h3 className="font-semibold">Daily research videos</h3>
    <p className="text-sm text-slate-300">One new published brief per Pacific day becomes a video draft. You receive an email, watch the video and approve its caption before publishing. Existing briefs are not backfilled when enabled.</p>
    <p className="text-sm">Daily drafts: {value.config.enabled ? "enabled" : "paused"} · Email: {value.email_enabled ? "enabled" : "not enabled"} · Buffer API: {value.buffer_key_configured ? "configured" : "not configured"}</p>
    <button className={button} disabled={busy} onClick={() => void run(() => growthVideoRequest("/automation", "PUT", {enabled: !value.config.enabled}), value.config.enabled ? "Daily creation paused. Existing jobs remain in the queue." : "Daily creation enabled for newly published briefs.")}>{value.config.enabled ? "Pause daily drafts" : "Enable daily drafts"}</button>{" "}
    <button className={button} disabled={busy || !value.buffer_key_configured} onClick={() => void run(async () => {
      const result = await growthVideoRequest<{channels: Array<{name: string; service: string}>}>("/buffer-status");
      setConnection(result.channels.map(c => `${c.service}: ${c.name}`).join(" · "));
    }, "Buffer connections verified.")}>Check Buffer connection</button>
    {connection && <p className="text-sm text-emerald-200">{connection}</p>}
    <p className="text-xs text-slate-400">Buffer Free: the optional first comment is added manually. Approved videos publish immediately through Buffer using your channel settings. No social posts are sent by enabling daily drafts.</p>
    <GrowthDisclosure title="Automation history and skipped briefs">
    {value.last_pass?.status && <p className="text-xs text-slate-400">Last worker result: {label(value.last_pass.status)}</p>}
    {value.last_pass?.skipped?.map(s => <p className="text-xs text-amber-100" key={s.brief_id}>{s.reason}</p>)}
    {value.runs.map(r => <p className="text-xs" key={r.day}>{r.day}: {r.status}{r.error ? ` — ${r.error}` : ""}</p>)}
    </GrowthDisclosure>
  </div>;
}

function PublishVideo({item, caption, publications, bufferReady, previewLoaded, run, busy}: {
  item: VideoJob; caption: string; publications: Publication[]; bufferReady: boolean; previewLoaded: boolean; run: Run; busy: boolean;
}) {
  const [text, setText] = useState(caption);
  const [platforms, setPlatforms] = useState(["instagram", "tiktok"]);
  const [reviewed, setReviewed] = useState(false);
  const [postIds, setPostIds] = useState<Record<string,string>>({});
  const [noPost, setNoPost] = useState<Record<string,boolean>>({});
  if (publications.length) return <div className="space-y-2 rounded border border-white/15 p-3">
    <h4 className="font-semibold">Publishing</h4>
    <details className="text-sm"><summary>Approved caption</summary><p className="mt-2 whitespace-pre-wrap">{publications[0].caption}</p></details>
    {publications.map(p => <div className="space-y-2 text-sm" key={p.platform}>
      <p>{p.platform}: {label(p.status)} {p.external_url && <a className="text-emerald-200 underline" href={p.external_url} target="_blank" rel="noreferrer">View post</a>}</p>
      {p.error && <p className="text-rose-200">{p.error}</p>}
      {["FAILED","UNCERTAIN"].includes(p.status) && <div>
        <label className="text-xs">If the post exists in Buffer, paste its Buffer post ID to reconcile:
          <input className={input} value={postIds[p.platform] || ""} onChange={e => setPostIds({...postIds,[p.platform]:e.target.value})} />
        </label>
        <button className={button} disabled={busy || !postIds[p.platform]} onClick={() => void run(() => growthVideoRequest(`/jobs/${item.id}/reconcile`, "POST", {platform:p.platform, post_id:postIds[p.platform]}), "Existing Buffer post linked; no new post created.")}>Link existing post</button>
        {!p.post_id && <div className="mt-3 space-y-2">
          <label className="flex gap-2 text-xs"><input type="checkbox" checked={noPost[p.platform] || false} onChange={e => setNoPost({...noPost,[p.platform]:e.target.checked})} />I checked the Buffer queue and sent posts. This video was not posted or queued on {p.platform}.</label>
          <button className={button} disabled={busy || !noPost[p.platform]} onClick={() => void run(() => growthVideoRequest(`/jobs/${item.id}/retry-publish`, "POST", {platform:p.platform, confirmed_no_buffer_post:true}), "Publishing retry queued for this destination only.")}>Retry this destination</button>
        </div>}
      </div>}
    </div>)}
    <a className="text-sm text-emerald-200" href="https://publish.buffer.com/" target="_blank" rel="noreferrer">Open Buffer to inspect or manage posts</a>
  </div>;
  return <div className="space-y-3 rounded-lg border border-emerald-300/25 p-4">
    <h4 className="font-semibold text-emerald-100">Publish video</h4>
    <p className="text-sm text-slate-300">Your video and caption post directly through Buffer. No download needed.</p>
    <label className="block text-sm">Post caption<textarea className={`${input} mt-1`} rows={3} maxLength={2200} value={text} onChange={e => {setText(e.target.value);setReviewed(false);}} /></label>
    <div className="flex gap-4">{["instagram","tiktok"].map(p => <label key={p} className="text-sm capitalize"><input type="checkbox" checked={platforms.includes(p)} onChange={e => {setPlatforms(e.target.checked ? [...platforms,p] : platforms.filter(x => x !== p));setReviewed(false);}} /> {p}</label>)}</div>
    <label className="flex gap-2 text-sm"><input type="checkbox" checked={reviewed} disabled={!previewLoaded} onChange={e => setReviewed(e.target.checked)} />I reviewed this video, its claims and caption, and approve publishing now to the selected Walnut accounts using their Buffer audience, interaction and commercial-content settings. AI assistance will be disclosed.</label>
    {!bufferReady && <p className="text-sm text-amber-100">Configure the Buffer API key before publishing.</p>}
    {!previewLoaded && <p className="text-xs text-slate-400">Load and watch the preview above first.</p>}
    <p className="text-xs text-slate-400">Your optional first comment is a manual step on Buffer Free. Review any “links in comments” promise before publishing.</p>
    {previewLoaded && !reviewed && <p className="text-sm text-emerald-200">After reviewing, check the approval box above to enable publishing.</p>}
    {!platforms.length && <p className="text-sm text-amber-100">Choose at least one account.</p>}
    {!text.trim() && <p className="text-sm text-amber-100">Add a caption before publishing.</p>}
    <button className="rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 disabled:opacity-40" disabled={busy || !bufferReady || !reviewed || !platforms.length || !text.trim()} onClick={() => void run(() => growthVideoRequest(`/jobs/${item.id}/publish`, "POST", {platforms,caption:text,reviewed_video_and_caption:reviewed,confirm_buffer_channel_settings:reviewed}), "Approved for publishing. Delivery status will appear here.")}>{busy ? "Working…" : `Publish now${platforms.length ? ` to ${platforms.map(p => p === "instagram" ? "Instagram" : "TikTok").join(" & ")}` : ""}`}</button>
  </div>;
}

function MemoryCard({
  item,
  run,
  busy,
}: {
  item: Memory;
  run: Run;
  busy: boolean;
}) {
  const [feedback, setFeedback] = useState(item.payload.feedback || "");
  return (
    <section className={card}>
      <p className="text-sm font-semibold">
        {label(item.action)} · {label(item.payload.format)} ·{" "}
        {item.active ? "Active" : "Excluded"}
      </p>
      <p className="my-2 text-sm text-slate-400">
        {item.payload.previous_hook}{" "}
        {item.payload.duration ? `· ${item.payload.duration}s` : ""}
      </p>
      <p className="mb-3 whitespace-pre-wrap text-sm text-slate-300">{item.payload.feedback}</p>
      <GrowthDisclosure title="Edit or exclude feedback">
      <textarea
        aria-label="Memory feedback"
        className={input}
        value={feedback}
        maxLength={1500}
        onChange={(e) => setFeedback(e.target.value)}
      />
      <div className="mt-2 flex gap-2">
        <button
          className={button}
          disabled={busy}
          onClick={() =>
            void run(() =>
              growthVideoRequest(`/memory/${item.id}`, "PUT", {
                feedback,
                active: true,
              }),
            )
          }
        >
          Save lesson
        </button>
        <button
          className={button}
          disabled={busy}
          onClick={() =>
            void run(() =>
              growthVideoRequest(`/memory/${item.id}`, "PUT", {
                feedback,
                active: !item.active,
              }),
            )
          }
        >
          {item.active ? "Exclude lesson" : "Restore lesson"}
        </button>
      </div>
      </GrowthDisclosure>
    </section>
  );
}
