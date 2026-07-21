"use client";

import { useEffect, useMemo, useState } from "react";
import {
  deleteAdminResearchBriefDraft,
  generateAdminResearchBriefDraft,
  getAdminResearchBriefDrafts,
  getAdminResearchBriefOptions,
  publishAdminResearchBriefDraft,
  unpublishAdminResearchBriefDraft,
  updateAdminResearchBriefDraft,
  validateAdminResearchBriefTicker,
  type AdminResearchBriefArticle,
  type AdminResearchBriefConfig,
  type AdminResearchBriefDraft,
} from "@/lib/api";
import { normalizeTickerSymbol } from "@/lib/ticker";

type Toast = (message: string, tone?: "success" | "error" | "info") => void;

const DEFAULT_SECTIONS = [
  "Executive thesis",
  "What changed",
  "Business and fundamentals",
  "Valuation",
  "Price / volume and technicals",
  "Congress activity",
  "Insider activity",
  "Reported institutional activity",
  "Government contracts",
  "Options flow",
  "Catalysts",
  "Risks",
  "What to watch next",
  "Final Walnut judgment",
  "Data freshness and limitations",
];

const DEFAULT_CONFIG: AdminResearchBriefConfig = {
  ticker: "MU",
  research_question: "Is MU's momentum trade breaking down, or do the fundamentals still support the cycle?",
  desired_angle: "Full company DD",
  comparison_ticker: "",
  time_horizon: "Near term",
  intended_audience: "Walnut Research Brief",
  judgment_preference: "Let the data decide",
  additional_context: "",
  include_sections: DEFAULT_SECTIONS,
  length: "Standard: 1,500-2,500 words",
  tone: "Walnut market-native",
  include_charts: false,
  include_source_links: true,
  hero_image: "",
};

const fallbackOptions = {
  angles: [
    "Full company DD",
    "Bull case",
    "Bear case",
    "Earnings setup",
    "Post-earnings review",
    "Momentum analysis",
    "Fundamental analysis",
    "Valuation analysis",
    "Technical setup",
    "Congress activity",
    "Insider activity",
    "Institutional activity",
    "Government contracts",
    "Macro or sector impact",
    "Peer comparison",
    "Custom",
  ],
  time_horizons: ["Near term", "3-6 months", "6-12 months", "Long term", "Custom"],
  audiences: ["General investors", "Active traders", "Long-term investors", "Professional / advanced", "Reddit DD", "Walnut Research Brief"],
  judgment_preferences: ["Let the data decide", "Bull case", "Bear case", "Balanced debate"],
  lengths: ["Short: 800-1,200 words", "Standard: 1,500-2,500 words", "Deep dive: 3,000-5,000 words"],
  tones: ["Walnut market-native", "Institutional research", "Reddit DD", "Concise executive brief"],
  sections: DEFAULT_SECTIONS,
};

function fieldClassName(extra = "") {
  return `w-full rounded-lg border border-white/10 bg-slate-950/60 px-3 py-2 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-emerald-300/45 focus:ring-2 focus:ring-emerald-300/10 ${extra}`;
}

function Button({
  children,
  onClick,
  disabled,
  tone = "neutral",
  type = "button",
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  tone?: "primary" | "danger" | "neutral";
  type?: "button" | "submit";
}) {
  const styles =
    tone === "primary"
      ? "border-emerald-300/50 bg-emerald-300 text-slate-950 hover:bg-emerald-200"
      : tone === "danger"
        ? "border-rose-300/35 bg-rose-300/10 text-rose-100 hover:bg-rose-300/15"
        : "border-white/10 bg-slate-950/45 text-slate-100 hover:border-white/20";
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`inline-flex min-h-10 items-center justify-center rounded-lg border px-3.5 py-2 text-sm font-semibold transition disabled:cursor-not-allowed disabled:opacity-50 ${styles}`}
    >
      {children}
    </button>
  );
}

function articleToMarkdown(article: AdminResearchBriefArticle) {
  return (article.sections || []).map((section) => `## ${section.heading}\n\n${section.body_markdown}`).join("\n\n");
}

function markdownToSections(markdown: string): AdminResearchBriefArticle["sections"] {
  const chunks = markdown.split(/\n(?=##\s+)/g).map((chunk) => chunk.trim()).filter(Boolean);
  if (!chunks.length) return [{ key: "body", heading: "Research Brief", body_markdown: markdown.trim() }];
  return chunks.map((chunk, index) => {
    const match = chunk.match(/^##\s+(.+?)(?:\n+([\s\S]*))?$/);
    const heading = match?.[1]?.trim() || `Section ${index + 1}`;
    return {
      key: heading.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "") || `section_${index + 1}`,
      heading,
      body_markdown: (match?.[2] || chunk).trim(),
    };
  });
}

export function AdminResearchBriefGeneratorView({ showToast }: { showToast?: Toast }) {
  const [options, setOptions] = useState(fallbackOptions);
  const [config, setConfig] = useState<AdminResearchBriefConfig>(DEFAULT_CONFIG);
  const [tickerIdentity, setTickerIdentity] = useState<Record<string, unknown> | null>(null);
  const [drafts, setDrafts] = useState<AdminResearchBriefDraft[]>([]);
  const [selectedDraft, setSelectedDraft] = useState<AdminResearchBriefDraft | null>(null);
  const [articleDraft, setArticleDraft] = useState<AdminResearchBriefArticle | null>(null);
  const [bodyMarkdown, setBodyMarkdown] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [activePane, setActivePane] = useState<"create" | "drafts" | "published" | "settings">("create");

  const selectedWarnings = selectedDraft?.validation?.warnings ?? [];
  const blockingWarnings = selectedWarnings.filter((warning) => warning.blocking);
  const selectedCard = articleDraft?.suggested_card;

  useEffect(() => {
    let alive = true;
    getAdminResearchBriefOptions()
      .then((payload) => {
        if (!alive) return;
        setOptions({
          angles: payload.angles,
          time_horizons: payload.time_horizons,
          audiences: payload.audiences,
          judgment_preferences: payload.judgment_preferences,
          lengths: payload.lengths,
          tones: payload.tones,
          sections: payload.sections,
        });
        setConfig((current) => ({ ...current, include_sections: payload.sections.length ? payload.sections : current.include_sections }));
      })
      .catch(() => undefined);
    getAdminResearchBriefDrafts().then((payload) => alive && setDrafts(payload.items)).catch(() => undefined);
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    const symbol = normalizeTickerSymbol(config.ticker);
    if (!symbol) {
      setTickerIdentity(null);
      return;
    }
    const handle = window.setTimeout(() => {
      validateAdminResearchBriefTicker(symbol)
        .then((payload) => setTickerIdentity(payload.identity))
        .catch(() => setTickerIdentity(null));
    }, 300);
    return () => window.clearTimeout(handle);
  }, [config.ticker]);

  useEffect(() => {
    if (!selectedDraft) {
      setArticleDraft(null);
      setBodyMarkdown("");
      return;
    }
    setArticleDraft(selectedDraft.article);
    setBodyMarkdown(articleToMarkdown(selectedDraft.article));
  }, [selectedDraft]);

  const generatedDrafts = useMemo(() => drafts.filter((draft) => draft.status !== "published"), [drafts]);
  const publishedDrafts = useMemo(() => drafts.filter((draft) => draft.status === "published"), [drafts]);

  function updateConfig<K extends keyof AdminResearchBriefConfig>(key: K, value: AdminResearchBriefConfig[K]) {
    setConfig((current) => ({ ...current, [key]: value }));
  }

  function updateArticle<K extends keyof AdminResearchBriefArticle>(key: K, value: AdminResearchBriefArticle[K]) {
    setArticleDraft((current) => (current ? { ...current, [key]: value } : current));
  }

  async function refreshDrafts(nextSelected?: AdminResearchBriefDraft) {
    const payload = await getAdminResearchBriefDrafts();
    setDrafts(payload.items);
    if (nextSelected) {
      const match = payload.items.find((draft) => draft.id === nextSelected.id) ?? nextSelected;
      setSelectedDraft(match);
    }
  }

  async function generateDraft() {
    setBusy("generate");
    setError("");
    try {
      const draft = await generateAdminResearchBriefDraft(config);
      setSelectedDraft(draft);
      await refreshDrafts(draft);
      showToast?.("Research brief draft generated.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to generate draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function saveDraft(status?: string) {
    if (!selectedDraft || !articleDraft) return;
    setBusy("save");
    setError("");
    try {
      const article = { ...articleDraft, sections: markdownToSections(bodyMarkdown) };
      const draft = await updateAdminResearchBriefDraft(selectedDraft.id, { status, article });
      setSelectedDraft(draft);
      await refreshDrafts(draft);
      showToast?.("Draft saved.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to save draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function publishSelected() {
    if (!selectedDraft) return;
    if (!window.confirm("Publish this research brief from local/test storage into public Research Briefs?")) return;
    setBusy("publish");
    try {
      const draft = await publishAdminResearchBriefDraft(selectedDraft.id);
      setSelectedDraft(draft);
      await refreshDrafts(draft);
      showToast?.("Draft published in local/test storage.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to publish draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function unpublishSelected() {
    if (!selectedDraft) return;
    if (!window.confirm("Unpublish this research brief from public Research Briefs?")) return;
    setBusy("unpublish");
    try {
      const draft = await unpublishAdminResearchBriefDraft(selectedDraft.id);
      setSelectedDraft(draft);
      await refreshDrafts(draft);
      showToast?.("Draft unpublished.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to unpublish draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function deleteSelected() {
    if (!selectedDraft) return;
    if (window.prompt("Type DELETE to remove this draft.") !== "DELETE") return;
    setBusy("delete");
    try {
      await deleteAdminResearchBriefDraft(selectedDraft.id);
      setSelectedDraft(null);
      await refreshDrafts();
      showToast?.("Draft deleted.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to delete draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  function regenerateWith(change: string) {
    updateConfig("additional_context", `${config.additional_context || ""}\n\nRevision request: ${change}`.trim());
    void generateDraft();
  }

  return (
    <div className="space-y-5">
      <section className="rounded-lg border border-emerald-300/15 bg-slate-950/55 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Research Briefs</p>
            <h2 className="mt-1 text-2xl font-semibold text-white">Admin Research Brief Generator</h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
              Generate grounded Walnut DD drafts with OpenAI Responses, edit them, and publish from local/test storage after explicit review.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            {(["create", "drafts", "published", "settings"] as const).map((pane) => (
              <button
                key={pane}
                type="button"
                onClick={() => setActivePane(pane)}
                className={`rounded-lg border px-3 py-2 text-sm font-semibold capitalize ${
                  activePane === pane ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-300"
                }`}
              >
                {pane === "create" ? "Create Brief" : pane}
              </button>
            ))}
          </div>
        </div>
      </section>

      {error ? <div className="rounded-lg border border-rose-300/30 bg-rose-950/30 px-4 py-3 text-sm text-rose-100">{error}</div> : null}

      {activePane === "create" ? (
        <div className="grid gap-5 xl:grid-cols-[minmax(0,0.92fr)_minmax(28rem,1.08fr)]">
          <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
            <h3 className="text-base font-semibold text-white">Create Brief</h3>
            <div className="mt-4 grid gap-4">
              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Ticker</span>
                <input
                  value={config.ticker}
                  onChange={(event) => updateConfig("ticker", event.target.value.toUpperCase())}
                  className={fieldClassName("mt-2")}
                  placeholder="MU"
                />
                <span className="mt-1 block text-xs text-slate-500">
                  {tickerIdentity ? String(tickerIdentity.company_name || tickerIdentity.symbol || "Supported ticker") : "Enter a supported Walnut ticker."}
                </span>
              </label>

              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">What should this brief investigate?</span>
                <textarea
                  value={config.research_question}
                  onChange={(event) => updateConfig("research_question", event.target.value)}
                  className={fieldClassName("mt-2 min-h-32")}
                  placeholder="Is MU's momentum trade breaking down, or do the fundamentals still support the cycle?"
                />
              </label>

              <div className="grid gap-4 md:grid-cols-2">
                <Select label="Desired angle" value={config.desired_angle} options={options.angles} onChange={(value) => updateConfig("desired_angle", value)} />
                <Select label="Time horizon" value={config.time_horizon} options={options.time_horizons} onChange={(value) => updateConfig("time_horizon", value)} />
                <Select label="Intended audience" value={config.intended_audience} options={options.audiences} onChange={(value) => updateConfig("intended_audience", value)} />
                <Select label="Judgment preference" value={config.judgment_preference} options={options.judgment_preferences} onChange={(value) => updateConfig("judgment_preference", value)} />
              </div>

              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Comparison ticker</span>
                <input
                  value={config.comparison_ticker || ""}
                  onChange={(event) => updateConfig("comparison_ticker", event.target.value.toUpperCase())}
                  className={fieldClassName("mt-2")}
                  placeholder="Optional, e.g. NVDA"
                />
              </label>

              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Additional context</span>
                <textarea
                  value={config.additional_context || ""}
                  onChange={(event) => updateConfig("additional_context", event.target.value)}
                  className={fieldClassName("mt-2 min-h-24")}
                  placeholder="Specific earnings issue, known catalyst, metric to investigate, requested comparison, or notes."
                />
              </label>

              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Include sections</p>
                <div className="mt-2 grid gap-2 sm:grid-cols-2">
                  {options.sections.map((section) => (
                    <label key={section} className="flex items-center gap-2 rounded-lg border border-white/10 bg-slate-950/40 px-3 py-2 text-sm text-slate-300">
                      <input
                        type="checkbox"
                        checked={config.include_sections.includes(section)}
                        onChange={(event) => {
                          const next = event.target.checked ? [...config.include_sections, section] : config.include_sections.filter((item) => item !== section);
                          updateConfig("include_sections", next);
                        }}
                      />
                      {section}
                    </label>
                  ))}
                </div>
              </div>

              <button type="button" onClick={() => setAdvancedOpen((open) => !open)} className="text-left text-sm font-semibold text-emerald-200">
                {advancedOpen ? "Hide" : "Show"} advanced controls
              </button>
              {advancedOpen ? (
                <div className="grid gap-4 rounded-lg border border-white/10 bg-slate-950/40 p-3 md:grid-cols-2">
                  <Select label="Length" value={config.length} options={options.lengths} onChange={(value) => updateConfig("length", value)} />
                  <Select label="Tone" value={config.tone} options={options.tones} onChange={(value) => updateConfig("tone", value)} />
                  <Toggle label="Include existing Walnut charts" checked={config.include_charts} onChange={(value) => updateConfig("include_charts", value)} />
                  <Toggle label="Include genuine source links" checked={config.include_source_links} onChange={(value) => updateConfig("include_source_links", value)} />
                  <label className="md:col-span-2">
                    <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Hero image</span>
                    <input value={config.hero_image || ""} onChange={(event) => updateConfig("hero_image", event.target.value)} className={fieldClassName("mt-2")} placeholder="Existing asset URL/path, or leave blank for fallback" />
                  </label>
                </div>
              ) : null}

              <div className="flex flex-wrap gap-2">
                <Button tone="primary" disabled={busy === "generate"} onClick={generateDraft}>
                  {busy === "generate" ? "Generating..." : "Generate Draft"}
                </Button>
              </div>
            </div>
          </section>

          <EditorPanel
            draft={selectedDraft}
            article={articleDraft}
            bodyMarkdown={bodyMarkdown}
            busy={busy}
            onArticleChange={updateArticle}
            onBodyChange={setBodyMarkdown}
            onSave={() => saveDraft("draft")}
            onReady={() => saveDraft("ready_for_review")}
            onPublish={publishSelected}
            onUnpublish={unpublishSelected}
            onDelete={deleteSelected}
            onRegenerate={regenerateWith}
            blockingWarnings={blockingWarnings.length}
          />
        </div>
      ) : null}

      {activePane === "drafts" || activePane === "published" ? (
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">{activePane === "drafts" ? "Drafts" : "Published"}</h3>
          <div className="mt-4 grid gap-3">
            {(activePane === "drafts" ? generatedDrafts : publishedDrafts).map((draft) => (
              <button
                key={draft.id}
                type="button"
                onClick={() => {
                  setSelectedDraft(draft);
                  setActivePane("create");
                }}
                className="rounded-lg border border-white/10 bg-slate-950/45 p-4 text-left transition hover:border-emerald-300/35"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="font-semibold text-white">{draft.article?.title || draft.id}</p>
                  <span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.status}</span>
                </div>
                <p className="mt-2 text-sm text-slate-400">{draft.article?.summary}</p>
              </button>
            ))}
            {(activePane === "drafts" ? generatedDrafts : publishedDrafts).length === 0 ? <p className="text-sm text-slate-500">No items yet.</p> : null}
          </div>
        </section>
      ) : null}

      {activePane === "settings" ? (
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">Settings</h3>
          <div className="mt-4 grid gap-3 md:grid-cols-3">
            <Metric label="Storage" value="Local JSON" />
            <Metric label="Default status" value="Draft" />
            <Metric label="OpenAI API" value="Server-side Responses" />
          </div>
          <p className="mt-4 text-sm leading-6 text-slate-400">
            Production database publishing and migrations are intentionally not enabled in this local implementation.
          </p>
        </section>
      ) : null}

      <section className="grid gap-4 lg:grid-cols-[1fr_0.9fr]">
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">Validation</h3>
          <div className="mt-3 grid gap-2">
            {selectedWarnings.length ? (
              selectedWarnings.map((warning) => (
                <div key={`${warning.code}-${warning.message}`} className={`rounded-lg border px-3 py-2 text-sm ${warning.blocking ? "border-rose-300/30 bg-rose-950/25 text-rose-100" : "border-amber-300/30 bg-amber-950/20 text-amber-100"}`}>
                  {warning.message}
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-500">Generate or select a draft to see validation results.</p>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">Research Brief Card Preview</h3>
          <div className="mt-3 rounded-lg border border-white/10 bg-slate-950/60 p-4">
            <span className="rounded-md border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-[10px] font-semibold uppercase text-emerald-200">
              {selectedCard?.judgment || articleDraft?.judgment || "Draft"}
            </span>
            <p className="mt-3 text-lg font-semibold text-white">{selectedCard?.title || articleDraft?.title || "No draft selected"}</p>
            <p className="mt-2 text-sm leading-6 text-slate-400">{selectedCard?.description || articleDraft?.summary || "Generated card copy will appear here."}</p>
            <p className="mt-4 text-xs text-slate-500">
              {selectedDraft?.article?.slug ? `/research/${selectedDraft.article.slug}` : "No route yet"}
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}

function Select({ label, value, options, onChange }: { label: string; value: string; options: string[]; onChange: (value: string) => void }) {
  return (
    <label className="block">
      <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} className={fieldClassName("mt-2")}>
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </label>
  );
}

function Toggle({ label, checked, onChange }: { label: string; checked: boolean; onChange: (value: boolean) => void }) {
  return (
    <label className="flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-slate-950/45 px-3 py-2 text-sm text-slate-300">
      <span>{label}</span>
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
    </label>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-100">{value}</p>
    </div>
  );
}

function EditorPanel({
  draft,
  article,
  bodyMarkdown,
  busy,
  onArticleChange,
  onBodyChange,
  onSave,
  onReady,
  onPublish,
  onUnpublish,
  onDelete,
  onRegenerate,
  blockingWarnings,
}: {
  draft: AdminResearchBriefDraft | null;
  article: AdminResearchBriefArticle | null;
  bodyMarkdown: string;
  busy: string | null;
  onArticleChange: <K extends keyof AdminResearchBriefArticle>(key: K, value: AdminResearchBriefArticle[K]) => void;
  onBodyChange: (value: string) => void;
  onSave: () => void;
  onReady: () => void;
  onPublish: () => void;
  onUnpublish: () => void;
  onDelete: () => void;
  onRegenerate: (change: string) => void;
  blockingWarnings: number;
}) {
  if (!draft || !article) {
    return (
      <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
        <h3 className="text-base font-semibold text-white">Draft Review</h3>
        <p className="mt-3 text-sm leading-6 text-slate-400">Generate or select a draft to open the two-column editor.</p>
      </section>
    );
  }
  return (
    <section className="grid gap-4 rounded-lg border border-white/10 bg-slate-950/55 p-4 lg:grid-cols-[minmax(0,1fr)_18rem]">
      <div className="min-w-0 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-base font-semibold text-white">Draft Review</h3>
          <span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.status}</span>
        </div>
        <input value={article.title} onChange={(event) => onArticleChange("title", event.target.value)} className={fieldClassName()} />
        <input value={article.subtitle} onChange={(event) => onArticleChange("subtitle", event.target.value)} className={fieldClassName()} placeholder="Subtitle" />
        <textarea value={article.summary} onChange={(event) => onArticleChange("summary", event.target.value)} className={fieldClassName("min-h-20")} placeholder="Summary" />
        <div className="grid gap-3 md:grid-cols-3">
          <input value={article.judgment} onChange={(event) => onArticleChange("judgment", event.target.value)} className={fieldClassName()} placeholder="Judgment" />
          <input value={article.category} onChange={(event) => onArticleChange("category", event.target.value)} className={fieldClassName()} placeholder="Category" />
          <input value={String(article.reading_minutes || "")} onChange={(event) => onArticleChange("reading_minutes", Number(event.target.value) || 1)} className={fieldClassName()} placeholder="Reading time" />
        </div>
        <div className="grid gap-3 md:grid-cols-2">
          <input value={article.slug} onChange={(event) => onArticleChange("slug", event.target.value)} className={fieldClassName()} placeholder="Slug" />
          <input value={article.hero_image || ""} onChange={(event) => onArticleChange("hero_image", event.target.value)} className={fieldClassName()} placeholder="Hero image" />
        </div>
        <textarea value={bodyMarkdown} onChange={(event) => onBodyChange(event.target.value)} className={fieldClassName("min-h-[32rem] font-mono text-xs leading-6")} />
      </div>

      <aside className="space-y-3">
        <Metric label="Validation" value={draft.validation?.status || "Unknown"} />
        <Metric label="Numeric claims" value={String(draft.validation?.numeric_claims?.length || 0)} />
        <Metric label="Source links" value={String(draft.validation?.source_link_count || 0)} />
        <Metric label="Reading time" value={`${draft.validation?.estimated_reading_minutes || article.reading_minutes || 1} min`} />
        <Metric label="Model" value={draft.model || "OpenAI"} />
        <div className="grid gap-2">
          <Button disabled={Boolean(busy)} onClick={onSave}>Save Draft</Button>
          <Button disabled={Boolean(busy)} onClick={onReady}>Ready for Review</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Regenerate the entire brief with the same data.")}>Regenerate Entire Brief</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Shorten the brief while preserving all supported evidence.")}>Shorten</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Expand the brief with more detail from supplied Walnut data only.")}>Expand</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Make the framing more bullish using only genuine bullish evidence.")}>Make More Bullish</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Make the framing more bearish using only genuine bearish evidence.")}>Make More Bearish</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Make the framing more balanced without weakening the final judgment.")}>Make More Neutral</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Improve the title.")}>Improve Title</Button>
          <Button disabled={Boolean(busy)} onClick={() => onRegenerate("Improve the final Walnut judgment.")}>Improve Walnut Judgment</Button>
          <Button tone="primary" disabled={Boolean(busy) || blockingWarnings > 0} onClick={onPublish}>Publish</Button>
          <Button disabled={Boolean(busy) || draft.status !== "published"} onClick={onUnpublish}>Unpublish</Button>
          <Button tone="danger" disabled={Boolean(busy)} onClick={onDelete}>Delete Draft</Button>
        </div>
      </aside>
    </section>
  );
}
