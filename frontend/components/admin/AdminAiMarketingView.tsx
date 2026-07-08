"use client";

import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  analyzeAdminAiMarketingManualUrl,
  archiveAdminAiGrowthDraft,
  createAdminAiGrowthDraft,
  emailAdminAiGrowthDraft,
  getAdminAiGrowthDrafts,
  getAdminAiMarketingCampaigns,
  getAdminAiMarketingSettings,
  markAdminAiGrowthDraftCopied,
  markAdminAiGrowthDraftPosted,
  rejectAdminAiGrowthDraft,
  sendAdminAiMarketingEmailDigest,
  testAdminAiMarketingOpenAI,
  testAdminAiMarketingReddit,
  updateAdminAiGrowthDraftStatus,
  type AdminAiGrowthAsset,
  type AdminAiMarketingCampaign,
  type AdminAiMarketingConfig,
  type AdminAiMarketingEmailDigestResponse,
  type AdminAiMarketingOpportunity,
  type AdminAiMarketingSetting,
  type AdminAiMarketingSettingsTestResponse,
  type AdminAiMarketingStatus,
} from "@/lib/api";

type AdminAiMarketingViewProps = {
  showToast?: (message: string, tone?: "success" | "error" | "info") => void;
};

type TabKey =
  | "dashboard"
  | "drafts"
  | "manual"
  | "x_chart_drops"
  | "influencer_packs"
  | "reddit_threads"
  | "reddit_ads"
  | "settings";

const TABS: Array<{ key: TabKey; label: string }> = [
  { key: "dashboard", label: "Dashboard" },
  { key: "drafts", label: "Content Drafts" },
  { key: "manual", label: "Manual Research Input" },
  { key: "x_chart_drops", label: "X Chart Drops" },
  { key: "influencer_packs", label: "Influencer Packs" },
  { key: "reddit_threads", label: "Reddit Research Threads" },
  { key: "reddit_ads", label: "Reddit Paid Ads" },
  { key: "settings", label: "Settings" },
];

const STATUS_FILTERS: Array<{ value: "all" | AdminAiMarketingStatus; label: string }> = [
  { value: "all", label: "All" },
  { value: "new", label: "New" },
  { value: "needs_review", label: "Needs review" },
  { value: "emailed", label: "Emailed" },
  { value: "copied", label: "Copied" },
  { value: "approved", label: "Approved" },
  { value: "posted_manually", label: "Posted manually" },
  { value: "archived", label: "Archived" },
  { value: "rejected", label: "Rejected" },
];

const SETTING_KEYS = [
  "OPENAI_API_KEY",
  "AI_MARKETING_MODEL",
  "OPENAI_WEB_SEARCH_ENABLED",
  "REDDIT_CLIENT_ID",
  "REDDIT_CLIENT_SECRET",
  "REDDIT_USER_AGENT",
] as const;

const SOURCE_PLATFORMS = ["X", "Reddit", "Facebook", "LinkedIn", "Other"] as const;

function emptyManualForm() {
  return {
    source_platform: "Reddit",
    url: "",
    text: "",
    ticker_theme: "",
    desired_output_type: "reply",
    destination_url: "https://walnutmarkets.com",
  };
}

function emptyXForm() {
  return {
    ticker_theme: "",
    source_types: ["signals", "price/volume"],
    timeframe: "past 30 days",
    tone: "sharp",
    include_link: true,
    destination_url: "https://walnutmarkets.com",
    asset_url: "",
    asset_caption: "",
  };
}

function emptyInfluencerForm() {
  return {
    influencer: "",
    platform: "X",
    audience: "active traders",
    tickers_themes: "",
    report_type: "PDF outline",
    offer_type: "free data report",
  };
}

function emptyRedditThreadForm() {
  return {
    subreddit: "",
    topic: "",
    post_type: "case study",
    disclosure_style: "founder disclosure",
    rule_notes: "",
  };
}

function emptyAdForm() {
  return {
    audience: "",
    landing_page: "https://walnutmarkets.com",
    offer: "Free plan",
    plan_focus: "Free",
    tone: "professional",
    pain_point: "",
  };
}

export function AdminAiMarketingView({ showToast }: AdminAiMarketingViewProps) {
  const [activeTab, setActiveTab] = useState<TabKey>("dashboard");
  const [drafts, setDrafts] = useState<AdminAiMarketingOpportunity[]>([]);
  const [campaigns, setCampaigns] = useState<AdminAiMarketingCampaign[]>([]);
  const [config, setConfig] = useState<AdminAiMarketingConfig | null>(null);
  const [settings, setSettings] = useState<AdminAiMarketingSetting[]>([]);
  const [statusFilter, setStatusFilter] = useState<"all" | AdminAiMarketingStatus>("all");
  const [busy, setBusy] = useState<string | null>(null);
  const [loadStatus, setLoadStatus] = useState<string | null>(null);
  const [digestPreview, setDigestPreview] = useState<AdminAiMarketingEmailDigestResponse | null>(null);
  const [settingsTest, setSettingsTest] = useState<Record<"openai" | "reddit", AdminAiMarketingSettingsTestResponse | null>>({
    openai: null,
    reddit: null,
  });
  const [manualForm, setManualForm] = useState(() => emptyManualForm());
  const [xForm, setXForm] = useState(() => emptyXForm());
  const [influencerForm, setInfluencerForm] = useState(() => emptyInfluencerForm());
  const [redditThreadForm, setRedditThreadForm] = useState(() => emptyRedditThreadForm());
  const [adForm, setAdForm] = useState(() => emptyAdForm());

  const pendingReviewCount = useMemo(
    () => drafts.filter((draft) => ["new", "draft", "needs_review", "emailed", "approved"].includes(draft.status)).length,
    [drafts],
  );
  const highFitCount = useMemo(() => drafts.filter((draft) => (draft.fit_score ?? draft.relevance_score ?? 0) >= 75).length, [drafts]);
  const assetCount = useMemo(() => drafts.reduce((total, draft) => total + (draft.assets?.length ?? 0), 0), [drafts]);
  const legacyCampaigns = campaigns.filter((campaign) => campaign.legacy);

  const notify = (message: string, tone: "success" | "error" | "info" = "info") => {
    showToast?.(message, tone);
    setLoadStatus(message);
  };

  const load = async () => {
    setBusy("load");
    try {
      const [draftData, settingsData, campaignData] = await Promise.all([
        getAdminAiGrowthDrafts({ status: statusFilter === "all" ? "all" : statusFilter, limit: 100 }),
        getAdminAiMarketingSettings(),
        getAdminAiMarketingCampaigns(),
      ]);
      setDrafts(draftData.items);
      setConfig(settingsData.config);
      setSettings(settingsData.items);
      setCampaigns(campaignData.items);
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to load AI Growth Engine.", "error");
    } finally {
      setBusy(null);
    }
  };

  useEffect(() => {
    void load();
  }, [statusFilter]);

  const refreshDrafts = async () => {
    const data = await getAdminAiGrowthDrafts({ status: statusFilter === "all" ? "all" : statusFilter, limit: 100 });
    setDrafts(data.items);
    setConfig(data.config);
  };

  const replaceDraft = (next: AdminAiMarketingOpportunity) => {
    setDrafts((current) => current.map((draft) => (draft.id === next.id ? next : draft)));
  };

  const prependDraft = (next: AdminAiMarketingOpportunity) => {
    setDrafts((current) => [next, ...current.filter((draft) => draft.id !== next.id)]);
  };

  const copyText = async (draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => {
    const text = value?.trim();
    if (!text) {
      notify(`${label} is empty.`, "error");
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      const updated = await markAdminAiGrowthDraftCopied(draft.id);
      replaceDraft(updated);
      notify(`${label} copied.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : `Unable to copy ${label.toLowerCase()}.`, "error");
    }
  };

  const updateDraftStatus = async (draft: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => {
    setBusy(`status:${draft.id}`);
    try {
      const updated = await updateAdminAiGrowthDraftStatus(draft.id, { status });
      replaceDraft(updated);
      notify(`Draft marked ${status.replaceAll("_", " ")}.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to update draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const runDraftAction = async (
    draft: AdminAiMarketingOpportunity,
    action: "email" | "copied" | "posted" | "archive" | "reject",
  ) => {
    setBusy(`${action}:${draft.id}`);
    try {
      if (action === "email") {
        await emailAdminAiGrowthDraft(draft.id);
        notify(`Draft emailed to ${config?.recipient ?? "Jarod"}.`, "success");
        await refreshDrafts();
        return;
      }
      const updated =
        action === "copied"
          ? await markAdminAiGrowthDraftCopied(draft.id)
          : action === "posted"
            ? await markAdminAiGrowthDraftPosted(draft.id)
            : action === "archive"
              ? await archiveAdminAiGrowthDraft(draft.id)
              : await rejectAdminAiGrowthDraft(draft.id);
      replaceDraft(updated);
      notify(`Draft marked ${action === "posted" ? "posted manually" : action}.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to update draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const previewDigest = async (send: boolean) => {
    setBusy(send ? "digest-send" : "digest-preview");
    try {
      const result = await sendAdminAiMarketingEmailDigest({
        send,
        statuses: statusFilter === "all" ? ["new", "needs_review", "approved"] : [statusFilter],
        limit: 25,
      });
      setDigestPreview(result);
      notify(send ? `Digest sent to ${result.to_email ?? config?.recipient ?? "Jarod"}.` : "Digest preview refreshed.", send ? "success" : "info");
      if (send) await refreshDrafts();
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to prepare digest.", "error");
    } finally {
      setBusy(null);
    }
  };

  const testConnection = async (kind: "openai" | "reddit") => {
    setBusy(`settings-test:${kind}`);
    try {
      const result = kind === "openai" ? await testAdminAiMarketingOpenAI() : await testAdminAiMarketingReddit();
      setSettingsTest((current) => ({ ...current, [kind]: result }));
      notify(result.message, result.ok ? "success" : "error");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Connection test failed.";
      setSettingsTest((current) => ({ ...current, [kind]: { ok: false, message } }));
      notify(message, "error");
    } finally {
      setBusy(null);
    }
  };

  const submitManual = async () => {
    if (!manualForm.text.trim() && !manualForm.url.trim()) {
      notify("Paste source text or add a source URL first.", "error");
      return;
    }
    setBusy("manual");
    try {
      const result = await analyzeAdminAiMarketingManualUrl({
        url: manualForm.url.trim() || null,
        text: manualForm.text.trim() || null,
        title: `${manualForm.source_platform} manual research`,
        source_platform: manualForm.source_platform,
        ticker_theme: manualForm.ticker_theme.trim() || null,
        desired_output_type: manualForm.desired_output_type,
        destination_url: manualForm.destination_url.trim() || null,
        campaign_type: "manual_research_input",
        generate: true,
      });
      prependDraft(result.opportunity);
      setManualForm(emptyManualForm());
      notify(result.warning || "Manual research draft created.", result.warning ? "info" : "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to create manual research draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const submitGrowthDraft = async (kind: TabKey) => {
    setBusy(kind);
    try {
      const payload = buildGrowthPayload(kind, { xForm, influencerForm, redditThreadForm, adForm });
      const result = await createAdminAiGrowthDraft(payload);
      prependDraft(result.opportunity);
      if (kind === "x_chart_drops") setXForm(emptyXForm());
      if (kind === "influencer_packs") setInfluencerForm(emptyInfluencerForm());
      if (kind === "reddit_threads") setRedditThreadForm(emptyRedditThreadForm());
      if (kind === "reddit_ads") setAdForm(emptyAdForm());
      notify(result.warning || "AI Growth draft created.", result.warning ? "info" : "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to create draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="space-y-5">
      <section className="rounded-lg border border-amber-300/20 bg-amber-300/10 p-4 text-sm font-semibold text-amber-100">
        Human review required. No auto-posting, bot posting, automatic DMs, or platform credential collection.
      </section>

      <div className="flex gap-2 overflow-x-auto pb-1">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            type="button"
            onClick={() => setActiveTab(tab.key)}
            className={`shrink-0 rounded-md border px-3 py-2 text-sm font-semibold ${
              activeTab === tab.key
                ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                : "border-white/10 text-slate-300"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {loadStatus ? (
        <p className="rounded-lg border border-white/10 bg-slate-950/50 p-3 text-sm text-slate-300">{loadStatus}</p>
      ) : null}

      {activeTab === "dashboard" ? (
        <Dashboard
          config={config}
          pendingReviewCount={pendingReviewCount}
          highFitCount={highFitCount}
          assetCount={assetCount}
          drafts={drafts}
          legacyCampaigns={legacyCampaigns}
        />
      ) : null}

      {activeTab === "drafts" ? (
        <DraftsView
          drafts={drafts}
          busy={busy}
          statusFilter={statusFilter}
          digestPreview={digestPreview}
          onStatusFilter={setStatusFilter}
          onPreviewDigest={() => void previewDigest(false)}
          onSendDigest={() => void previewDigest(true)}
          onCopy={copyText}
          onStatus={updateDraftStatus}
          onAction={runDraftAction}
        />
      ) : null}

      {activeTab === "manual" ? (
        <ManualResearchForm
          form={manualForm}
          busy={busy}
          setForm={setManualForm}
          onSubmit={() => void submitManual()}
        />
      ) : null}

      {activeTab === "x_chart_drops" ? (
        <XChartDropForm
          form={xForm}
          busy={busy}
          setForm={setXForm}
          onSubmit={() => void submitGrowthDraft("x_chart_drops")}
        />
      ) : null}

      {activeTab === "influencer_packs" ? (
        <InfluencerPackForm
          form={influencerForm}
          busy={busy}
          setForm={setInfluencerForm}
          onSubmit={() => void submitGrowthDraft("influencer_packs")}
        />
      ) : null}

      {activeTab === "reddit_threads" ? (
        <RedditThreadForm
          form={redditThreadForm}
          busy={busy}
          setForm={setRedditThreadForm}
          onSubmit={() => void submitGrowthDraft("reddit_threads")}
        />
      ) : null}

      {activeTab === "reddit_ads" ? (
        <RedditAdForm
          form={adForm}
          busy={busy}
          setForm={setAdForm}
          onSubmit={() => void submitGrowthDraft("reddit_ads")}
        />
      ) : null}

      {activeTab === "settings" ? (
        <SettingsView
          config={config}
          settings={settings}
          settingsTest={settingsTest}
          busy={busy}
          onTest={testConnection}
        />
      ) : null}
    </div>
  );
}

function Dashboard({
  config,
  pendingReviewCount,
  highFitCount,
  assetCount,
  drafts,
  legacyCampaigns,
}: {
  config: AdminAiMarketingConfig | null;
  pendingReviewCount: number;
  highFitCount: number;
  assetCount: number;
  drafts: AdminAiMarketingOpportunity[];
  legacyCampaigns: AdminAiMarketingCampaign[];
}) {
  const recent = drafts.slice(0, 5);
  return (
    <section className="space-y-4">
      <div className="grid gap-3 md:grid-cols-3">
        <MetricCard label="OpenAI" value={config?.openai_configured ? "Configured" : "Missing"} tone={config?.openai_configured ? "good" : "bad"} />
        <MetricCard label="Reddit API" value={config?.reddit_status ?? "missing"} tone={config?.reddit_status === "configured" ? "good" : "warn"} />
        <MetricCard label="Review queue" value={String(pendingReviewCount)} tone={pendingReviewCount ? "warn" : "good"} />
        <MetricCard label="Recent assets" value={String(assetCount)} />
        <MetricCard label="High-fit drafts" value={String(highFitCount)} tone={highFitCount ? "good" : "muted"} />
        <MetricCard label="Posting" value="Manual only" tone="good" />
      </div>

      {config?.warnings.length ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <h3 className="text-sm font-semibold uppercase tracking-wide text-amber-200">Spam and compliance risk warnings</h3>
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {config.warnings.map((warning) => (
              <p key={warning} className="rounded-md border border-white/10 bg-slate-950/50 p-3 text-sm text-slate-300">
                {warning}
              </p>
            ))}
          </div>
        </section>
      ) : null}

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
        <h3 className="text-base font-semibold text-white">Recent generated assets</h3>
        <div className="mt-3 space-y-2">
          {recent.length ? recent.map((draft) => (
            <div key={draft.id} className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-white/10 bg-slate-950/40 p-3">
              <span className="text-sm font-semibold text-slate-100">{draft.title}</span>
              <div className="flex flex-wrap gap-2">
                <Badge label={contentTypeLabel(draft.content_type)} />
                <Badge label={draft.status} tone={statusTone(draft.status)} />
              </div>
            </div>
          )) : (
            <p className="text-sm text-slate-400">No drafts yet.</p>
          )}
        </div>
      </section>

      {legacyCampaigns.length ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <h3 className="text-base font-semibold text-white">Legacy Outreach Campaigns</h3>
          <div className="mt-3 flex flex-wrap gap-2">
            {legacyCampaigns.map((campaign) => <Badge key={campaign.id} label={campaign.display_name ?? "Legacy Outreach Campaign"} />)}
          </div>
        </section>
      ) : null}
    </section>
  );
}

function DraftsView({
  drafts,
  busy,
  statusFilter,
  digestPreview,
  onStatusFilter,
  onPreviewDigest,
  onSendDigest,
  onCopy,
  onStatus,
  onAction,
}: {
  drafts: AdminAiMarketingOpportunity[];
  busy: string | null;
  statusFilter: "all" | AdminAiMarketingStatus;
  digestPreview: AdminAiMarketingEmailDigestResponse | null;
  onStatusFilter: (status: "all" | AdminAiMarketingStatus) => void;
  onPreviewDigest: () => void;
  onSendDigest: () => void;
  onCopy: (draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => void;
  onStatus: (draft: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => void;
  onAction: (draft: AdminAiMarketingOpportunity, action: "email" | "copied" | "posted" | "archive" | "reject") => void;
}) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h3 className="text-lg font-semibold text-white">Content Drafts</h3>
          <p className="mt-1 text-sm text-slate-400">{drafts.length} drafts in the current filter.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          {STATUS_FILTERS.map((filter) => (
            <button
              key={filter.value}
              type="button"
              onClick={() => onStatusFilter(filter.value)}
              className={`rounded-md border px-3 py-2 text-sm font-semibold ${
                statusFilter === filter.value
                  ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                  : "border-white/10 text-slate-300"
              }`}
            >
              {filter.label}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-3">
        <button type="button" disabled={Boolean(busy)} onClick={onPreviewDigest} className="rounded-md border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50">
          {busy === "digest-preview" ? "Previewing..." : "Preview email"}
        </button>
        <button type="button" disabled={Boolean(busy)} onClick={onSendDigest} className="rounded-md border border-emerald-300/30 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50">
          {busy === "digest-send" ? "Sending..." : "Email selected drafts"}
        </button>
      </div>

      {digestPreview ? (
        <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <p className="font-semibold text-white">{digestPreview.subject ?? "Walnut AI Growth digest"}</p>
          <p className="text-sm text-slate-400">{digestPreview.count} drafts - {digestPreview.to_email ?? "jarod@walnutmarkets.com"}</p>
          {digestPreview.body_text ? <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap rounded-md bg-slate-950 p-3 text-xs leading-5 text-slate-300">{digestPreview.body_text}</pre> : null}
        </div>
      ) : null}

      <div className="mt-5 space-y-4">
        {busy === "load" ? (
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-5 text-sm text-slate-400">Loading AI Growth drafts...</div>
        ) : drafts.length ? (
          drafts.map((draft) => (
            <DraftCard
              key={draft.id}
              draft={draft}
              busy={busy}
              onCopy={onCopy}
              onStatus={onStatus}
              onAction={onAction}
            />
          ))
        ) : (
          <p className="rounded-lg border border-white/10 bg-slate-950/40 p-5 text-sm text-slate-400">No content drafts match this filter.</p>
        )}
      </div>
    </section>
  );
}

function DraftCard({
  draft,
  busy,
  onCopy,
  onStatus,
  onAction,
}: {
  draft: AdminAiMarketingOpportunity;
  busy: string | null;
  onCopy: (draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => void;
  onStatus: (draft: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => void;
  onAction: (draft: AdminAiMarketingOpportunity, action: "email" | "copied" | "posted" | "archive" | "reject") => void;
}) {
  const suggestion = draft.suggestion;
  const fullDraft = draft.generated_content || suggestion?.suggested_post || suggestion?.suggested_reply || "";
  const shortVariant = suggestion?.alternate_hooks?.[0] || suggestion?.alternate_reply_more_direct || "";
  const disclosure = suggestion?.disclosure_text || disclosureFromDraft(fullDraft);
  const walnutLink = suggestion?.suggested_destination_url || draft.suggested_destination_url || "";
  const sourceUrl = draft.source_url || "";
  const checklist = postingChecklist(draft, fullDraft, disclosure, walnutLink);
  const links = draft.posting_links ?? {};

  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap gap-2">
            <Badge label={platformLabel(draft.source_platform ?? draft.platform)} />
            <Badge label={contentTypeLabel(draft.content_type)} />
            <Badge label={draft.status} tone={statusTone(draft.status)} />
            <ScoreBadge label="Fit" value={draft.fit_score ?? draft.relevance_score} />
            <ScoreBadge label="Spam" value={draft.spam_risk_score} invert />
          </div>
          <h4 className="mt-3 text-base font-semibold text-white">{draft.title}</h4>
          <p className="mt-1 text-sm text-slate-400">{draft.ticker_theme || draft.community || draft.source_platform || "AI Growth draft"}</p>
          {draft.excerpt ? <p className="mt-3 line-clamp-3 text-sm text-slate-300">{draft.excerpt}</p> : null}
        </div>
        <div className="flex flex-wrap gap-2">
          <AssistLink href={links.open_source_post ?? sourceUrl} label="Open source post" />
          <AssistLink href={links.open_walnut_link ?? walnutLink} label="Open Walnut link" />
          <AssistLink href={links.open_x ?? null} label="Login/Open X" />
          <AssistLink href={links.open_x_compose ?? null} label="Open X compose" />
          <AssistLink href={links.open_reddit ?? null} label="Login/Open Reddit" />
          <AssistLink href={links.open_reddit_thread ?? null} label="Open Reddit thread" />
          <AssistLink href={links.open_reddit_submit ?? null} label="Open Reddit submit" />
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1.25fr)_minmax(0,0.75fr)]">
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3">
          <p className="text-sm font-semibold text-white">Draft content</p>
          {suggestion?.recommended_action === "skip" ? <p className="mt-2 rounded-md border border-rose-300/30 bg-rose-300/10 p-2 text-sm font-semibold text-rose-100">Probably do not post.</p> : null}
          <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap text-sm leading-6 text-slate-200">{fullDraft || "No generated content yet."}</pre>
          {shortVariant ? (
            <div className="mt-3 border-t border-white/10 pt-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Short variant</p>
              <p className="mt-2 text-sm text-slate-300">{shortVariant}</p>
            </div>
          ) : null}
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3 text-sm text-slate-300">
          <p><span className="font-semibold text-slate-100">Action:</span> {draft.recommended_action ?? suggestion?.recommended_action ?? "pending"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Content angle:</span> {suggestion?.content_angle || suggestion?.reply_angle || "pending"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Walnut link:</span> {walnutLink || "none"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Disclosure:</span> {disclosure || "Review manually."}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Compliance:</span> {suggestion?.compliance_notes ?? draft.compliance_notes ?? "Human review required."}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Data points:</span> {suggestion?.value_added_insight || "Pending"}</p>
        </div>
      </div>

      {draft.assets?.length ? (
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {draft.assets.map((asset, index) => (
            <AssetPreview key={`${asset.url ?? asset.thumbnail_url ?? index}`} asset={asset} onCopy={(label, value) => onCopy(draft, label, value)} />
          ))}
        </div>
      ) : null}

      <div className="mt-4 flex flex-wrap gap-2">
        <Button onClick={() => onCopy(draft, "Copy full draft", fullDraft)} disabled={Boolean(busy)}>Copy full draft</Button>
        <Button onClick={() => onCopy(draft, "Copy short variant", shortVariant)} disabled={Boolean(busy)}>Copy short variant</Button>
        <Button onClick={() => onCopy(draft, "Copy disclosure line", disclosure)} disabled={Boolean(busy)}>Copy disclosure line</Button>
        <Button onClick={() => onCopy(draft, "Copy Walnut link", walnutLink)} disabled={Boolean(busy)}>Copy Walnut link</Button>
        <Button onClick={() => onCopy(draft, "Copy source URL", sourceUrl)} disabled={Boolean(busy)}>Copy source URL</Button>
        <Button onClick={() => onCopy(draft, "Copy all assets/links as a posting checklist", checklist)} disabled={Boolean(busy)}>Copy posting checklist</Button>
        {draft.content_type === "x_post" ? (
          <>
            <Button onClick={() => onCopy(draft, "Copy X post text", fullDraft)} disabled={Boolean(busy)}>Copy X post text</Button>
            <Button onClick={() => onCopy(draft, "Copy alternate hooks", (suggestion?.alternate_hooks ?? []).join("\n"))} disabled={Boolean(busy)}>Copy alternate hooks</Button>
            <Button onClick={() => onCopy(draft, "Copy image/chart caption", draft.assets?.[0]?.suggested_caption)} disabled={Boolean(busy)}>Copy image/chart caption</Button>
          </>
        ) : null}
        {draft.content_type === "reddit_thread" || draft.content_type === "reddit_reply" ? (
          <>
            <Button onClick={() => onCopy(draft, "Copy Reddit post title", suggestion?.title_options?.[0] || draft.title)} disabled={Boolean(busy)}>Copy Reddit post title</Button>
            <Button onClick={() => onCopy(draft, "Copy Reddit post body", fullDraft)} disabled={Boolean(busy)}>Copy Reddit post body</Button>
            <Button onClick={() => onCopy(draft, "Copy Reddit comment reply", suggestion?.suggested_reply)} disabled={Boolean(busy)}>Copy Reddit comment reply</Button>
            <Button onClick={() => onCopy(draft, "Copy disclosure text", disclosure)} disabled={Boolean(busy)}>Copy disclosure text</Button>
          </>
        ) : null}
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <Button onClick={() => onAction(draft, "email")} disabled={Boolean(busy)}>{busy === `email:${draft.id}` ? "Sending..." : "Send/re-send email to Jarod"}</Button>
        <Button onClick={() => onAction(draft, "copied")} disabled={Boolean(busy)}>Mark copied</Button>
        <Button onClick={() => onStatus(draft, "approved")} disabled={Boolean(busy)}>Approve</Button>
        <Button onClick={() => onAction(draft, "posted")} disabled={Boolean(busy)}>Mark posted manually</Button>
        <Button onClick={() => onAction(draft, "archive")} disabled={Boolean(busy)}>Archive</Button>
        <Button onClick={() => onAction(draft, "reject")} disabled={Boolean(busy)}>Reject</Button>
      </div>
    </article>
  );
}

function ManualResearchForm({
  form,
  setForm,
  busy,
  onSubmit,
}: {
  form: ReturnType<typeof emptyManualForm>;
  setForm: (value: ReturnType<typeof emptyManualForm>) => void;
  busy: string | null;
  onSubmit: () => void;
}) {
  return (
    <FormShell title="Manual Research Input">
      <div className="grid gap-4 lg:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
        <div className="space-y-3">
          <SelectField label="Source platform" value={form.source_platform} onChange={(value) => setForm({ ...form, source_platform: value })} options={[...SOURCE_PLATFORMS]} />
          <TextField label="URL optional" value={form.url} onChange={(value) => setForm({ ...form, url: value })} placeholder="https://..." />
          <TextField label="Ticker/theme optional" value={form.ticker_theme} onChange={(value) => setForm({ ...form, ticker_theme: value })} placeholder="NVDA, AI infrastructure, filings..." />
          <SelectField label="Desired output type" value={form.desired_output_type} onChange={(value) => setForm({ ...form, desired_output_type: value })} options={["reply", "X post", "Reddit research thread", "influencer pitch", "paid ad copy", "report pack outline"]} />
          <TextField label="Destination URL optional" value={form.destination_url} onChange={(value) => setForm({ ...form, destination_url: value })} placeholder="https://walnutmarkets.com/ticker/NVDA" />
        </div>
        <TextareaField label="Pasted post/comment/thread text" value={form.text} onChange={(value) => setForm({ ...form, text: value })} rows={11} />
      </div>
      <SubmitButton busy={busy === "manual"} onClick={onSubmit} label="Create draft" busyLabel="Creating..." />
    </FormShell>
  );
}

function XChartDropForm({
  form,
  setForm,
  busy,
  onSubmit,
}: {
  form: ReturnType<typeof emptyXForm>;
  setForm: (value: ReturnType<typeof emptyXForm>) => void;
  busy: string | null;
  onSubmit: () => void;
}) {
  const sourceTypes = ["signals", "Congress", "insiders", "government contracts", "price/volume", "financials/filings"];
  return (
    <FormShell title="X Chart Drops">
      <div className="grid gap-4 md:grid-cols-2">
        <TextField label="Ticker or theme" value={form.ticker_theme} onChange={(value) => setForm({ ...form, ticker_theme: value })} />
        <TextField label="Timeframe" value={form.timeframe} onChange={(value) => setForm({ ...form, timeframe: value })} />
        <SelectField label="Tone" value={form.tone} onChange={(value) => setForm({ ...form, tone: value })} options={["sharp", "educational", "contrarian", "professional"]} />
        <TextField label="Suggested destination URL" value={form.destination_url} onChange={(value) => setForm({ ...form, destination_url: value })} />
        <TextField label="Chart/image URL optional" value={form.asset_url} onChange={(value) => setForm({ ...form, asset_url: value })} />
        <TextField label="Image/chart caption optional" value={form.asset_caption} onChange={(value) => setForm({ ...form, asset_caption: value })} />
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        {sourceTypes.map((sourceType) => (
          <label key={sourceType} className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={form.source_types.includes(sourceType)}
              onChange={(event) => {
                const next = event.target.checked
                  ? [...form.source_types, sourceType]
                  : form.source_types.filter((item) => item !== sourceType);
                setForm({ ...form, source_types: next });
              }}
            />
            {sourceType}
          </label>
        ))}
        <label className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
          <input type="checkbox" checked={form.include_link} onChange={(event) => setForm({ ...form, include_link: event.target.checked })} />
          Include link
        </label>
      </div>
      <SubmitButton busy={busy === "x_chart_drops"} onClick={onSubmit} label="Generate X draft" busyLabel="Generating..." />
    </FormShell>
  );
}

function InfluencerPackForm({
  form,
  setForm,
  busy,
  onSubmit,
}: {
  form: ReturnType<typeof emptyInfluencerForm>;
  setForm: (value: ReturnType<typeof emptyInfluencerForm>) => void;
  busy: string | null;
  onSubmit: () => void;
}) {
  return (
    <FormShell title="Influencer Report Packs">
      <div className="grid gap-4 md:grid-cols-2">
        <TextField label="Influencer name/handle" value={form.influencer} onChange={(value) => setForm({ ...form, influencer: value })} />
        <SelectField label="Platform" value={form.platform} onChange={(value) => setForm({ ...form, platform: value })} options={["X", "Reddit", "Facebook", "LinkedIn", "Other"]} />
        <TextField label="Audience type" value={form.audience} onChange={(value) => setForm({ ...form, audience: value })} />
        <TextField label="Tickers/themes" value={form.tickers_themes} onChange={(value) => setForm({ ...form, tickers_themes: value })} />
        <SelectField label="Report type" value={form.report_type} onChange={(value) => setForm({ ...form, report_type: value })} options={["PDF outline", "CSV export idea", "screenshot pack", "custom screener export", "weekly dashboard idea"]} />
        <SelectField label="Offer type" value={form.offer_type} onChange={(value) => setForm({ ...form, offer_type: value })} options={["free data report", "custom research pack", "co-branded analysis", "trial access"]} />
      </div>
      <SubmitButton busy={busy === "influencer_packs"} onClick={onSubmit} label="Generate pack draft" busyLabel="Generating..." />
    </FormShell>
  );
}

function RedditThreadForm({
  form,
  setForm,
  busy,
  onSubmit,
}: {
  form: ReturnType<typeof emptyRedditThreadForm>;
  setForm: (value: ReturnType<typeof emptyRedditThreadForm>) => void;
  busy: string | null;
  onSubmit: () => void;
}) {
  return (
    <FormShell title="Reddit Research Threads">
      <div className="grid gap-4 md:grid-cols-2">
        <TextField label="Subreddit" value={form.subreddit} onChange={(value) => setForm({ ...form, subreddit: value })} placeholder="stocks" />
        <TextField label="Topic/ticker/theme" value={form.topic} onChange={(value) => setForm({ ...form, topic: value })} />
        <SelectField label="Post type" value={form.post_type} onChange={(value) => setForm({ ...form, post_type: value })} options={["case study", "backtest write-up", "research guide", "data walkthrough", "tool comparison"]} />
        <TextField label="Disclosure style" value={form.disclosure_style} onChange={(value) => setForm({ ...form, disclosure_style: value })} />
      </div>
      <TextareaField label="Pasted subreddit rule notes optional" value={form.rule_notes} onChange={(value) => setForm({ ...form, rule_notes: value })} rows={4} />
      <SubmitButton busy={busy === "reddit_threads"} onClick={onSubmit} label="Generate Reddit thread draft" busyLabel="Generating..." />
    </FormShell>
  );
}

function RedditAdForm({
  form,
  setForm,
  busy,
  onSubmit,
}: {
  form: ReturnType<typeof emptyAdForm>;
  setForm: (value: ReturnType<typeof emptyAdForm>) => void;
  busy: string | null;
  onSubmit: () => void;
}) {
  return (
    <FormShell title="Reddit Paid Ad Ideas">
      <div className="grid gap-4 md:grid-cols-2">
        <TextField label="Subreddit/audience" value={form.audience} onChange={(value) => setForm({ ...form, audience: value })} />
        <TextField label="Landing page" value={form.landing_page} onChange={(value) => setForm({ ...form, landing_page: value })} />
        <TextField label="Offer" value={form.offer} onChange={(value) => setForm({ ...form, offer: value })} />
        <SelectField label="Plan focus" value={form.plan_focus} onChange={(value) => setForm({ ...form, plan_focus: value })} options={["Free", "Premium", "Pro"]} />
        <TextField label="Tone" value={form.tone} onChange={(value) => setForm({ ...form, tone: value })} />
        <TextField label="Pain point" value={form.pain_point} onChange={(value) => setForm({ ...form, pain_point: value })} />
      </div>
      <SubmitButton busy={busy === "reddit_ads"} onClick={onSubmit} label="Generate ad ideas" busyLabel="Generating..." />
    </FormShell>
  );
}

function SettingsView({
  config,
  settings,
  settingsTest,
  busy,
  onTest,
}: {
  config: AdminAiMarketingConfig | null;
  settings: AdminAiMarketingSetting[];
  settingsTest: Record<"openai" | "reddit", AdminAiMarketingSettingsTestResponse | null>;
  busy: string | null;
  onTest: (kind: "openai" | "reddit") => void;
}) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h3 className="text-lg font-semibold text-white">Settings</h3>
          <p className="mt-1 text-sm text-slate-400">Provider credentials are read from server environment variables and Fly secrets only.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button disabled={Boolean(busy)} onClick={() => onTest("openai")}>{busy === "settings-test:openai" ? "Testing..." : "Test OpenAI connection"}</Button>
          <Button disabled={Boolean(busy)} onClick={() => onTest("reddit")}>{busy === "settings-test:reddit" ? "Testing..." : "Test Reddit connection"}</Button>
        </div>
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-3">
        <MetricCard label="OpenAI" value={config?.openai_configured ? "Configured" : "Missing"} tone={config?.openai_configured ? "good" : "bad"} />
        <MetricCard label="AI model" value={config?.openai_model ?? "Default"} />
        <MetricCard label="OpenAI Web Search" value={config?.openai_web_search_status ?? "disabled"} tone={config?.openai_web_search_status === "enabled" ? "good" : "warn"} />
        <MetricCard label="Manual input" value={config?.manual_text_status ?? "available"} tone="good" />
        <MetricCard label="Reddit API" value={config?.reddit_status ?? "missing"} tone={config?.reddit_status === "configured" ? "good" : "warn"} />
        <MetricCard label="Recipient" value={config?.recipient ?? "jarod@walnutmarkets.com"} />
        <MetricCard label="Posting" value="Manual only" tone="good" />
      </div>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        {SETTING_KEYS.map((key) => <SettingField key={key} settingKey={key} item={settings.find((setting) => setting.key === key)} />)}
      </div>
      <div className="mt-4 flex flex-wrap gap-3">
        <ConnectionResult result={settingsTest.openai} />
        <ConnectionResult result={settingsTest.reddit} />
      </div>
    </section>
  );
}

function buildGrowthPayload(
  kind: TabKey,
  forms: {
    xForm: ReturnType<typeof emptyXForm>;
    influencerForm: ReturnType<typeof emptyInfluencerForm>;
    redditThreadForm: ReturnType<typeof emptyRedditThreadForm>;
    adForm: ReturnType<typeof emptyAdForm>;
  },
) {
  if (kind === "x_chart_drops") {
    const assets = forms.xForm.asset_url.trim()
      ? [{
          title: "Suggested X chart",
          asset_type: "chart",
          url: forms.xForm.asset_url.trim(),
          thumbnail_url: forms.xForm.asset_url.trim(),
          suggested_caption: forms.xForm.asset_caption.trim(),
          source_data_notes: forms.xForm.source_types.join(", "),
        } satisfies AdminAiGrowthAsset]
      : [];
    return {
      campaign_type: "x_chart_drop",
      content_type: "x_post",
      source_platform: "X",
      title: `X Chart Drop: ${forms.xForm.ticker_theme || "Market tell"}`,
      ticker_theme: forms.xForm.ticker_theme,
      destination_url: forms.xForm.include_link ? forms.xForm.destination_url : "",
      tone: forms.xForm.tone,
      assets,
      inputs: {
        source_types: forms.xForm.source_types,
        timeframe: forms.xForm.timeframe,
        include_link: forms.xForm.include_link,
      },
      generate: true,
    };
  }
  if (kind === "influencer_packs") {
    return {
      campaign_type: "influencer_report_pack",
      content_type: "influencer_dm",
      source_platform: forms.influencerForm.platform,
      title: `Influencer Pack: ${forms.influencerForm.influencer || "Prospect"}`,
      ticker_theme: forms.influencerForm.tickers_themes,
      audience: forms.influencerForm.audience,
      inputs: {
        influencer: forms.influencerForm.influencer,
        report_type: forms.influencerForm.report_type,
        offer_type: forms.influencerForm.offer_type,
      },
      generate: true,
    };
  }
  if (kind === "reddit_threads") {
    return {
      campaign_type: "reddit_research_thread",
      content_type: "reddit_thread",
      source_platform: "Reddit",
      title: `Reddit Research Thread: ${forms.redditThreadForm.topic || forms.redditThreadForm.subreddit || "Draft"}`,
      ticker_theme: forms.redditThreadForm.topic,
      audience: forms.redditThreadForm.subreddit ? `r/${forms.redditThreadForm.subreddit.replace(/^r\//i, "")}` : "Reddit",
      inputs: {
        subreddit: forms.redditThreadForm.subreddit.replace(/^r\//i, ""),
        post_type: forms.redditThreadForm.post_type,
        disclosure_style: forms.redditThreadForm.disclosure_style,
        subreddit_rule_notes: forms.redditThreadForm.rule_notes,
      },
      generate: true,
    };
  }
  return {
    campaign_type: "reddit_paid_ad",
    content_type: "paid_ad",
    source_platform: "Reddit",
    title: `Reddit Paid Ad: ${forms.adForm.audience || "Audience"}`,
    destination_url: forms.adForm.landing_page,
    audience: forms.adForm.audience,
    tone: forms.adForm.tone,
    inputs: {
      offer: forms.adForm.offer,
      plan_focus: forms.adForm.plan_focus,
      pain_point: forms.adForm.pain_point,
      landing_page: forms.adForm.landing_page,
    },
    generate: true,
  };
}

function FormShell({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <h3 className="text-lg font-semibold text-white">{title}</h3>
      <div className="mt-4">{children}</div>
    </section>
  );
}

function TextField({ label, value, onChange, placeholder }: { label: string; value: string; onChange: (value: string) => void; placeholder?: string }) {
  return (
    <label className="block text-sm">
      <span className="block font-medium text-slate-200">{label}</span>
      <input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} className="mt-1 w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50" />
    </label>
  );
}

function TextareaField({ label, value, onChange, rows = 6 }: { label: string; value: string; onChange: (value: string) => void; rows?: number }) {
  return (
    <label className="block text-sm">
      <span className="block font-medium text-slate-200">{label}</span>
      <textarea value={value} onChange={(event) => onChange(event.target.value)} rows={rows} className="mt-1 w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50" />
    </label>
  );
}

function SelectField({ label, value, onChange, options }: { label: string; value: string; onChange: (value: string) => void; options: string[] }) {
  return (
    <label className="block text-sm">
      <span className="block font-medium text-slate-200">{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} className="mt-1 w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">
        {options.map((option) => <option key={option} value={option}>{option}</option>)}
      </select>
    </label>
  );
}

function SubmitButton({ busy, onClick, label, busyLabel }: { busy: boolean; onClick: () => void; label: string; busyLabel: string }) {
  return (
    <div className="mt-4">
      <button type="button" disabled={busy} onClick={onClick} className="rounded-md border border-emerald-300/30 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50">
        {busy ? busyLabel : label}
      </button>
    </div>
  );
}

function Button({ children, onClick, disabled }: { children: ReactNode; onClick: () => void; disabled?: boolean }) {
  return (
    <button type="button" disabled={disabled} onClick={onClick} className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50">
      {children}
    </button>
  );
}

function AssistLink({ href, label }: { href?: string | null; label: string }) {
  if (!href) return null;
  return <a href={href} target="_blank" rel="noreferrer" className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200">{label}</a>;
}

function AssetPreview({ asset, onCopy }: { asset: AdminAiGrowthAsset; onCopy: (label: string, value?: string | null) => void }) {
  const url = asset.url || asset.thumbnail_url || "";
  return (
    <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-semibold text-white">{asset.title || "Asset"}</p>
          <p className="text-xs uppercase tracking-wide text-slate-500">{asset.asset_type || "asset"}</p>
        </div>
        <AssistLink href={url} label="Open/download asset" />
      </div>
      {asset.thumbnail_url ? <img src={asset.thumbnail_url} alt={asset.title || "Asset thumbnail"} className="mt-3 max-h-44 w-full rounded-md object-cover" /> : null}
      {asset.suggested_caption ? <p className="mt-3 text-sm text-slate-300">{asset.suggested_caption}</p> : null}
      <div className="mt-3 flex flex-wrap gap-2">
        <Button onClick={() => onCopy("Copy image caption", asset.suggested_caption)}>Copy image caption</Button>
        <Button onClick={() => onCopy("Copy asset link", url)}>Copy asset link</Button>
      </div>
    </div>
  );
}

function SettingField({ item, settingKey }: { settingKey: string; item?: AdminAiMarketingSetting }) {
  const configured = Boolean(item?.configured);
  const source = item?.source ?? "missing";
  const badgeTone = source === "missing" ? "bad" : source === "server_env" || source === "default" ? "good" : "warn";
  const statusText =
    settingKey === "OPENAI_WEB_SEARCH_ENABLED"
      ? configured
        ? "Enabled"
        : "Disabled"
      : configured
        ? item?.source_label
        : "Missing";
  const helperText =
    settingKey === "AI_MARKETING_MODEL"
      ? "Set AI_MARKETING_MODEL on the backend to override the default model."
      : settingKey === "OPENAI_WEB_SEARCH_ENABLED"
        ? "Managed outside the admin UI with OPENAI_WEB_SEARCH_ENABLED=true."
        : "Managed outside the admin UI.";
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-sm font-semibold text-slate-100">{item?.label ?? settingKey}</p>
          <p className="mt-1 text-xs text-slate-500">{item?.required_for ?? "AI Growth"}</p>
        </div>
        <Badge label={item?.source_label ?? "Loading"} tone={badgeTone} />
      </div>
      <div className="mt-3 rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-300">{statusText}</div>
      <p className="mt-2 text-xs text-slate-500">{helperText}</p>
    </div>
  );
}

function ConnectionResult({ result }: { result: AdminAiMarketingSettingsTestResponse | null }) {
  if (!result) return null;
  return <span className={`text-sm font-medium ${result.ok ? "text-emerald-200" : "text-rose-200"}`}>{result.message}</span>;
}

function MetricCard({ label, value, tone = "muted" }: { label: string; value: string; tone?: "muted" | "good" | "warn" | "bad" }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</p>
      <p className={`mt-2 text-lg font-semibold ${toneClass(tone)}`}>{value}</p>
    </div>
  );
}

function Badge({ label, tone = "muted" }: { label: string; tone?: "muted" | "good" | "warn" | "bad" }) {
  const classes = {
    muted: "border-white/10 bg-slate-950/50 text-slate-300",
    good: "border-emerald-300/30 bg-emerald-300/10 text-emerald-100",
    warn: "border-amber-300/30 bg-amber-300/10 text-amber-100",
    bad: "border-rose-300/30 bg-rose-300/10 text-rose-100",
  };
  return <span className={`rounded-md border px-2 py-1 text-xs font-semibold ${classes[tone]}`}>{label}</span>;
}

function ScoreBadge({ label, value, invert = false }: { label: string; value?: number | null; invert?: boolean }) {
  if (typeof value !== "number") return <Badge label={`${label}: pending`} />;
  const good = invert ? value <= 30 : value >= 70;
  const bad = invert ? value >= 70 : value <= 40;
  return <Badge label={`${label}: ${value}`} tone={good ? "good" : bad ? "bad" : "warn"} />;
}

function toneClass(tone: "muted" | "good" | "warn" | "bad") {
  if (tone === "good") return "text-emerald-100";
  if (tone === "warn") return "text-amber-100";
  if (tone === "bad") return "text-rose-100";
  return "text-slate-100";
}

function statusTone(status: string): "muted" | "good" | "warn" | "bad" {
  if (["new", "draft", "needs_review"].includes(status)) return "warn";
  if (["approved", "copied", "emailed", "posted_manually"].includes(status)) return "good";
  if (["rejected", "dismissed"].includes(status)) return "bad";
  return "muted";
}

function platformLabel(platform?: string | null) {
  const normalized = String(platform || "other").toLowerCase();
  if (normalized === "x" || normalized === "x_stub") return "X";
  if (normalized === "facebook_manual") return "Facebook";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function contentTypeLabel(contentType?: string | null) {
  const labels: Record<string, string> = {
    x_post: "X post",
    reddit_reply: "Reddit reply",
    reddit_thread: "Reddit thread",
    paid_ad: "Paid ad",
    influencer_dm: "Influencer DM",
    report_pack: "Report pack",
  };
  return labels[String(contentType || "")] ?? "Draft";
}

function disclosureFromDraft(value: string) {
  return value.toLowerCase().includes("walnut") ? "Disclosure: I am building Walnut." : "";
}

function postingChecklist(draft: AdminAiMarketingOpportunity, fullDraft: string, disclosure: string, walnutLink: string) {
  const assetLines = (draft.assets ?? []).map((asset, index) => `${index + 1}. ${asset.title || "Asset"}: ${asset.url || asset.thumbnail_url || "no link"}`);
  return [
    "Open source post",
    draft.source_url,
    "Copy draft",
    fullDraft,
    "Paste into platform",
    "Attach image if relevant",
    ...assetLines,
    "Review disclosure",
    disclosure || "No Walnut mention detected.",
    "Walnut link",
    walnutLink || "none",
    "Post manually",
  ].filter(Boolean).join("\n");
}
