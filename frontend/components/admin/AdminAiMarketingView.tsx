"use client";

import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  analyzeAdminAiMarketingManualUrl,
  archiveAdminAiGrowthDraft,
  createAdminAiMarketingCampaign,
  createAdminAiGrowthDraft,
  deleteAdminAiMarketingCampaign,
  emailAdminAiGrowthDraft,
  getAdminAiGrowthDrafts,
  getAdminAiMarketingCampaigns,
  getAdminAiMarketingSettings,
  markAdminAiGrowthDraftCopied,
  markAdminAiGrowthDraftPosted,
  regenerateAdminAiGrowthDraft,
  rejectAdminAiGrowthDraft,
  runAdminAiMarketingCampaign,
  testAdminAiMarketingOpenAI,
  testAdminAiMarketingReddit,
  updateAdminAiGrowthDraftStatus,
  updateAdminAiMarketingCampaign,
  type AdminAiGrowthAsset,
  type AdminAiMarketingCampaign,
  type AdminAiMarketingCampaignPayload,
  type AdminAiMarketingConfig,
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
  | "assets"
  | "article_reactive_x"
  | "manual_x_draft"
  | "scheduled_x_campaigns"
  | "reddit_threads"
  | "settings";

type DraftAction = "archive" | "reject" | "delete";

const DRAFT_QUEUE_STATUSES: AdminAiMarketingStatus[] = [
  "new",
  "draft",
  "needs_review",
  "emailed",
  "opened",
  "copied",
  "approved",
  "posted_manually",
  "archived",
  "rejected",
  "regeneration_needed",
  "quality_failed",
];

const TABS: Array<{ key: TabKey; label: string }> = [
  { key: "dashboard", label: "Dashboard" },
  { key: "article_reactive_x", label: "Article-Reactive X" },
  { key: "manual_x_draft", label: "Manual X Draft" },
  { key: "scheduled_x_campaigns", label: "Scheduled X Campaigns" },
  { key: "reddit_threads", label: "Reddit Research Threads" },
  { key: "drafts", label: "Draft Queue" },
  { key: "assets", label: "Assets" },
  { key: "settings", label: "Settings" },
];

const STATUS_FILTERS: Array<{ value: "all" | AdminAiMarketingStatus; label: string }> = [
  { value: "all", label: "All" },
  { value: "new", label: "New" },
  { value: "needs_review", label: "Needs review" },
  { value: "approved", label: "Approved" },
  { value: "archived", label: "Archived" },
  { value: "rejected", label: "Denied" },
];

const SETTING_KEYS = [
  "OPENAI_API_KEY",
  "AI_MARKETING_MODEL",
  "OPENAI_WEB_SEARCH_ENABLED",
  "FMP_API_KEY",
  "X_CLIENT_ID",
  "X_CLIENT_SECRET",
  "X_REDIRECT_URI",
  "REDDIT_CLIENT_ID",
  "REDDIT_CLIENT_SECRET",
  "REDDIT_USER_AGENT",
] as const;

const SOURCE_PLATFORMS = ["X", "Reddit", "LinkedIn", "Other"] as const;
const SCHEDULED_X_SOURCE_TYPES = [
  "watchlist",
  "saved_screen",
  "saved_view",
  "bullish_confirmation",
  "bearish_confirmation",
  "signal_feed",
  "congress_activity",
  "insider_activity",
  "institutional_activity",
  "government_contracts",
  "ticker_context",
] as const;

const SCHEDULED_X_TEMPLATES = [
  { name: "Daily Watchlist Opportunities", source_type: "watchlist", schedule: "daily", tone: "market-native" },
  { name: "Daily Bullish Confirmation", source_type: "bullish_confirmation", schedule: "daily", tone: "sharp" },
  { name: "Daily Bearish Confirmation", source_type: "bearish_confirmation", schedule: "daily", tone: "sharp" },
  { name: "Daily Congress Activity Spotlight", source_type: "congress_activity", schedule: "daily", tone: "market-native" },
  { name: "Daily Insider Activity Spotlight", source_type: "insider_activity", schedule: "daily", tone: "market-native" },
  { name: "Daily Institutional Increases", source_type: "institutional_activity", schedule: "daily", tone: "professional" },
  { name: "Daily Government Contracts", source_type: "government_contracts", schedule: "daily", tone: "market-native" },
  { name: "Daily Signal Stack Names", source_type: "signal_feed", schedule: "daily", tone: "sharp" },
] as const;

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

function emptyArticleCampaignForm() {
  return {
    name: "Daily Article-Reactive X Campaign",
    status: "active",
    schedule: "weekdays",
    run_time: "07:35",
    timezone: "America/Los_Angeles",
    max_drafts_per_day: "1",
    recipient_email: "jarod@walnutmarkets.com",
    include_image_card: true,
    include_walnut_link: true,
    tone: "market-native",
    hashtag_mode: "ticker/theme only",
    cta_mode: "soft",
  };
}

function emptyRedditThreadForm() {
  return {
    subreddit: "",
    topic: "",
    post_type: "case study",
    disclosure_style: "founder disclosure",
    rule_notes: "",
    pasted_context: "",
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
  const [settingsTest, setSettingsTest] = useState<Record<"openai" | "reddit", AdminAiMarketingSettingsTestResponse | null>>({
    openai: null,
    reddit: null,
  });
  const [manualForm, setManualForm] = useState(() => emptyManualForm());
  const [articleForm, setArticleForm] = useState(() => emptyArticleCampaignForm());
  const [xForm, setXForm] = useState(() => emptyXForm());
  const [scheduledXForm, setScheduledXForm] = useState(() => emptyScheduledXCampaignForm());
  const [selectedScheduledXId, setSelectedScheduledXId] = useState<number | null>(null);
  const [scheduledXSavedSnapshot, setScheduledXSavedSnapshot] = useState<ReturnType<typeof emptyScheduledXCampaignForm> | null>(null);
  const [redditThreadForm, setRedditThreadForm] = useState(() => emptyRedditThreadForm());
  const [changeRequests, setChangeRequests] = useState<Record<number, string>>({});

  const pendingReviewCount = useMemo(
    () => drafts.filter((draft) => ["new", "draft", "needs_review", "emailed", "approved"].includes(draft.status)).length,
    [drafts],
  );
  const highFitCount = useMemo(() => drafts.filter((draft) => (draft.fit_score ?? draft.relevance_score ?? 0) >= 75).length, [drafts]);
  const assetCount = useMemo(() => drafts.reduce((total, draft) => total + (draft.assets?.length ?? 0), 0), [drafts]);
  const legacyCampaigns = campaigns.filter((campaign) => campaign.legacy);
  const scheduledXCampaigns = campaigns.filter((campaign) => campaign.campaign_type === "scheduled_x_campaign" || campaign.mode === "scheduled_x_campaign");
  const scheduledXUnsaved = selectedScheduledXId !== null && scheduledXSavedSnapshot !== null && JSON.stringify(scheduledXForm) !== JSON.stringify(scheduledXSavedSnapshot);

  const notify = (message: string, tone: "success" | "error" | "info" = "info") => {
    showToast?.(message, tone);
    setLoadStatus(message);
  };

  const load = async () => {
    setBusy("load");
    try {
      const draftStatus = statusFilter === "all" ? DRAFT_QUEUE_STATUSES.join(",") : statusFilter;
      const [draftData, settingsData, campaignData] = await Promise.all([
        getAdminAiGrowthDrafts({ status: draftStatus, limit: 100 }),
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

  const replaceDraft = (next: AdminAiMarketingOpportunity) => {
    setDrafts((current) => current.map((draft) => (draft.id === next.id ? next : draft)));
  };

  const removeDraft = (draftId: number) => {
    setDrafts((current) => current.filter((draft) => draft.id !== draftId));
  };

  const prependDraft = (next: AdminAiMarketingOpportunity) => {
    setDrafts((current) => [next, ...current.filter((draft) => draft.id !== next.id)]);
  };

  const copyText = async (_draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => {
    const text = value?.trim();
    if (!text) {
      notify(`${label} is empty.`, "error");
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
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
    action: DraftAction,
  ) => {
    setBusy(`${action}:${draft.id}`);
    try {
      if (action === "delete") {
        await updateAdminAiGrowthDraftStatus(draft.id, { status: "dismissed" });
        removeDraft(draft.id);
        notify("Draft deleted.", "success");
        return;
      }
      const updated = action === "archive" ? await archiveAdminAiGrowthDraft(draft.id) : await rejectAdminAiGrowthDraft(draft.id);
      replaceDraft(updated);
      notify(action === "archive" ? "Draft archived." : "Draft denied.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to update draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const updateChangeRequest = (draftId: number, value: string) => {
    setChangeRequests((current) => ({ ...current, [draftId]: value }));
  };

  const regenerateDraft = async (draft: AdminAiMarketingOpportunity) => {
    const changeRequest = changeRequests[draft.id]?.trim() ?? "";
    if (!changeRequest) {
      notify("Describe the changes to make before regenerating.", "error");
      return;
    }
    setBusy(`regenerate:${draft.id}`);
    try {
      const updated = await regenerateAdminAiGrowthDraft(draft.id, { change_request: changeRequest });
      replaceDraft(updated);
      setChangeRequests((current) => {
        const next = { ...current };
        delete next[draft.id];
        return next;
      });
      notify("Draft regenerated with requested changes.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to regenerate draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const emailDraft = async (draft: AdminAiMarketingOpportunity) => {
    setBusy(`email:${draft.id}`);
    try {
      await emailAdminAiGrowthDraft(draft.id);
      const updated = await updateAdminAiGrowthDraftStatus(draft.id, { status: "emailed" });
      replaceDraft(updated);
      notify("Draft emailed to Jarod.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to email draft.", "error");
    } finally {
      setBusy(null);
    }
  };

  const markDraftCopied = async (draft: AdminAiMarketingOpportunity) => {
    setBusy(`mark-copied:${draft.id}`);
    try {
      const updated = await markAdminAiGrowthDraftCopied(draft.id);
      replaceDraft(updated);
      notify("Draft marked copied.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to mark draft copied.", "error");
    } finally {
      setBusy(null);
    }
  };

  const markDraftPosted = async (draft: AdminAiMarketingOpportunity) => {
    setBusy(`mark-posted:${draft.id}`);
    try {
      const updated = await markAdminAiGrowthDraftPosted(draft.id);
      replaceDraft(updated);
      notify("Draft marked posted manually.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to mark draft posted manually.", "error");
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

  const scheduledXFormFromCampaign = (campaign: AdminAiMarketingCampaign): ReturnType<typeof emptyScheduledXCampaignForm> => {
    const preferences = campaign.output_preferences ?? {};
    const schedule = campaign.schedule_config ?? {};
    return {
      name: campaign.name ?? "",
      status: campaign.status ?? (campaign.enabled ? "active" : "paused"),
      schedule: String(schedule.cadence ?? (campaign.weekdays_only ? "weekdays" : "daily")),
      run_time: campaign.run_time ?? "07:45",
      timezone: campaign.timezone ?? "America/Los_Angeles",
      max_drafts_per_run: String(campaign.max_drafts_per_day ?? 1),
      recipient_email: campaign.recipient_email ?? "jarod@walnutmarkets.com",
      source_type: campaign.source_type ?? "watchlist",
      source_reference_id: campaign.source_reference_id ?? "",
      filters_json: JSON.stringify(campaign.filters ?? {}, null, 2),
      tone: String(preferences.tone ?? "market-native"),
      cta_mode: String(preferences.cta_mode ?? "soft"),
      hashtag_mode: String(preferences.hashtag_mode ?? "ticker/theme only"),
      include_image_card: preferences.include_image_card !== false,
      include_walnut_link: preferences.include_walnut_link !== false,
    };
  };

  const selectScheduledXCampaign = (campaign: AdminAiMarketingCampaign) => {
    const next = scheduledXFormFromCampaign(campaign);
    setSelectedScheduledXId(campaign.id);
    setScheduledXForm(next);
    setScheduledXSavedSnapshot(next);
  };

  const resetScheduledXCreateForm = () => {
    const next = emptyScheduledXCampaignForm();
    setSelectedScheduledXId(null);
    setScheduledXForm(next);
    setScheduledXSavedSnapshot(null);
  };

  const applyScheduledXTemplate = (template: (typeof SCHEDULED_X_TEMPLATES)[number]) => {
    setSelectedScheduledXId(null);
    setScheduledXForm({
      ...emptyScheduledXCampaignForm(),
      name: template.name,
      source_type: template.source_type,
      schedule: template.schedule,
      tone: template.tone,
    });
    setScheduledXSavedSnapshot(null);
  };

  const scheduledXPayload = (): AdminAiMarketingCampaignPayload => {
    let filters: Record<string, unknown> = {};
    try {
      filters = JSON.parse(scheduledXForm.filters_json || "{}");
    } catch {
      throw new Error("Filters JSON is invalid.");
    }
    const maxDrafts = Math.max(1, Math.min(10, Number(scheduledXForm.max_drafts_per_run) || 1));
    return {
      name: scheduledXForm.name.trim() || "Scheduled X Campaign",
      enabled: scheduledXForm.status === "active",
      status: scheduledXForm.status,
      mode: "scheduled_x_campaign",
      campaign_type: "scheduled_x_campaign",
      content_type: "x_post",
      schedule_config: { cadence: scheduledXForm.schedule },
      weekdays_only: scheduledXForm.schedule === "weekdays",
      run_time: scheduledXForm.run_time,
      timezone: scheduledXForm.timezone,
      recipient_email: scheduledXForm.recipient_email,
      source_type: scheduledXForm.source_type,
      source_reference_id: scheduledXForm.source_reference_id.trim() || null,
      filters,
      output_preferences: {
        tone: scheduledXForm.tone,
        cta_mode: scheduledXForm.cta_mode,
        hashtag_mode: scheduledXForm.hashtag_mode,
        include_image_card: scheduledXForm.include_image_card,
        include_walnut_link: scheduledXForm.include_walnut_link,
      },
      platforms: ["x"],
      keywords: [],
      tickers: [],
      subreddits: [],
      query_templates: [],
      minimum_relevance_score: 60,
      max_items_per_run: maxDrafts,
      max_drafts_per_day: maxDrafts,
      recency: "day",
      default_destination_page: scheduledXForm.include_walnut_link ? "https://walnutmarkets.com" : "",
      include_disclosure: true,
      scheduled_digest_enabled: true,
    };
  };

  const submitScheduledXCampaign = async () => {
    setBusy("scheduled_x_campaign");
    try {
      const payload = scheduledXPayload();
      const campaign = selectedScheduledXId
        ? await updateAdminAiMarketingCampaign(selectedScheduledXId, payload)
        : await createAdminAiMarketingCampaign(payload);
      setCampaigns((current) => [campaign, ...current.filter((item) => item.id !== campaign.id)]);
      const snapshot = scheduledXFormFromCampaign(campaign);
      setSelectedScheduledXId(campaign.id);
      setScheduledXForm(snapshot);
      setScheduledXSavedSnapshot(snapshot);
      notify(selectedScheduledXId ? "Scheduled X campaign updated." : "Scheduled X campaign created.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to save scheduled X campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const submitArticleCampaign = async () => {
    setBusy("article_campaign");
    try {
      const maxDrafts = Math.max(1, Math.min(2, Number(articleForm.max_drafts_per_day) || 1));
      const campaign = await createAdminAiMarketingCampaign({
        name: articleForm.name.trim() || "Article-Reactive X Campaign",
        enabled: articleForm.status === "active",
        status: articleForm.status,
        mode: "article_reactive_x",
        campaign_type: "article_reactive_x",
        content_type: "x_post",
        schedule_config: { cadence: "daily", weekdays_only: articleForm.schedule === "weekdays" },
        weekdays_only: articleForm.schedule === "weekdays",
        run_time: articleForm.run_time,
        timezone: articleForm.timezone,
        recipient_email: articleForm.recipient_email,
        source_type: "fmp_articles",
        source_reference_id: null,
        filters: {},
        output_preferences: {
          include_image_card: articleForm.include_image_card,
          include_walnut_link: articleForm.include_walnut_link,
          tone: articleForm.tone,
          hashtag_mode: articleForm.hashtag_mode,
          cta_mode: articleForm.cta_mode,
        },
        platforms: ["x"],
        keywords: [],
        tickers: [],
        subreddits: [],
        query_templates: [],
        minimum_relevance_score: 58,
        max_items_per_run: 20,
        max_drafts_per_day: maxDrafts,
        recency: "day",
        default_destination_page: "https://walnutmarkets.com",
        include_disclosure: true,
        scheduled_digest_enabled: false,
      });
      setCampaigns((current) => [campaign, ...current.filter((item) => item.id !== campaign.id)]);
      notify("Article-Reactive X campaign saved.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to save article campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const runCampaignNow = async (campaign: AdminAiMarketingCampaign) => {
    setBusy(`run-campaign:${campaign.id}`);
    try {
      const result = await runAdminAiMarketingCampaign(campaign.id);
      setDrafts((current) => [
        ...result.opportunities,
        ...current.filter((draft) => !result.opportunities.some((next) => next.id === draft.id)),
      ]);
      await load();
      notify(`Campaign run complete: ${result.drafts_generated ?? result.created ?? 0} drafts, ${result.emails_sent ?? 0} emails.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to run campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const setCampaignLifecycleStatus = async (campaign: AdminAiMarketingCampaign, status: "active" | "paused" | "stopped") => {
    setBusy(`campaign-status:${campaign.id}:${status}`);
    try {
      const updated = await updateAdminAiMarketingCampaign(campaign.id, {
        enabled: status === "active",
        status,
      });
      setCampaigns((current) => current.map((item) => (item.id === updated.id ? updated : item)));
      notify(`Campaign ${status === "active" ? "started" : status}.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to update campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const deleteCampaign = async (campaign: AdminAiMarketingCampaign) => {
    if (!window.confirm(`Delete ${campaign.name}? Existing drafts will stay in the queue.`)) return;
    setBusy(`delete-campaign:${campaign.id}`);
    try {
      await deleteAdminAiMarketingCampaign(campaign.id);
      setCampaigns((current) => current.filter((item) => item.id !== campaign.id));
      if (selectedScheduledXId === campaign.id) resetScheduledXCreateForm();
      notify("Campaign deleted.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to delete campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const submitGrowthDraft = async (kind: TabKey) => {
    setBusy(kind);
    try {
      const payload = buildGrowthPayload(kind, { xForm, redditThreadForm });
      const result = await createAdminAiGrowthDraft(payload);
      prependDraft(result.opportunity);
      if (kind === "manual_x_draft") setXForm(emptyXForm());
      if (kind === "reddit_threads") setRedditThreadForm(emptyRedditThreadForm());
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
          changeRequests={changeRequests}
          onStatusFilter={setStatusFilter}
          onCopy={copyText}
          onStatus={updateDraftStatus}
          onAction={runDraftAction}
          onEmail={emailDraft}
          onMarkCopied={markDraftCopied}
          onMarkPosted={markDraftPosted}
          onChangeRequest={updateChangeRequest}
          onRegenerate={regenerateDraft}
        />
      ) : null}

      {activeTab === "assets" ? <AssetsView drafts={drafts} /> : null}

      {activeTab === "article_reactive_x" ? (
        <ArticleReactiveCampaignsView
          form={articleForm}
          setForm={setArticleForm}
          campaigns={campaigns.filter((campaign) => campaign.campaign_type === "article_reactive_x" || campaign.mode === "article_reactive_x")}
          config={config}
          busy={busy}
          onSubmit={() => void submitArticleCampaign()}
          onRun={(campaign) => void runCampaignNow(campaign)}
          onSetStatus={(campaign, status) => void setCampaignLifecycleStatus(campaign, status)}
          onDelete={(campaign) => void deleteCampaign(campaign)}
        />
      ) : null}

      {activeTab === "manual_x_draft" ? (
        <XChartDropForm
          form={xForm}
          busy={busy}
          setForm={setXForm}
          onSubmit={() => void submitGrowthDraft("manual_x_draft")}
        />
      ) : null}

      {activeTab === "scheduled_x_campaigns" ? (
        <ScheduledXCampaignsView
          form={scheduledXForm}
          setForm={setScheduledXForm}
          campaigns={scheduledXCampaigns}
          selectedCampaignId={selectedScheduledXId}
          unsaved={scheduledXUnsaved}
          busy={busy}
          onCreateNew={resetScheduledXCreateForm}
          onTemplate={applyScheduledXTemplate}
          onSubmit={() => void submitScheduledXCampaign()}
          onEdit={selectScheduledXCampaign}
          onRun={(campaign) => void runCampaignNow(campaign)}
          onSetStatus={(campaign, status) => void setCampaignLifecycleStatus(campaign, status)}
          onDelete={(campaign) => void deleteCampaign(campaign)}
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
  const openAiCredits = openAiCreditsMetric(config);
  return (
    <section className="space-y-4">
      <div className="grid gap-3 md:grid-cols-3">
        <MetricCard label="OpenAI" value={config?.openai_configured ? "Configured" : "Missing"} tone={config?.openai_configured ? "good" : "bad"} />
        <MetricCard label="FMP Articles API" value={config?.fmp_articles_status === "configured" ? "Configured" : "Missing"} tone={config?.fmp_articles_status === "configured" ? "good" : "bad"} />
        <MetricCard label="OpenAI credits left" value={openAiCredits.value} tone={openAiCredits.tone} />
        <MetricCard label="X API" value={config?.x_status ?? "missing"} tone={config?.x_oauth_configured ? "good" : "warn"} />
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
  changeRequests,
  onStatusFilter,
  onCopy,
  onStatus,
  onAction,
  onEmail,
  onMarkCopied,
  onMarkPosted,
  onChangeRequest,
  onRegenerate,
}: {
  drafts: AdminAiMarketingOpportunity[];
  busy: string | null;
  statusFilter: "all" | AdminAiMarketingStatus;
  changeRequests: Record<number, string>;
  onStatusFilter: (status: "all" | AdminAiMarketingStatus) => void;
  onCopy: (draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => void;
  onStatus: (draft: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => void;
  onAction: (draft: AdminAiMarketingOpportunity, action: DraftAction) => void;
  onEmail: (draft: AdminAiMarketingOpportunity) => void;
  onMarkCopied: (draft: AdminAiMarketingOpportunity) => void;
  onMarkPosted: (draft: AdminAiMarketingOpportunity) => void;
  onChangeRequest: (draftId: number, value: string) => void;
  onRegenerate: (draft: AdminAiMarketingOpportunity) => void;
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

      <div className="mt-5 space-y-4">
        {busy === "load" ? (
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-5 text-sm text-slate-400">Loading AI Growth drafts...</div>
        ) : drafts.length ? (
          drafts.map((draft) => (
            <DraftCard
              key={draft.id}
              draft={draft}
              busy={busy}
              changeRequest={changeRequests[draft.id] ?? ""}
              onCopy={onCopy}
              onStatus={onStatus}
              onAction={onAction}
              onEmail={onEmail}
              onMarkCopied={onMarkCopied}
              onMarkPosted={onMarkPosted}
              onChangeRequest={onChangeRequest}
              onRegenerate={onRegenerate}
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
  changeRequest,
  onCopy,
  onStatus,
  onAction,
  onEmail,
  onMarkCopied,
  onMarkPosted,
  onChangeRequest,
  onRegenerate,
}: {
  draft: AdminAiMarketingOpportunity;
  busy: string | null;
  changeRequest: string;
  onCopy: (draft: AdminAiMarketingOpportunity, label: string, value?: string | null) => void;
  onStatus: (draft: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => void;
  onAction: (draft: AdminAiMarketingOpportunity, action: DraftAction) => void;
  onEmail: (draft: AdminAiMarketingOpportunity) => void;
  onMarkCopied: (draft: AdminAiMarketingOpportunity) => void;
  onMarkPosted: (draft: AdminAiMarketingOpportunity) => void;
  onChangeRequest: (draftId: number, value: string) => void;
  onRegenerate: (draft: AdminAiMarketingOpportunity) => void;
}) {
  const suggestion = draft.suggestion;
  const rawDraft = draft.full_markdown || draft.generated_content || suggestion?.suggested_post || suggestion?.suggested_reply || "";
  const fullDraft = draft.content_type === "x_post" ? formatXDraftForDisplay(rawDraft, draft) : rawDraft;
  const disclosure = suggestion?.disclosure_text || disclosureFromDraft(fullDraft);
  const walnutLink = suggestion?.suggested_destination_url || draft.suggested_destination_url || "";
  const sourceUrl = draft.source_url || "";
  const links = draft.posting_links ?? {};
  const sourceLink = links.open_source_post || sourceUrl || links.open_reddit_thread;
  const sourceLabel = sourceLinkLabel(draft);
  const xCharacterCount = draft.content_type === "x_post" ? fullDraft.length : null;
  const articleUrl = textFromUnknown(draft.metadata?.article_url) || sourceUrl;
  const alternateVersions = draft.alternate_versions ?? {};
  const shortVersion = textFromUnknown(alternateVersions.short_version);
  const directVersion = textFromUnknown(alternateVersions.more_direct_version) || textFromUnknown(alternateVersions.alternate_reply_more_direct);
  const hashtagCashtagBlock = textFromUnknown(alternateVersions.copy_hashtags_cashtags) || draft.matched_tickers.map((ticker) => `$${ticker}`).join(" ");
  const xComposeLink = links.open_x_compose;
  const openXLink = links.open_x || (draft.content_type === "x_post" ? "https://x.com/home" : "");

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
            {draft.content_type === "reddit_thread" ? <QualityBadges scores={draft.quality_scores} /> : null}
          </div>
          <h4 className="mt-3 text-base font-semibold text-white">{draft.title}</h4>
          <p className="mt-1 text-sm text-slate-400">{draft.ticker_theme || draft.community || draft.source_platform || "AI Growth draft"}</p>
          {draft.excerpt ? <p className="mt-3 line-clamp-3 text-sm text-slate-300">{draft.excerpt}</p> : null}
        </div>
        <div className="flex flex-wrap gap-2">
          <AssistLink href={sourceLink} label={sourceLabel} />
          <AssistLink href={articleUrl} label="Open article" />
          <AssistLink href={walnutLink} label="Open Walnut URL" />
          <AssistLink href={openXLink} label="Open X" />
          <AssistLink href={xComposeLink} label="Open X compose" />
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1.25fr)_minmax(0,0.75fr)]">
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-sm font-semibold text-white">Draft content</p>
            {xCharacterCount !== null ? (
              <span className={`text-xs font-semibold ${xCharacterCount <= 280 ? "text-emerald-100" : "text-rose-100"}`}>{xCharacterCount}/280</span>
            ) : null}
          </div>
          {suggestion?.recommended_action === "skip" ? <p className="mt-2 rounded-md border border-rose-300/30 bg-rose-300/10 p-2 text-sm font-semibold text-rose-100">Probably do not post.</p> : null}
          <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap text-sm leading-6 text-slate-200">{fullDraft || "No generated content yet."}</pre>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3 text-sm text-slate-300">
          <p><span className="font-semibold text-slate-100">Action:</span> {draft.recommended_action ?? suggestion?.recommended_action ?? "pending"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Content angle:</span> {suggestion?.content_angle || suggestion?.reply_angle || "pending"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Walnut link:</span> <span className="break-all">{walnutLink || "none"}</span></p>
          {draft.content_type === "x_post" ? null : <p className="mt-2"><span className="font-semibold text-slate-100">Disclosure:</span> {disclosure || "Review manually."}</p>}
          <p className="mt-2"><span className="font-semibold text-slate-100">Compliance:</span> {suggestion?.compliance_notes ?? draft.compliance_notes ?? "Human review required."}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Data points:</span> {suggestion?.value_added_insight || "Pending"}</p>
          {draft.source_notes?.length ? <p className="mt-2"><span className="font-semibold text-slate-100">Source notes:</span> {draft.source_notes.join("; ")}</p> : null}
          {draft.missing_data_notes?.length ? <p className="mt-2"><span className="font-semibold text-slate-100">Missing data:</span> {draft.missing_data_notes.join("; ")}</p> : null}
        </div>
      </div>

      {draft.assets?.length ? (
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {draft.assets.map((asset, index) => (
            <AssetPreview key={`${asset.url ?? asset.thumbnail_url ?? index}`} asset={asset} />
          ))}
        </div>
      ) : null}

      <div className="mt-4 flex flex-wrap gap-2">
        <Button onClick={() => onCopy(draft, "Primary post", fullDraft)} disabled={Boolean(busy)}>Copy primary post</Button>
        <Button onClick={() => onCopy(draft, "Short version", shortVersion)} disabled={Boolean(busy)}>Copy short version</Button>
        <Button onClick={() => onCopy(draft, "Direct version", directVersion)} disabled={Boolean(busy)}>Copy direct version</Button>
        <Button onClick={() => onCopy(draft, "Hashtags/cashtags", hashtagCashtagBlock)} disabled={Boolean(busy)}>Copy hashtags/cashtags</Button>
        <Button onClick={() => onCopy(draft, "Walnut link", walnutLink)} disabled={Boolean(busy)}>Copy Walnut link</Button>
        <Button onClick={() => onCopy(draft, "Article URL", articleUrl)} disabled={Boolean(busy)}>Copy article URL</Button>
      </div>

      <div className="mt-3 flex flex-col gap-2 md:flex-row">
        <input
          value={changeRequest}
          onChange={(event) => onChangeRequest(draft.id, event.target.value)}
          placeholder="Shorter, sharper, add the TSM margin angle..."
          aria-label="Requested draft changes"
          className="min-w-0 flex-1 rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
        />
        <Button onClick={() => onRegenerate(draft)} disabled={Boolean(busy)}>
          {busy === `regenerate:${draft.id}` ? "Regenerating..." : "Regenerate"}
        </Button>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <Button onClick={() => onEmail(draft)} disabled={Boolean(busy)}>
          {busy === `email:${draft.id}` ? "Emailing..." : "Email to Jarod"}
        </Button>
        <Button onClick={() => onStatus(draft, "approved")} disabled={Boolean(busy)}>Approve</Button>
        <Button onClick={() => onMarkCopied(draft)} disabled={Boolean(busy)}>
          {busy === `mark-copied:${draft.id}` ? "Marking..." : "Mark copied"}
        </Button>
        <Button onClick={() => onMarkPosted(draft)} disabled={Boolean(busy)}>
          {busy === `mark-posted:${draft.id}` ? "Marking..." : "Mark posted manually"}
        </Button>
        <Button onClick={() => onAction(draft, "reject")} disabled={Boolean(busy)}>Reject</Button>
        <Button onClick={() => onAction(draft, "archive")} disabled={Boolean(busy)}>Archive</Button>
        <Button onClick={() => onAction(draft, "delete")} disabled={Boolean(busy)}>Delete</Button>
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
          <SelectField label="Desired output type" value={form.desired_output_type} onChange={(value) => setForm({ ...form, desired_output_type: value })} options={["reply", "X post", "Reddit research thread", "paid ad copy"]} />
          <TextField label="Destination URL optional" value={form.destination_url} onChange={(value) => setForm({ ...form, destination_url: value })} placeholder="https://walnutmarkets.com/ticker/NVDA" />
        </div>
        <TextareaField label="Pasted post/comment/thread text" value={form.text} onChange={(value) => setForm({ ...form, text: value })} rows={11} />
      </div>
      <SubmitButton busy={busy === "manual"} onClick={onSubmit} label="Create draft" busyLabel="Creating..." />
    </FormShell>
  );
}

function ArticleReactiveCampaignsView({
  form,
  setForm,
  campaigns,
  config,
  busy,
  onSubmit,
  onRun,
  onSetStatus,
  onDelete,
}: {
  form: ReturnType<typeof emptyArticleCampaignForm>;
  setForm: (value: ReturnType<typeof emptyArticleCampaignForm>) => void;
  campaigns: AdminAiMarketingCampaign[];
  config: AdminAiMarketingConfig | null;
  busy: string | null;
  onSubmit: () => void;
  onRun: (campaign: AdminAiMarketingCampaign) => void;
  onSetStatus: (campaign: AdminAiMarketingCampaign, status: "active" | "paused" | "stopped") => void;
  onDelete: (campaign: AdminAiMarketingCampaign) => void;
}) {
  return (
    <section className="space-y-4">
      <div className="grid gap-3 md:grid-cols-3">
        <MetricCard label="FMP Articles API" value={config?.fmp_articles_status === "configured" ? "Configured" : "Missing"} tone={config?.fmp_articles_status === "configured" ? "good" : "bad"} />
        <MetricCard label="Source provider" value="FMP Articles" />
        <MetricCard label="Secrets" value="Managed outside admin UI" tone="good" />
      </div>

      <FormShell title="Article-Reactive X Campaigns">
        <div className="grid gap-4 md:grid-cols-2">
          <TextField label="Name" value={form.name} onChange={(value) => setForm({ ...form, name: value })} />
          <SelectField label="Status" value={form.status} onChange={(value) => setForm({ ...form, status: value })} options={["active", "paused"]} />
          <SelectField label="Schedule" value={form.schedule} onChange={(value) => setForm({ ...form, schedule: value })} options={["daily", "weekdays"]} />
          <TextField label="Run time" value={form.run_time} onChange={(value) => setForm({ ...form, run_time: value })} placeholder="07:35" />
          <TextField label="Timezone" value={form.timezone} onChange={(value) => setForm({ ...form, timezone: value })} />
          <SelectField label="Max drafts per day" value={form.max_drafts_per_day} onChange={(value) => setForm({ ...form, max_drafts_per_day: value })} options={["1", "2"]} />
          <TextField label="Recipient email" value={form.recipient_email} onChange={(value) => setForm({ ...form, recipient_email: value })} />
          <SelectField label="Tone" value={form.tone} onChange={(value) => setForm({ ...form, tone: value })} options={["professional", "sharp", "educational", "market-native"]} />
          <SelectField label="Hashtag mode" value={form.hashtag_mode} onChange={(value) => setForm({ ...form, hashtag_mode: value })} options={["none", "minimal", "ticker/theme only"]} />
          <SelectField label="CTA mode" value={form.cta_mode} onChange={(value) => setForm({ ...form, cta_mode: value })} options={["none", "soft", "direct"]} />
        </div>
        <div className="mt-4 flex flex-wrap gap-3">
          <label className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
            <input type="checkbox" checked={form.include_image_card} onChange={(event) => setForm({ ...form, include_image_card: event.target.checked })} />
            Include image/card
          </label>
          <label className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
            <input type="checkbox" checked={form.include_walnut_link} onChange={(event) => setForm({ ...form, include_walnut_link: event.target.checked })} />
            Include Walnut link
          </label>
        </div>
        <SubmitButton busy={busy === "article_campaign"} onClick={onSubmit} label="Save campaign" busyLabel="Saving..." />
      </FormShell>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h3 className="text-lg font-semibold text-white">Saved Article-Reactive X Campaigns</h3>
        <div className="mt-4 space-y-3">
          {campaigns.length ? campaigns.map((campaign) => (
            <div key={campaign.id} className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-white/10 bg-slate-950/40 p-3">
              <div>
                <p className="font-semibold text-slate-100">{campaign.name}</p>
                <p className="mt-1 text-sm text-slate-400">
                  {campaign.status ?? "active"} · {campaign.weekdays_only ? "weekdays only" : "daily"} · {campaign.run_time ?? "scheduled"} {campaign.timezone ?? ""}
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Badge label={`max ${campaign.max_drafts_per_day ?? 1}/day`} />
                <Button disabled={Boolean(busy) || campaign.status === "active"} onClick={() => onSetStatus(campaign, "active")}>
                  {busy === `campaign-status:${campaign.id}:active` ? "Starting..." : "Start"}
                </Button>
                <Button disabled={Boolean(busy) || campaign.status === "paused"} onClick={() => onSetStatus(campaign, "paused")}>
                  {busy === `campaign-status:${campaign.id}:paused` ? "Pausing..." : "Pause"}
                </Button>
                <Button disabled={Boolean(busy) || campaign.status === "stopped"} onClick={() => onSetStatus(campaign, "stopped")}>
                  {busy === `campaign-status:${campaign.id}:stopped` ? "Stopping..." : "Stop"}
                </Button>
                <Button disabled={Boolean(busy) || campaign.status !== "active"} onClick={() => onRun(campaign)}>
                  {busy === `run-campaign:${campaign.id}` ? "Running..." : "Run now"}
                </Button>
                <Button disabled={Boolean(busy)} onClick={() => onDelete(campaign)}>
                  {busy === `delete-campaign:${campaign.id}` ? "Deleting..." : "Delete"}
                </Button>
              </div>
            </div>
          )) : (
            <p className="text-sm text-slate-400">No Article-Reactive X campaigns saved yet.</p>
          )}
        </div>
      </section>
      <RunHistoryPanel title="Article-Reactive X run history" campaigns={campaigns} />
    </section>
  );
}

function ScheduledXCampaignsView({
  form,
  setForm,
  campaigns,
  selectedCampaignId,
  unsaved,
  busy,
  onCreateNew,
  onTemplate,
  onSubmit,
  onEdit,
  onRun,
  onSetStatus,
  onDelete,
}: {
  form: ReturnType<typeof emptyScheduledXCampaignForm>;
  setForm: (value: ReturnType<typeof emptyScheduledXCampaignForm>) => void;
  campaigns: AdminAiMarketingCampaign[];
  selectedCampaignId: number | null;
  unsaved: boolean;
  busy: string | null;
  onCreateNew: () => void;
  onTemplate: (template: (typeof SCHEDULED_X_TEMPLATES)[number]) => void;
  onSubmit: () => void;
  onEdit: (campaign: AdminAiMarketingCampaign) => void;
  onRun: (campaign: AdminAiMarketingCampaign) => void;
  onSetStatus: (campaign: AdminAiMarketingCampaign, status: "active" | "paused" | "stopped") => void;
  onDelete: (campaign: AdminAiMarketingCampaign) => void;
}) {
  const selected = campaigns.find((campaign) => campaign.id === selectedCampaignId);
  return (
    <section className="space-y-4">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="text-lg font-semibold text-white">Scheduled X Campaigns</h3>
            <p className="mt-1 text-sm text-slate-400">Walnut-native automation that generates reviewed X drafts only. No X or Reddit auto-posting.</p>
          </div>
          <Button disabled={Boolean(busy)} onClick={onCreateNew}>Create campaign</Button>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {SCHEDULED_X_TEMPLATES.map((template) => (
            <Button key={template.name} disabled={Boolean(busy)} onClick={() => onTemplate(template)}>{template.name}</Button>
          ))}
        </div>
      </section>

      <FormShell title={selected ? `Editing: ${selected.name}` : "Create Scheduled X Campaign"}>
        {unsaved ? <p className="mb-3 text-sm font-semibold text-amber-200">Unsaved changes.</p> : null}
        <div className="grid gap-4 md:grid-cols-2">
          <TextField label="Campaign name" value={form.name} onChange={(value) => setForm({ ...form, name: value })} />
          <SelectField label="Campaign status" value={form.status} onChange={(value) => setForm({ ...form, status: value })} options={["active", "paused"]} />
          <SelectField label="Schedule" value={form.schedule} onChange={(value) => setForm({ ...form, schedule: value })} options={["daily", "weekdays", "weekly"]} />
          <TextField label="Run time" value={form.run_time} onChange={(value) => setForm({ ...form, run_time: value })} placeholder="07:45" />
          <TextField label="Timezone" value={form.timezone} onChange={(value) => setForm({ ...form, timezone: value })} />
          <SelectField label="Max drafts per run" value={form.max_drafts_per_run} onChange={(value) => setForm({ ...form, max_drafts_per_run: value })} options={["1", "2", "3", "4", "5"]} />
          <TextField label="Recipient email" value={form.recipient_email} onChange={(value) => setForm({ ...form, recipient_email: value })} />
          <SelectField label="Source type" value={form.source_type} onChange={(value) => setForm({ ...form, source_type: value })} options={[...SCHEDULED_X_SOURCE_TYPES]} />
          <TextField label="Source selector" value={form.source_reference_id} onChange={(value) => setForm({ ...form, source_reference_id: value })} placeholder={sourceSelectorPlaceholder(form.source_type)} />
          <SelectField label="Tone" value={form.tone} onChange={(value) => setForm({ ...form, tone: value })} options={["market-native", "sharp", "educational", "contrarian", "professional"]} />
          <SelectField label="CTA mode" value={form.cta_mode} onChange={(value) => setForm({ ...form, cta_mode: value })} options={["none", "soft", "direct"]} />
          <SelectField label="Hashtag mode" value={form.hashtag_mode} onChange={(value) => setForm({ ...form, hashtag_mode: value })} options={["none", "minimal", "ticker/theme only"]} />
        </div>
        <TextareaField label="Filters JSON / preferences" value={form.filters_json} onChange={(value) => setForm({ ...form, filters_json: value })} rows={6} />
        <div className="mt-4 flex flex-wrap gap-3">
          <label className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
            <input type="checkbox" checked={form.include_image_card} onChange={(event) => setForm({ ...form, include_image_card: event.target.checked })} />
            Include image/card
          </label>
          <label className="flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-200">
            <input type="checkbox" checked={form.include_walnut_link} onChange={(event) => setForm({ ...form, include_walnut_link: event.target.checked })} />
            Include Walnut link
          </label>
        </div>
        <SubmitButton busy={busy === "scheduled_x_campaign"} onClick={onSubmit} label={selected ? "Save changes" : "Create campaign"} busyLabel="Saving..." />
      </FormShell>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h3 className="text-lg font-semibold text-white">Saved Scheduled X Campaigns</h3>
        <div className="mt-4 space-y-3">
          {campaigns.length ? campaigns.map((campaign) => (
            <div key={campaign.id} className="rounded-md border border-white/10 bg-slate-950/40 p-3">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="font-semibold text-slate-100">{campaign.name}</p>
                  <p className="mt-1 text-sm text-slate-400">{campaign.source_type ?? "watchlist"} - {campaign.status ?? "active"} - {campaignScheduleLabel(campaign)}</p>
                  <p className="mt-1 text-xs text-slate-500">Next run: {formatDateTime(campaign.next_run_at)} - Last run: {formatDateTime(campaign.last_run_at)} - Last status: {campaign.last_status ?? "none"}</p>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge label={`max ${campaign.max_drafts_per_day ?? 1}/day`} />
                  <Button disabled={Boolean(busy) || campaign.status === "active"} onClick={() => onSetStatus(campaign, "active")}>Start</Button>
                  <Button disabled={Boolean(busy) || campaign.status === "paused"} onClick={() => onSetStatus(campaign, "paused")}>Pause</Button>
                  <Button disabled={Boolean(busy) || campaign.status === "stopped"} onClick={() => onSetStatus(campaign, "stopped")}>Stop</Button>
                  <Button disabled={Boolean(busy) || campaign.status !== "active"} onClick={() => onRun(campaign)}>{busy === `run-campaign:${campaign.id}` ? "Running..." : "Run now"}</Button>
                  <Button disabled={Boolean(busy)} onClick={() => onEdit(campaign)}>Edit</Button>
                  <Button disabled={Boolean(busy)} onClick={() => onDelete(campaign)}>{busy === `delete-campaign:${campaign.id}` ? "Deleting..." : "Delete"}</Button>
                </div>
              </div>
            </div>
          )) : (
            <p className="text-sm text-slate-400">No Scheduled X campaigns saved yet.</p>
          )}
        </div>
      </section>

      <RunHistoryPanel title="Scheduled X Campaigns run history" campaigns={campaigns} />
    </section>
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
    <FormShell title="Manual X Draft">
      <p className="mb-4 text-sm text-slate-400">One-off manual X draft generation. This does not create, save, schedule, or run a campaign.</p>
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
      <SubmitButton busy={busy === "manual_x_draft"} onClick={onSubmit} label="Generate X draft" busyLabel="Generating..." />
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
      <TextareaField label="Manual pasted URL/text/context optional" value={form.pasted_context} onChange={(value) => setForm({ ...form, pasted_context: value })} rows={6} />
      <SubmitButton busy={busy === "reddit_threads"} onClick={onSubmit} label="Generate Reddit thread draft" busyLabel="Generating..." />
    </FormShell>
  );
}

function AssetsView({
  drafts,
}: {
  drafts: AdminAiMarketingOpportunity[];
}) {
  const assetDrafts = drafts.filter((draft) => (draft.assets?.length ?? 0) > 0);
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <h3 className="text-lg font-semibold text-white">Assets</h3>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        {assetDrafts.length ? assetDrafts.map((draft) => (
          <div key={draft.id} className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <p className="mb-3 text-sm font-semibold text-slate-100">{draft.title}</p>
            <div className="space-y-3">
              {(draft.assets ?? []).map((asset, index) => (
                <AssetPreview key={`${draft.id}-${index}-${asset.url ?? asset.thumbnail_url ?? asset.title}`} asset={asset} />
              ))}
            </div>
          </div>
        )) : (
          <p className="text-sm text-slate-400">No draft assets yet.</p>
        )}
      </div>
    </section>
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
        <MetricCard label="FMP Articles API" value={config?.fmp_articles_status === "configured" ? "Configured" : "Missing"} tone={config?.fmp_articles_status === "configured" ? "good" : "bad"} />
        <MetricCard label="AI model" value={config?.openai_model ?? "Default"} />
        <MetricCard label="OpenAI Web Search" value={config?.openai_web_search_status ?? "disabled"} tone={config?.openai_web_search_status === "enabled" ? "good" : "warn"} />
        <MetricCard label="X API" value={config?.x_status ?? "missing"} tone={config?.x_oauth_configured ? "good" : "warn"} />
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

function RunHistoryPanel({ title, campaigns }: { title: string; campaigns: AdminAiMarketingCampaign[] }) {
  const runs = campaigns.flatMap((campaign) => (campaign.recent_runs ?? []).map((run) => ({ ...run, campaignName: campaign.name })));
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <h3 className="text-lg font-semibold text-white">{title}</h3>
      <div className="mt-4 space-y-2">
        {runs.length ? runs.map((run) => (
          <div key={`${run.campaign_id}-${run.id}`} className="grid gap-2 rounded-md border border-white/10 bg-slate-950/40 p-3 text-sm text-slate-300 md:grid-cols-6">
            <span>{formatDateTime(run.run_at)}</span>
            <span>{run.status}</span>
            <span>{run.candidates_considered} considered</span>
            <span>{run.drafts_generated} drafts</span>
            <span>{run.emails_sent} emails</span>
            <span>{run.failure_reason || run.campaignName}</span>
          </div>
        )) : (
          <p className="text-sm text-slate-400">No recent campaign runs yet.</p>
        )}
      </div>
    </section>
  );
}

function sourceSelectorPlaceholder(sourceType: string) {
  if (sourceType === "watchlist") return "Watchlist name or ID";
  if (sourceType === "saved_screen") return "Saved screen name or ID";
  if (sourceType === "saved_view") return "Saved view name or ID";
  if (sourceType === "ticker_context") return "Ticker or comma-separated tickers";
  return "Optional selector";
}

function campaignScheduleLabel(campaign: AdminAiMarketingCampaign) {
  const cadence = String(campaign.schedule_config?.cadence ?? (campaign.weekdays_only ? "weekdays" : "daily"));
  return `${cadence} at ${campaign.run_time ?? "scheduled"} ${campaign.timezone ?? ""}`.trim();
}

function formatDateTime(value?: string | null) {
  if (!value) return "none";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function buildGrowthPayload(
  kind: TabKey,
  forms: {
    xForm: ReturnType<typeof emptyXForm>;
    redditThreadForm: ReturnType<typeof emptyRedditThreadForm>;
  },
) {
  if (kind === "manual_x_draft") {
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
      title: `X Campaign: ${forms.xForm.ticker_theme || "Market tell"}`,
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
        pasted_url_or_text: forms.redditThreadForm.pasted_context,
      },
      text: forms.redditThreadForm.pasted_context,
      generate: true,
    };
  }
  throw new Error("Unsupported AI Growth workflow.");
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

function AssetPreview({ asset }: { asset: AdminAiGrowthAsset }) {
  const url = isAssetFileUrl(asset.url) ? asset.url || "" : "";
  const imageUrl = isAssetImageUrl(asset.thumbnail_url) ? asset.thumbnail_url || "" : isAssetImageUrl(asset.url) ? asset.url || "" : "";
  return (
    <div className="rounded-lg border border-white/10 bg-slate-900/70 p-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-semibold text-white">{asset.title || "Asset"}</p>
          <p className="text-xs uppercase tracking-wide text-slate-500">{asset.asset_type || "asset"}</p>
        </div>
        <AssistLink href={url} label="Open/download asset" />
      </div>
      {imageUrl ? <img src={imageUrl} alt={asset.title || "Asset thumbnail"} className="mt-3 max-h-44 w-full rounded-md object-cover" /> : null}
      {asset.suggested_caption ? <p className="mt-3 text-sm text-slate-300">{asset.suggested_caption}</p> : null}
    </div>
  );
}

function isAssetFileUrl(value?: string | null) {
  const url = String(value || "").trim();
  if (!url) return false;
  const lower = url.split("?", 1)[0].toLowerCase();
  return lower.startsWith("data:image/") || lower.startsWith("blob:") || /\.(png|jpe?g|webp|gif|svg|pdf|csv)$/.test(lower);
}

function isAssetImageUrl(value?: string | null) {
  const url = String(value || "").trim();
  if (!url) return false;
  const lower = url.split("?", 1)[0].toLowerCase();
  return lower.startsWith("data:image/") || lower.startsWith("blob:") || /\.(png|jpe?g|webp|gif|svg)$/.test(lower);
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
        : settingKey === "FMP_API_KEY"
          ? "Managed outside the admin UI with FMP_API_KEY."
        : settingKey.startsWith("X_")
          ? "Managed outside the admin UI with Fly secrets for X OAuth status."
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

function QualityBadges({ scores }: { scores?: Record<string, number> }) {
  if (!scores) return null;
  return (
    <>
      <ScoreBadge label="Depth" value={scores.research_depth_score} />
      <ScoreBadge label="Evidence" value={scores.evidence_score} />
      <ScoreBadge label="Balance" value={scores.balance_score} />
      <ScoreBadge label="Promo risk" value={scores.promotional_risk_score} invert />
    </>
  );
}

function toneClass(tone: "muted" | "good" | "warn" | "bad") {
  if (tone === "good") return "text-emerald-100";
  if (tone === "warn") return "text-amber-100";
  if (tone === "bad") return "text-rose-100";
  return "text-slate-100";
}

function statusTone(status: string): "muted" | "good" | "warn" | "bad" {
  if (["new", "draft", "needs_review", "regeneration_needed", "quality_failed"].includes(status)) return "warn";
  if (["approved", "copied", "emailed", "posted_manually"].includes(status)) return "good";
  if (["rejected", "dismissed"].includes(status)) return "bad";
  return "muted";
}

function textFromUnknown(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

function openAiCreditsMetric(config: AdminAiMarketingConfig | null): { value: string; tone: "muted" | "good" | "warn" | "bad" } {
  const status = config?.openai_credits_status;
  if (status === "ok") {
    return { value: config?.openai_credits_label ?? "Configured", tone: "good" };
  }
  if (status === "low") {
    return { value: config?.openai_credits_label ?? "Low", tone: "warn" };
  }
  if (status === "missing") {
    return { value: config?.openai_credits_label ?? "OpenAI API key missing", tone: "warn" };
  }
  if (status === "unavailable") {
    return { value: config?.openai_credits_label ?? "OpenAI balance unavailable", tone: "warn" };
  }
  return { value: "Unavailable", tone: "warn" };
}

function emptyScheduledXCampaignForm() {
  return {
    name: "",
    status: "active",
    schedule: "daily",
    run_time: "07:45",
    timezone: "America/Los_Angeles",
    max_drafts_per_run: "1",
    recipient_email: "jarod@walnutmarkets.com",
    source_type: "watchlist",
    source_reference_id: "",
    filters_json: "{}",
    tone: "market-native",
    cta_mode: "soft",
    hashtag_mode: "ticker/theme only",
    include_image_card: true,
    include_walnut_link: true,
  };
}

function platformLabel(platform?: string | null) {
  const normalized = String(platform || "other").toLowerCase();
  if (normalized === "x" || normalized === "x_stub") return "X";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function sourceLinkLabel(draft: AdminAiMarketingOpportunity) {
  const platform = String(draft.source_platform ?? draft.platform ?? "").toLowerCase();
  const contentType = String(draft.content_type ?? "").toLowerCase();
  const sourceUrl = String(draft.source_url ?? "").toLowerCase();
  const isReddit = platform.includes("reddit") || contentType.startsWith("reddit") || sourceUrl.includes("reddit.com");
  if (!isReddit) return "Open source";
  return contentType === "reddit_reply" ? "Open Reddit comment" : "Open Reddit thread";
}

function formatXDraftForDisplay(value: string, draft: AdminAiMarketingOpportunity) {
  let cleaned = value.replace(/\s+/g, " ").trim();
  cleaned = cleaned
    .replace(/^(?:i'm|i am)\s+building\s+walnut,\s*so\s+obvious\s+bias,\s*but\s+/i, "")
    .replace(/^(?:bias disclosed:\s*)?(?:i'm|i am)\s+building\s+walnut(?:[^:.\n]*[:.])\s*/i, "")
    .replace(/^bias disclosed:\s*/i, "")
    .trim();
  if (!/#[A-Za-z0-9_]+/.test(cleaned)) {
    const tickers = (draft.matched_tickers?.length ? draft.matched_tickers : tickerTagsFromTheme(draft.ticker_theme)).slice(0, 2);
    const tags = [...new Set([...tickers.map((ticker) => `#${ticker.toUpperCase()}`), "#Markets"])];
    for (let count = tags.length; count > 0; count -= 1) {
      const suffix = ` ${tags.slice(0, count).join(" ")}`;
      const candidate = `${fitXText(cleaned, 280 - suffix.length)}${suffix}`;
      if (candidate.length <= 280) return candidate;
    }
  }
  return fitXText(cleaned, 280);
}

function tickerTagsFromTheme(value?: string | null) {
  return Array.from(String(value || "").matchAll(/\b[A-Z]{1,5}\b/g)).map((match) => match[0]);
}

function fitXText(value: string, limit: number) {
  const boundedLimit = Math.max(4, Math.min(limit, 280));
  if (value.length <= boundedLimit) return value;
  const suffix = "...";
  const body = value.slice(0, boundedLimit - suffix.length).trimEnd();
  const wordBoundary = body.includes(" ") ? body.replace(/\s+\S*$/, "").replace(/[ ,;:-]+$/, "") : body;
  return `${wordBoundary.length >= Math.min(160, boundedLimit - suffix.length) ? wordBoundary : body.replace(/[ ,;:-]+$/, "")}${suffix}`.slice(0, boundedLimit);
}

function contentTypeLabel(contentType?: string | null) {
  const labels: Record<string, string> = {
    x_post: "X post",
    reddit_reply: "Reddit reply",
    reddit_thread: "Reddit thread",
    paid_ad: "Paid ad",
  };
  return labels[String(contentType || "")] ?? "Draft";
}

function disclosureFromDraft(value: string) {
  return value.toLowerCase().includes("walnut") ? "Disclosure: I am building Walnut." : "";
}
