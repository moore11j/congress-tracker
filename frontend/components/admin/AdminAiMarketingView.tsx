"use client";

import { useEffect, useMemo, useState } from "react";
import {
  analyzeAdminAiMarketingManualUrl,
  createAdminAiMarketingCampaign,
  getAdminAiMarketingCampaigns,
  getAdminAiMarketingOpportunities,
  getAdminAiMarketingSettings,
  regenerateAdminAiMarketingSuggestion,
  runAdminAiMarketingCampaign,
  sendAdminAiMarketingEmailDigest,
  testAdminAiMarketingOpenAI,
  testAdminAiMarketingReddit,
  updateAdminAiMarketingCampaign,
  updateAdminAiMarketingOpportunity,
  type AdminAiMarketingCampaign,
  type AdminAiMarketingCampaignPayload,
  type AdminAiMarketingConfig,
  type AdminAiMarketingEmailDigestResponse,
  type AdminAiMarketingMode,
  type AdminAiMarketingOpportunity,
  type AdminAiMarketingPlatform,
  type AdminAiMarketingSetting,
  type AdminAiMarketingSettingsTestResponse,
  type AdminAiMarketingStatus,
} from "@/lib/api";

type AdminAiMarketingViewProps = {
  showToast?: (message: string, tone?: "success" | "error" | "info") => void;
};

type CampaignFormState = {
  id?: number;
  name: string;
  enabled: boolean;
  mode: AdminAiMarketingMode;
  platforms: AdminAiMarketingPlatform[];
  keywords: string;
  tickers: string;
  subreddits: string;
  minimum_relevance_score: number;
  max_items_per_run: number;
  default_destination_page: string;
  include_disclosure: boolean;
  scheduled_digest_enabled: boolean;
};

const MODE_OPTIONS: Array<{ value: AdminAiMarketingMode; label: string }> = [
  { value: "ticker_thread_assist", label: "Ticker thread assist" },
  { value: "congress_trade_angle", label: "Congress trade angle" },
  { value: "insider_buying_angle", label: "Insider buying angle" },
  { value: "unusual_signal_angle", label: "Unusual signal angle" },
  { value: "pain_point_tool_alternative", label: "Tool alternative" },
  { value: "manual_url_review", label: "Manual URL review" },
];

const PLATFORM_OPTIONS: Array<{ value: AdminAiMarketingPlatform; label: string }> = [
  { value: "reddit", label: "Reddit" },
  { value: "x_stub", label: "X stub" },
  { value: "facebook_manual", label: "Facebook manual" },
];

const STATUS_FILTERS: Array<{ value: "all" | AdminAiMarketingStatus; label: string }> = [
  { value: "new", label: "New" },
  { value: "emailed", label: "Emailed" },
  { value: "dismissed", label: "Dismissed" },
  { value: "copied", label: "Copied" },
  { value: "archived", label: "Archived" },
  { value: "all", label: "All" },
];

const SETTING_KEYS = [
  "OPENAI_API_KEY",
  "AI_MARKETING_MODEL",
  "REDDIT_CLIENT_ID",
  "REDDIT_CLIENT_SECRET",
  "REDDIT_USER_AGENT",
] as const;

function emptyForm(): CampaignFormState {
  return {
    name: "Reddit ticker replies",
    enabled: true,
    mode: "ticker_thread_assist",
    platforms: ["reddit"],
    keywords: "stock research\ninsider buying\ncongress trades",
    tickers: "",
    subreddits: "stocks\ninvesting\nwallstreetbets",
    minimum_relevance_score: 60,
    max_items_per_run: 10,
    default_destination_page: "https://walnutmarkets.com",
    include_disclosure: true,
    scheduled_digest_enabled: false,
  };
}

function formFromCampaign(campaign: AdminAiMarketingCampaign): CampaignFormState {
  return {
    id: campaign.id,
    name: campaign.name,
    enabled: campaign.enabled,
    mode: campaign.mode,
    platforms: campaign.platforms,
    keywords: campaign.keywords.join("\n"),
    tickers: campaign.tickers.join("\n"),
    subreddits: campaign.subreddits.join("\n"),
    minimum_relevance_score: campaign.minimum_relevance_score,
    max_items_per_run: campaign.max_items_per_run,
    default_destination_page: campaign.default_destination_page,
    include_disclosure: campaign.include_disclosure,
    scheduled_digest_enabled: campaign.scheduled_digest_enabled,
  };
}

function payloadFromForm(form: CampaignFormState): AdminAiMarketingCampaignPayload {
  return {
    name: form.name.trim(),
    enabled: form.enabled,
    mode: form.mode,
    platforms: form.platforms.length ? form.platforms : ["reddit"],
    keywords: splitList(form.keywords),
    tickers: splitList(form.tickers).map((ticker) => ticker.toUpperCase().replace(/^\$/, "")),
    subreddits: splitList(form.subreddits).map((subreddit) => subreddit.replace(/^r\//i, "")),
    minimum_relevance_score: form.minimum_relevance_score,
    max_items_per_run: form.max_items_per_run,
    default_destination_page: form.default_destination_page.trim() || "https://walnutmarkets.com",
    include_disclosure: form.include_disclosure,
    scheduled_digest_enabled: form.scheduled_digest_enabled,
  };
}

function splitList(value: string) {
  return value
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function AdminAiMarketingView({ showToast }: AdminAiMarketingViewProps) {
  const [campaigns, setCampaigns] = useState<AdminAiMarketingCampaign[]>([]);
  const [opportunities, setOpportunities] = useState<AdminAiMarketingOpportunity[]>([]);
  const [config, setConfig] = useState<AdminAiMarketingConfig | null>(null);
  const [settings, setSettings] = useState<AdminAiMarketingSetting[]>([]);
  const [settingsTest, setSettingsTest] = useState<Record<"openai" | "reddit", AdminAiMarketingSettingsTestResponse | null>>({
    openai: null,
    reddit: null,
  });
  const [form, setForm] = useState<CampaignFormState>(() => emptyForm());
  const [statusFilter, setStatusFilter] = useState<"all" | AdminAiMarketingStatus>("new");
  const [busy, setBusy] = useState<string | null>(null);
  const [loadStatus, setLoadStatus] = useState<string | null>(null);
  const [digestPreview, setDigestPreview] = useState<AdminAiMarketingEmailDigestResponse | null>(null);
  const [manualUrl, setManualUrl] = useState("");
  const [manualTitle, setManualTitle] = useState("");
  const [manualText, setManualText] = useState("");
  const [manualCampaignId, setManualCampaignId] = useState<number | "">("");

  const selectedCampaign = useMemo(
    () => campaigns.find((campaign) => campaign.id === form.id) ?? null,
    [campaigns, form.id],
  );

  const notify = (message: string, tone: "success" | "error" | "info" = "info") => {
    showToast?.(message, tone);
    setLoadStatus(message);
  };

  const load = async () => {
    setBusy("load");
    setLoadStatus(null);
    try {
      const [campaignData, opportunityData, settingsData] = await Promise.all([
        getAdminAiMarketingCampaigns(),
        getAdminAiMarketingOpportunities({ status: statusFilter === "all" ? "all" : statusFilter, limit: 80 }),
        getAdminAiMarketingSettings(),
      ]);
      setCampaigns(campaignData.items);
      setConfig(settingsData.config);
      setOpportunities(opportunityData.items);
      setSettings(settingsData.items);
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to load AI Outreach.", "error");
    } finally {
      setBusy(null);
    }
  };

  const refreshSettings = async () => {
    const data = await getAdminAiMarketingSettings();
    setSettings(data.items);
    setConfig(data.config);
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

  useEffect(() => {
    void load();
  }, [statusFilter]);

  const saveCampaign = async () => {
    setBusy("campaign");
    try {
      const payload = payloadFromForm(form);
      const saved = form.id
        ? await updateAdminAiMarketingCampaign(form.id, payload)
        : await createAdminAiMarketingCampaign(payload);
      setCampaigns((current) => {
        const exists = current.some((campaign) => campaign.id === saved.id);
        return exists
          ? current.map((campaign) => (campaign.id === saved.id ? saved : campaign))
          : [saved, ...current];
      });
      setForm(formFromCampaign(saved));
      notify("Campaign saved.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to save campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const runCampaign = async (campaign: AdminAiMarketingCampaign) => {
    setBusy(`run:${campaign.id}`);
    try {
      const result = await runAdminAiMarketingCampaign(campaign.id);
      if (result.opportunities.length) {
        mergeOpportunities(result.opportunities);
      }
      notify(
        `Run complete: ${result.created} new, ${result.deduped} existing, ${result.suggested} suggested.`,
        result.warnings.length ? "info" : "success",
      );
      if (result.warnings.length) setLoadStatus(result.warnings.join(" "));
      await refreshOpportunities();
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to run campaign.", "error");
    } finally {
      setBusy(null);
    }
  };

  const refreshOpportunities = async () => {
    const data = await getAdminAiMarketingOpportunities({ status: statusFilter === "all" ? "all" : statusFilter, limit: 80 });
    setOpportunities(data.items);
    setConfig(data.config);
  };

  const analyzeManualUrl = async () => {
    if (!manualUrl.trim() && !manualText.trim()) {
      notify("Paste a source URL or post/comment text first.", "error");
      return;
    }
    setBusy("manual");
    try {
      const result = await analyzeAdminAiMarketingManualUrl({
        url: manualUrl.trim() || null,
        title: manualTitle.trim() || null,
        text: manualText.trim() || null,
        campaign_id: typeof manualCampaignId === "number" ? manualCampaignId : null,
        generate: true,
      });
      mergeOpportunities([result.opportunity]);
      setManualUrl("");
      setManualTitle("");
      setManualText("");
      notify(result.warning || "Manual opportunity saved.", result.warning ? "info" : "success");
      await refreshOpportunities();
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to analyze manual URL.", "error");
    } finally {
      setBusy(null);
    }
  };

  const regenerateSuggestion = async (opportunity: AdminAiMarketingOpportunity) => {
    setBusy(`regen:${opportunity.id}`);
    try {
      const suggestion = await regenerateAdminAiMarketingSuggestion(opportunity.id);
      replaceOpportunity({
        ...opportunity,
        suggestion,
        relevance_score: suggestion.relevance_score,
        spam_risk_score: suggestion.spam_risk_score,
        suggested_destination_url: suggestion.suggested_destination_url,
        short_reason: suggestion.short_reason,
        compliance_notes: suggestion.compliance_notes,
      });
      notify("Suggestion regenerated.", "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to regenerate suggestion.", "error");
    } finally {
      setBusy(null);
    }
  };

  const updateStatus = async (opportunity: AdminAiMarketingOpportunity, status: AdminAiMarketingStatus) => {
    setBusy(`status:${opportunity.id}`);
    try {
      const updated = await updateAdminAiMarketingOpportunity(opportunity.id, { status });
      replaceOpportunity(updated);
      notify(`Opportunity marked ${status}.`, "success");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to update opportunity.", "error");
    } finally {
      setBusy(null);
    }
  };

  const copyReply = async (opportunity: AdminAiMarketingOpportunity) => {
    const reply = opportunity.suggestion?.suggested_reply;
    if (!reply) {
      notify("No suggested reply to copy yet.", "error");
      return;
    }
    try {
      await navigator.clipboard.writeText(reply);
      await updateStatus(opportunity, "copied");
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to copy reply.", "error");
    }
  };

  const previewDigest = async (send: boolean) => {
    setBusy(send ? "digest-send" : "digest-preview");
    try {
      const result = await sendAdminAiMarketingEmailDigest({
        send,
        statuses: statusFilter === "all" ? ["new"] : [statusFilter],
        limit: 25,
      });
      setDigestPreview(result);
      notify(send ? `Digest sent to ${config?.recipient ?? "admin"}.` : "Digest preview refreshed.", send ? "success" : "info");
      if (send) await refreshOpportunities();
    } catch (error) {
      notify(error instanceof Error ? error.message : "Unable to prepare digest.", "error");
    } finally {
      setBusy(null);
    }
  };

  const mergeOpportunities = (next: AdminAiMarketingOpportunity[]) => {
    setOpportunities((current) => {
      const byId = new Map(current.map((item) => [item.id, item]));
      next.forEach((item) => byId.set(item.id, item));
      return Array.from(byId.values()).sort((a, b) => (b.relevance_score ?? 0) - (a.relevance_score ?? 0));
    });
  };

  const replaceOpportunity = (next: AdminAiMarketingOpportunity) => {
    setOpportunities((current) => current.map((item) => (item.id === next.id ? next : item)));
  };

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-amber-300/20 bg-amber-300/10 p-4 text-sm text-amber-100">
        Use platform APIs and review each reply before posting. Avoid spam and always disclose affiliation when relevant.
      </section>

      {config?.warnings.length ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <h3 className="text-sm font-semibold uppercase tracking-wide text-emerald-300">Credential warnings</h3>
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {config.warnings.map((warning) => (
              <p key={warning} className="rounded-lg border border-white/10 bg-slate-950/50 p-3 text-sm text-slate-300">
                {warning}
              </p>
            ))}
          </div>
        </section>
      ) : null}

      {loadStatus ? (
        <p className="rounded-lg border border-white/10 bg-slate-950/50 p-3 text-sm text-slate-300">{loadStatus}</p>
      ) : null}

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-white">Settings</h3>
            <p className="mt-1 text-sm text-slate-400">
              Provider credentials are read from server environment variables and Fly secrets only.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              disabled={Boolean(busy)}
              onClick={() => void testConnection("openai")}
              className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
            >
              {busy === "settings-test:openai" ? "Testing..." : "Test OpenAI connection"}
            </button>
            <button
              type="button"
              disabled={Boolean(busy)}
              onClick={() => void testConnection("reddit")}
              className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
            >
              {busy === "settings-test:reddit" ? "Testing..." : "Test Reddit connection"}
            </button>
          </div>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {SETTING_KEYS.map((key) => {
            const item = settings.find((setting) => setting.key === key);
            return (
              <SettingField
                key={key}
                settingKey={key}
                item={item}
              />
            );
          })}
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <ConnectionResult result={settingsTest.openai} />
          <ConnectionResult result={settingsTest.reddit} />
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 className="text-lg font-semibold text-white">Campaigns</h3>
              <p className="mt-1 text-sm text-slate-400">Discovery rules and reply routing.</p>
            </div>
            <button
              type="button"
              onClick={() => setForm(emptyForm())}
              className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200"
            >
              New campaign
            </button>
          </div>

          <div className="mt-4 space-y-3">
            {campaigns.length ? (
              campaigns.map((campaign) => (
                <div key={campaign.id} className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <button
                      type="button"
                      onClick={() => setForm(formFromCampaign(campaign))}
                      className="text-left"
                    >
                      <span className="block font-semibold text-white">{campaign.name}</span>
                      <span className="mt-1 block text-xs text-slate-500">{campaign.mode.replaceAll("_", " ")}</span>
                    </button>
                    <div className="flex flex-wrap gap-2">
                      <Badge label={campaign.enabled ? "Enabled" : "Paused"} tone={campaign.enabled ? "good" : "muted"} />
                      {campaign.platforms.map((platform) => <Badge key={platform} label={platformLabel(platform)} />)}
                    </div>
                  </div>
                  <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
                    <span>{campaign.keywords.length} keywords · {campaign.tickers.length} tickers · max {campaign.max_items_per_run}</span>
                    <button
                      type="button"
                      disabled={Boolean(busy)}
                      onClick={() => void runCampaign(campaign)}
                      className="rounded-md border border-emerald-300/30 px-3 py-1.5 font-semibold text-emerald-100 disabled:opacity-50"
                    >
                      {busy === `run:${campaign.id}` ? "Running..." : "Run"}
                    </button>
                  </div>
                </div>
              ))
            ) : (
              <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">
                No campaigns yet. Create a Reddit campaign to start collecting outreach candidates.
              </p>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
          <h3 className="text-lg font-semibold text-white">{selectedCampaign ? "Edit campaign" : "Create campaign"}</h3>
          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <label className="text-sm md:col-span-2">
              <span className="block font-medium text-slate-200">Name</span>
              <input
                value={form.name}
                onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
              />
            </label>

            <label className="text-sm">
              <span className="block font-medium text-slate-200">Mode</span>
              <select
                value={form.mode}
                onChange={(event) => setForm((current) => ({ ...current, mode: event.target.value as AdminAiMarketingMode }))}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
              >
                {MODE_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>

            <div className="text-sm">
              <span className="block font-medium text-slate-200">Platforms</span>
              <div className="mt-2 flex flex-wrap gap-2">
                {PLATFORM_OPTIONS.map((option) => (
                  <label key={option.value} className="flex items-center gap-2 rounded-lg border border-white/10 bg-slate-950/50 px-3 py-2 text-slate-300">
                    <input
                      type="checkbox"
                      checked={form.platforms.includes(option.value)}
                      onChange={(event) => {
                        setForm((current) => ({
                          ...current,
                          platforms: event.target.checked
                            ? Array.from(new Set([...current.platforms, option.value]))
                            : current.platforms.filter((platform) => platform !== option.value),
                        }));
                      }}
                      className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                    />
                    {option.label}
                  </label>
                ))}
              </div>
            </div>

            <TextareaField label="Keywords" value={form.keywords} onChange={(value) => setForm((current) => ({ ...current, keywords: value }))} />
            <TextareaField label="Tickers" value={form.tickers} onChange={(value) => setForm((current) => ({ ...current, tickers: value }))} />
            <TextareaField label="Subreddits" value={form.subreddits} onChange={(value) => setForm((current) => ({ ...current, subreddits: value }))} />

            <label className="text-sm">
              <span className="block font-medium text-slate-200">Minimum relevance score</span>
              <input
                type="number"
                min={0}
                max={100}
                value={form.minimum_relevance_score}
                onChange={(event) => setForm((current) => ({ ...current, minimum_relevance_score: Number(event.target.value) }))}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
              />
            </label>

            <label className="text-sm">
              <span className="block font-medium text-slate-200">Max items per run</span>
              <input
                type="number"
                min={1}
                max={50}
                value={form.max_items_per_run}
                onChange={(event) => setForm((current) => ({ ...current, max_items_per_run: Number(event.target.value) }))}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
              />
            </label>

            <label className="text-sm md:col-span-2">
              <span className="block font-medium text-slate-200">Default destination page</span>
              <input
                value={form.default_destination_page}
                onChange={(event) => setForm((current) => ({ ...current, default_destination_page: event.target.value }))}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
              />
            </label>

            <label className="flex items-center gap-3 text-sm text-slate-300">
              <input
                type="checkbox"
                checked={form.enabled}
                onChange={(event) => setForm((current) => ({ ...current, enabled: event.target.checked }))}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Enabled
            </label>
            <label className="flex items-center gap-3 text-sm text-slate-300">
              <input
                type="checkbox"
                checked={form.include_disclosure}
                onChange={(event) => setForm((current) => ({ ...current, include_disclosure: event.target.checked }))}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Include affiliation disclosure
            </label>
            <label className="flex items-center gap-3 text-sm text-slate-300">
              <input
                type="checkbox"
                checked={form.scheduled_digest_enabled}
                onChange={(event) => setForm((current) => ({ ...current, scheduled_digest_enabled: event.target.checked }))}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Scheduled digest flag
            </label>
          </div>
          <div className="mt-5 flex flex-wrap gap-3">
            <button
              type="button"
              disabled={Boolean(busy)}
              onClick={() => void saveCampaign()}
              className="rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50"
            >
              {busy === "campaign" ? "Saving..." : "Save campaign"}
            </button>
            {form.id ? (
              <button
                type="button"
                onClick={() => setForm(emptyForm())}
                className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
              >
                Clear form
              </button>
            ) : null}
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-white">Manual URL analyzer</h3>
            <p className="mt-1 text-sm text-slate-400">Paste a source URL and optional thread text for a reviewed suggestion.</p>
          </div>
          <select
            value={manualCampaignId}
            onChange={(event) => setManualCampaignId(event.target.value ? Number(event.target.value) : "")}
            className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value="">No campaign</option>
            {campaigns.map((campaign) => (
              <option key={campaign.id} value={campaign.id}>{campaign.name}</option>
            ))}
          </select>
        </div>
        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.25fr)]">
          <div className="space-y-3">
            <label className="block text-sm">
              <span className="block font-medium text-slate-200">Source URL</span>
              <input
                value={manualUrl}
                onChange={(event) => setManualUrl(event.target.value)}
                placeholder="https://www.reddit.com/r/..."
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
              />
            </label>
            <label className="block text-sm">
              <span className="block font-medium text-slate-200">Title</span>
              <input
                value={manualTitle}
                onChange={(event) => setManualTitle(event.target.value)}
                placeholder="Optional title"
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
              />
            </label>
            <button
              type="button"
              disabled={Boolean(busy)}
              onClick={() => void analyzeManualUrl()}
              className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50"
            >
              {busy === "manual" ? "Analyzing..." : "Analyze source"}
            </button>
          </div>
          <label className="block text-sm">
            <span className="block font-medium text-slate-200">Post/comment text or thread excerpt</span>
            <textarea
              value={manualText}
              onChange={(event) => setManualText(event.target.value)}
              rows={6}
              placeholder="Paste the discussion text to generate a reviewed reply"
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
            />
          </label>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-white">Opportunities queue</h3>
            <p className="mt-1 text-sm text-slate-400">{opportunities.length} items in the current filter.</p>
          </div>
          <div className="flex flex-wrap gap-2">
            {STATUS_FILTERS.map((filter) => (
              <button
                key={filter.value}
                type="button"
                onClick={() => setStatusFilter(filter.value)}
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
          <button
            type="button"
            disabled={Boolean(busy)}
            onClick={() => void previewDigest(false)}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            {busy === "digest-preview" ? "Previewing..." : "Preview digest"}
          </button>
          <button
            type="button"
            disabled={Boolean(busy)}
            onClick={() => void previewDigest(true)}
            className="rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50"
          >
            {busy === "digest-send" ? "Sending..." : "Send digest"}
          </button>
        </div>

        {digestPreview ? (
          <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="font-semibold text-white">{digestPreview.subject ?? "AI Outreach digest"}</p>
                <p className="text-sm text-slate-400">{digestPreview.count} items · {digestPreview.to_email ?? config?.recipient}</p>
              </div>
              {digestPreview.email_log ? <Badge label={digestPreview.email_log.status} tone="good" /> : <Badge label="Preview" />}
            </div>
            {digestPreview.body_text ? (
              <pre className="mt-3 max-h-72 overflow-auto whitespace-pre-wrap rounded-lg bg-slate-950 p-3 text-xs leading-5 text-slate-300">
                {digestPreview.body_text}
              </pre>
            ) : null}
          </div>
        ) : null}

        <div className="mt-5 space-y-4">
          {busy === "load" ? (
            <div className="rounded-lg border border-white/10 bg-slate-950/40 p-5 text-sm text-slate-400">Loading AI Outreach queue...</div>
          ) : opportunities.length ? (
            opportunities.map((opportunity) => (
              <OpportunityRow
                key={opportunity.id}
                opportunity={opportunity}
                busy={busy}
                onCopy={() => void copyReply(opportunity)}
                onRegenerate={() => void regenerateSuggestion(opportunity)}
                onDismiss={() => void updateStatus(opportunity, "dismissed")}
                onArchive={() => void updateStatus(opportunity, "archived")}
              />
            ))
          ) : (
            <p className="rounded-lg border border-white/10 bg-slate-950/40 p-5 text-sm text-slate-400">
              No opportunities match this filter. Run a campaign or add a manual URL.
            </p>
          )}
        </div>
      </section>
    </div>
  );
}

function OpportunityRow({
  opportunity,
  busy,
  onCopy,
  onRegenerate,
  onDismiss,
  onArchive,
}: {
  opportunity: AdminAiMarketingOpportunity;
  busy: string | null;
  onCopy: () => void;
  onRegenerate: () => void;
  onDismiss: () => void;
  onArchive: () => void;
}) {
  const suggestion = opportunity.suggestion;
  const destination = suggestion?.suggested_destination_url || opportunity.suggested_destination_url;
  const hasSourceUrl = opportunity.metadata?.source_url_provided !== false;

  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap gap-2">
            <Badge label={platformLabel(opportunity.platform)} />
            <Badge label={opportunity.status} tone={statusTone(opportunity.status)} />
            <ScoreBadge label="Relevance" value={suggestion?.relevance_score ?? opportunity.relevance_score} />
            <ScoreBadge label="Spam" value={suggestion?.spam_risk_score ?? opportunity.spam_risk_score} invert />
          </div>
          <h4 className="mt-3 text-base font-semibold text-white">{opportunity.title}</h4>
          <p className="mt-1 text-sm text-slate-400">
            {opportunity.community ? `r/${opportunity.community}` : opportunity.platform} · {opportunity.author || "unknown author"}
            {typeof opportunity.comment_count === "number" ? ` · ${opportunity.comment_count} comments` : ""}
          </p>
          {opportunity.excerpt ? <p className="mt-3 line-clamp-3 text-sm text-slate-300">{opportunity.excerpt}</p> : null}
        </div>
        <div className="flex flex-wrap gap-2">
          {hasSourceUrl ? (
            <a href={opportunity.source_url} target="_blank" rel="noreferrer" className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200">
              Open thread
            </a>
          ) : null}
          {destination ? (
            <a href={destination} target="_blank" rel="noreferrer" className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200">
              Open Walnut page
            </a>
          ) : null}
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,0.7fr)]">
        <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
          <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
            <span className="text-sm font-semibold text-white">Suggested reply</span>
            {suggestion ? <span className="text-xs text-slate-500">{suggestion.model}</span> : null}
          </div>
          <pre className="max-h-64 overflow-auto whitespace-pre-wrap text-sm leading-6 text-slate-200">
            {suggestion?.suggested_reply ?? "No suggestion yet. Configure OPENAI_API_KEY or regenerate after setup."}
          </pre>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3 text-sm text-slate-300">
          <p><span className="font-semibold text-slate-100">Tickers:</span> {listOrNone(suggestion?.detected_tickers ?? opportunity.matched_tickers)}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Keywords:</span> {listOrNone(opportunity.matched_keywords)}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Reason:</span> {suggestion?.short_reason ?? opportunity.short_reason ?? "Pending"}</p>
          <p className="mt-2"><span className="font-semibold text-slate-100">Compliance:</span> {suggestion?.compliance_notes ?? opportunity.compliance_notes ?? "Review manually."}</p>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={Boolean(busy) || !suggestion}
          onClick={onCopy}
          className="rounded-md border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50"
        >
          Copy reply
        </button>
        <button
          type="button"
          disabled={Boolean(busy)}
          onClick={onRegenerate}
          className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
        >
          {busy === `regen:${opportunity.id}` ? "Regenerating..." : "Regenerate"}
        </button>
        <button
          type="button"
          disabled={Boolean(busy)}
          onClick={onDismiss}
          className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
        >
          Dismiss
        </button>
        <button
          type="button"
          disabled={Boolean(busy)}
          onClick={onArchive}
          className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
        >
          Archive
        </button>
      </div>
    </article>
  );
}

function SettingField({
  item,
  settingKey,
}: {
  settingKey: string;
  item?: AdminAiMarketingSetting;
}) {
  const label = item?.label ?? "Setting";
  const configured = Boolean(item?.configured);
  const source = item?.source ?? "missing";
  const sourceLabel = item?.source_label ?? "Loading";
  const badgeTone = source === "missing" ? "bad" : source === "server_env" ? "good" : "warn";

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <label className="block text-sm font-semibold text-slate-100">{label}</label>
          <p className="mt-1 text-xs text-slate-500">{item?.required_for ?? "AI Outreach"}</p>
        </div>
        <Badge
          label={sourceLabel}
          tone={badgeTone}
        />
      </div>
      <div className="mt-3 rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-300">
        {configured ? sourceLabel : "Missing"}
      </div>
      <p className="mt-2 text-xs text-slate-500">
        {settingKey === "AI_MARKETING_MODEL"
          ? "Set AI_MARKETING_MODEL on the backend to override the default model."
          : "Set this value as a backend environment variable or Fly secret."}
      </p>
    </div>
  );
}

function ConnectionResult({ result }: { result: AdminAiMarketingSettingsTestResponse | null }) {
  if (!result) return null;
  return (
    <span className={`text-sm font-medium ${result.ok ? "text-emerald-200" : "text-rose-200"}`}>
      {result.message}
    </span>
  );
}

function TextareaField({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <label className="text-sm">
      <span className="block font-medium text-slate-200">{label}</span>
      <textarea
        value={value}
        onChange={(event) => onChange(event.target.value)}
        rows={4}
        className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-white outline-none focus:border-emerald-300/50"
      />
    </label>
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

function statusTone(status: string): "muted" | "good" | "warn" | "bad" {
  if (status === "new") return "good";
  if (status === "emailed" || status === "copied") return "warn";
  if (status === "dismissed") return "bad";
  return "muted";
}

function platformLabel(platform: string) {
  if (platform === "x_stub") return "X stub";
  if (platform === "facebook_manual") return "Facebook manual";
  return platform.charAt(0).toUpperCase() + platform.slice(1);
}

function listOrNone(values?: string[] | null) {
  return values && values.length ? values.join(", ") : "none";
}
