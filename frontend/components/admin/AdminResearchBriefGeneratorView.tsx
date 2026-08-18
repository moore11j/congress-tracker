"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  approveScheduledAdminResearchBriefDraft,
  createAdminResearchCampaign,
  discoverAdminResearchKeywordOpportunities,
  deleteAdminResearchBriefDraft,
  deleteAdminResearchCampaign,
  getAdminResearchCampaigns,
  getAdminResearchKeywordOpportunities,
  getAdminResearchPublishingHealth,
  getAdminResearchBriefDraft,
  getAdminResearchBriefGenerationDraft,
  getAdminResearchBriefGenerationJob,
  getAdminResearchBriefDrafts,
  getAdminResearchBriefContext,
  getAdminResearchBriefOptions,
  publishAdminResearchBriefDraft,
  publishNowAdminResearchBriefDraft,
  recordProductEvent,
  refreshAdminResearchBriefSources,
  regenerateAdminResearchKeywordOpportunity,
  rejectAdminResearchBriefDraft,
  rescheduleAdminResearchCampaignItem,
  rescheduleAdminResearchBriefDraft,
  runAdminResearchCampaignItemNow,
  runAdminResearchCampaignNow,
  setAdminResearchCampaignActive,
  startAdminResearchBriefGeneration,
  unpublishAdminResearchBriefDraft,
  updateAdminResearchBriefDraft,
  updateAdminResearchCampaign,
  updateAdminResearchKeywordOpportunityStatus,
  validateAdminResearchBriefTicker,
  type AdminResearchBriefArticle,
  type AdminResearchBriefConfig,
  type AdminResearchBriefDraft,
  type AdminResearchBriefJob,
  type AdminResearchCampaign,
  type AdminResearchCampaignPayload,
  type AdminResearchCampaignTheme,
  type AdminResearchKeywordOpportunity,
  type AdminResearchPublishingHealth,
} from "@/lib/api";
import { normalizeTickerSymbol } from "@/lib/ticker";

type Toast = (message: string, tone?: "success" | "error" | "info") => void;
type ResearchBriefOptions = {
  angles: string[];
  time_horizons: string[];
  audiences: string[];
  judgment_preferences: string[];
  lengths: string[];
  tones: string[];
  external_research_modes: string[];
  section_formats: string[];
  model_options: string[];
  model_default: string;
  model_descriptions: Record<string, string>;
  model_labels: Record<string, string>;
  sections: string[];
  campaign_themes: AdminResearchCampaignTheme[];
};

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
  comparison_tickers: [],
  time_horizon: "Near term",
  intended_audience: "Walnut Research Brief",
  judgment_preference: "Let the data decide",
  additional_context: "",
  include_sections: DEFAULT_SECTIONS,
  length: "Standard: 1,500-2,500 words",
  tone: "Walnut market-native",
  external_research_mode: "Standard",
  section_format: "Walnut Research Brief",
  selected_model: "",
  include_charts: false,
  include_source_links: true,
  include_confirmation_score: false,
  include_cross_source_confirmations: false,
  premium_required: false,
  required_plan: null,
  generate_thumbnail: true,
  hero_image: "",
  manual_source_url: "",
};

const GENERATION_POLL_TIMEOUT_MS = 6 * 60 * 1000;
const WALNUT_CALL_VALUES = [
  "Very bullish",
  "Bullish",
  "Bullish with capex risk",
  "Bullish but expensive",
  "Neutral",
  "Neutral but expensive",
  "Neutral with capex risk",
  "Mixed with capex risk",
  "Mixed",
  "Bearish",
  "Very bearish",
  "Insufficient data to make a call",
];

const fallbackOptions: ResearchBriefOptions = {
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
  external_research_modes: ["Off", "Standard", "Deep"],
  section_formats: [
    "Walnut Research Brief",
    "Reddit DD - Issue / Risk / Data / Conclusion",
    "Reddit DD - Bull Case / Bear Case / The Data / The Call",
    "ValueInvesting - Business / Valuation / Risks / Margin of Safety",
    "X Thread",
    "Internal Analyst Note",
  ],
  model_options: ["gpt-5.6-luna", "gpt-5.6-terra"],
  model_default: "gpt-5.6-luna",
  model_descriptions: {
    "gpt-5.6-luna": "Fast / cheaper",
    "gpt-5.6-terra": "Balanced",
  },
  model_labels: {
    "gpt-5.6-luna": "GPT-5.6 Luna",
    "gpt-5.6-terra": "GPT-5.6 Terra",
  },
  sections: DEFAULT_SECTIONS,
  campaign_themes: [
    { key: "good_buy_now", label: "Good Buy Now", content_type: "ticker", intent: "Is [TICKER] a Good Stock to Buy Right Now?" },
    { key: "why_is_it_moving", label: "Why Is It Moving", content_type: "ticker", intent: "Why Is [TICKER] Stock Moving?" },
    { key: "insider_activity", label: "Insider Buying / Selling", content_type: "ticker", intent: "What does the latest insider activity mean for [TICKER]?" },
    { key: "insider_and_institutional_buying", label: "Insider and Institutional Buying", content_type: "non_ticker", intent: "Which stocks have both insider and institutional buying?" },
  ],
};

const DEFAULT_CAMPAIGN_FORM: AdminResearchCampaignPayload = {
  name: "",
  theme: "good_buy_now",
  content_type: "ticker",
  tickers: [],
  topic: "",
  cadence: "one_time",
  publish_start_at: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().slice(0, 16),
  publish_time: "",
  article_count: 1,
  window_days: 5,
  active: true,
  target_keywords: {},
  target_search_intents: {},
  secondary_keywords: [],
  search_intent: "Is [TICKER] a good stock to buy right now?",
};

function campaignToForm(campaign: AdminResearchCampaign): AdminResearchCampaignPayload {
  const config = campaign.config || {};
  const firstPublishAt = campaign.items?.[0]?.publish_at || config.publish_start_at;
  return {
    ...DEFAULT_CAMPAIGN_FORM,
    name: campaign.name,
    theme: campaign.theme,
    content_type: campaign.content_type === "non_ticker" ? "non_ticker" : "ticker",
    tickers: Array.isArray(config.tickers) ? config.tickers.map(String) : [],
    topic: typeof config.topic === "string" ? config.topic : "",
    cadence: campaign.cadence,
    publish_start_at: toDateTimeLocal(typeof firstPublishAt === "string" ? firstPublishAt : null),
    publish_time: typeof config.publish_time === "string" ? config.publish_time : "",
    article_count: Number(config.article_count || campaign.item_count || 1),
    window_days: Number(config.window_days || 1),
    active: campaign.active,
    target_keyword: typeof config.target_keyword === "string" ? config.target_keyword : "",
    secondary_keywords: Array.isArray(config.secondary_keywords) ? config.secondary_keywords.map(String) : [],
    search_intent: typeof config.search_intent === "string" ? config.search_intent : "",
    target_keywords: typeof config.target_keywords === "object" && config.target_keywords ? config.target_keywords as Record<string, string> : {},
    target_search_intents: typeof config.target_search_intents === "object" && config.target_search_intents ? config.target_search_intents as Record<string, string> : {},
    source_opportunity_ids: [],
  };
}

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

const PAYWALL_MARKER = "<!-- walnut:paywall -->";
const DEFAULT_PAYWALL_CTA_LABEL = "Subscribe to Premium";
const paywallMarkerPattern = /^\s*(?:<!--\s*walnut:paywall\s*-->|::walnut-paywall::|\[\[WALNUT_PAYWALL\]\])\s*$/im;
const paywallBlockPattern = /\n*\s*<paywall\b([^>]*)>\s*([\s\S]*?)\s*<\/paywall>\s*/i;
const buttonTagPattern = /<button\b([^>]*)\/?>/i;

function escapePseudoAttribute(value: string) {
  return value.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function unescapePseudoAttribute(value: string) {
  return value.replace(/&quot;/g, '"').replace(/&gt;/g, ">").replace(/&lt;/g, "<").replace(/&amp;/g, "&");
}

function pseudoAttributes(value: string) {
  const attributes: Record<string, string> = {};
  value.replace(/([a-zA-Z_][a-zA-Z0-9_-]*)="([^"]*)"/g, (_match, key: string, rawValue: string) => {
    attributes[key] = unescapePseudoAttribute(rawValue);
    return "";
  });
  return attributes;
}

function paywallBlockForArticle(article: AdminResearchBriefArticle) {
  if (!article.premium_required && !article.paywall_copy) return "";
  const heading = article.paywall_copy?.heading || "Unlock Walnut's Full Research Brief";
  const description = article.paywall_copy?.description || "See the full analysis, investment implications, and risks.";
  const ctaLabel = article.paywall_copy?.cta_label || DEFAULT_PAYWALL_CTA_LABEL;
  return [
    `<paywall heading="${escapePseudoAttribute(heading)}">`,
    description,
    `<button text="${escapePseudoAttribute(ctaLabel)}" link="premium_checkout" />`,
    "</paywall>",
  ].join("\n");
}

function parsePaywallBlock(markdown: string) {
  const match = markdown.match(paywallBlockPattern);
  if (!match) return { markdown, paywallCopy: null as AdminResearchBriefArticle["paywall_copy"] | null };
  const paywallAttributes = pseudoAttributes(match[1] || "");
  const body = match[2] || "";
  const buttonMatch = body.match(buttonTagPattern);
  const buttonAttributes = buttonMatch ? pseudoAttributes(buttonMatch[1] || "") : {};
  const description = body.replace(buttonTagPattern, "").trim();
  return {
    markdown: `${markdown.slice(0, match.index)}\n\n${markdown.slice((match.index || 0) + match[0].length)}`.trim(),
    paywallCopy: {
      heading: (paywallAttributes.heading || "").trim(),
      description,
      cta_label: (buttonAttributes.text || DEFAULT_PAYWALL_CTA_LABEL).trim(),
    },
  };
}

function articleToMarkdown(article: AdminResearchBriefArticle) {
  const sections = article.sections || [];
  const markdownSections = sections.map((section) => `## ${section.heading}\n\n${section.body_markdown}`);
  const rawMarkdown = markdownSections.join("\n\n");
  const paywallBlock = paywallBlockForArticle(article);
  const withPaywallBlock = (markdown: string) => (paywallBlock ? `${markdown.trimEnd()}\n\n${paywallBlock}`.trim() : markdown);
  if (paywallMarkerPattern.test(rawMarkdown)) return withPaywallBlock(rawMarkdown);
  const previewCount = typeof article.preview_section_count === "number" ? Math.max(0, Math.min(article.preview_section_count, markdownSections.length)) : null;
  if (previewCount === null) return withPaywallBlock(rawMarkdown);
  if (previewCount === 0) return withPaywallBlock(`${PAYWALL_MARKER}\n\n${rawMarkdown}`.trim());
  if (previewCount >= markdownSections.length) return withPaywallBlock(`${rawMarkdown}\n\n${PAYWALL_MARKER}`.trim());
  return withPaywallBlock([...markdownSections.slice(0, previewCount), PAYWALL_MARKER, ...markdownSections.slice(previewCount)].join("\n\n"));
}

function researchBriefJobTimedOut(job: AdminResearchBriefJob) {
  const timestamp = job.updated_at || job.started_at || job.created_at;
  if (!timestamp) return false;
  const startedAt = Date.parse(timestamp);
  return Number.isFinite(startedAt) && Date.now() - startedAt > GENERATION_POLL_TIMEOUT_MS;
}

function paywallMarkerSectionCount(markdown: string, sections: AdminResearchBriefArticle["sections"]) {
  if (!paywallMarkerPattern.test(markdown)) return null;
  for (let index = 0; index < sections.length; index += 1) {
    const body = sections[index]?.body_markdown || "";
    const match = body.match(paywallMarkerPattern);
    if (!match) continue;
    const beforeMarker = body.slice(0, match.index).trim();
    return beforeMarker ? index + 1 : index;
  }
  return sections.length;
}

function markdownToSections(markdown: string): { sections: AdminResearchBriefArticle["sections"]; previewSectionCount: number | null; paywallCopy: AdminResearchBriefArticle["paywall_copy"] | null } {
  const parsedPaywall = parsePaywallBlock(markdown);
  const editableMarkdown = parsedPaywall.markdown;
  const chunks = editableMarkdown.split(/\n(?=##\s+)/g).map((chunk) => chunk.trim()).filter(Boolean);
  if (!chunks.length) {
    const sections = [{ key: "body", heading: "Research Brief", body_markdown: editableMarkdown.trim() }];
    return { sections, previewSectionCount: paywallMarkerSectionCount(editableMarkdown, sections), paywallCopy: parsedPaywall.paywallCopy };
  }
  const sections = chunks.map((chunk, index) => {
    const match = chunk.match(/^##\s+(.+?)(?:\n+([\s\S]*))?$/);
    const heading = match?.[1]?.trim() || `Section ${index + 1}`;
    return {
      key: heading.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "") || `section_${index + 1}`,
      heading,
      body_markdown: (match?.[2] || chunk).trim(),
    };
  });
  return { sections, previewSectionCount: paywallMarkerSectionCount(editableMarkdown, sections), paywallCopy: parsedPaywall.paywallCopy };
}

function sectionKey(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
}

const optionalSectionHeadingKeys: Record<string, Set<string>> = {
  "Catalysts": new Set(["catalysts"]),
  "Risks": new Set(["risks"]),
  "What to watch next": new Set(["what_to_watch_next", "what_to_watch"]),
  "Final Walnut judgment": new Set(["final_walnut_judgment", "walnut_judgment", "the_call"]),
};

function applySectionSelections(article: AdminResearchBriefArticle, includeSections: string[]): AdminResearchBriefArticle {
  const included = new Set(includeSections);
  const excludedHeadingKeys = new Set<string>();
  Object.entries(optionalSectionHeadingKeys).forEach(([section, headingKeys]) => {
    if (included.has(section)) return;
    headingKeys.forEach((key) => excludedHeadingKeys.add(key));
  });

  const next: AdminResearchBriefArticle = {
    ...article,
    sections: (article.sections || []).filter((section) => !excludedHeadingKeys.has(sectionKey(section.heading || section.key || ""))),
  };

  if (!included.has("Catalysts")) next.catalysts = [];
  if (!included.has("Risks")) next.risks = [];
  if (!included.has("What to watch next")) next.watch_items = [];
  if (!included.has("Final Walnut judgment")) {
    next.judgment = "";
    next.suggested_card = {
      ...next.suggested_card,
      judgment: "",
    };
  }
  return next;
}

function parseComparisonTickers(value: string | string[] | null | undefined) {
  const values = Array.isArray(value) ? value : [value || ""];
  const seen = new Set<string>();
  const symbols: string[] = [];
  values.flatMap((item) => String(item || "").split(",")).forEach((item) => {
    const symbol = normalizeTickerSymbol(item);
    if (!symbol || seen.has(symbol)) return;
    symbols.push(symbol);
    seen.add(symbol);
  });
  return symbols;
}

function createClientRequestId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return crypto.randomUUID();
  return `rb_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

type ResearchBriefRequiredPlan = "free" | "premium" | "pro";

function articleRequiredPlan(article?: Pick<AdminResearchBriefArticle, "premium_required" | "required_plan"> | null): ResearchBriefRequiredPlan {
  if (!article?.premium_required) return "free";
  return article.required_plan === "pro" ? "pro" : "premium";
}

function configRequiredPlan(config: AdminResearchBriefConfig): ResearchBriefRequiredPlan {
  if (!config.premium_required) return "free";
  return config.required_plan === "pro" ? "pro" : "premium";
}

function accessPatchForPlan(plan: ResearchBriefRequiredPlan) {
  return {
    premium_required: plan !== "free",
    required_plan: plan === "free" ? null : plan,
  } as const;
}

function accessLabel(plan: ResearchBriefRequiredPlan) {
  if (plan === "pro") return "Pro only";
  if (plan === "premium") return "Premium/Pro";
  return "Free";
}

const PUBLISH_HARD_STOP_WARNING_CODES = new Set([
  "missing_title",
  "thin_body",
  "missing_disclaimer",
  "missing_source_links",
  "insufficient_source_links",
  "missing_official_earnings_source",
  "official_earnings_retrieval_failed",
  "missing_sec_or_ir_source",
  "official_source_link_omitted",
  "primary_ticker_context_mismatch",
  "duplicate_slug",
  "internal_wording",
  "internal_workflow_language",
  "unsupported_language",
  "markdown_structure",
]);

function isPublishHardStopWarning(warning: { code?: string; blocking?: boolean }) {
  return Boolean(warning.blocking && warning.code && PUBLISH_HARD_STOP_WARNING_CODES.has(warning.code));
}

function isThematicResearchDraft(draft: AdminResearchBriefDraft | null | undefined, article: AdminResearchBriefArticle | null | undefined) {
  const analytics = objectValue(article?.analytics);
  const context = objectValue(draft?.research_context);
  const config = objectValue(draft?.config);
  const text = [
    analytics.research_type,
    analytics.theme,
    context.research_type,
    context.theme,
    config.desired_angle,
    config.research_question,
    article?.category,
    article?.primary_ticker,
  ]
    .map((value) => String(value || "").toLowerCase())
    .join(" ");
  return ["thematic", "macro", "sector", "industry", "semiconductor_memory", "semiconductor memory", "memory shortage"].some((marker) =>
    text.includes(marker),
  );
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
  const [publishDialogOpen, setPublishDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteConfirmationText, setDeleteConfirmationText] = useState("");
  const [activeJob, setActiveJob] = useState<AdminResearchBriefJob | null>(null);
  const [comparisonTickerInput, setComparisonTickerInput] = useState("");
  const [comparisonTickerErrors, setComparisonTickerErrors] = useState<Record<string, string>>({});
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [activePane, setActivePane] = useState<"create" | "scheduled" | "drafts" | "published" | "campaigns" | "settings">("create");
  const [preflightReadiness, setPreflightReadiness] = useState<Record<string, unknown> | null>(null);
  const [campaigns, setCampaigns] = useState<AdminResearchCampaign[]>([]);
  const [keywordOpportunities, setKeywordOpportunities] = useState<AdminResearchKeywordOpportunity[]>([]);
  const [keywordMarketNote, setKeywordMarketNote] = useState("");
  const [selectedKeywordOpportunityIds, setSelectedKeywordOpportunityIds] = useState<string[]>([]);
  const [keywordOpportunityInstructions, setKeywordOpportunityInstructions] = useState<Record<string, string>>({});
  const [publishingHealth, setPublishingHealth] = useState<AdminResearchPublishingHealth | null>(null);
  const [campaignForm, setCampaignForm] = useState<AdminResearchCampaignPayload>(DEFAULT_CAMPAIGN_FORM);
  const [editingCampaign, setEditingCampaign] = useState<AdminResearchCampaign | null>(null);

  const selectedWarnings = selectedDraft?.validation?.warnings ?? [];
  const validationLabels = selectedDraft?.validation?.labels;
  const publishHardStopWarnings = selectedWarnings.filter(isPublishHardStopWarning);
  const selectedCard = articleDraft?.suggested_card;
  const comparisonTickers = config.comparison_tickers || [];
  const comparisonTickerLimitError = comparisonTickers.length > 5 ? "Comparison tickers are limited to 5 symbols." : "";
  const hasComparisonTickerErrors = Boolean(comparisonTickerLimitError || Object.keys(comparisonTickerErrors).length);
  const generationJobActive = activeJob?.status === "queued" || activeJob?.status === "running";
  const walnutCallInvalid = Boolean(
    articleDraft?.walnut_call && !WALNUT_CALL_VALUES.includes(articleDraft.walnut_call) && !isThematicResearchDraft(selectedDraft, articleDraft),
  );

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
          external_research_modes: payload.external_research_modes,
          section_formats: payload.section_formats,
          model_options: payload.model_options,
          model_default: payload.model_default,
          model_descriptions: payload.model_descriptions,
          model_labels: payload.model_labels,
          sections: payload.sections,
          campaign_themes: payload.campaign_themes?.length ? payload.campaign_themes : fallbackOptions.campaign_themes,
        });
        setConfig((current) => ({
          ...current,
          selected_model: current.selected_model || payload.model_options?.[payload.model_options.length - 1] || payload.model_default || "",
          include_sections: payload.sections.length ? payload.sections : current.include_sections,
        }));
      })
      .catch(() => undefined);
    getAdminResearchBriefDrafts().then((payload) => alive && setDrafts(payload.items)).catch(() => undefined);
    getAdminResearchCampaigns().then((payload) => alive && setCampaigns(payload.items)).catch(() => undefined);
    getAdminResearchKeywordOpportunities().then((payload) => alive && setKeywordOpportunities(payload.items)).catch(() => undefined);
    getAdminResearchPublishingHealth().then((payload) => alive && setPublishingHealth(payload)).catch(() => undefined);
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
    let alive = true;
    const primary = normalizeTickerSymbol(config.ticker);
    const errors: Record<string, string> = {};
    comparisonTickers.forEach((symbol) => {
      if (symbol === primary) errors[symbol] = "Primary ticker cannot appear in comparison tickers.";
    });
    setComparisonTickerErrors(errors);
    const toValidate = comparisonTickers.filter((symbol) => symbol !== primary);
    if (!toValidate.length) return () => {
      alive = false;
    };
    const handle = window.setTimeout(() => {
      Promise.all(
        toValidate.map((symbol) =>
          validateAdminResearchBriefTicker(symbol)
            .then(() => ({ symbol, error: "" }))
            .catch(() => ({ symbol, error: `${symbol} is not currently supported as a comparison ticker.` })),
        ),
      ).then((results) => {
        if (!alive) return;
        setComparisonTickerErrors((current) => {
          const next = { ...current };
          results.forEach((result) => {
            if (result.error) next[result.symbol] = result.error;
            else delete next[result.symbol];
          });
          return next;
        });
      });
    }, 300);
    return () => {
      alive = false;
      window.clearTimeout(handle);
    };
  }, [comparisonTickers, config.ticker]);

  useEffect(() => {
    if (!selectedDraft) {
      setArticleDraft(null);
      setBodyMarkdown("");
      return;
    }
    setArticleDraft(selectedDraft.article);
    setBodyMarkdown(articleToMarkdown(selectedDraft.article));
    const selectedConfig = { ...DEFAULT_CONFIG, ...(selectedDraft.config || {}) };
    setConfig(selectedConfig);
    setComparisonTickerInput((selectedConfig.comparison_tickers || []).join(", "));
  }, [selectedDraft]);

  useEffect(() => {
    if (!activeJob?.job_id || !generationJobActive) return;
    let alive = true;
    const poll = async () => {
      try {
        const job = await getAdminResearchBriefGenerationJob(activeJob.job_id);
        if (!alive) return;
        if ((job.status === "queued" || job.status === "running") && researchBriefJobTimedOut(job)) {
          const message = "Research brief generation timed out. Please start a fresh draft.";
          setActiveJob({ ...job, status: "failed", progress_step: "failed", progress_message: message, error_message_safe: message });
          setError(message);
          setBusy(null);
          showToast?.(message, "error");
          return;
        }
        if (job.status === "completed") {
          setError("");
          try {
            let draft = await getAdminResearchBriefGenerationDraft(job.job_id);
            if (!alive) return;
            setSelectedDraft(draft);
            await refreshDrafts(draft);
            setActiveJob(job);
            setBusy(null);
            showToast?.("Research brief draft generated.", "success");
          } catch {
            if (!alive) return;
            if (job.draft_id) {
              try {
                const draft = await getAdminResearchBriefDraft(job.draft_id);
                if (!alive) return;
                setSelectedDraft(draft);
                await refreshDrafts(draft);
                setActiveJob(job);
                setBusy(null);
                showToast?.("Research brief draft generated.", "success");
                return;
              } catch {
                // Fall through to the load-specific error below.
              }
            }
            setError("Research brief generated, but the draft could not be loaded yet. Open Drafts or try again in a moment.");
            setActiveJob(job);
            setBusy(null);
          }
        } else if (job.status === "failed") {
          setActiveJob(job);
          const message = job.error_message_safe || "Research brief generation failed. Try again or reduce research depth.";
          setError(message);
          setBusy(null);
          showToast?.(message, "error");
        } else {
          setActiveJob(job);
        }
      } catch {
        if (!alive) return;
        setError("Research brief generation failed. Try again or reduce research depth.");
        setBusy(null);
      }
    };
    const handle = window.setInterval(poll, 3000);
    void poll();
    return () => {
      alive = false;
      window.clearInterval(handle);
    };
  }, [activeJob?.job_id, generationJobActive, showToast]);

  const scheduledDrafts = useMemo(() => drafts.filter((draft) => ["scheduled_review", "approved_scheduled"].includes(draft.status)), [drafts]);
  // A campaign review is still a draft. Keep it in the Drafts workspace as well
  // as the Scheduled queue so the review email always has an obvious in-app home.
  const generatedDrafts = useMemo(() => drafts.filter((draft) => draft.status !== "published"), [drafts]);
  const publishedDrafts = useMemo(() => drafts.filter((draft) => draft.status === "published"), [drafts]);

  function updateConfig<K extends keyof AdminResearchBriefConfig>(key: K, value: AdminResearchBriefConfig[K]) {
    setConfig((current) => ({ ...current, [key]: value }));
  }

  function updateArticle<K extends keyof AdminResearchBriefArticle>(key: K, value: AdminResearchBriefArticle[K]) {
    setArticleDraft((current) => (current ? { ...current, [key]: value } : current));
  }

  function updateSuggestedCard(updates: Partial<AdminResearchBriefArticle["suggested_card"]>) {
    setArticleDraft((current) => {
      if (!current) return current;
      const currentCard = current.suggested_card || {};
      return {
        ...current,
        suggested_card: {
          title: currentCard.title || current.title || "",
          description: currentCard.description || current.preview_body || current.summary || "",
          judgment: currentCard.judgment || current.judgment || "mixed",
          tickers: currentCard.tickers?.length ? currentCard.tickers : [current.primary_ticker].filter(Boolean),
          ...updates,
        },
      };
    });
  }

  async function refreshDrafts(nextSelected?: AdminResearchBriefDraft) {
    const payload = await getAdminResearchBriefDrafts();
    if (nextSelected) {
      const seen = new Set<string>();
      const items = payload.items.map((draft) => {
        if (draft.id !== nextSelected.id) {
          seen.add(draft.id);
          return draft;
        }
        seen.add(nextSelected.id);
        return nextSelected;
      });
      if (!seen.has(nextSelected.id)) items.unshift(nextSelected);
      setDrafts(items);
      setSelectedDraft(nextSelected);
      return;
    }
    setDrafts(payload.items);
  }

  async function refreshCampaigns() {
    const payload = await getAdminResearchCampaigns();
    setCampaigns(payload.items);
  }

  async function refreshKeywordOpportunities() {
    const payload = await getAdminResearchKeywordOpportunities();
    setKeywordOpportunities(payload.items);
  }

  async function discoverKeywordOpportunities(maxCandidates = 5) {
    setBusy("keyword-discovery");
    try {
      const result = await discoverAdminResearchKeywordOpportunities({
        tickers: campaignForm.content_type === "ticker" ? campaignForm.tickers : [],
        seed_topics: [campaignForm.topic || "", campaignForm.search_intent || "", campaignForm.target_keyword || ""].filter(Boolean),
        theme: campaignForm.theme,
        max_candidates: maxCandidates,
      });
      setKeywordMarketNote(result.market_note || "");
      await refreshKeywordOpportunities();
      showToast?.(`${result.items.length} keyword opportunities saved for review.`, "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to discover keyword opportunities.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function useKeywordOpportunity(opportunity: AdminResearchKeywordOpportunity) {
    const contentType = opportunity.content_type === "ticker" && opportunity.ticker ? "ticker" : "non_ticker";
    setCampaignForm((current) => ({
      ...current,
      name: current.name || opportunity.target_keyword,
      theme: options.campaign_themes.some((theme) => theme.key === opportunity.recommended_theme) ? opportunity.recommended_theme : current.theme,
      content_type: contentType,
      tickers: contentType === "ticker" && opportunity.ticker ? Array.from(new Set([...current.tickers, opportunity.ticker])) : [],
      topic: contentType === "non_ticker" ? (opportunity.topic || opportunity.target_keyword) : current.topic,
      article_count: contentType === "ticker" && opportunity.ticker ? Array.from(new Set([...current.tickers, opportunity.ticker])).length : 1,
      target_keyword: (opportunity.target_keyword || "").slice(0, 240),
      secondary_keywords: (opportunity.secondary_keywords || []).map((keyword) => keyword.slice(0, 120)).slice(0, 12),
      search_intent: (opportunity.search_intent || opportunity.target_keyword || "").slice(0, 120),
      target_keywords: contentType === "ticker" && opportunity.ticker ? { ...current.target_keywords, [opportunity.ticker]: (opportunity.target_keyword || "").slice(0, 240) } : {},
      target_search_intents: contentType === "ticker" && opportunity.ticker ? { ...current.target_search_intents, [opportunity.ticker]: (opportunity.search_intent || opportunity.target_keyword || "").slice(0, 120) } : {},
    }));
    setSelectedKeywordOpportunityIds((current) => current.includes(opportunity.id) ? current : [...current, opportunity.id]);
    showToast?.("Opportunity added to the campaign plan. It will remain saved until you dismiss it or create the campaign.", "success");
  }

  async function dismissKeywordOpportunity(opportunity: AdminResearchKeywordOpportunity) {
    setBusy(`keyword-dismiss-${opportunity.id}`);
    try {
      await updateAdminResearchKeywordOpportunityStatus(opportunity.id, "dismissed");
      await refreshKeywordOpportunities();
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to dismiss keyword opportunity.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function regenerateKeywordOpportunity(opportunity: AdminResearchKeywordOpportunity) {
    setBusy(`keyword-regenerate-${opportunity.id}`);
    try {
      const revised = await regenerateAdminResearchKeywordOpportunity(opportunity.id, {
        instructions: keywordOpportunityInstructions[opportunity.id] || null,
      });
      setKeywordOpportunities((current) => current.map((item) => (item.id === revised.id ? revised : item)));
      setKeywordOpportunityInstructions((current) => ({ ...current, [opportunity.id]: "" }));
      if (selectedKeywordOpportunityIds.includes(revised.id)) {
        setCampaignForm((current) => {
          if (revised.content_type === "ticker" && revised.ticker) {
            return {
              ...current,
              target_keyword: revised.target_keyword.slice(0, 240),
              search_intent: revised.search_intent.slice(0, 120),
              secondary_keywords: revised.secondary_keywords.slice(0, 12),
              target_keywords: { ...current.target_keywords, [revised.ticker]: revised.target_keyword.slice(0, 240) },
              target_search_intents: { ...current.target_search_intents, [revised.ticker]: revised.search_intent.slice(0, 120) },
            };
          }
          return {
            ...current,
            topic: revised.topic || revised.target_keyword,
            target_keyword: revised.target_keyword.slice(0, 240),
            search_intent: revised.search_intent.slice(0, 120),
            secondary_keywords: revised.secondary_keywords.slice(0, 12),
          };
        });
      }
      showToast?.("Opportunity regenerated from current signals. Your campaign plan was updated if it was loaded.", "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to regenerate keyword opportunity.", "error");
    } finally {
      setBusy(null);
    }
  }

  function applySavedDraft(draft: AdminResearchBriefDraft) {
    setSelectedDraft(draft);
    setDrafts((current) => {
      const withoutDraft = current.filter((item) => item.id !== draft.id);
      return [draft, ...withoutDraft].sort((left, right) =>
        String(right.updated_at || right.published_at || "").localeCompare(String(left.updated_at || left.published_at || "")),
      );
    });
  }

  async function generateDraft() {
    if (generationJobActive) return;
    setBusy("generate");
    setError("");
    try {
      const generationConfig = currentEditedConfig();
      const preflight = await getAdminResearchBriefContext(generationConfig);
      const readiness = objectValue(objectValue(preflight.research_context).research_readiness);
      setPreflightReadiness(readiness);
      if (Boolean(readiness.required_for_earnings_setup) && String(readiness.status || "") === "not_ready") {
        const missing = Array.isArray(readiness.missing_requirements) ? readiness.missing_requirements.join(", ") : "required research";
        const message = `Research not ready: ${missing}. Refresh sources or add a verified primary source before generating.`;
        setError(message);
        showToast?.(message, "error");
        setBusy(null);
        return;
      }
      const job = await startAdminResearchBriefGeneration({
        ...generationConfig,
        client_request_id: createClientRequestId(),
      });
      setActiveJob(job);
      showToast?.("Research brief generation started.", "info");
    } catch (err) {
      const rawMessage = err instanceof Error ? err.message : "";
      const message = rawMessage.includes("ROUTER_EXTERNAL_TARGET_ERROR") ? "Research brief generation failed. Try again or reduce research depth." : rawMessage || "Unable to start research brief generation.";
      setError(message);
      showToast?.(message, "error");
      setBusy(null);
    }
  }

  async function saveDraft(status?: string) {
    if (!selectedDraft || !articleDraft) return;
    setBusy("save");
    setError("");
    try {
      const article = currentEditedArticle();
      if (!article) return;
      const draft = await updateAdminResearchBriefDraft(selectedDraft.id, { status, article, config: currentEditedConfig() });
      applySavedDraft(draft);
      showToast?.("Draft saved.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to save draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function refreshSources() {
    if (!selectedDraft) return;
    setBusy("refresh-sources");
    setError("");
    try {
      const draft = await refreshAdminResearchBriefSources(selectedDraft.id);
      applySavedDraft(draft);
      showToast?.("Source notes refreshed.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to refresh sources.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  function requestPublishSelected() {
    if (!selectedDraft) return;
    setPublishDialogOpen(true);
  }

  function closePublishDialog() {
    if (busy === "publish") return;
    setPublishDialogOpen(false);
  }

  async function publishSelected() {
    if (!selectedDraft || !articleDraft) return;
    setBusy("publish");
    try {
      const article = currentEditedArticle();
      if (!article) return;
      const savedDraft = await updateAdminResearchBriefDraft(selectedDraft.id, { article, config: currentEditedConfig() });
      const draft = await publishAdminResearchBriefDraft(savedDraft.id);
      applySavedDraft(draft);
      setPublishDialogOpen(false);
      setActivePane("published");
      showToast?.("Draft saved and published.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to publish draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function unpublishDraft(draftToUnpublish: AdminResearchBriefDraft) {
    if (!window.confirm("Unpublish this research brief from public Research Briefs?")) return;
    setBusy("unpublish");
    try {
      const draft = await unpublishAdminResearchBriefDraft(draftToUnpublish.id);
      applySavedDraft(draft);
      setActivePane("drafts");
      showToast?.("Article unpublished and retained as an editable draft.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to unpublish draft.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  function requestDeleteSelected() {
    if (!selectedDraft) return;
    setDeleteConfirmationText("");
    setDeleteDialogOpen(true);
  }

  async function publishNowSelected() {
    if (!selectedDraft || !articleDraft) return;
    setBusy("publish-now");
    try {
      const article = currentEditedArticle();
      if (!article) return;
      const savedDraft = await updateAdminResearchBriefDraft(selectedDraft.id, { article, config: currentEditedConfig() });
      const draft = await publishNowAdminResearchBriefDraft(savedDraft.id);
      applySavedDraft(draft);
      setActivePane("published");
      showToast?.("Scheduled brief saved and published.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to publish scheduled brief.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function approveScheduledSelected() {
    if (!selectedDraft) return;
    setBusy("approve-scheduled");
    try {
      const article = currentEditedArticle();
      const savedDraft = article ? await updateAdminResearchBriefDraft(selectedDraft.id, { article, config: currentEditedConfig() }) : selectedDraft;
      const draft = await approveScheduledAdminResearchBriefDraft(savedDraft.id);
      applySavedDraft(draft);
      await refreshCampaigns();
      showToast?.("Scheduled brief approved.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to approve scheduled brief.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function rejectSelected(correctionInstructions: string) {
    if (!selectedDraft) return;
    setBusy("reject");
    try {
      const draft = await rejectAdminResearchBriefDraft(selectedDraft.id, correctionInstructions);
      applySavedDraft(draft);
      await refreshCampaigns();
      showToast?.("Rejected draft replaced and sent for review.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to reject scheduled brief.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function rescheduleSelected(scheduledAt: string) {
    if (!selectedDraft) return;
    setBusy("reschedule");
    try {
      const draft = await rescheduleAdminResearchBriefDraft(selectedDraft.id, scheduledAt);
      applySavedDraft(draft);
      showToast?.("Scheduled time updated.", "success");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to reschedule brief.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  async function submitCampaign() {
    setBusy("campaign");
    setError("");
    try {
      const payload: AdminResearchCampaignPayload = {
        ...campaignForm,
        tickers: campaignForm.content_type === "ticker" ? campaignForm.tickers.map((ticker) => normalizeTickerSymbol(ticker)).filter((ticker): ticker is string => Boolean(ticker)) : [],
        article_count: campaignForm.content_type === "ticker" ? Math.max(1, campaignForm.tickers.length) : 1,
        target_keyword: (campaignForm.target_keyword || "").slice(0, 240),
        secondary_keywords: (campaignForm.secondary_keywords || []).map((keyword) => keyword.slice(0, 120)).slice(0, 12),
        search_intent: (campaignForm.search_intent || "").slice(0, 120),
        target_keywords: Object.fromEntries(Object.entries(campaignForm.target_keywords || {}).map(([ticker, keyword]) => [ticker, keyword.slice(0, 240)])),
        target_search_intents: Object.fromEntries(Object.entries(campaignForm.target_search_intents || {}).map(([ticker, intent]) => [ticker, intent.slice(0, 120)])),
        source_opportunity_ids: selectedKeywordOpportunityIds,
      };
      const campaign = editingCampaign
        ? await updateAdminResearchCampaign(editingCampaign.id, payload)
        : await createAdminResearchCampaign(payload);
      await refreshCampaigns();
      if (editingCampaign) {
        setEditingCampaign(campaign);
        setCampaignForm(campaignToForm(campaign));
        showToast?.("Campaign changes saved. Pending items were rescheduled from the updated plan.", "success");
      } else {
        await refreshKeywordOpportunities();
        setSelectedKeywordOpportunityIds([]);
        setCampaignForm((current) => ({ ...current, name: current.name || campaign.name }));
        showToast?.("Research campaign created.", "success");
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to create research campaign.";
      setError(message);
      showToast?.(message, "error");
    } finally {
      setBusy(null);
    }
  }

  function editCampaign(campaign: AdminResearchCampaign) {
    setEditingCampaign(campaign);
    setCampaignForm(campaignToForm(campaign));
    setActivePane("campaigns");
  }

  async function toggleCampaign(campaign: AdminResearchCampaign) {
    setBusy(`campaign-${campaign.id}`);
    try {
      await setAdminResearchCampaignActive(campaign.id, !campaign.active);
      await refreshCampaigns();
      showToast?.(campaign.active ? "Campaign paused." : "Campaign resumed.", "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to update campaign.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function runCampaignNow(campaign: AdminResearchCampaign) {
    setBusy(`campaign-run-${campaign.id}`);
    try {
      const result = await runAdminResearchCampaignNow(campaign.id);
      await refreshCampaigns();
      await refreshDrafts();
      showToast?.(`Campaign run complete: ${result.generated} generated, ${result.failed} failed.`, result.failed ? "error" : "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to run campaign.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function runCampaignItemNow(campaign: AdminResearchCampaign, itemId: string) {
    setBusy(`campaign-item-run-${itemId}`);
    try {
      const result = await runAdminResearchCampaignItemNow(campaign.id, itemId);
      await refreshCampaigns();
      await refreshDrafts();
      showToast?.(`Draft generation complete: ${result.generated} generated, ${result.failed} failed.`, result.failed ? "error" : "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to generate this campaign draft.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function unpublishSelected() {
    if (!selectedDraft) return;
    await unpublishDraft(selectedDraft);
  }

  async function rescheduleCampaignItem(campaign: AdminResearchCampaign, itemId: string, publishAt: string) {
    setBusy(`campaign-item-reschedule-${itemId}`);
    try {
      await rescheduleAdminResearchCampaignItem(campaign.id, itemId, publishAt);
      await refreshCampaigns();
      showToast?.("Campaign item schedule updated.", "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to update this campaign schedule.", "error");
    } finally {
      setBusy(null);
    }
  }

  async function removeCampaign(campaign: AdminResearchCampaign) {
    if (!window.confirm(`Delete campaign "${campaign.name}"? Generated articles are kept.`)) return;
    setBusy(`campaign-delete-${campaign.id}`);
    try {
      await deleteAdminResearchCampaign(campaign.id);
      await refreshCampaigns();
      showToast?.("Campaign deleted.", "success");
    } catch (err) {
      showToast?.(err instanceof Error ? err.message : "Unable to delete campaign.", "error");
    } finally {
      setBusy(null);
    }
  }

  function currentEditedArticle() {
    if (!articleDraft) return null;
    const parsed = markdownToSections(bodyMarkdown);
    const previewSectionCount = parsed.previewSectionCount ?? articleDraft.preview_section_count;
    const editedConfig = currentEditedConfig();
    const primaryTicker = articleDraft.primary_ticker || selectedDraft?.primary_ticker || editedConfig.ticker;
    const cardTickers = Array.from(new Set([primaryTicker, ...editedConfig.comparison_tickers].filter(Boolean)));
    return applySectionSelections(
      {
        ...articleDraft,
        comparison_tickers: editedConfig.comparison_tickers,
        suggested_card: {
          ...articleDraft.suggested_card,
          tickers: cardTickers,
        },
        paywall_copy: parsed.paywallCopy || articleDraft.paywall_copy,
        sections: parsed.sections,
        preview_section_count: typeof previewSectionCount === "number" ? previewSectionCount : null,
      },
      config.include_sections,
    );
  }

  function currentEditedConfig() {
    const normalizedComparisonTickers = parseComparisonTickers(config.comparison_tickers);
    return {
      ...config,
      comparison_ticker: normalizedComparisonTickers[0] || null,
      comparison_tickers: normalizedComparisonTickers,
    };
  }

  function updateConfigAccess(plan: ResearchBriefRequiredPlan) {
    setConfig((current) => ({ ...current, ...accessPatchForPlan(plan) }));
  }

  function closeDeleteDialog() {
    if (busy === "delete") return;
    setDeleteDialogOpen(false);
    setDeleteConfirmationText("");
  }

  async function deleteSelected() {
    if (!selectedDraft) return;
    setBusy("delete");
    try {
      await deleteAdminResearchBriefDraft(selectedDraft.id);
      setSelectedDraft(null);
      setDeleteDialogOpen(false);
      setDeleteConfirmationText("");
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

  function updateComparisonTickers(value: string) {
    const nextInput = value.toUpperCase();
    const nextTickers = parseComparisonTickers(nextInput);
    setComparisonTickerInput(nextInput);
    setConfig((current) => ({ ...current, comparison_ticker: nextTickers[0] || null, comparison_tickers: nextTickers }));
  }

  function removeComparisonTicker(symbol: string) {
    const nextTickers = comparisonTickers.filter((item) => item !== symbol);
    setComparisonTickerInput(nextTickers.join(", "));
    setConfig((current) => ({ ...current, comparison_ticker: nextTickers[0] || null, comparison_tickers: nextTickers }));
  }

  return (
    <div className="space-y-5">
      <section className="rounded-lg border border-emerald-300/15 bg-slate-950/55 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">Research Briefs</p>
            <h2 className="mt-1 text-2xl font-semibold text-white">Admin Research Brief Generator</h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
              Generate grounded Walnut research briefs with OpenAI Responses, schedule campaign drafts, edit them, and publish after explicit review.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            {(["create", "scheduled", "drafts", "published", "campaigns", "settings"] as const).map((pane) => (
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

              <div className="grid gap-4 md:grid-cols-2">
                <Select
                  label="Model"
                  value={config.selected_model || options.model_options[options.model_options.length - 1] || options.model_default}
                  options={options.model_options}
                  descriptions={options.model_descriptions}
                  labels={options.model_labels}
                  onChange={(value) => updateConfig("selected_model", value)}
                />
                <Select label="External research mode" value={config.external_research_mode} options={options.external_research_modes} onChange={(value) => updateConfig("external_research_mode", value)} />
                <Select label="Section format" value={config.section_format} options={options.section_formats} onChange={(value) => updateConfig("section_format", value)} />
                <Toggle label="Generate thumbnail / hero image" checked={config.generate_thumbnail} onChange={(value) => updateConfig("generate_thumbnail", value)} />
              </div>

              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Comparison tickers</span>
                <input
                  value={comparisonTickerInput}
                  onChange={(event) => updateComparisonTickers(event.target.value)}
                  className={fieldClassName("mt-2")}
                  placeholder="GOOGL, AMZN, MSFT"
                />
                <span className="mt-1 block text-xs text-slate-500">Separate multiple tickers with commas.</span>
                {comparisonTickers.length ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {comparisonTickers.map((symbol) => (
                      <span key={symbol} className="inline-flex items-center gap-1 rounded-md border border-emerald-300/25 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">
                        {symbol}
                        <button type="button" onClick={() => removeComparisonTicker(symbol)} className="text-emerald-100/70 hover:text-white" aria-label={`Remove ${symbol}`}>
                          &times;
                        </button>
                      </span>
                    ))}
                  </div>
                ) : null}
                {comparisonTickerLimitError ? <span className="mt-2 block text-xs text-rose-200">{comparisonTickerLimitError}</span> : null}
                {Object.entries(comparisonTickerErrors).map(([symbol, message]) => (
                  <span key={symbol} className="mt-2 block text-xs text-rose-200">{message}</span>
                ))}
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
              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Target keyword</span>
                <input value={config.target_keyword || ""} onChange={(event) => updateConfig("target_keyword", event.target.value)} className={fieldClassName("mt-2")} placeholder="NBIS stock buy now" />
              </label>

              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Add source URL manually</span>
                <input
                  value={config.manual_source_url || ""}
                  onChange={(event) => updateConfig("manual_source_url", event.target.value)}
                  className={fieldClassName("mt-2")}
                  placeholder="https://www.apple.com/ca/newsroom/2026/04/apple-reports-second-quarter-results/"
                />
                <span className="mt-1 block text-xs leading-5 text-slate-500">Use when official source discovery misses a company earnings release or filing.</span>
              </label>

              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Access</p>
                <div className="mt-2 grid gap-2 sm:grid-cols-2">
                  <label className="rounded-lg border border-emerald-300/20 bg-emerald-300/5 px-3 py-2 text-sm text-slate-200">
                    <span className="flex items-center gap-2 font-medium">
                      <input
                        type="checkbox"
                        checked={configRequiredPlan(config) === "premium"}
                        onChange={(event) => updateConfigAccess(event.target.checked ? "premium" : "free")}
                      />
                      Premium/Pro
                    </span>
                    <span className="mt-1 block text-xs leading-5 text-slate-500">Free and logged-out readers see the public preview and Premium gate.</span>
                  </label>
                  <label className="rounded-lg border border-cyan-300/20 bg-cyan-300/5 px-3 py-2 text-sm text-slate-200">
                    <span className="flex items-center gap-2 font-medium">
                      <input
                        type="checkbox"
                        checked={configRequiredPlan(config) === "pro"}
                        onChange={(event) => updateConfigAccess(event.target.checked ? "pro" : "free")}
                      />
                      Pro only
                    </span>
                    <span className="mt-1 block text-xs leading-5 text-slate-500">Only Pro and admin users receive the full brief.</span>
                  </label>
                </div>
              </div>

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
                <div className="mt-3 grid gap-2 sm:grid-cols-2">
                  <label className="rounded-lg border border-emerald-300/20 bg-emerald-300/5 px-3 py-2 text-sm text-slate-200">
                    <span className="flex items-center gap-2 font-medium">
                      <input
                        type="checkbox"
                        checked={config.include_confirmation_score}
                        onChange={(event) => updateConfig("include_confirmation_score", event.target.checked)}
                      />
                      Walnut confirmation score
                    </span>
                    <span className="mt-1 block text-xs leading-5 text-slate-500">Include Walnut&apos;s proprietary confirmation score, shown separately from underlying data.</span>
                  </label>
                  <label className="rounded-lg border border-emerald-300/20 bg-emerald-300/5 px-3 py-2 text-sm text-slate-200">
                    <span className="flex items-center gap-2 font-medium">
                      <input
                        type="checkbox"
                        checked={config.include_cross_source_confirmations}
                        onChange={(event) => updateConfig("include_cross_source_confirmations", event.target.checked)}
                      />
                      Cross-source confirmations
                    </span>
                    <span className="mt-1 block text-xs leading-5 text-slate-500">Mention which data categories support or contradict the thesis.</span>
                  </label>
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

              {activeJob ? (
                <div className={`rounded-lg border px-3 py-2 text-sm ${
                  activeJob.status === "failed"
                    ? "border-rose-300/30 bg-rose-950/25 text-rose-100"
                    : activeJob.status === "completed"
                      ? "border-emerald-300/25 bg-emerald-950/20 text-emerald-100"
                      : "border-sky-300/25 bg-sky-950/20 text-sky-100"
                }`}>
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-semibold">{activeJob.status === "completed" ? "Completed" : activeJob.status === "failed" ? "Generation failed" : "Generating research brief"}</span>
                    <span className="text-xs uppercase tracking-[0.16em] opacity-75">{activeJob.progress_step || activeJob.status}</span>
                  </div>
                  <p className="mt-1 text-xs leading-5 opacity-85">{activeJob.progress_message || "Working in the background."}</p>
                  {activeJob.status === "completed" && activeJob.draft_id ? <p className="mt-1 text-xs opacity-75">Draft saved: {activeJob.draft_id}</p> : null}
                </div>
              ) : null}

              {preflightReadiness ? <ResearchReadinessSummary readiness={preflightReadiness} /> : null}

              <div className="flex flex-wrap gap-2">
                <Button tone="primary" disabled={generationJobActive || hasComparisonTickerErrors} onClick={generateDraft}>
                  {generationJobActive ? "Generating..." : activeJob?.status === "failed" ? "Retry Generate" : "Generate Draft"}
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
            onSuggestedCardChange={updateSuggestedCard}
            onBodyChange={setBodyMarkdown}
            onSave={() => saveDraft()}
            onDiscard={() => {
              if (!selectedDraft) return;
              setArticleDraft(selectedDraft.article);
              setBodyMarkdown(articleToMarkdown(selectedDraft.article));
              setConfig({ ...DEFAULT_CONFIG, ...(selectedDraft.config || {}) });
              showToast?.("Unsaved changes discarded.", "info");
            }}
            onPublishNow={publishNowSelected}
            onApproveScheduled={approveScheduledSelected}
            onReject={rejectSelected}
            onReschedule={rescheduleSelected}
            onUnpublish={unpublishSelected}
            onDelete={requestDeleteSelected}
            blockingWarnings={publishHardStopWarnings.length}
            walnutCallInvalid={walnutCallInvalid}
          />
        </div>
      ) : null}

      {activePane === "scheduled" ? (
        <ScheduledBriefsPanel
          drafts={scheduledDrafts}
          campaigns={campaigns}
          busy={busy}
          onRunPendingItem={runCampaignItemNow}
          onReschedulePendingItem={rescheduleCampaignItem}
          onEditCampaign={editCampaign}
          onOpen={(draft) => {
            setSelectedDraft(draft);
            setActivePane("create");
          }}
        />
      ) : null}

      {activePane === "campaigns" ? (
        <CampaignsPanel
          themes={options.campaign_themes}
          campaigns={campaigns}
          health={publishingHealth}
          form={campaignForm}
          editingCampaign={editingCampaign}
          busy={busy}
          onFormChange={setCampaignForm}
          onSubmit={submitCampaign}
          onCancelEdit={() => {
            setEditingCampaign(null);
            setCampaignForm(DEFAULT_CAMPAIGN_FORM);
          }}
          onEdit={editCampaign}
          onToggle={toggleCampaign}
          onRunNow={runCampaignNow}
          onDelete={removeCampaign}
          opportunities={keywordOpportunities}
          selectedOpportunityIds={selectedKeywordOpportunityIds}
          marketNote={keywordMarketNote}
          onDiscover={discoverKeywordOpportunities}
          onUseOpportunity={useKeywordOpportunity}
          onDismissOpportunity={dismissKeywordOpportunity}
          opportunityInstructions={keywordOpportunityInstructions}
          onOpportunityInstructionsChange={(opportunityId, instructions) => setKeywordOpportunityInstructions((current) => ({ ...current, [opportunityId]: instructions }))}
          onRegenerateOpportunity={regenerateKeywordOpportunity}
        />
      ) : null}

      {activePane === "drafts" || activePane === "published" ? (
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">{activePane === "drafts" ? "Drafts" : "Published"}</h3>
          <div className="mt-4 grid gap-3">
            {(activePane === "drafts" ? generatedDrafts : publishedDrafts).map((draft) => (
              <article
                key={draft.id}
                className="rounded-lg border border-white/10 bg-slate-950/45 p-4 text-left transition hover:border-emerald-300/35"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="font-semibold text-white">{draft.article?.title || draft.id}</p>
                  <span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.status}</span>
                </div>
                <p className="mt-2 text-sm text-slate-400">{draft.article?.summary}</p>
                <div className="mt-4 flex flex-wrap gap-2">
                  <Button
                    onClick={() => {
                      setSelectedDraft(draft);
                      setActivePane("create");
                    }}
                  >
                    Edit
                  </Button>
                  {activePane === "published" ? <Button tone="danger" disabled={Boolean(busy)} onClick={() => void unpublishDraft(draft)}>Unpublish</Button> : null}
                </div>
              </article>
            ))}
            {(activePane === "drafts" ? generatedDrafts : publishedDrafts).length === 0 ? <p className="text-sm text-slate-500">No items yet.</p> : null}
          </div>
        </section>
      ) : null}

      {activePane === "settings" ? (
        <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">Settings</h3>
          <div className="mt-4 grid gap-3 md:grid-cols-3">
            <Metric label="Storage" value="Database" />
            <Metric label="Default status" value="Draft / Scheduled Review" />
            <Metric label="OpenAI API" value="Server-side Responses" />
          </div>
          <p className="mt-4 text-sm leading-6 text-slate-400">
            Campaign generation is cron-gated and approved scheduled briefs publish only after owner approval.
          </p>
        </section>
      ) : null}

      <section className="grid gap-4 lg:grid-cols-[1fr_0.9fr]">
        <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
          <h3 className="text-base font-semibold text-white">Validation</h3>
          {validationLabels ? (
            <div className="mt-3 grid grid-cols-2 gap-2 text-xs sm:grid-cols-4">
              {[
                ["Readiness", validationLabels.research_readiness],
                ["Identity", validationLabels.company_identity],
                ["Structure", validationLabels.structure],
                ["Internal language", validationLabels.internal_language],
                ["Source support", validationLabels.source_support],
                ["Numeric claims", validationLabels.numeric_validation],
                ["Missing data", validationLabels.missing_data_language],
                ["Style", validationLabels.style],
              ].map(([label, value]) => (
                <div key={label} className="rounded-lg border border-white/10 bg-slate-950/50 px-2.5 py-2">
                  <p className="text-slate-500">{label}</p>
                  <p className={`mt-1 font-semibold capitalize ${value === "failed" ? "text-rose-200" : value === "repaired" ? "text-amber-200" : "text-emerald-200"}`}>{value || "passed"}</p>
                </div>
              ))}
            </div>
          ) : null}
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
            <div className="flex flex-wrap gap-2">
              <span className="rounded-md border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-[10px] font-semibold uppercase text-emerald-200">
                {articleDraft?.walnut_call || selectedCard?.judgment || articleDraft?.judgment || "Draft"}
              </span>
              {articleDraft?.premium_required ? (
                <span className="rounded-md border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-[10px] font-semibold uppercase text-cyan-100">
                  {accessLabel(articleRequiredPlan(articleDraft))}
                </span>
              ) : null}
            </div>
            <p className="mt-3 text-lg font-semibold text-white">{selectedCard?.title || articleDraft?.title || "No draft selected"}</p>
            <p className="mt-2 text-sm leading-6 text-slate-400">{selectedCard?.description || articleDraft?.summary || "Generated card copy will appear here."}</p>
            <p className="mt-4 text-xs text-slate-500">
              {selectedDraft?.article?.slug ? `/research/${selectedDraft.article.slug}` : "No route yet"}
            </p>
          </div>
        </div>
      </section>

      <DeleteDraftDialog
        open={deleteDialogOpen}
        draft={selectedDraft}
        value={deleteConfirmationText}
        busy={busy === "delete"}
        onValueChange={setDeleteConfirmationText}
        onCancel={closeDeleteDialog}
        onConfirm={() => void deleteSelected()}
      />
      <PublishDraftDialog
        open={publishDialogOpen}
        busy={busy === "publish"}
        onCancel={closePublishDialog}
        onConfirm={() => void publishSelected()}
      />
    </div>
  );
}

function Select({
  label,
  value,
  options,
  descriptions,
  labels,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  descriptions?: Record<string, string>;
  labels?: Record<string, string>;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} className={fieldClassName("mt-2")}>
        {options.map((option) => (
          <option key={option} value={option}>
            {descriptions?.[option] ? `${labels?.[option] || option} - ${descriptions[option]}` : labels?.[option] || option}
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

function objectValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function sourceStatus(value: unknown) {
  const data = objectValue(value);
  return String(data.status || "unknown");
}

function statusTone(value: string) {
  if (value === "found" || value === "passed" || value === "ready") return "text-emerald-200";
  if (value === "missing" || value === "failed" || value === "not_ready") return "text-rose-200";
  return "text-slate-300";
}

function ResearchReadinessSummary({ readiness }: { readiness: Record<string, unknown> }) {
  const rows = Array.isArray(readiness.rows) ? readiness.rows.map(objectValue) : [];
  const status = String(readiness.status || "unknown");
  const missing = Array.isArray(readiness.missing_requirements) ? readiness.missing_requirements.map(String) : [];
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3 text-sm">
      <div className="flex items-center justify-between gap-3">
        <span className="font-semibold text-white">Research readiness</span>
        <span className={`text-xs font-semibold uppercase tracking-[0.16em] ${statusTone(status)}`}>{status.replace("_", " ")}</span>
      </div>
      {missing.length ? <p className="mt-2 text-xs leading-5 text-rose-100">Missing: {missing.join(", ")}</p> : null}
      {rows.length ? (
        <div className="mt-2 grid gap-1.5 sm:grid-cols-2">
          {rows.slice(0, 8).map((row) => {
            const label = String(row.label || "Requirement");
            const value = String(row.status || "unknown");
            return (
              <div key={label} className="flex items-center justify-between gap-2 text-xs">
                <span className="text-slate-400">{label}</span>
                <span className={`font-semibold capitalize ${statusTone(value)}`}>{value.replace("_", " ")}</span>
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function ResearchReadinessPanel({ draft }: { draft: AdminResearchBriefDraft }) {
  const context = objectValue(draft.research_context);
  const readiness = objectValue(context.research_readiness || draft.validation?.research_readiness);
  const rows = Array.isArray(readiness.rows) ? readiness.rows.map(objectValue) : [];
  const status = String(readiness.status || (draft.validation?.status === "passed" ? "ready" : "unknown"));
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Research readiness</p>
        <span className={`text-xs font-semibold uppercase tracking-[0.16em] ${statusTone(status)}`}>{status.replace("_", " ")}</span>
      </div>
      <div className="mt-2 space-y-1.5">
        {rows.length ? (
          rows.map((row) => {
            const label = String(row.label || "Requirement");
            const value = String(row.status || "unknown");
            const detail = row.detail ? String(row.detail) : "";
            return (
              <div key={label} className="flex items-start justify-between gap-3 text-xs">
                <span className="text-slate-400">{label}</span>
                <span className={`max-w-[55%] text-right font-semibold ${statusTone(value)}`}>
                  <span className="capitalize">{value.replace("_", " ")}</span>
                  {detail ? <span className="block truncate font-normal text-slate-500">{detail}</span> : null}
                </span>
              </div>
            );
          })
        ) : (
          <p className="text-xs text-slate-500">Generate or refresh sources to calculate readiness.</p>
        )}
      </div>
    </div>
  );
}

function SourceDiscoveryDiagnostics({ draft }: { draft: AdminResearchBriefDraft }) {
  const context = objectValue(draft.research_context);
  const config = objectValue(draft.config);
  const article = objectValue(draft.article);
  const discovery = objectValue(context.source_discovery || draft.validation?.source_discovery);
  const primary = objectValue(context.primary);
  const identity = objectValue(primary.identity);
  const confirmation = objectValue(primary.confirmation);
  const expectedSymbol = String(identity.symbol || draft.primary_ticker || "").toUpperCase();
  const contextSymbol = String(confirmation.symbol || expectedSymbol || "").toUpperCase();
  const includeConfirmationScore = Boolean(config.include_confirmation_score || article.confirmation_score_included);
  const confirmationScoreStatus = includeConfirmationScore
    ? confirmation.score || confirmation.confirmation_score
      ? "found"
      : "missing"
    : isThematicResearchDraft(draft, draft.article)
      ? "not applicable"
      : "not requested";
  const rows = [
    ["Official earnings release", sourceStatus(discovery.official_earnings_release)],
    ["SEC filing", sourceStatus(discovery.sec_filing)],
    ["Walnut ticker context", primary && Object.keys(primary).length ? "found" : "missing"],
    ["Primary ticker context match", expectedSymbol && contextSymbol && expectedSymbol !== contextSymbol ? "failed" : "passed"],
    ["Confirmation score", confirmationScoreStatus],
    ["Comparison tickers loaded", (draft.comparison_tickers || draft.config?.comparison_tickers || []).join(", ") || "None"],
  ];
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Source discovery</p>
      <div className="mt-2 space-y-1.5">
        {rows.map(([label, value]) => (
          <div key={label} className="flex items-center justify-between gap-3 text-xs">
            <span className="text-slate-400">{label}</span>
            <span className={`text-right font-semibold capitalize ${statusTone(value)}`}>{value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function PublicationReadinessPanel({ draft }: { draft: AdminResearchBriefDraft }) {
  const readiness = objectValue(draft.validation?.publication_readiness);
  const overlaps = Array.isArray(readiness.potential_cannibalization) ? readiness.potential_cannibalization : [];
  const rows = [
    ["Search intent identified", readiness.search_intent_identified ? "yes" : "missing"],
    ["Walnut-native data", readiness.walnut_native_data_included ? "yes" : "needs review"],
    ["Confirmation Score", readiness.confirmation_score_included ? "included" : "not used"],
    ["Internal links out", String(readiness.internal_links_out ?? 0)],
    ["Inbound opportunities", String(readiness.inbound_link_opportunities ?? 0)],
    ["Potential overlap", overlaps.length ? "review" : "none"],
  ];
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Publication readiness</p>
      <div className="mt-2 space-y-1.5">
        {rows.map(([label, value]) => <div key={label} className="flex items-center justify-between gap-3 text-xs"><span className="text-slate-400">{label}</span><span className="font-semibold capitalize text-slate-200">{value}</span></div>)}
      </div>
    </div>
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

function PublishDraftDialog({
  open,
  busy,
  onCancel,
  onConfirm,
}: {
  open: boolean;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 px-4 py-6 backdrop-blur-sm" role="dialog" aria-modal="true" aria-labelledby="publish-draft-title">
      <div className="w-full max-w-md overflow-hidden rounded-lg border border-emerald-300/25 bg-slate-950 shadow-2xl shadow-emerald-950/30">
        <div className="border-b border-white/10 bg-emerald-300/10 px-5 py-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-200">Publish brief</p>
          <h3 id="publish-draft-title" className="mt-1 text-lg font-semibold text-white">Confirm publish</h3>
        </div>
        <div className="px-5 py-5">
          <p className="text-sm leading-6 text-slate-300">
            Double-check source links, access level, preview boundary, and any hard-stop validation warnings before publishing.
          </p>
        </div>
        <div className="flex flex-col-reverse gap-2 border-t border-white/10 bg-slate-950/80 px-5 py-4 sm:flex-row sm:justify-end">
          <Button disabled={busy} onClick={onCancel}>Cancel</Button>
          <Button tone="primary" disabled={busy} onClick={onConfirm}>{busy ? "Publishing..." : "Publish"}</Button>
        </div>
      </div>
    </div>
  );
}

function DeleteDraftDialog({
  open,
  draft,
  value,
  busy,
  onValueChange,
  onCancel,
  onConfirm,
}: {
  open: boolean;
  draft: AdminResearchBriefDraft | null;
  value: string;
  busy: boolean;
  onValueChange: (value: string) => void;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  if (!open || !draft) return null;
  const canDelete = value === "DELETE";
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 px-4 py-6 backdrop-blur-sm" role="dialog" aria-modal="true" aria-labelledby="delete-draft-title">
      <div className="w-full max-w-md overflow-hidden rounded-lg border border-rose-300/25 bg-slate-950 shadow-2xl shadow-rose-950/30">
        <div className="border-b border-white/10 bg-rose-300/10 px-5 py-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-rose-200">Destructive action</p>
          <h3 id="delete-draft-title" className="mt-1 text-lg font-semibold text-white">Delete research draft</h3>
        </div>
        <div className="space-y-4 px-5 py-5">
          <p className="text-sm leading-6 text-slate-300">
            This removes the draft from the admin queue. Type <span className="font-semibold text-white">DELETE</span> to confirm.
          </p>
          <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Draft</p>
            <p className="mt-1 truncate text-sm font-semibold text-slate-100">{draft.article?.title || draft.id}</p>
            <p className="mt-1 text-xs text-slate-500">{draft.id}</p>
          </div>
          <label className="block">
            <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Confirmation</span>
            <input
              autoFocus
              value={value}
              onChange={(event) => onValueChange(event.target.value)}
              className={fieldClassName("mt-2 border-rose-300/25 focus:border-rose-200/70 focus:ring-rose-300/10")}
              placeholder="Type DELETE"
            />
          </label>
        </div>
        <div className="flex flex-col-reverse gap-2 border-t border-white/10 bg-slate-950/80 px-5 py-4 sm:flex-row sm:justify-end">
          <Button disabled={busy} onClick={onCancel}>Cancel</Button>
          <Button tone="danger" disabled={busy || !canDelete} onClick={onConfirm}>{busy ? "Deleting..." : "Delete Draft"}</Button>
        </div>
      </div>
    </div>
  );
}

function formatDateTime(value?: string | null) {
  if (!value) return "-";
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return value;
  return new Date(parsed).toLocaleString();
}

function toDateTimeLocal(value?: string | null) {
  if (!value) return "";
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return "";
  const date = new Date(parsed);
  const offsetMs = date.getTimezoneOffset() * 60 * 1000;
  return new Date(date.getTime() - offsetMs).toISOString().slice(0, 16);
}

function ScheduledBriefsPanel({
  drafts,
  campaigns,
  busy,
  onOpen,
  onEditCampaign,
  onRunPendingItem,
  onReschedulePendingItem,
}: {
  drafts: AdminResearchBriefDraft[];
  campaigns: AdminResearchCampaign[];
  busy: string | null;
  onOpen: (draft: AdminResearchBriefDraft) => void;
  onEditCampaign: (campaign: AdminResearchCampaign) => void;
  onRunPendingItem: (campaign: AdminResearchCampaign, itemId: string) => void;
  onReschedulePendingItem: (campaign: AdminResearchCampaign, itemId: string, publishAt: string) => void;
}) {
  const pendingItems = campaigns.flatMap((campaign) =>
    (campaign.items || [])
      .filter((item) => item.status === "pending" || item.status === "generating")
      .map((item) => ({ ...item, campaign, campaignName: campaign.name })),
  );
  const [pendingPublishTimes, setPendingPublishTimes] = useState<Record<string, string>>({});
  const campaignEnd = (campaign: AdminResearchCampaign) => {
    const times = (campaign.items || []).map((item) => item.publish_at).filter((value): value is string => Boolean(value)).sort();
    return times.at(-1) || null;
  };
  const campaignArticleNumber = (campaign: AdminResearchCampaign, itemId: string) => Math.max(1, (campaign.items || []).findIndex((item) => item.id === itemId) + 1);
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-base font-semibold text-white">Scheduled</h3>
        <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{pendingItems.length} pending · {drafts.length} draft review{drafts.length === 1 ? "" : "s"}</span>
      </div>
      {pendingItems.length ? (
        <div className="mt-4 rounded-lg border border-cyan-300/20 bg-cyan-300/[0.03] p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h4 className="text-sm font-semibold text-cyan-100">Pending campaign queue</h4>
              <p className="mt-1 text-xs leading-5 text-slate-400">These are campaign articles without a draft yet. Generate a review draft now to edit it before its scheduled publish time; it will not publish until you approve it.</p>
            </div>
            <Button onClick={() => pendingItems[0] && onEditCampaign(pendingItems[0].campaign)}>Edit Campaign</Button>
          </div>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-xs uppercase tracking-[0.14em] text-slate-500">
                <tr><th className="px-3 py-2">Publish time</th><th className="px-3 py-2">Ticker/topic</th><th className="px-3 py-2">Campaign</th><th className="px-3 py-2">Draft generation</th><th className="px-3 py-2">Status</th><th className="px-3 py-2">Actions</th></tr>
              </thead>
              <tbody className="divide-y divide-white/10">
                {pendingItems.map((item) => (
                  <tr key={item.id} className="text-slate-300">
                    <td className="px-3 py-3">
                      <input
                        type="datetime-local"
                        value={pendingPublishTimes[item.id] ?? toDateTimeLocal(item.publish_at)}
                        onChange={(event) => setPendingPublishTimes((current) => ({ ...current, [item.id]: event.target.value }))}
                        className={fieldClassName("min-w-52 text-xs")}
                      />
                    </td>
                    <td className="px-3 py-3 font-semibold text-slate-100">{item.ticker || item.topic || "Topic"}</td>
                    <td className="px-3 py-3">
                      <button type="button" onClick={() => onEditCampaign(item.campaign)} className="text-left font-medium text-emerald-200 hover:text-emerald-100 hover:underline">{item.campaignName}</button>
                      <p className="mt-1 text-xs text-slate-500">Article {campaignArticleNumber(item.campaign, item.id)} of {item.campaign.items?.length || 1} · Campaign ends {formatDateTime(campaignEnd(item.campaign))}</p>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap text-xs text-slate-400">{formatDateTime(item.generate_at)}</td>
                    <td className="px-3 py-3"><span className="rounded-md border border-cyan-300/25 px-2 py-1 text-xs font-semibold uppercase text-cyan-100">{item.status}</span></td>
                    <td className="px-3 py-3">
                      <div className="flex flex-wrap gap-2">
                        <Button
                          disabled={Boolean(busy) || !((pendingPublishTimes[item.id] ?? toDateTimeLocal(item.publish_at)).trim())}
                          onClick={() => onReschedulePendingItem(item.campaign, item.id, new Date(pendingPublishTimes[item.id] ?? toDateTimeLocal(item.publish_at)).toISOString())}
                        >
                          {busy === `campaign-item-reschedule-${item.id}` ? "Saving..." : "Save publish time"}
                        </Button>
                        <Button disabled={Boolean(busy)} onClick={() => onRunPendingItem(item.campaign, item.id)}>
                          {busy === `campaign-item-run-${item.id}` ? "Generating..." : "Generate draft now"}
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full text-left text-sm">
          <thead className="text-xs uppercase tracking-[0.14em] text-slate-500">
            <tr>
              <th className="px-3 py-2">Scheduled</th>
              <th className="px-3 py-2">Ticker/topic</th>
              <th className="px-3 py-2">Title</th>
              <th className="px-3 py-2">Campaign</th>
              <th className="px-3 py-2">Target keyword</th>
              <th className="px-3 py-2">Status</th>
              <th className="px-3 py-2">Indexation</th>
              <th className="px-3 py-2">Data as of</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {drafts.map((draft) => (
              <tr key={draft.id} className="align-top text-slate-300">
                <td className="px-3 py-3 whitespace-nowrap">{formatDateTime(draft.scheduled_at)}</td>
                <td className="px-3 py-3 font-semibold text-slate-100">{draft.primary_ticker || draft.article?.primary_ticker || "Topic"}</td>
                <td className="px-3 py-3">
                  <button type="button" onClick={() => onOpen(draft)} className="text-left font-semibold text-emerald-200 hover:text-emerald-100">
                    {draft.article?.title || draft.id}
                  </button>
                  <p className="mt-1 max-w-xl text-xs leading-5 text-slate-500">{draft.article?.summary}</p>
                </td>
                <td className="px-3 py-3">{draft.campaign_name || draft.campaign_id || "-"}</td>
                <td className="px-3 py-3 text-xs text-slate-400">{draft.target_keyword || draft.config?.target_keyword || "-"}</td>
                <td className="px-3 py-3">
                  <span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.status}</span>
                  {draft.last_publish_error ? <p className="mt-2 text-xs leading-5 text-rose-200">{draft.last_publish_error}</p> : null}
                </td>
                <td className="px-3 py-3"><span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.index_status || "unknown"}</span></td>
                <td className="px-3 py-3 whitespace-nowrap">{formatDateTime(draft.data_as_of || draft.generated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {drafts.length === 0 ? <p className="py-6 text-sm text-slate-500">No generated draft reviews yet.</p> : null}
      </div>
    </section>
  );
}

function CampaignsPanel({
  themes,
  campaigns,
  health,
  form,
  editingCampaign,
  busy,
  opportunities,
  selectedOpportunityIds,
  marketNote,
  onFormChange,
  onSubmit,
  onCancelEdit,
  onEdit,
  onToggle,
  onRunNow,
  onDelete,
  onDiscover,
  onUseOpportunity,
  onDismissOpportunity,
  opportunityInstructions,
  onOpportunityInstructionsChange,
  onRegenerateOpportunity,
}: {
  themes: AdminResearchCampaignTheme[];
  campaigns: AdminResearchCampaign[];
  health: AdminResearchPublishingHealth | null;
  form: AdminResearchCampaignPayload;
  editingCampaign: AdminResearchCampaign | null;
  busy: string | null;
  opportunities: AdminResearchKeywordOpportunity[];
  selectedOpportunityIds: string[];
  marketNote: string;
  onFormChange: (form: AdminResearchCampaignPayload) => void;
  onSubmit: () => void;
  onCancelEdit: () => void;
  onEdit: (campaign: AdminResearchCampaign) => void;
  onToggle: (campaign: AdminResearchCampaign) => void;
  onRunNow: (campaign: AdminResearchCampaign) => void;
  onDelete: (campaign: AdminResearchCampaign) => void;
  onDiscover: (maxCandidates?: number) => void;
  onUseOpportunity: (opportunity: AdminResearchKeywordOpportunity) => void;
  onDismissOpportunity: (opportunity: AdminResearchKeywordOpportunity) => void;
  opportunityInstructions: Record<string, string>;
  onOpportunityInstructionsChange: (opportunityId: string, instructions: string) => void;
  onRegenerateOpportunity: (opportunity: AdminResearchKeywordOpportunity) => void;
}) {
  const selectedTheme = themes.find((theme) => theme.key === form.theme) || themes[0];
  const [opportunityCount, setOpportunityCount] = useState(5);
  const newOpportunities = opportunities
    .filter((opportunity) => opportunity.status === "new")
    .sort((left, right) => Number(right.opportunity_score || 0) - Number(left.opportunity_score || 0));
  const isOpportunityLoaded = (opportunity: AdminResearchKeywordOpportunity) => {
    if (selectedOpportunityIds.includes(opportunity.id)) return true;
    if (opportunity.content_type === "ticker" && opportunity.ticker && form.tickers.includes(opportunity.ticker)) {
      const configuredQuery = form.target_keywords?.[opportunity.ticker]?.trim().toLowerCase();
      return configuredQuery === opportunity.target_keyword.trim().toLowerCase();
    }
    return opportunity.content_type === "non_ticker" && Boolean(form.topic && opportunity.topic && form.topic.trim().toLowerCase() === opportunity.topic.trim().toLowerCase());
  };
  return (
    <section className="grid gap-5 xl:grid-cols-[minmax(22rem,0.7fr)_minmax(0,1.3fr)]">
      <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h3 className="text-base font-semibold text-white">{editingCampaign ? "Edit Campaign" : "Create Campaign"}</h3>
            {editingCampaign ? <p className="mt-1 text-xs text-slate-500">Only pending queue items change when you save. Generated and published briefs stay intact.</p> : null}
          </div>
          {editingCampaign ? <Button disabled={Boolean(busy)} onClick={onCancelEdit}>Cancel edit</Button> : null}
        </div>
        <div className="mt-4 grid gap-4">
          <label>
            <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Campaign name</span>
            <input value={form.name} onChange={(event) => onFormChange({ ...form, name: event.target.value })} className={fieldClassName("mt-2")} />
          </label>
          <Select
            label="Theme"
            value={form.theme}
            options={themes.map((theme) => theme.key)}
            labels={Object.fromEntries(themes.map((theme) => [theme.key, theme.label]))}
            descriptions={Object.fromEntries(themes.map((theme) => [theme.key, theme.intent]))}
            onChange={(theme) => {
              const nextTheme = themes.find((item) => item.key === theme);
              onFormChange({ ...form, theme, content_type: nextTheme?.content_type === "non_ticker" ? "non_ticker" : "ticker" });
            }}
          />
          <div className="grid gap-2 sm:grid-cols-2">
            <label className="rounded-lg border border-white/10 bg-slate-950/40 px-3 py-2 text-sm text-slate-300">
              <input type="radio" checked={form.content_type === "ticker"} onChange={() => onFormChange({ ...form, content_type: "ticker" })} className="mr-2" />
              Ticker
            </label>
            <label className="rounded-lg border border-white/10 bg-slate-950/40 px-3 py-2 text-sm text-slate-300">
              <input type="radio" checked={form.content_type === "non_ticker"} onChange={() => onFormChange({ ...form, content_type: "non_ticker" })} className="mr-2" />
              Non-ticker
            </label>
          </div>
          {form.content_type === "ticker" ? (
            <div>
            <label>
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Tickers</span>
              <input
                value={form.tickers.join(", ")}
                onChange={(event) => {
                  const tickers = parseComparisonTickers(event.target.value);
                  onFormChange({ ...form, tickers, article_count: Math.max(1, tickers.length) });
                }}
                className={fieldClassName("mt-2")}
                placeholder="NBIS, CRWV, COHR"
              />
            </label>
              {form.tickers.length ? (
                <div className="mt-2 rounded-lg border border-emerald-300/15 bg-emerald-300/[0.03] p-2 text-xs text-slate-300">
                  <p className="font-semibold text-emerald-100">Ticker plan: {form.tickers.length} distinct draft{form.tickers.length === 1 ? "" : "s"}</p>
                  {form.tickers.map((ticker) => <p key={ticker} className="mt-1 truncate">{ticker}: {form.target_keywords?.[ticker] || "No opportunity query selected yet"}</p>)}
                </div>
              ) : null}
            </div>
          ) : (
            <label>
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Topic</span>
              <input value={form.topic || ""} onChange={(event) => onFormChange({ ...form, topic: event.target.value })} className={fieldClassName("mt-2")} placeholder={selectedTheme?.intent || "Research topic"} />
            </label>
          )}
          <label>
            <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Primary query</span>
            <input value={form.target_keyword || ""} onChange={(event) => onFormChange({ ...form, target_keyword: event.target.value })} className={fieldClassName("mt-2")} placeholder="NBIS stock buy now" />
            <span className="mt-1 block text-xs text-slate-500">A question is valid here. Each loaded opportunity keeps its own ticker/query pair.</span>
            <span className="mt-1 block text-xs text-slate-500">For Good Buy Now, Walnut defaults each ticker to “[ticker] stock buy now” when empty.</span>
          </label>
          <label>
            <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Secondary keywords</span>
            <input value={(form.secondary_keywords || []).join(", ")} onChange={(event) => onFormChange({ ...form, secondary_keywords: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) })} className={fieldClassName("mt-2")} placeholder="earnings analysis, institutional ownership" />
          </label>
          <div className="rounded-lg border border-cyan-300/15 bg-cyan-300/[0.03] p-3 text-xs text-slate-300">
            <p className="font-semibold text-cyan-100">Live keyword discovery</p>
            <p className="mt-1 leading-5 text-slate-400">Searches current web signals—including Google Trends pages and relevant Reddit discussions where available—then ranks only questions Walnut can answer with original data. It never creates or publishes a campaign automatically.</p>
            <p className="mt-2 leading-5 text-slate-400">The selected theme, tickers, and topic are included in every discovery request; results are ranked highest score first.</p>
            <label className="mt-3 block">
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Ideas to rank</span>
              <select value={opportunityCount} onChange={(event) => setOpportunityCount(Number(event.target.value))} className={fieldClassName("mt-2")}>
                {[3, 5, 7, 8].map((count) => <option key={count} value={count}>{count} opportunities</option>)}
              </select>
            </label>
            <Button disabled={Boolean(busy)} onClick={() => onDiscover(opportunityCount)}>{busy === "keyword-discovery" ? "Finding opportunities..." : `Find ${opportunityCount} high-impact opportunities`}</Button>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <Select label="Cadence" value={form.cadence} options={["one_time", "daily", "weekly", "custom"]} onChange={(cadence) => onFormChange({ ...form, cadence })} />
            <label>
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Start publish time</span>
              <input type="datetime-local" value={form.publish_start_at || ""} onChange={(event) => onFormChange({ ...form, publish_start_at: event.target.value })} className={fieldClassName("mt-2")} />
            </label>
            <label>
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Articles</span>
              <input type="number" min={1} max={50} value={form.article_count} onChange={(event) => onFormChange({ ...form, article_count: Math.max(1, Number(event.target.value) || 1) })} className={fieldClassName("mt-2")} />
            </label>
            <label>
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Over days</span>
              <input type="number" min={1} max={30} value={form.window_days} onChange={(event) => onFormChange({ ...form, window_days: Math.max(1, Number(event.target.value) || 1) })} className={fieldClassName("mt-2")} />
            </label>
          </div>
          <p className="text-xs leading-5 text-slate-500">For three different briefs, add three ticker opportunities, set Articles to 3 and Over days to 3. Added opportunities stay saved through refreshes until you dismiss them or create the campaign. Then use Run Now to preview its drafts immediately. Each generated draft appears in Drafts/Scheduled and emails the campaign owner for review; publication still requires approval.</p>
          <Button tone="primary" disabled={busy === "campaign"} onClick={onSubmit}>{busy === "campaign" ? (editingCampaign ? "Saving..." : "Creating...") : (editingCampaign ? "Save Changes" : "Create Campaign")}</Button>
        </div>
      </div>
      <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
        <div className="rounded-lg border border-cyan-300/20 bg-cyan-300/[0.03] p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h3 className="text-base font-semibold text-white">Keyword opportunities</h3>
              <p className="mt-1 text-xs leading-5 text-slate-400">Directional opportunity scores blend freshness, answerability, Walnut data fit, and a SERP assessment. They are not verified search volume or keyword-difficulty metrics.</p>
            </div>
            <Button disabled={Boolean(busy)} onClick={() => onDiscover(opportunityCount)}>{busy === "keyword-discovery" ? "Searching..." : "Refresh signals"}</Button>
          </div>
          {marketNote ? <p className="mt-2 text-xs text-cyan-100">{marketNote}</p> : null}
          <div className="mt-3 grid gap-3">
            {newOpportunities.slice(0, 8).map((opportunity) => (
              <div key={opportunity.id} className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <p className="font-semibold text-white">{opportunity.target_keyword}</p>
                    <p className="mt-1 text-xs text-slate-400">{opportunity.search_intent}</p>
                  </div>
                  <span className="rounded-md border border-cyan-300/25 px-2 py-1 text-xs font-semibold text-cyan-100">Score {opportunity.opportunity_score}</span>
                </div>
                <p className="mt-2 text-xs leading-5 text-slate-300">{opportunity.rationale}</p>
                <p className="mt-1 text-xs leading-5 text-emerald-100">Walnut angle: {opportunity.walnut_angle}</p>
                <p className="mt-2 text-xs text-slate-500">Trend: {opportunity.trend_signal} · Competition: {opportunity.competition_assessment} · {opportunity.metric_note}</p>
                {opportunity.source_urls?.length ? <p className="mt-2 text-xs text-slate-500">Signals: {opportunity.source_urls.map((url) => <a key={url} href={url} target="_blank" rel="noreferrer" className="mr-2 text-cyan-200 hover:underline">source</a>)}</p> : null}
                <label className="mt-3 block">
                  <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Change request (optional)</span>
                  <textarea
                    value={opportunityInstructions[opportunity.id] || ""}
                    onChange={(event) => onOpportunityInstructionsChange(opportunity.id, event.target.value.slice(0, 2000))}
                    rows={2}
                    className={fieldClassName("mt-2 resize-y")}
                    placeholder="e.g. focus on valuation risk after earnings, avoid backlog, or make this more AEO-friendly"
                  />
                  <span className="mt-1 block text-xs text-slate-500">Walnut will re-check current web signals and replace this saved opportunity. It will not create a campaign.</span>
                </label>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Button disabled={Boolean(busy)} onClick={() => onUseOpportunity(opportunity)}>{isOpportunityLoaded(opportunity) ? "Loaded in campaign plan" : "Add to campaign"}</Button>
                  <Button disabled={Boolean(busy)} onClick={() => onRegenerateOpportunity(opportunity)}>{busy === `keyword-regenerate-${opportunity.id}` ? "Regenerating..." : "Regenerate opportunity"}</Button>
                  <Button disabled={Boolean(busy)} onClick={() => onDismissOpportunity(opportunity)}>Dismiss</Button>
                </div>
              </div>
            ))}
            {newOpportunities.length === 0 ? <p className="text-sm text-slate-500">No saved opportunities yet. Run a discovery pass when you want fresh signals.</p> : null}
          </div>
        </div>
        <h3 className="text-base font-semibold text-white">Campaigns</h3>
        {health ? (
          <div className="mt-3 rounded-lg border border-white/10 bg-slate-950/45 p-3 text-xs text-slate-400">
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Metric label="30d published" value={String(health.published_last_30_days)} />
              <Metric label="Indexed" value={String(health.indexed)} />
              <Metric label="Index rate" value={health.indexation_rate == null ? "-" : `${health.indexation_rate}%`} />
              <Metric label="Daily cap" value={String(health.daily_automated_publish_cap)} />
            </div>
            <p className="mt-3 text-slate-500">Campaign scheduler: {health.campaign_schedule_enabled ? "enabled — drafts will be generated ahead of their publish times." : "off — create campaigns freely, then use Run Now to preview drafts. Automatic draft emails and scheduled publishing will not run until it is enabled."}</p>
            {health.cadence_warning ? <p className="mt-3 text-amber-100">{health.cadence_warning}</p> : null}
          </div>
        ) : null}
        <div className="mt-4 grid gap-3">
          {campaigns.map((campaign) => (
            <div key={campaign.id} className="rounded-lg border border-white/10 bg-slate-950/45 p-4">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="font-semibold text-white">{campaign.name}</p>
                  <p className="mt-1 text-sm text-slate-400">{campaign.theme} · {campaign.content_type} · {campaign.cadence}</p>
                  <p className="mt-1 text-xs text-slate-500">Next generation: {campaign.items?.find((item) => item.status === "pending")?.generate_at || "No pending items"}</p>
                </div>
                <span className={`rounded-md border px-2 py-1 text-xs font-semibold uppercase ${campaign.active ? "border-emerald-300/25 text-emerald-200" : "border-white/10 text-slate-400"}`}>
                  {campaign.active ? "Active" : "Paused"}
                </span>
              </div>
              <div className="mt-3 grid gap-2 text-xs text-slate-400 sm:grid-cols-4">
                <Metric label="Generated" value={String(campaign.generated_count || 0)} />
                <Metric label="Approved" value={String(campaign.approved_count || 0)} />
                <Metric label="Published" value={String(campaign.published_count || 0)} />
                <Metric label="Pending" value={String(campaign.pending_count || 0)} />
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <Button disabled={Boolean(busy)} onClick={() => onToggle(campaign)}>{campaign.active ? "Pause" : "Resume"}</Button>
                <Button disabled={Boolean(busy)} onClick={() => onEdit(campaign)}>Edit Campaign</Button>
                <Button disabled={Boolean(busy)} onClick={() => onRunNow(campaign)}>Generate pending drafts</Button>
                <Button tone="danger" disabled={Boolean(busy)} onClick={() => onDelete(campaign)}>Delete</Button>
              </div>
            </div>
          ))}
          {campaigns.length === 0 ? <p className="text-sm text-slate-500">No campaigns yet.</p> : null}
        </div>
      </div>
    </section>
  );
}

function EditorPanel({
  draft,
  article,
  bodyMarkdown,
  busy,
  onArticleChange,
  onSuggestedCardChange,
  onBodyChange,
  onSave,
  onDiscard,
  onPublishNow,
  onApproveScheduled,
  onReject,
  onReschedule,
  onUnpublish,
  onDelete,
  blockingWarnings,
  walnutCallInvalid,
}: {
  draft: AdminResearchBriefDraft | null;
  article: AdminResearchBriefArticle | null;
  bodyMarkdown: string;
  busy: string | null;
  onArticleChange: <K extends keyof AdminResearchBriefArticle>(key: K, value: AdminResearchBriefArticle[K]) => void;
  onSuggestedCardChange: (updates: Partial<AdminResearchBriefArticle["suggested_card"]>) => void;
  onBodyChange: (value: string) => void;
  onSave: () => void;
  onDiscard: () => void;
  onPublishNow: () => void;
  onApproveScheduled: () => void;
  onReject: (correctionInstructions: string) => void;
  onReschedule: (scheduledAt: string) => void;
  onUnpublish: () => void;
  onDelete: () => void;
  blockingWarnings: number;
  walnutCallInvalid: boolean;
}) {
  const bodyTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const [scheduledAtValue, setScheduledAtValue] = useState("");
  const [rejectionInstructions, setRejectionInstructions] = useState("");
  useEffect(() => {
    setScheduledAtValue(toDateTimeLocal(draft?.scheduled_at));
    setRejectionInstructions("");
  }, [draft?.id, draft?.scheduled_at]);
  if (!draft || !article) {
    return (
      <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
        <h3 className="text-base font-semibold text-white">Draft Review</h3>
        <p className="mt-3 text-sm leading-6 text-slate-400">Generate or select a draft to open the two-column editor.</p>
      </section>
    );
  }
  const activeDraft = draft;
  const activeArticle = article;
  const isScheduledDraft = ["scheduled_review", "approved_scheduled"].includes(draft.status);
  function copyRedditPost() {
    const redditPost = String(activeArticle.reddit_post || "");
    if (!redditPost.trim()) return;
    void navigator.clipboard?.writeText(redditPost);
    recordProductEvent({
      event_name: "research_reddit_copy_generated",
      path: `/research/${activeArticle.slug}`,
      properties: {
        ...(activeArticle.analytics || {}),
        article_slug: activeArticle.slug,
        ticker: activeArticle.primary_ticker || activeDraft.primary_ticker || null,
        tickers: [activeArticle.primary_ticker || activeDraft.primary_ticker, ...(activeArticle.comparison_tickers || activeDraft.comparison_tickers || [])].filter(Boolean).join(","),
        user_entitlement: "admin",
      },
    });
  }
  function insertPaywallMarker() {
    const textarea = bodyTextareaRef.current;
    let text = bodyMarkdown;
    let start = textarea?.selectionStart ?? text.length;
    let end = textarea?.selectionEnd ?? start;
    const existing = text.match(paywallMarkerPattern);
    if (existing && typeof existing.index === "number") {
      const existingStart = existing.index;
      const existingEnd = existingStart + existing[0].length;
      const selectionContainsExisting = start <= existingStart && end >= existingEnd;
      if (!selectionContainsExisting) {
        text = `${text.slice(0, existingStart)}${text.slice(existingEnd)}`;
        if (existingStart < start) {
          const removedLength = existingEnd - existingStart;
          start = Math.max(existingStart, start - removedLength);
          end = Math.max(start, end - removedLength);
        }
      }
    }
    const before = text.slice(0, start).trimEnd();
    const after = text.slice(end).trimStart();
    const prefix = before ? `${before}\n\n` : "";
    const suffix = after ? `\n\n${after}` : "";
    const nextBody = `${prefix}${PAYWALL_MARKER}${suffix}`;
    const nextCursor = prefix.length + PAYWALL_MARKER.length;
    onBodyChange(nextBody);
    window.requestAnimationFrame(() => {
      bodyTextareaRef.current?.focus();
      bodyTextareaRef.current?.setSelectionRange(nextCursor, nextCursor);
    });
  }
  return (
    <section className="grid gap-4 rounded-lg border border-white/10 bg-slate-950/55 p-4 lg:grid-cols-[minmax(0,1fr)_18rem]">
      <div className="min-w-0 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-base font-semibold text-white">Draft Review</h3>
          <span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold uppercase text-slate-300">{draft.status}</span>
        </div>
        <label className="block">
          <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Title</span>
          <input value={article.title} onChange={(event) => onArticleChange("title", event.target.value)} className={fieldClassName("mt-2")} />
        </label>
        <label className="block">
          <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Insights card title</span>
          <input
            value={article.suggested_card?.title || ""}
            onChange={(event) => onSuggestedCardChange({ title: event.target.value })}
            className={fieldClassName("mt-2")}
            placeholder="Short title for Insights cards"
          />
        </label>
        <label className="block">
          <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Insights preview body</span>
          <textarea
            value={article.preview_body || article.summary || ""}
            onChange={(event) => {
              onArticleChange("preview_body", event.target.value);
              onArticleChange("summary", event.target.value);
              onSuggestedCardChange({ description: event.target.value });
            }}
            className={fieldClassName("mt-2 min-h-24")}
            placeholder="1-3 sentences for the Insights card."
          />
        </label>
        <label className="block">
          <span className="flex flex-wrap items-center justify-between gap-2">
            <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Full post body</span>
            <Button type="button" onClick={insertPaywallMarker}>Insert Paywall Marker</Button>
          </span>
          <textarea ref={bodyTextareaRef} value={bodyMarkdown} onChange={(event) => onBodyChange(event.target.value)} className={fieldClassName("mt-2 min-h-[34rem] font-mono text-xs leading-6")} />
        </label>
        {article.reddit_post ? (
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Reddit output</p>
              <Button onClick={copyRedditPost}>Copy Reddit Post</Button>
            </div>
            <textarea readOnly value={article.reddit_post} className={fieldClassName("mt-3 min-h-48 font-mono text-xs leading-6")} />
          </div>
        ) : null}
        <details className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
          <summary className="cursor-pointer text-sm font-semibold text-emerald-200">Advanced metadata</summary>
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            <input value={article.slug} onChange={(event) => onArticleChange("slug", event.target.value)} className={fieldClassName()} placeholder="Slug" />
            <input value={article.hero_image || ""} onChange={(event) => onArticleChange("hero_image", event.target.value)} className={fieldClassName()} placeholder="Hero image URL/path" />
            <input value={article.subtitle} onChange={(event) => onArticleChange("subtitle", event.target.value)} className={fieldClassName()} placeholder="Hero title / subtitle" />
            <input value={article.judgment} onChange={(event) => onArticleChange("judgment", event.target.value)} className={fieldClassName()} placeholder="Judgment" />
            <select value={article.walnut_call || ""} onChange={(event) => onArticleChange("walnut_call", event.target.value)} className={fieldClassName()}>
              <option value="">Walnut call</option>
              {WALNUT_CALL_VALUES.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
            <input value={article.category} onChange={(event) => onArticleChange("category", event.target.value)} className={fieldClassName()} placeholder="Category / tone" />
            <input value={String(article.reading_minutes || "")} onChange={(event) => onArticleChange("reading_minutes", Number(event.target.value) || 1)} className={fieldClassName()} placeholder="Reading time" />
            <input
              type="number"
              min={0}
              max={Math.max(0, article.sections?.length || 0)}
              value={article.preview_section_count ?? ""}
              onChange={(event) => onArticleChange("preview_section_count", event.target.value === "" ? null : Math.max(0, Number(event.target.value) || 0))}
              className={fieldClassName()}
              placeholder="Free sections before gate"
            />
          </div>
          {article.thumbnail_asset ? (
            <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/50 p-3">
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Hero image prompt</p>
              <p className="mt-2 text-xs leading-5 text-slate-300">{article.thumbnail_asset.image_prompt}</p>
              {article.thumbnail_asset.thumbnail_url ? <img src={article.thumbnail_asset.thumbnail_url} alt={article.thumbnail_asset.image_title || "Generated thumbnail"} className="mt-3 max-h-44 w-full rounded-md object-cover" /> : null}
            </div>
          ) : null}
        </details>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Access</p>
          <div className="mt-3 grid gap-2 sm:grid-cols-2">
            <label className="rounded-lg border border-emerald-300/20 bg-emerald-300/5 px-3 py-2 text-sm text-slate-200">
              <span className="flex items-center gap-2 font-medium">
                <input
                  type="checkbox"
                  checked={articleRequiredPlan(article) === "premium"}
                  onChange={(event) => {
                    const patch = accessPatchForPlan(event.target.checked ? "premium" : "free");
                    onArticleChange("premium_required", patch.premium_required);
                    onArticleChange("required_plan", patch.required_plan);
                  }}
                />
                Premium/Pro
              </span>
              <span className="mt-1 block text-xs leading-5 text-slate-500">Premium, Pro, and admin users can read the full brief.</span>
            </label>
            <label className="rounded-lg border border-cyan-300/20 bg-cyan-300/5 px-3 py-2 text-sm text-slate-200">
              <span className="flex items-center gap-2 font-medium">
                <input
                  type="checkbox"
                  checked={articleRequiredPlan(article) === "pro"}
                  onChange={(event) => {
                    const patch = accessPatchForPlan(event.target.checked ? "pro" : "free");
                    onArticleChange("premium_required", patch.premium_required);
                    onArticleChange("required_plan", patch.required_plan);
                  }}
                />
                Pro only
              </span>
              <span className="mt-1 block text-xs leading-5 text-slate-500">Only Pro and admin users can read the full brief.</span>
            </label>
          </div>
        </div>
      </div>

      <aside className="space-y-3">
        <Metric label="Validation" value={draft.validation?.status || "Unknown"} />
        <Metric label="Numeric claims" value={String(draft.validation?.numeric_claims?.length || 0)} />
        <Metric label="Source links" value={String(draft.validation?.source_link_count || 0)} />
        <Metric label="Reading time" value={`${draft.validation?.estimated_reading_minutes || article.reading_minutes || 1} min`} />
        <Metric label="Model" value={draft.model || "OpenAI"} />
        <Metric label="Access" value={accessLabel(articleRequiredPlan(article))} />
        <Metric label="Generated at" value={(draft.updated_at || draft.created_at || "").slice(0, 16)} />
        {draft.campaign_id ? <Metric label="Campaign" value={draft.campaign_name || draft.campaign_id} /> : null}
        {draft.scheduled_at ? <Metric label="Scheduled" value={formatDateTime(draft.scheduled_at)} /> : null}
        {draft.data_as_of ? <Metric label="Data as of" value={formatDateTime(draft.data_as_of)} /> : null}
        {draft.earnings_period_used ? <Metric label="Earnings period" value={draft.earnings_period_used} /> : null}
        <ResearchReadinessPanel draft={draft} />
        <PublicationReadinessPanel draft={draft} />
        <SourceDiscoveryDiagnostics draft={draft} />
        {draft.validation?.source_link_count === 0 ? (
          <div className="rounded-lg border border-rose-300/30 bg-rose-950/25 px-3 py-2 text-sm text-rose-100">
            This draft has no source links. Regenerate with External Research Mode enabled or add sources manually.
          </div>
        ) : null}
        {walnutCallInvalid ? (
          <div className="rounded-lg border border-rose-300/30 bg-rose-950/25 px-3 py-2 text-sm text-rose-100">
            Walnut call must use the approved expanded call taxonomy.
          </div>
        ) : null}
        {article.source_links?.length ? (
          <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Sources</p>
            <div className="mt-2 space-y-2">
              {article.source_links.slice(0, 5).map((source) => (
                <a key={source.url} href={source.url} target="_blank" rel="noreferrer" className="block break-all text-xs text-emerald-200 hover:text-emerald-100">
                  {source.label || source.url}
                </a>
              ))}
            </div>
          </div>
        ) : null}
        {article.missing_data_notes?.length ? (
          <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Missing data notes</p>
            <p className="mt-2 text-xs leading-5 text-slate-300">{article.missing_data_notes.slice(0, 6).join("; ")}</p>
          </div>
        ) : null}
        <div className="grid gap-2">
          <Button disabled={Boolean(busy)} onClick={onSave}>Save Draft</Button>
          <Button disabled={Boolean(busy)} onClick={onDiscard}>Discard Changes</Button>
          {isScheduledDraft ? (
            <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
              <label className="block">
                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Scheduled time</span>
                <input type="datetime-local" value={scheduledAtValue} onChange={(event) => setScheduledAtValue(event.target.value)} className={fieldClassName("mt-2")} />
              </label>
              <div className="mt-3 grid gap-2">
                <Button disabled={Boolean(busy) || !scheduledAtValue} onClick={() => onReschedule(new Date(scheduledAtValue).toISOString())}>Reschedule</Button>
                <Button disabled={Boolean(busy)} onClick={onApproveScheduled}>Approve Scheduled</Button>
                <label className="block">
                  <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Correction instructions</span>
                  <textarea
                    value={rejectionInstructions}
                    onChange={(event) => setRejectionInstructions(event.target.value)}
                    className={fieldClassName("mt-2 min-h-20")}
                    placeholder="Optional: explain what to change in the replacement draft."
                    maxLength={2000}
                  />
                </label>
                <Button disabled={Boolean(busy)} onClick={() => onReject(rejectionInstructions)}>Apply Corrections</Button>
                <Button tone="primary" disabled={Boolean(busy) || walnutCallInvalid || blockingWarnings > 0 || (draft.validation?.source_link_count || 0) === 0} onClick={onPublishNow}>Publish Now</Button>
              </div>
            </div>
          ) : null}
          {draft.status === "published" ? <Button disabled={Boolean(busy)} onClick={onUnpublish}>Unpublish</Button> : null}
          <Button tone="danger" disabled={Boolean(busy)} onClick={onDelete}>Delete Draft</Button>
        </div>
      </aside>
    </section>
  );
}
