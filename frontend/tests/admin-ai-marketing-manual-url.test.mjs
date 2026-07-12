import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const viewSource = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminAiMarketingView.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");

test("AI Growth Engine exposes the new top-level IA", () => {
  for (const label of [
    "Dashboard",
    "Article-Reactive X",
    "X Campaigns",
    "Reddit Research Threads",
    "Draft Queue",
    "Assets",
    "Settings",
  ]) {
    assert.match(viewSource, new RegExp(label));
  }
  assert.match(viewSource, /API credits left/);
  assert.match(viewSource, /getAdminProviderUsageFmp/);
  assert.match(viewSource, /apiCreditsMetric/);
  assert.match(apiSource, /\/api\/admin\/provider-usage\/fmp/);
  assert.doesNotMatch(viewSource, /Email Delivery/);
  assert.doesNotMatch(viewSource, /Influencer Packs|Influencer Report Packs|Reddit Paid Ads/);
  assert.doesNotMatch(viewSource, /Ticker thread assist|Congress trade angle|Insider buying angle|Unusual signal angle|Tool alternative/);
});

test("manual research input supports platform, pasted text, output type, and destination URL", () => {
  assert.match(viewSource, /Source platform/);
  assert.match(viewSource, /Pasted post\/comment\/thread text/);
  assert.match(viewSource, /Desired output type/);
  assert.match(viewSource, /Destination URL optional/);
  assert.match(viewSource, /campaign_type: "manual_research_input"/);
  assert.match(apiSource, /desired_output_type\?: string \| null/);
  assert.match(apiSource, /destination_url\?: string \| null/);
});

test("content draft cards include copy and manual lifecycle actions", () => {
  for (const label of [
    "Copy primary post",
    "Copy short version",
    "Copy direct version",
    "Copy hashtags/cashtags",
    "Copy Walnut link",
    "Copy article URL",
    "Email to Jarod",
    "Approve",
    "Mark copied",
    "Mark posted manually",
    "Reject",
    "Archive",
    "Delete",
    "Regenerate",
  ]) {
    assert.match(viewSource, new RegExp(label.replace(/[/-]/g, "\\$&")));
  }
  assert.match(viewSource, /Requested draft changes/);
  assert.match(viewSource, /regenerateAdminAiGrowthDraft/);
  assert.match(viewSource, /emailAdminAiGrowthDraft/);
  assert.match(viewSource, /markAdminAiGrowthDraftCopied/);
  assert.match(viewSource, /markAdminAiGrowthDraftPosted/);
  assert.doesNotMatch(viewSource, /Copy full draft|Copy short variant|Copy disclosure line|Copy posting checklist/);
  assert.doesNotMatch(viewSource, /Copy X post text|Copy alternate hooks|Copy image\/chart caption/);
  assert.doesNotMatch(viewSource, /Copy Reddit post title|Copy Reddit post body|Copy Reddit comment reply|Copy disclosure text|Copy markdown/);
  assert.doesNotMatch(viewSource, /Send\/re-send email to Jarod/);
});

test("draft queue keeps source, Walnut, and X links visible without auto-posting", () => {
  for (const label of [
    "Open Reddit thread",
    "Open Reddit comment",
    "Open source",
    "Open article",
    "Open Walnut URL",
    "Open X",
    "Open X compose",
  ]) {
    assert.match(viewSource, new RegExp(label.replace(/[/-]/g, "\\$&")));
  }
  assert.doesNotMatch(viewSource, /Login\/Open X|Login\/Open Reddit|Open Reddit submit|Open Walnut link/);
  assert.match(apiSource, /markAdminAiGrowthDraftPosted/);
  assert.doesNotMatch(apiSource, /auto-post|autopost|auto_post/);
});

test("Article-Reactive X campaign form exposes provider status and no FMP secret input", () => {
  for (const label of [
    "Article-Reactive X Campaigns",
    "FMP Articles API",
    "Source provider",
    "Managed outside admin UI",
    "Max drafts per day",
    "Recipient email",
    "Include image/card",
    "Include Walnut link",
    "Hashtag mode",
    "CTA mode",
    "Run now",
    "Start",
    "Pause",
    "Stop",
    "Delete",
  ]) {
    assert.match(viewSource, new RegExp(label.replace(/[/-]/g, "\\$&")));
  }
  assert.match(viewSource, /campaign_type: "article_reactive_x"/);
  assert.match(viewSource, /source_type: "fmp_articles"/);
  assert.match(viewSource, /setCampaignLifecycleStatus/);
  assert.match(viewSource, /deleteCampaign/);
  assert.match(apiSource, /updateAdminAiMarketingCampaign/);
  assert.match(apiSource, /deleteAdminAiMarketingCampaign/);
  assert.match(apiSource, /method: "DELETE"/);
  assert.match(apiSource, /fmp_articles_status/);
  assert.doesNotMatch(viewSource, /FMP API Key|FMP_API_KEY.*<input|type="password"/);
});

test("AI Growth API uses draft endpoints and asset metadata", () => {
  assert.match(apiSource, /\/api\/admin\/ai-growth\/drafts/);
  assert.match(apiSource, /emailAdminAiGrowthDraft/);
  assert.match(apiSource, /markAdminAiGrowthDraftCopied/);
  assert.match(apiSource, /archiveAdminAiGrowthDraft/);
  assert.match(apiSource, /rejectAdminAiGrowthDraft/);
  assert.match(apiSource, /updateAdminAiGrowthDraftStatus/);
  assert.match(apiSource, /regenerateAdminAiGrowthDraft/);
  assert.match(viewSource, /status: "dismissed"/);
  assert.match(viewSource, /DRAFT_QUEUE_STATUSES\.join\(","\)/);
  assert.doesNotMatch(viewSource, /label: "Regeneration needed"/);
  assert.match(apiSource, /type AdminAiGrowthAsset/);
  assert.match(viewSource, /Open\/download asset/);
  assert.match(viewSource, /break-all/);
  assert.match(viewSource, /isAssetFileUrl/);
  assert.match(viewSource, /isAssetImageUrl/);
});

test("X drafts expose the 280 character guardrail", () => {
  assert.match(viewSource, /xCharacterCount <= 280/);
  assert.match(viewSource, /\/280/);
  assert.match(viewSource, /formatXDraftForDisplay/);
  assert.match(viewSource, /#Markets/);
  assert.match(viewSource, /bias disclosed/);
});

test("settings remain env-only for provider credentials", () => {
  assert.match(viewSource, /Provider credentials are read from server environment variables and Fly secrets only\./);
  assert.match(viewSource, /OPENAI_WEB_SEARCH_ENABLED/);
  assert.match(viewSource, /OpenAI Web Search/);
  assert.doesNotMatch(viewSource, /BING_SEARCH_API_KEY/);
  assert.doesNotMatch(viewSource, /Bing Search API Key/);
  assert.doesNotMatch(viewSource, /updateAdminAiMarketingSettings/);
  assert.doesNotMatch(viewSource, /type="password"/);
});
