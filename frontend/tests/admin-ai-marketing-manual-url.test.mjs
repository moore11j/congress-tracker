import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const viewSource = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminAiMarketingView.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");

test("AI Growth Engine exposes the new top-level IA", () => {
  for (const label of [
    "Dashboard",
    "X Campaigns",
    "Reddit Research Threads",
    "Draft Queue",
    "Assets",
    "Settings",
  ]) {
    assert.match(viewSource, new RegExp(label));
  }
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
    "Copy draft",
    "Copy source URL",
    "Approve",
    "Deny",
    "Archive",
    "Delete",
  ]) {
    assert.match(viewSource, new RegExp(label.replace(/[/-]/g, "\\$&")));
  }
  assert.doesNotMatch(viewSource, /Copy full draft|Copy short variant|Copy disclosure line|Copy Walnut link|Copy posting checklist/);
  assert.doesNotMatch(viewSource, /Copy X post text|Copy alternate hooks|Copy image\/chart caption/);
  assert.doesNotMatch(viewSource, /Copy Reddit post title|Copy Reddit post body|Copy Reddit comment reply|Copy disclosure text|Copy markdown/);
  assert.doesNotMatch(viewSource, /Mark copied|Mark posted manually|Send\/re-send email to Jarod|Reject/);
});

test("draft queue keeps source links visible without platform login clutter", () => {
  for (const label of [
    "Open Reddit thread",
    "Open Reddit comment",
    "Open source",
  ]) {
    assert.match(viewSource, new RegExp(label.replace(/[/-]/g, "\\$&")));
  }
  assert.doesNotMatch(viewSource, /Login\/Open X|Open X compose|Login\/Open Reddit|Open Reddit submit|Open Walnut link/);
  assert.match(apiSource, /markAdminAiGrowthDraftPosted/);
  assert.doesNotMatch(apiSource, /auto-post|autopost|auto_post/);
});

test("AI Growth API uses draft endpoints and asset metadata", () => {
  assert.match(apiSource, /\/api\/admin\/ai-growth\/drafts/);
  assert.match(apiSource, /emailAdminAiGrowthDraft/);
  assert.match(apiSource, /markAdminAiGrowthDraftCopied/);
  assert.match(apiSource, /archiveAdminAiGrowthDraft/);
  assert.match(apiSource, /rejectAdminAiGrowthDraft/);
  assert.match(apiSource, /updateAdminAiGrowthDraftStatus/);
  assert.match(viewSource, /status: "dismissed"/);
  assert.match(apiSource, /type AdminAiGrowthAsset/);
  assert.match(viewSource, /Open\/download asset/);
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
