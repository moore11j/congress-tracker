import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const viewSource = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminAiMarketingView.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");

test("manual AI outreach form accepts pasted text as the source body", () => {
  assert.match(viewSource, /Post\/comment text or thread excerpt/);
  assert.match(viewSource, /!manualUrl\.trim\(\) && !manualText\.trim\(\)/);
  assert.match(viewSource, /Paste a source URL or post\/comment text first\./);
  assert.match(viewSource, /url: manualUrl\.trim\(\) \|\| null/);
  assert.match(viewSource, /text: manualText\.trim\(\) \|\| null/);
  assert.match(viewSource, /source_url_provided !== false/);
  assert.doesNotMatch(viewSource, /Paste a Reddit, X, or Facebook URL first\./);
});

test("manual AI outreach opportunity card shows stored OpenAI failure reasons", () => {
  assert.match(viewSource, /ai_suggestion_error/);
  assert.match(viewSource, /const emptySuggestionMessage = suggestionError \?\? "No suggestion yet\. Regenerate when ready\."/);
  assert.match(viewSource, /suggestion\?\.suggested_reply \?\? emptySuggestionMessage/);
  assert.match(viewSource, /suggestion\?\.short_reason \?\? suggestionError/);
  assert.match(viewSource, /await refreshOpportunities\(\);/);
  assert.doesNotMatch(viewSource, /Configure OPENAI_API_KEY or regenerate after setup/);
});

test("manual AI outreach card surfaces skip and angle metadata", () => {
  assert.match(viewSource, /Action: \$\{recommendedAction\}/);
  assert.match(viewSource, /Probably do not reply\./);
  assert.match(viewSource, /Recommended action:/);
  assert.match(viewSource, /Reply angle:/);
  assert.match(viewSource, /Walnut feature:/);
  assert.match(viewSource, /Value added:/);
  assert.match(viewSource, /recommended_action !== "reply"/);
  assert.match(apiSource, /recommended_action: "reply" \| "skip" \| "monitor"/);
  assert.match(apiSource, /alternate_reply_more_direct: string/);
});

test("manual AI outreach API and errors preserve backend validation detail", () => {
  assert.match(apiSource, /url\?: string \| null/);
  assert.match(apiSource, /function structuredDetailMessage/);
  assert.match(apiSource, /Array\.isArray\(detail\)/);
  assert.match(apiSource, /return messages\.join\(" "\)/);
});

test("AI outreach supports compliant Reddit web search campaigns", () => {
  assert.match(viewSource, /Reddit via Web Search/);
  assert.match(viewSource, /BING_SEARCH_API_KEY/);
  assert.match(viewSource, /Query templates/);
  assert.match(viewSource, /Search recency/);
  assert.match(
    viewSource,
    /Uses search-provider snippets and URLs only\. Does not scrape Reddit\. Paste full thread text for better reply quality\./,
  );
  assert.match(viewSource, /ProviderStatusCard label="Reddit API"/);
  assert.match(viewSource, /ProviderStatusCard label="Web Search Reddit"/);
  assert.match(viewSource, /ProviderStatusCard label="Manual Text"/);
  assert.match(apiSource, /"web_search_reddit"/);
  assert.match(apiSource, /source_provider\?: string \| null/);
  assert.match(apiSource, /query_templates: string\[\]/);
});
