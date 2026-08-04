import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const apiSource = fs.readFileSync(path.join(root, "lib/api.ts"), "utf8");
const tickerPage = fs.readFileSync(path.join(root, "app/ticker/[symbol]/page.tsx"), "utf8");
const comparePage = fs.readFileSync(path.join(root, "app/compare/[left]/[right]/page.tsx"), "utf8");
const compareLoading = fs.readFileSync(path.join(root, "app/compare/[left]/[right]/loading.tsx"), "utf8");
const globalStyles = fs.readFileSync(path.join(root, "app/globals.css"), "utf8");
const selectorSource = fs.readFileSync(path.join(root, "components/compare/PeerCompareSelector.tsx"), "utf8");

test("peer compare API client targets the shareable two-symbol route", () => {
  assert.match(apiSource, /export async function getPeerCompare/);
  assert.match(apiSource, /\/api\/compare\/\$\{tickerPathSymbol\(leftSymbol\)\}\/\$\{tickerPathSymbol\(rightSymbol\)\}/);
});

test("ticker page exposes compare entry without silently choosing a peer", () => {
  assert.match(tickerPage, /\/compare\/\$\{encodeURIComponent\(normalizedSymbol\)\}\/_/);
  assert.match(tickerPage, />Compare<\/Link>/);
});

test("peer compare page renders report and selector recovery", () => {
  assert.match(comparePage, /getPeerCompare\(left, right/);
  assert.match(comparePage, /hasLeft && hasRight/);
  assert.match(comparePage, /<PeerCompareSelector leftSymbol=\{left\} rightSymbol=\{right\}/);
  assert.equal((comparePage.match(/<PeerCompareSelector/g) || []).length, 1);
  assert.match(comparePage, /Compare two tickers/);
  assert.match(comparePage, /Search for two tickers to compare\./);
  assert.match(comparePage, /Search for a first ticker to compare\./);
  assert.match(comparePage, /Compare tickers to see which setup has stronger support/);
  assert.match(comparePage, /Our Call/);
});

test("peer compare uses the full app width and shared loading treatment", () => {
  assert.match(comparePage, /min-h-screen bg-\[#06111f\] py-6 text-slate-100/);
  assert.match(compareLoading, /min-h-screen bg-\[#06111f\] py-6 text-slate-100/);
  assert.doesNotMatch(comparePage, /px-4 py-6 text-slate-100 sm:px-6 lg:px-8/);
  assert.doesNotMatch(compareLoading, /px-4 py-6 text-slate-100 sm:px-6 lg:px-8/);
  assert.match(comparePage, /mx-auto w-full max-w-none space-y-5/);
  assert.match(compareLoading, /mx-auto w-full max-w-none space-y-5/);
  assert.match(compareLoading, /text-emerald-300">Peer Compare<\/p>[\s\S]*terminal-loading-progress-fill/);
  assert.match(compareLoading, /terminal-loading-progress-fill/);
  assert.match(compareLoading, /from-emerald-500 via-emerald-300 to-lime-100/);
  assert.match(compareLoading, /terminal-loading-progress-percent/);
  assert.match(compareLoading, /terminal-loading-message/);
  assert.doesNotMatch(compareLoading, /from-cyan-300 via-emerald-300 to-violet-300/);
  assert.match(globalStyles, /\.terminal-loading-progress-fill/);
  assert.match(globalStyles, /\.terminal-loading-message/);
  assert.doesNotMatch(globalStyles, /peer-compare-progress-fill/);
});

test("peer compare page renders compact locked state and pricing return CTAs", () => {
  assert.match(comparePage, /data\?\.status === "locked"/);
  assert.match(comparePage, /function LockedCompareState/);
  assert.match(comparePage, /const requiredPlan = data\.access\?\.required_plan === "pro" \? "pro" : "premium"/);
  assert.match(comparePage, /Unlock Compare with \$\{requiredPlanLabel\}/);
  assert.match(comparePage, /One comparison answers today's question\./);
  assert.match(comparePage, /Walnut helps you compare the rest of your portfolio, monitor what changes and see when the better setup shifts\./);
  assert.match(comparePage, /Unlock deeper confirmation, institutional activity and options-flow context with Walnut Premium or Pro\./);
  assert.match(comparePage, /Categories Walnut evaluates/);
  assert.match(comparePage, /Walnut&apos;s proprietary confirmation score summarizes whether the available data supports or conflicts with each stock setup\./);
  assert.match(comparePage, /pricingHref\(currentPath\)/);
  assert.match(comparePage, /\/login\?return_to=\$\{encodeURIComponent\(currentPath\)\}/);
});

test("peer compare premium pro locks stay contextual inside existing cards", () => {
  assert.match(comparePage, /function proLockCopy/);
  assert.match(comparePage, /See which ticker institutions are accumulating or reducing\./);
  assert.match(comparePage, /See whether options positioning confirms or contradicts the comparison\./);
  assert.match(comparePage, /Upgrade to Pro/);
  assert.match(comparePage, /compare_pro_upgrade_click/);
  assert.match(comparePage, /compare_premium_upgrade_click/);
  assert.match(comparePage, /compare_locked_view/);
  assert.match(comparePage, /compare_unlocked_after_upgrade/);
});

test("peer compare page forwards server auth to protected compare data", () => {
  assert.match(comparePage, /optionalPageAuthState/);
  assert.match(comparePage, /const authState = await optionalPageAuthState\(\)/);
  assert.match(comparePage, /authToken: authState\.token \?\? undefined/);
});

test("peer compare selector reuses symbol suggestions", () => {
  assert.match(selectorSource, /const \[active, setActive\]/);
  assert.match(selectorSource, /if \(!active \|\| trimmed\.length < 1\)/);
  assert.match(selectorSource, /suggestSymbols\(trimmed, "all"/);
  assert.match(selectorSource, /function bestSuggestionForQuery/);
  assert.match(selectorSource, /function canCommitRawTicker/);
  assert.match(selectorSource, /async function commitQuery/);
  assert.match(selectorSource, /source: "PeerCompareSelectorCommit"/);
  assert.match(selectorSource, /void commitQuery\(\)/);
  assert.doesNotMatch(selectorSource, /commit\(items\[0\]\?\.symbol \|\| normalized\)/);
  assert.match(selectorSource, /return symbol === "_" \? "" : symbol/);
  assert.match(selectorSource, /placeholder="Search ticker or company"/);
  assert.match(selectorSource, /onCommit=\{\(symbol\) => navigate\(symbol, right \|\| "_"\)\}/);
  assert.match(selectorSource, /router\.push\(`\/compare\/\$\{encodeURIComponent\(normalizedLeft\)\}\/\$\{encodeURIComponent\(normalizedRight\)\}`\)/);
});
