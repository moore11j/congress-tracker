import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const apiSource = fs.readFileSync(path.join(root, "lib/api.ts"), "utf8");
const tickerPage = fs.readFileSync(path.join(root, "app/ticker/[symbol]/page.tsx"), "utf8");
const comparePage = fs.readFileSync(path.join(root, "app/compare/[left]/[right]/page.tsx"), "utf8");
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
  assert.match(comparePage, /right !== "_"/);
  assert.match(comparePage, /<PeerCompareSelector leftSymbol=\{left\} rightSymbol=\{right\}/);
  assert.equal((comparePage.match(/<PeerCompareSelector/g) || []).length, 1);
  assert.match(comparePage, /Our Call/);
});

test("peer compare page renders compact locked state and pricing return CTAs", () => {
  assert.match(comparePage, /data\?\.status === "locked"/);
  assert.match(comparePage, /function LockedCompareState/);
  assert.match(comparePage, /Unlock Compare with Premium/);
  assert.match(comparePage, /Premium unlocks the full comparison, Walnut verdict and proprietary confirmation-score analysis\./);
  assert.match(comparePage, /Categories Walnut evaluates/);
  assert.match(comparePage, /Walnut's proprietary confirmation score measures how strongly the available evidence supports or contradicts each investment case\./);
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
  assert.match(selectorSource, /router\.push\(`\/compare\/\$\{encodeURIComponent\(normalizedLeft\)\}\/\$\{encodeURIComponent\(normalizedRight\)\}`\)/);
});
