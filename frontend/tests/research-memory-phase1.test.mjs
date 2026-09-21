import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const read = (relative) => fs.readFileSync(path.join(process.cwd(), relative), "utf8");
const workspace = read("components/research-memory/ResearchMemoryWorkspace.tsx");
const evidencePanel = read("components/research-memory/ResearchEvidencePanel.tsx");
const monitoring = read("app/monitoring/page.tsx");
const tickerPage = read("app/ticker/[symbol]/page.tsx");
const tickerCard = read("components/ticker/TickerResearchMemoryCard.tsx");
const operationalCard = read("components/ticker/TickerOperationalIntelligenceCard.tsx");
const operationalProvider = read("components/ticker/TickerOperationalIntelligenceProvider.tsx");
const contextCard = read("components/ticker/TickerContextCard.tsx");
const indexPage = read("app/monitoring/research/page.tsx");
const detailPage = read("app/monitoring/research/[id]/page.tsx");
const api = read("lib/api.ts");

test("Research Memory feature flag controls navigation and ticker entry", () => {
  assert.match(monitoring, /NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED !== "false"/);
  assert.match(tickerCard, /NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED === "false"/);
  assert.match(tickerPage, /TickerResearchMemoryCard/);
  assert.match(tickerCard, /No active thesis/);
  assert.match(tickerCard, /Create thesis/);
});

test("index, creation chooser, and three paths have coherent states", () => {
  for (const label of [
    "No Research Memories yet.",
    "Loading Research Memories…",
    "Unable to load Research Memories.",
    "Walnut Suggested Thesis",
    "Thesis Template",
    "Custom Thesis",
    "No suggested theses available",
    "Suggested theses are temporarily unavailable.",
    "Templates are temporarily unavailable.",
    "Walnut could not structure that thesis. Please retry.",
  ]) assert.ok(workspace.includes(label), label);
  assert.match(indexPage, /overflow-x-auto/);
});

test("review supports draft save, explicit activation, and nested editing", () => {
  for (const label of [
    "Here is what Walnut believes your thesis depends on.",
    "Save draft",
    "Start Monitoring",
    "Core Assumptions",
    "Catalysts",
    "Risks",
    "Invalidation Conditions",
    "Threshold (optional)",
    "Add",
    "Delete",
  ]) assert.ok(workspace.includes(label), label);
  assert.match(workspace, /canActivate \? <button/);
  assert.match(workspace, /structure\.status === "draft" \|\| structure\.status === "paused"/);
});

test("private detail route is guarded and handles unavailable ownership state", () => {
  assert.match(detailPage, /VerifiedSessionGuard/);
  assert.match(detailPage, /Research Memory not found or unavailable/);
  assert.match(api, /\/api\/research-memory/);
  assert.match(api, /activateResearchMemory/);
});

test("activated surfaces truthfully describe source-linked monitoring without a thesis-health claim", () => {
  assert.match(workspace, /Operational-source evidence is processed and matched to eligible claims/);
  assert.match(evidencePanel, /This is not a thesis-health score/);
  assert.match(workspace, /Coverage labels describe expected source coverage/);
  assert.match(tickerCard, /operating-source evidence is matched as it is processed/);
});

test("ticker operating intelligence is source-linked and avoids a fake thesis-health state", () => {
  assert.match(operationalProvider, /getTickerOperationalIntelligence/);
  assert.match(operationalCard, /Source-grounded company developments/);
  assert.match(operationalCard, /source_url/);
  assert.match(operationalCard, /Evidence excerpt/);
  assert.match(operationalCard, /extraction confidence/);
  assert.match(operationalCard, /Try again/);
  assert.doesNotMatch(tickerPage, /TickerOperationalIntelligenceCard/);
  assert.match(contextCard, /TickerOperationalIntelligenceCard/);
});
