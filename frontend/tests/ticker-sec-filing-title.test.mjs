import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const tickerContextCard = readFileSync(join(root, "components/ticker/TickerContextCard.tsx"), "utf8");

test("ticker filings table maps common SEC forms to meaningful titles", () => {
  assert.match(tickerContextCard, /"6-K": "Report of Foreign Private Issuer"/);
  assert.match(tickerContextCard, /"SD": "Specialized Disclosure Report"/);
  assert.match(tickerContextCard, /"4": "Statement of Changes in Beneficial Ownership"/);
  assert.match(tickerContextCard, /return "SEC Filing"/);
});

test("ticker filings table prefers non-generic provider titles before form fallbacks", () => {
  assert.match(tickerContextCard, /if \(title && title\.toLowerCase\(\) !== "sec filing"\) return title/);
  assert.match(tickerContextCard, /const mapped = SEC_FORM_TITLES\[normalized\]/);
});
