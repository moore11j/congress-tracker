import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const landingPage = fs.readFileSync(path.join(root, "app/landing/page.tsx"), "utf8");

test("landing insights link keeps label and arrow on one line", () => {
  assert.match(landingPage, /inline-flex[^"]*whitespace-nowrap[^"]*/);
  assert.match(landingPage, /Open insights/);
  assert.match(landingPage, /aria-hidden="true">→<\/span>/);
});

test("landing macro rows resolve Core CPI by label variants", () => {
  assert.match(landingPage, /landingMacroLabelGroups/);
  assert.match(landingPage, /"Core CPI YoY"/);
  assert.match(landingPage, /"core_cpi_yoy"/);
  assert.match(landingPage, /"CPILFESL"/);
  assert.match(landingPage, /const economics = landingMacroRows\(snapshot\.economics \?\? \[\]\)/);
});
