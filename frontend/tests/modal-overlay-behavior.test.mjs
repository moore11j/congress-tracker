import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const layout = read("app/layout.tsx");
const globals = read("app/globals.css");
const modal = read("components/ui/WalnutModal.tsx");
const watchlistPicker = read("components/watchlists/AddTickerToWatchlist.tsx");
const screenerExport = read("components/screener/ScreenerExportButton.tsx");
const screenerOverlay = read("components/screener/ScreenerUpgradeOverlay.tsx");
const savedViews = read("components/saved-views/SavedViewsBar.tsx");

test("app exports an accessible mobile viewport and avoids iOS form-control zoom", () => {
  assert.match(layout, /export const viewport: Viewport/);
  assert.match(layout, /width: "device-width"/);
  assert.match(layout, /initialScale: 1/);
  assert.match(layout, /viewportFit: "cover"/);
  assert.doesNotMatch(layout, /userScalable:\s*false/);
  assert.match(globals, /@media \(max-width: 767px\)/);
  assert.match(globals, /input,[\s\S]*select,[\s\S]*textarea[\s\S]*font-size: 16px !important/);
});

test("transactional popups render through a high-z-index body portal without backdrop click dismissal", () => {
  assert.match(modal, /createPortal/);
  assert.match(modal, /document\.body/);
  assert.match(modal, /z-\[5000\]/);
  assert.match(modal, /overscroll-contain/);
  assert.match(modal, /var\(--app-header-height,64px\)_\+_env\(safe-area-inset-top\)_\+_12px/);
  assert.match(modal, /document\.body\.style\.overflow = "hidden"/);
  assert.doesNotMatch(modal, /onClick=\{\(\) => onClose\(\)\}/);
});

test("watchlist and screener transactional popups use the shared modal overlay", () => {
  assert.match(watchlistPicker, /import \{ WalnutModal \}/);
  assert.match(watchlistPicker, /<WalnutModal[\s\S]*Close watchlist picker/);
  assert.doesNotMatch(watchlistPicker, /computePanelStyle/);
  assert.doesNotMatch(watchlistPicker, /document\.addEventListener\("pointerdown"/);
  assert.doesNotMatch(watchlistPicker, /fixed z-40/);

  assert.match(screenerExport, /<WalnutModal[\s\S]*Close export upgrade prompt/);
  assert.match(screenerOverlay, /<WalnutModal[\s\S]*Close upgrade prompt/);
  assert.match(savedViews, /<WalnutModal[\s\S]*Close saved view upgrade prompt/);
  assert.doesNotMatch(savedViews, /fixed inset-0 z-50/);
});
