import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

const root = new URL("../", import.meta.url);

async function text(path) {
  return readFile(new URL(path, root), "utf8");
}

test("manifest is Chrome Manifest V3 with narrow permissions", async () => {
  const manifest = JSON.parse(await text("manifest.json"));
  assert.equal(manifest.manifest_version, 3);
  assert.equal(manifest.background.type, "module");
  assert.ok(manifest.host_permissions.includes("https://ads.reddit.com/*"));
  assert.ok(!manifest.permissions.includes("webRequestBlocking"));
  assert.ok(!manifest.permissions.includes("cookies"));
});

test("extension uses official Walnut logo asset", async () => {
  const manifest = await text("manifest.json");
  const popup = await text("src/popup.html");
  assert.match(manifest, /assets\/walnut-markets-logo-lockup\.png/);
  assert.match(popup, /\.\.\/assets\/walnut-markets-logo-lockup\.png/);
});

test("popup supports clipboard actions and Reddit Ads Manager launch", async () => {
  const popup = await text("src/popup.js");
  assert.match(popup, /navigator\.clipboard\.writeText/);
  assert.match(popup, /https:\/\/ads\.reddit\.com\//);
  assert.match(popup, /fill-action/);
});

test("content script requires explicit fill click and supports undo", async () => {
  const script = await text("src/content-script.js");
  assert.match(script, /Fill approved Walnut draft/);
  assert.match(script, /addEventListener\("click", \(\) => fillFields/);
  assert.match(script, /undoFill/);
});

test("content script fails closed on Reddit DOM mismatch", async () => {
  const script = await text("src/content-script.js");
  assert.match(script, /Reddit Ads Manager fields were not recognized/);
  assert.match(script, /selectorVersion/);
});

test("content script never submits or clicks launch/payment controls", async () => {
  const script = await text("src/content-script.js");
  assert.doesNotMatch(script, /\.submit\(/);
  assert.doesNotMatch(script, /\.click\(/);
  assert.doesNotMatch(script, /querySelector\([^)]*(launch|payment|budget)/i);
  assert.match(script, /never submits/);
});
