import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import path from "node:path";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
const require = createRequire(import.meta.url);

function harness(fail = false) {
  const api = { getSeoSnapshotIndex: async () => {
    if (fail) throw new Error("upstream unavailable");
    return { items: Array.from({ length: 205 }, (_, i) => ({ indexable: true, entity_key: `S${String(i).padStart(3, "0")}`, canonical_path: `/ticker/S${String(i).padStart(3, "0")}`, payload: {} })) };
  } };
  const modules = new Map();
  function load(file) {
    if (modules.has(file)) return modules.get(file);
    const exports = {}; modules.set(file, exports);
    const source = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
    vm.runInNewContext(source, { exports, URL, console, process, require: name => {
      if (name === "@/lib/api") return api;
      if (name === "react") return { ...React, cache: fn => fn };
      if (name === "next/cache") return { unstable_cache: fn => fn };
      if (name === "next/navigation") return { notFound: () => { throw new Error("NOT_FOUND"); } };
      if (name === "next/link") return ({ children, prefetch, ...props }) => React.createElement("a", props, children);
      if (name.startsWith("@/")) return load(`${name.slice(2)}.ts`);
      return require(name);
    } }, { filename: file });
    return exports;
  }
  return load;
}

test("directory pagination exposes every profile exactly once through real HTML anchors", async () => {
  const load = harness();
  const page = load("app/explore/[...segments]/page.tsx");
  const links = [];
  for (let i = 1; i <= 3; i++) {
    const params = Promise.resolve({ segments: i === 1 ? ["stocks"] : ["stocks", String(i)] });
    const html = renderToStaticMarkup(await page.default({ params }));
    links.push(...Array.from(html.matchAll(/href="(\/ticker\/S\d+)"/g), match => match[1]));
    assert.match(html, /href="\/explore\/stocks\/3"/);
    const metadata = await page.generateMetadata({ params });
    assert.equal(metadata.alternates.canonical, `https://app.walnutmarkets.com/explore/stocks${i === 1 ? "" : `/${i}`}`);
    assert.equal(metadata.robots.index, true);
  }
  assert.equal(links.length, 205);
  assert.equal(new Set(links).size, 205);
});

test("directory rejects empty, duplicated, malformed and out-of-range page routes", async () => {
  const page = harness()("app/explore/[...segments]/page.tsx");
  for (const segments of [["unknown"], ["stocks", "1"], ["stocks", "01"], ["stocks", "0"], ["stocks", "4"], ["stocks", "2", "extra"]]) {
    await assert.rejects(page.default({ params: Promise.resolve({ segments }) }), /NOT_FOUND/);
  }
});

test("directory keeps verified canonical corrections and excludes foreign or filtered URLs", () => {
  const { normalizeDirectoryEntries } = harness()("lib/publicDirectory.ts");
  const rows = normalizeDirectoryEntries([
    { path: "/insider/martina-hundmejean-0001223622", name: "Martina Hundmejean" },
    { path: "/insider/hundmejean-martina-0001223622", name: "Martina Hundmejean" },
    { path: "/ticker/NVDA?lookback=30", name: "NVIDIA" },
    { path: "https://example.com/ticker/NVDA", name: "Foreign" },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].path, "/insider/hundmejean-martina-0001223622");
});

test("upstream failure does not masquerade as an empty successful directory", async () => {
  const { getDirectoryEntries } = harness(true)("lib/publicDirectory.ts");
  await assert.rejects(getDirectoryEntries("stocks"), /upstream unavailable/);
});

test("research lastmod respects actual updates without inventing a fresh date", () => {
  const { researchLastmod } = harness()("lib/researchLastmod.ts");
  assert.equal(researchLastmod({ slug: "untouched", publishedAt: "2026-07-20" }), "2026-07-20");
  assert.equal(researchLastmod({ slug: "untouched", publishedAt: "2026-07-20", updatedAt: "2026-08-20" }), "2026-08-20");
  assert.equal(researchLastmod({ slug: "public-companies-winning-nasa-contracts", publishedAt: "2026-08-20" }), "2026-09-25");
  assert.equal(researchLastmod({ slug: "unknown" }), "");
});
