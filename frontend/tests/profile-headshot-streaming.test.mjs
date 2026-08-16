import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

for (const route of ["member/[slug]", "insider/[slug]"]) {
  test(`${route} streams the existing Wikipedia headshot behind its initials fallback`, () => {
    const page = readFileSync(join(process.cwd(), `app/${route}/page.tsx`), "utf8");

    assert.match(page, /import \{ Suspense \} from "react"/);
    assert.match(page, /const headshotPromise = resolveWikipediaHeadshot/);
    assert.match(page, /<Suspense fallback=\{<\w+HeadshotFallback/);
    assert.match(page, /headshotPromise=\{headshotPromise\}/);
    assert.doesNotMatch(page, /headshotResult/);
  });
}
