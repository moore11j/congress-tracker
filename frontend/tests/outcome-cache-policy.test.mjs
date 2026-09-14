import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const source = readFileSync(new URL("../lib/api.ts", import.meta.url), "utf8");
const start = source.indexOf("async function fetchOutcomePublicJson<");
const end = source.indexOf("\nexport async function getOutcomeLedgerOverview", start);
assert.ok(start >= 0 && end > start);
const script = ts.transpileModule(source.slice(start, end), {
  compilerOptions: { target: ts.ScriptTarget.ES2022 },
}).outputText;

class ApiError extends Error {
  constructor(status) { super("upstream unavailable"); this.status = status; }
}

for (const browser of [false, true]) {
  test(`Outcome ${browser ? "browser requests respect HTTP expiry" : "server requests retain Next caching"}, including retries`, async () => {
    const calls = [];
    const context = vm.createContext({
      ...(browser ? { window: {} } : {}),
      ApiError,
      setTimeout: (callback) => callback(),
      fetchPublicJson: async (_url, init) => {
        calls.push(init);
        if (calls.length === 1) throw new ApiError(503);
        return { completed_events: 3061 };
      },
    });
    vm.runInContext(script, context);
    const result = await context.fetchOutcomePublicJson("/api/outcomes/snapshots", {
      cache: "force-cache", next: { revalidate: 300 },
    });
    assert.equal(result.completed_events, 3061);
    assert.equal(calls.length, 2);
    assert.deepEqual(calls.map((call) => call.cache), Array(2).fill(browser ? "default" : "force-cache"));
    assert.ok(calls.every((call) => call.next.revalidate === 300));
  });
}
