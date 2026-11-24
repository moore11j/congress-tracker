import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

// Execute the real component effects at their API boundary. Child components
// are opaque here; this is lifecycle coverage, not a browser/network capture.
const flush = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const nodes = tree => !tree || typeof tree !== "object" ? [] : Array.isArray(tree) ? tree.flatMap(nodes) : [tree, ...nodes(tree.props?.children)];
const same = (a, b) => a && b && a.length === b.length && a.every((v, i) => Object.is(v, b[i]));

function harness(file, exportName, api, query = "") {
  let cursor = 0, pending = [], mounted = true, lateWrites = 0;
  const slots = [], cleanups = [], sideEffects = [];
  const jsx = (type, props) => ({ type, props });
  const deny = name => () => { sideEffects.push(name); throw new Error(`Unexpected side effect: ${name}`); };
  const react = {
    useState(initial) {
      const i = cursor++;
      slots[i] ??= { value: typeof initial === "function" ? initial() : initial };
      return [slots[i].value, next => { if (!mounted) lateWrites++; slots[i].value = typeof next === "function" ? next(slots[i].value) : next; }];
    },
    useEffect(fn, deps) {
      const i = cursor++;
      if (!same(slots[i]?.deps, deps)) pending.push(() => { cleanups[i]?.(); slots[i] = { deps }; cleanups[i] = fn(); });
    },
    useMemo(fn, deps) { const i = cursor++; if (!same(slots[i]?.deps, deps)) slots[i] = { deps, value: fn() }; return slots[i].value; },
  };
  const modules = {
    react, "react/jsx-runtime": { jsx, jsxs: jsx }, "next/link": { default: "a" },
    "next/navigation": { useRouter: () => ({ push: deny("navigation") }), useSearchParams: () => new URLSearchParams(query) },
    "@/lib/api": new Proxy({ hasClientAuthHint: () => true, ...api }, { get: (target, name) => name in target ? target[name] : deny(name) }),
    "@/lib/entitlements": { defaultEntitlements: { tier: "free" } },
    "@/lib/ticker": { normalizeTickerSymbol: value => value?.trim().toUpperCase() || null },
    "@/lib/watchlistNames": { nextDefaultWatchlistName: () => "Watchlist" },
  };
  const exports = {};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 },
  }).outputText, { exports, require: name => modules[name] ?? new Proxy({}, { get: (_, key) => String(key) }),
    URLSearchParams, Date, Intl, window: { sessionStorage: { setItem: deny("storage write") } },
    fetch: deny("fetch"), setTimeout: deny("timer"), setInterval: deny("polling") });
  return {
    render(props = {}) { cursor = 0; pending = []; return exports[exportName](props); },
    commit() { const effects = pending; pending = []; effects.forEach(fn => fn()); },
    unmount() { mounted = false; cleanups.forEach(fn => fn?.()); assert.deepEqual(sideEffects, []); },
    get lateWrites() { assert.deepEqual(sideEffects, []); return lateWrites; },
  };
}

const dashboard = (api, query) => harness("components/watchlists/WatchlistsDashboard.tsx", "WatchlistsDashboard", api, query);
const discovery = api => harness("components/discover/DiscoveryStocks.tsx", "DiscoveryStocks", api);

for (const tier of ["free", "premium", "pro", "admin"]) {
  test(`My Stocks ${tier} load preserves saved rows and only reads entitlements/watchlists`, async () => {
    const saved = [{ id: 7, name: "Semiconductors", symbols: ["NBIS"], unread_count: 2 }];
    const calls = [];
    const h = dashboard({ getEntitlements: async () => { calls.push("entitlements"); return { tier }; }, listWatchlists: async () => { calls.push("watchlists"); return saved; } });
    h.render({ initialWatchlists: [], initialAuthPending: true }); h.commit(); await flush();
    const tree = h.render({ initialWatchlists: [], initialAuthPending: true }); h.commit();
    assert.equal(nodes(tree).find(n => n.type === "WatchlistList").props.items, saved);
    assert.equal(nodes(tree).find(n => n.type === "WatchlistCreateForm").props.entitlements.tier, tier);
    assert.deepEqual(calls, ["entitlements", "watchlists"]);
    h.unmount();
  });
}

test("an add-ticker URL opens the form but does not create or follow on mount", async () => {
  const h = dashboard({ getEntitlements: async () => ({ tier: "pro" }), listWatchlists: async () => [] }, "create=1&intent=addTicker&symbol=NBIS&returnTo=%2Fticker%2FNBIS");
  h.render({ initialWatchlists: [] }); h.commit(); await flush();
  const form = nodes(h.render({ initialWatchlists: [] })).find(n => n.type === "WatchlistCreateForm");
  assert.equal(form.props.open, true);
  assert.equal(form.props.pendingTickerSymbol, "NBIS");
  h.unmount();
});

test("failed watchlist load stays an error until an explicit successful retry", async () => {
  let calls = 0;
  const saved = [{ id: 7, name: "Saved" }];
  const h = dashboard({ getEntitlements: async () => ({ tier: "free" }), listWatchlists: async () => { if (++calls === 1) throw new Error("offline"); return saved; } });
  h.render({ initialWatchlists: [] }); h.commit(); await flush();
  const failed = h.render({ initialWatchlists: [] }); h.commit(); await flush();
  assert.equal(failed.props.role, "status");
  assert.equal(nodes(failed).some(n => n.type === "WatchlistList"), false);
  assert.equal(calls, 1);
  nodes(failed).find(n => n.type === "button").props.onClick();
  h.render({ initialWatchlists: [] }); h.commit(); await flush();
  assert.equal(nodes(h.render({ initialWatchlists: [] })).find(n => n.type === "WatchlistList").props.items, saved);
  assert.equal(calls, 2);
  h.unmount();
});

test("unmounted My Stocks ignores late responses", async () => {
  const ent = deferred(), lists = deferred();
  const h = dashboard({ getEntitlements: () => ent.promise, listWatchlists: () => lists.promise });
  h.render({ initialWatchlists: [] }); h.commit(); h.unmount();
  ent.resolve({ tier: "pro" }); lists.resolve([{ id: 7 }]); await flush();
  assert.equal(h.lateWrites, 0);
});

test("Discover loads cached rankings once and retries only on request", async () => {
  let calls = 0;
  const snapshot = { items: [], preview: true };
  const h = discovery({ getCachedLeaderboard: async section => { assert.equal(section, "top-stocks"); if (++calls === 1) throw new Error("offline"); return snapshot; } });
  h.render(); h.commit(); await flush();
  const failed = h.render(); h.commit(); await flush();
  assert.equal(failed.props.failed, true);
  assert.equal(calls, 1);
  failed.props.onRetry(); h.render(); h.commit(); await flush();
  const recovered = h.render(); h.commit();
  assert.equal(recovered.props.snapshot, snapshot);
  assert.equal(recovered.props.failed, false);
  assert.equal(recovered.props.loading, false);
  assert.equal(calls, 2);
  h.unmount();
});

test("unmounted Discover ignores a late ranking response", async () => {
  const result = deferred();
  const h = discovery({ getCachedLeaderboard: () => result.promise });
  h.render(); h.commit(); h.unmount(); result.resolve({ items: [] }); await flush();
  assert.equal(h.lateWrites, 0);
});
