import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

const require = createRequire(import.meta.url);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function pageFor(cookies) {
  const cookieStore = new Map(Object.entries(cookies).map(([key, value]) => [key, { value }]));
  const guard = () => null;
  const dashboard = () => null;
  const load = (relative) => {
    const filename = path.join(root, relative);
    const source = ts.transpileModule(fs.readFileSync(filename, "utf8"), { compilerOptions: {
      jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true, module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022,
    } }).outputText;
    const module = { exports: {} };
    const localRequire = (id) => {
      if (id === "next/headers") return { cookies: async () => cookieStore };
      if (id === "next/navigation") return { redirect: (href) => { throw Object.assign(new Error("redirect"), { href }); } };
      if (id === "next/link") return () => null;
      if (id === "@/lib/serverAuth") return load("lib/serverAuth.ts");
      if (id === "@/components/auth/VerifiedSessionGuard") return { VerifiedSessionGuard: guard };
      if (id === "@/components/watchlists/WatchlistsDashboard") return { WatchlistsDashboard: dashboard };
      if (id === "@/components/discover/ResearchWorkspaceNav") return { ResearchWorkspaceNav: () => null };
      return require(id);
    };
    vm.runInNewContext(source, { module, exports: module.exports, require: localRequire, URLSearchParams }, { filename });
    return module.exports;
  };
  return { page: load("app/my-stocks/page.tsx").default, guard, dashboard };
}

function find(element, type) {
  if (!element || typeof element !== "object") return undefined;
  if (element.type === type) return element;
  const children = element.props?.children;
  for (const child of Array.isArray(children) ? children : [children]) {
    const match = find(child, type);
    if (match) return match;
  }
}

const query = { from: "discover", intent: "addTicker", symbol: "BRK.B", filter: ["one", "two"], returnTo: "/ticker/BRK.B?tab=chart" };
const expected = "/my-stocks?from=discover&intent=addTicker&symbol=BRK.B&filter=one&filter=two&returnTo=%2Fticker%2FBRK.B%3Ftab%3Dchart";

test("My Stocks guest redirect preserves complete query through the actual server auth helper", async () => {
  const { page } = pageFor({});
  await assert.rejects(page({ searchParams: Promise.resolve(query) }), error =>
    error.href === `/login?return_to=${encodeURIComponent(expected)}`);
});

test("My Stocks server token and client hint carry the same return path to session revalidation", async () => {
  for (const [cookies, authorized] of [[{ ct_session: "fixture-local-token" }, true], [{ ct_auth_hint: "1" }, false]]) {
    const { page, guard, dashboard } = pageFor(cookies);
    const element = await page({ searchParams: Promise.resolve(query) });
    assert.equal(element.type, guard);
    assert.equal(element.props.returnTo, expected);
    assert.equal(element.props.initiallyAuthorized, authorized);
    assert.equal(find(element, dashboard).props.initialAuthPending, !authorized);
    assert.equal(JSON.stringify(element).includes("fixture-local-token"), false);
  }
});

test("My Stocks uses its fixed internal path even when a query contains an external return target", async () => {
  const { page } = pageFor({ ct_auth_hint: "1" });
  const element = await page({ searchParams: Promise.resolve({ returnTo: "//outside.invalid", from: "a&b" }) });
  assert.equal(element.props.returnTo, "/my-stocks?returnTo=%2F%2Foutside.invalid&from=a%26b");
  assert.equal((await page({})).props.returnTo, "/my-stocks");
});
