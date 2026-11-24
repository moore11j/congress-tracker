import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

const compile = (file, modules = {}, globals = {}) => {
  const exports = {};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 },
  }).outputText, { exports, URL, URLSearchParams, ...globals, require: name => {
    if (!(name in modules)) throw new Error(`Unexpected import: ${name}`);
    return modules[name];
  } });
  return exports;
};
const paths = compile("lib/returnPaths.ts");
const recovery = compile("lib/authRecovery.ts", { "./returnPaths": paths });
const preview = "https://congress-tracker-review.vercel.app";
const live = "https://app.walnutmarkets.com";
const authorization = (origin, path = "/auth/google/callback") =>
  `https://accounts.google.com/o/oauth2/v2/auth?redirect_uri=${encodeURIComponent(origin + path)}`;

test("production login retains app and canonical marketing origins", () => {
  for (const origin of [live, "https://walnutmarkets.com", "https://www.walnutmarkets.com"]) {
    assert.equal(recovery.googleSignInReturnsToCurrentSite(authorization(live), origin), true);
  }
});
test("preview login refuses the production callback and other previews", () => {
  for (const origin of [live, "https://other-preview.vercel.app"]) {
    assert.equal(recovery.googleSignInReturnsToCurrentSite(authorization(origin), preview), false);
  }
  assert.equal(recovery.googleSignInReturnsToCurrentSite(authorization(preview), preview), true);
});
test("malformed, absent, wrong-path and untrusted authorization URLs do not navigate", () => {
  for (const url of ["bad", "https://accounts.google.com/", authorization(preview, "/elsewhere"),
    authorization(preview).replace("accounts.google.com", "accounts.google.com.example.org")]) {
    assert.equal(recovery.googleSignInReturnsToCurrentSite(url, preview), false);
  }
});

const nodes = tree => !tree || typeof tree !== "object" ? [] : Array.isArray(tree) ? tree.flatMap(nodes) : [tree, ...nodes(tree.props?.children)];
function loginHarness(currentOrigin, callbackOrigin) {
  let cursor = 0;
  const slots = [], calls = [], writes = [];
  const react = {
    useState(initial) { const i = cursor++; if (!(i in slots)) slots[i] = initial; return [slots[i], value => { slots[i] = value; }]; },
    useRef(value) { return { current: value }; }, useMemo(fn) { return fn(); }, useEffect() {},
  };
  const jsx = (type, props) => ({ type, props });
  const url = authorization(callbackOrigin);
  const location = { origin: currentOrigin, set href(value) { writes.push(["navigate", value]); } };
  const exports = compile("components/auth/LoginRegisterPanel.tsx", {
    react, "react/jsx-runtime": { jsx, jsxs: jsx }, "next/link": { default: "a" },
    "next/navigation": { useRouter: () => ({}), useSearchParams: () => new URLSearchParams("return_to=%2Fmy-stocks%3Ffilter%3Da%26filter%3Db") },
    "@/lib/api": { getGoogleAuthUrl: async value => { calls.push(value); return { authorization_url: url }; } },
    "@/lib/authRecovery": { ...recovery, withAuthTimeout: promise => promise, rememberGoogleReturnPath: path => writes.push(["remember", path]) },
    "@/lib/returnPaths": paths, "@/lib/campaignAttribution": { campaignParamKeys: [] }, "@/lib/productAnalytics": {},
  }, { window: { location } });
  return { calls, writes, url, render() { cursor = 0; return exports.LoginRegisterPanel({}); } };
}
test("actual preview Google button remains on login with recovery link and no stored return hint", async () => {
  const h = loginHarness(preview, live);
  await nodes(h.render()).find(n => n.type === "button" && n.props.children?.includes("Continue with Google")).props.onClick();
  const tree = nodes(h.render());
  assert.deepEqual(h.calls, ["/my-stocks?filter=a&filter=b"]);
  assert.deepEqual(h.writes, []);
  assert.match(tree.find(n => n.props?.role === "status").props.children, /cannot return to this preview/);
  assert.equal(tree.find(n => n.props?.children === "Browse Discover preview").props.href, "/discover");
  assert.equal(tree.find(n => n.type === "button" && n.props.children?.includes("Continue with Google")).props.disabled, false);
});
test("actual production Google button preserves return query and navigates to unchanged authorization URL", async () => {
  const h = loginHarness(live, live);
  await nodes(h.render()).find(n => n.type === "button" && n.props.children?.includes("Continue with Google")).props.onClick();
  assert.deepEqual(h.calls, ["/my-stocks?filter=a&filter=b"]);
  assert.deepEqual(h.writes, [["remember", "/my-stocks?filter=a&filter=b"], ["navigate", h.url]]);
});
