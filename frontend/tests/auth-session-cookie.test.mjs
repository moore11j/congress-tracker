import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const apiPath = path.join(process.cwd(), "lib", "api.ts");
const middlewarePath = path.join(process.cwd(), "middleware.ts");
const serverAuthPath = path.join(process.cwd(), "lib", "serverAuth.ts");

const apiSource = fs.readFileSync(apiPath, "utf8");
const middlewareSource = fs.readFileSync(middlewarePath, "utf8");
const serverAuthSource = fs.readFileSync(serverAuthPath, "utf8");

test("api client includes credentials and does not attach localStorage bearer tokens", () => {
  assert.match(apiSource, /credentials:\s*fetchInit\.credentials \?\? "include"/);
  assert.match(apiSource, /function authHeaders\(sessionToken\?: string \| null\)/);
  assert.match(apiSource, /return \{ Cookie: `\$\{backendSessionCookieName\}=\$\{sessionToken\}` \}/);
  assert.doesNotMatch(apiSource, /headers\.set\("Authorization"/);
  assert.doesNotMatch(apiSource, /Bearer \$\{/);
  assert.doesNotMatch(apiSource, /localStorage\.getItem\(.*authToken/);
});

test("authenticated-session hint never stores or exposes a raw session token", () => {
  const rememberBody = apiSource.match(/function rememberAuthenticatedSession\(\) \{([\s\S]*?)\n\}/)?.[1] ?? "";

  assert.match(apiSource, /export const backendSessionCookieName = "ct_session"/);
  assert.match(apiSource, /export const authHintCookieName = "ct_auth_hint"/);
  assert.match(rememberBody, /document\.cookie = `\$\{authHintCookieName\}=1; Path=\/; SameSite=Lax; Max-Age=/);
  assert.doesNotMatch(rememberBody, /localStorage\.setItem|backendSessionCookieName|ct_session|token|Authorization|Bearer/);
});

test("same-origin legacy session route no longer accepts bearer tokens", () => {
  const bridgePath = path.join(process.cwd(), "app", "api", "auth", "session", "route.ts");
  const bridgeSource = fs.readFileSync(bridgePath, "utf8");

  assert.match(bridgeSource, /const authSessionCookieName = "ct_session"/);
  assert.match(bridgeSource, /export async function POST/);
  assert.match(bridgeSource, /status: "unsupported"/);
  assert.match(bridgeSource, /status: 410/);
  assert.match(bridgeSource, /export async function DELETE/);
  assert.doesNotMatch(bridgeSource, /authorization|bearer|Bearer|value: token|bearerToken/i);
  assert.doesNotMatch(apiSource, /syncServerAuthSession|response\.token|method: "POST"[\s\S]*\/api\/auth\/session/);
});

test("logout calls backend logout and clears legacy transition storage", () => {
  const forgetBody = apiSource.match(/function forgetAuthenticatedSession\(\) \{([\s\S]*?)\n\}/)?.[1] ?? "";

  assert.match(apiSource, /buildApiUrl\("\/api\/auth\/logout"\)/);
  assert.match(apiSource, /finally\s*\{\s*forgetAuthenticatedSession\(\);/);
  assert.match(apiSource, /function clearLegacyAuthStorage\(\)/);
  assert.match(apiSource, /window\.localStorage\.removeItem\(legacyAuthTokenStorageKey\)/);
  assert.match(apiSource, /window\.sessionStorage\.removeItem\(legacyServerSessionSyncStorageKey\)/);
  assert.match(forgetBody, /document\.cookie = `\$\{backendSessionCookieName\}=; Path=\/; SameSite=Lax; Max-Age=0`;/);
  assert.match(forgetBody, /document\.cookie = `\$\{authHintCookieName\}=; Path=\/; SameSite=Lax; Max-Age=0`;/);
});

test("middleware uses ct_auth_hint only as a redirect hint", () => {
  assert.match(middlewareSource, /const authSessionCookieName = "ct_session"/);
  assert.match(middlewareSource, /const authHintCookieName = "ct_auth_hint"/);
  assert.match(middlewareSource, /hasBackendSession \|\| hasAuthHint/);
  assert.doesNotMatch(middlewareSource, /Authorization|Bearer|decodeURIComponent/);
});

test("server auth returns backend session token or only a non-token hint", () => {
  assert.match(serverAuthSource, /const authSessionCookieName = "ct_session"/);
  assert.match(serverAuthSource, /const authHintCookieName = "ct_auth_hint"/);
  assert.match(serverAuthSource, /return "";/);
  assert.doesNotMatch(serverAuthSource, /Authorization|Bearer|decodeURIComponent/);
});
