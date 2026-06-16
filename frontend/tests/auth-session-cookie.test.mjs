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

test("api client includes credentials while preserving bearer-token compatibility", () => {
  assert.match(apiSource, /credentials:\s*fetchInit\.credentials \?\? "include"/);
  assert.match(apiSource, /window\.localStorage\.getItem\(authTokenStorageKey\)/);
  assert.match(apiSource, /headers\.set\("Authorization", `Bearer \$\{token\}`\)/);
});

test("rememberAuthToken never writes the auth token to the ct_session cookie from client JavaScript", () => {
  const rememberBody = apiSource.match(/function rememberAuthToken\(token: string\) \{([\s\S]*?)\n\}/)?.[1] ?? "";

  assert.match(apiSource, /export const backendSessionCookieName = "ct_session"/);
  assert.match(apiSource, /export const authHintCookieName = "ct_auth_hint"/);
  assert.match(rememberBody, /window\.localStorage\.setItem\(authTokenStorageKey, token\)/);
  assert.match(rememberBody, /document\.cookie = `\$\{authHintCookieName\}=1; Path=\/; SameSite=Lax; Max-Age=/);
  assert.doesNotMatch(rememberBody, /backendSessionCookieName|ct_session|encodeURIComponent\(token\)/);
});

test("same-origin session bridge sets the server-readable auth cookie as httpOnly", () => {
  const bridgePath = path.join(process.cwd(), "app", "api", "auth", "session", "route.ts");
  const bridgeSource = fs.readFileSync(bridgePath, "utf8");

  assert.match(bridgeSource, /const authSessionCookieName = "ct_session"/);
  assert.match(bridgeSource, /export async function POST/);
  assert.match(bridgeSource, /response\.cookies\.set\(\{/);
  assert.match(bridgeSource, /httpOnly: true/);
  assert.match(bridgeSource, /sameSite: "lax"/);
  assert.match(bridgeSource, /export async function DELETE/);
  assert.match(apiSource, /export async function syncServerAuthSession/);
  assert.match(apiSource, /fetch\("\/api\/auth\/session", \{\s*method: "POST"/);
  assert.match(apiSource, /await syncServerAuthSession\(response\.token\)/);
});

test("logout calls backend logout and clears transition storage", () => {
  const forgetBody = apiSource.match(/function forgetAuthToken\(\) \{([\s\S]*?)\n\}/)?.[1] ?? "";

  assert.match(apiSource, /buildApiUrl\("\/api\/auth\/logout"\)/);
  assert.match(apiSource, /finally\s*\{\s*forgetAuthToken\(\);/);
  assert.match(forgetBody, /window\.localStorage\.removeItem\(authTokenStorageKey\)/);
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
