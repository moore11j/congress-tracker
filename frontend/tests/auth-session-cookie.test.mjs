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
  assert.match(apiSource, /credentials:\s*init\?\.credentials \?\? "include"/);
  assert.match(apiSource, /window\.localStorage\.getItem\(authTokenStorageKey\)/);
  assert.match(apiSource, /headers\.set\("Authorization", `Bearer \$\{token\}`\)/);
});

test("logout calls backend logout and clears transition storage", () => {
  assert.match(apiSource, /buildApiUrl\("\/api\/auth\/logout"\)/);
  assert.match(apiSource, /finally\s*\{\s*forgetAuthToken\(\);/);
  assert.match(apiSource, /document\.cookie = `\$\{authSessionCookieName\}=; Path=\/; SameSite=Lax; Max-Age=0`;/);
});

test("middleware and server auth use the session cookie convention", () => {
  assert.match(middlewareSource, /const authSessionCookieName = "ct_session"/);
  assert.match(middlewareSource, /request\.cookies\.get\(authSessionCookieName\)\?\.value/);
  assert.match(serverAuthSource, /const authSessionCookieName = "ct_session"/);
  assert.match(serverAuthSource, /cookieStore\.get\(authSessionCookieName\)\?\.value/);
});
