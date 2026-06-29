import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");
const verifyPanel = fs.readFileSync(path.join(process.cwd(), "components", "auth", "VerifyEmailPanel.tsx"), "utf8");
const verificationNotice = fs.readFileSync(path.join(process.cwd(), "components", "auth", "EmailVerificationNotice.tsx"), "utf8");
const proxyHelper = fs.readFileSync(path.join(process.cwd(), "app", "api", "account", "proxy.ts"), "utf8");
const verifyRoute = fs.readFileSync(path.join(process.cwd(), "app", "api", "account", "verify-email", "route.ts"), "utf8");
const resendRoute = fs.readFileSync(path.join(process.cwd(), "app", "api", "account", "resend-verification", "route.ts"), "utf8");

function functionBody(source, name) {
  const match = source.match(new RegExp(`export async function ${name}[\\s\\S]*?\\n}`));
  assert.ok(match, `${name} function exists`);
  return match[0];
}

test("verify email API helper posts token JSON to the same-origin verify route", () => {
  const body = functionBody(apiSource, "verifyEmail");
  assert.match(body, /buildApiUrl\("\/api\/account\/verify-email"\)/);
  assert.match(body, /method:\s*"POST"/);
  assert.match(body, /headers:\s*\{\s*"Content-Type":\s*"application\/json"\s*\}/);
  assert.match(body, /body:\s*JSON\.stringify\(\{\s*token\s*\}\)/);
  assert.doesNotMatch(body, /buildApiUrl\("\/api\/account\/verify-email",\s*\{\s*token\s*\}\)/);
  assert.match(body, /resetClientApiCaches\(\)/);
  assert.match(body, /notifyAuthChanged\(\)/);
});

test("Next verify route redirects browser GET and proxies POST to backend verify", () => {
  assert.match(verifyRoute, /export function GET\(request: NextRequest\)/);
  assert.match(verifyRoute, /new URL\("\/account\/verify-email", request\.nextUrl\.origin\)/);
  assert.match(verifyRoute, /NextResponse\.redirect\(url\)/);
  assert.match(verifyRoute, /export async function POST\(request: NextRequest\)/);
  assert.match(verifyRoute, /new URL\("\/api\/account\/verify-email", API_BASE\)/);
  assert.match(verifyRoute, /method:\s*"POST"/);
  assert.match(verifyRoute, /buildBackendProxyHeaders\(request,\s*\{\s*fallbackRefererPath:\s*"\/account\/verify-email"\s*\}\)/);
  assert.match(verifyRoute, /body:\s*JSON\.stringify\(\{\s*token\s*\}\)/);
});

test("resend route proxies only to resend verification endpoint", () => {
  assert.match(resendRoute, /export async function POST\(request: NextRequest\)/);
  assert.match(resendRoute, /\/api\/account\/resend-verification/);
  assert.doesNotMatch(resendRoute, /\/api\/account\/verify-email/);
  assert.match(resendRoute, /method:\s*"POST"/);
  assert.match(resendRoute, /buildBackendProxyHeaders\(request,\s*\{\s*fallbackRefererPath:\s*"\/account\/settings"\s*\}\)/);
});

test("account API proxy builds CSRF-safe backend headers", () => {
  assert.match(proxyHelper, /const DEFAULT_APP_ORIGIN = "https:\/\/app\.walnutmarkets\.com"/);
  assert.match(proxyHelper, /const CSRF_TOKEN_HEADERS = \["x-csrf-token", "x-xsrf-token"\]/);
  assert.match(proxyHelper, /"content-type": "application\/json"/);
  assert.match(proxyHelper, /headers\.set\("cookie", cookie\)/);
  assert.match(proxyHelper, /headers\.set\("authorization", authorization\)/);
  assert.match(proxyHelper, /trustedHeaderOrigin\(request\.headers\.get\("origin"\), trustedOrigins\)/);
  assert.match(proxyHelper, /trustedHeaderOrigin\(request\.headers\.get\("referer"\), trustedOrigins\)/);
  assert.match(proxyHelper, /configuredAppOrigin\(\) \?\?/);
  assert.match(proxyHelper, /appOriginFromRequestUrl\(request\) \?\?/);
  assert.match(proxyHelper, /trustedReferer\(request\.headers\.get\("referer"\), trustedOrigins\) \?\?/);
  assert.match(proxyHelper, /trustedOrigins\.has\(origin\)/);
});

test("VerifyEmailPanel verifies only with a token and refreshes account state on success", () => {
  assert.match(verifyPanel, /if \(!token\) return;/);
  assert.match(verifyPanel, /verifyEmail\(token\)/);
  assert.match(verifyPanel, /getMe\(\{\s*force:\s*true,\s*source:\s*"VerifyEmailPanel"\s*\}\)/);
  assert.match(verifyPanel, /\/account\/settings\?verified=1/);
  assert.match(verifyPanel, /Your email is already verified\. Opening account settings\.\.\./);
  assert.match(verifyPanel, /Email verified\. Opening account settings\.\.\./);
});

test("VerifyEmailPanel resend and failures use friendly copy", () => {
  assert.match(verifyPanel, /resendVerificationEmail\(\)/);
  assert.match(verifyPanel, /We sent you a new verification link\./);
  assert.match(verifyPanel, /This verification link has expired\. Request a new link\./);
  assert.match(verifyPanel, /This verification link is invalid\. Request a new link\./);
  assert.match(verifyPanel, /This verification link could not be verified\. Request a new link\./);
  assert.doesNotMatch(verifyPanel, /setStatus\(error instanceof Error \? error\.message/);
  assert.doesNotMatch(verifyPanel, /Request failed \(405\)/);
});

test("Account Settings resend banner does not expose raw HTTP failures", () => {
  assert.match(verificationNotice, /resendVerificationEmail\(\)/);
  assert.match(verificationNotice, /We could not send a new verification link\. Please try again\./);
  assert.doesNotMatch(verificationNotice, /setStatus\(error instanceof Error \? error\.message/);
  assert.doesNotMatch(verificationNotice, /Request failed \(405\)/);
});
