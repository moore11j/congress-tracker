import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const pagePath = path.join(process.cwd(), "app", "watchlists", "[id]", "page.tsx");
const clientPath = path.join(process.cwd(), "components", "watchlists", "WatchlistDetailClient.tsx");
const apiPath = path.join(process.cwd(), "lib", "api.ts");
const serverAuthPath = path.join(process.cwd(), "lib", "serverAuth.ts");

const pageSource = fs.readFileSync(pagePath, "utf8");
const clientSource = fs.readFileSync(clientPath, "utf8");
const apiSource = fs.readFileSync(apiPath, "utf8");
const serverAuthSource = fs.readFileSync(serverAuthPath, "utf8");

test("watchlist detail renders a client fallback when SSR only has the auth hint", () => {
  assert.match(serverAuthSource, /if \(cookieStore\.get\(authHintCookieName\)\?\.value !== "1"\)/);
  assert.match(serverAuthSource, /return "";/);

  const fallbackIndex = pageSource.indexOf("if (!authToken)");
  const serverFetchIndex = pageSource.indexOf("await getWatchlist(watchlistId, authToken)");

  assert.ok(fallbackIndex > -1, "page should branch on missing server-readable token");
  assert.ok(serverFetchIndex > -1, "page should still SSR fetch when a backend session token exists");
  assert.ok(fallbackIndex < serverFetchIndex, "client fallback must run before the protected server fetch");
  assert.match(pageSource, /<WatchlistDetailClient watchlistId=\{watchlistId\} initialState=\{initialState\} initialAuthPending \/>/);
  assert.match(clientSource, /initialAuthPending \|\| hasClientAuthHint\(\)/);
});

test("client watchlist detail fetch relies on credentialed cookie auth", () => {
  assert.match(clientSource, /await getWatchlist\(watchlistId, undefined,/);
  assert.match(clientSource, /getWatchlistConfirmationEvents\(watchlistId, \{[\s\S]*?limit: 5/);
  assert.match(clientSource, /getWatchlistEvents\(watchlistId,/);
  assert.match(apiSource, /credentials:\s*fetchInit\.credentials \?\? "include"/);
  assert.match(apiSource, /return \{ Cookie: `\$\{backendSessionCookieName\}=\$\{sessionToken\}` \}/);
  assert.doesNotMatch(apiSource, /headers\.set\("Authorization"/);
  assert.doesNotMatch(apiSource, /Bearer \$\{/);
});

test("client watchlist detail handles auth and ownership failures without throwing during render", () => {
  assert.match(clientSource, /error instanceof ApiError \? error\.status : null/);
  assert.match(clientSource, /code === 401/);
  assert.match(clientSource, /Sign in to open this watchlist/);
  assert.match(clientSource, /code === 403/);
  assert.match(clientSource, /Access denied/);
  assert.match(clientSource, /code === 404/);
  assert.match(clientSource, /Watchlist not found/);
  assert.doesNotMatch(clientSource, /throw error/);
});
