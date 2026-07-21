import { NextResponse, type NextRequest } from "next/server";
import { isBioguideId, nameToSlug } from "./lib/memberSlug";

const authSessionCookieName = "ct_session";
const authHintCookieName = "ct_auth_hint";
const landingHeaderName = "x-walnut-public-landing";
const protectedPrefixes = ["/admin", "/account", "/screener", "/backtesting", "/watchlists", "/monitoring", "/signals", "/leaderboards"];
const publicStaticPaths = new Set([
  "/landing",
  "/about",
  "/pricing",
  "/terms",
  "/privacy",
  "/faq",
  "/congress-trades",
  "/insider-trading-tracker",
  "/government-contracts",
  "/institutional-filings",
  "/stock-confirmation-score",
  "/market-intelligence-terminal",
]);
const publicAccountPaths = new Set(["/account/verify-email", "/account/reactivate"]);
const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE_URL ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";
const canonicalMarketingHost = "walnutmarkets.com";
const canonicalMarketingHosts = new Set([canonicalMarketingHost]);
const legacyMarketingHosts = new Set(["walnut-intel.com", "www.walnut-intel.com", "www.walnutmarkets.com"]);
const publicLandingHosts = new Set([canonicalMarketingHost]);
const appHost = "app.walnutmarkets.com";
const terminalRouteFamilies = ["ticker", "insider", "member", "institution"] as const;
const robotsDisallowPaths = [
  "/insider/",
  "/member/",
  "/institution/",
  "/signals",
  "/screener",
  "/watchlists",
  "/monitoring",
  "/feed",
  "/account",
  "/billing",
  "/admin",
];
const noindexAppRoutePrefixes = [
  "/ticker/",
  "/insider/",
  "/member/",
  "/institution/",
  "/signals",
  "/screener",
  "/watchlists",
  "/monitoring",
  "/feed",
  "/account",
  "/billing",
  "/admin",
];

function routeFamily(pathname: string): string {
  const normalized = (pathname || "/").toLowerCase();
  const segment = normalized.split("/").filter(Boolean)[0] ?? "feed";
  if ((terminalRouteFamilies as readonly string[]).includes(segment)) return segment;
  if (segment === "signals" || segment === "screener") return segment;
  if (normalized === "/" || segment === "feed") return "feed";
  return segment || "unknown";
}

function isTerminalRoute(pathname: string): boolean {
  return (terminalRouteFamilies as readonly string[]).some((family) => pathname === `/${family}` || pathname.startsWith(`/${family}/`));
}

function isPublicTickerRoute(pathname: string): boolean {
  const normalized = (pathname || "/").toLowerCase();
  return normalized === "/ticker" || normalized.startsWith("/ticker/");
}

function isPublicMarketingAsset(pathname: string): boolean {
  const normalized = (pathname || "/").toLowerCase();
  return normalized === "/sitemap.xml"
    || normalized.startsWith("/og/")
    || normalized === "/walnut-intel-logo-mark.png"
    || normalized === "/walnut-intel-logo-mark.svg"
    || normalized === "/apple-touch-icon.png";
}

function isNoindexAppRoute(pathname: string): boolean {
  const normalized = (pathname || "/").toLowerCase();
  return noindexAppRoutePrefixes.some((prefix) => {
    const exact = prefix.replace(/\/$/, "");
    return normalized === exact || normalized.startsWith(`${exact}/`);
  });
}

function withNoindex(response: NextResponse): NextResponse {
  response.headers.set("x-robots-tag", "noindex, nofollow");
  return response;
}

function robotsTxtResponse(host: string): NextResponse {
  const disallow = robotsDisallowPaths.map((path) => `Disallow: ${path}`).join("\n");
  const marketingAllow = publicLandingHosts.has(host)
    ? "\nAllow: /\nAllow: /about\nAllow: /pricing\nAllow: /faq\nAllow: /terms\nAllow: /privacy\nAllow: /congress-trades\nAllow: /insider-trading-tracker\nAllow: /government-contracts\nAllow: /institutional-filings\nAllow: /stock-confirmation-score\nAllow: /market-intelligence-terminal\nAllow: /ticker/\n"
    : "\n";
  const sitemap = publicLandingHosts.has(host) ? "\nSitemap: https://walnutmarkets.com/sitemap.xml\n" : "";
  return new NextResponse(`User-agent: *${marketingAllow}${disallow}${sitemap}`, {
    status: 200,
    headers: {
      "cache-control": "public, max-age=300",
      "content-type": "text/plain; charset=utf-8",
    },
  });
}

function isPrefetchRequest(request: NextRequest): boolean {
  const headers = request.headers;
  return (
    headers.get("purpose")?.toLowerCase() === "prefetch" ||
    headers.get("sec-purpose")?.toLowerCase().includes("prefetch") === true ||
    headers.get("next-router-prefetch") === "1" ||
    headers.get("x-middleware-prefetch") === "1" ||
    headers.get("x-nextjs-data") === "1"
  );
}

function isBotUserAgent(userAgent: string): boolean {
  return /bot|crawler|spider|slurp|duckduckbot|baiduspider|yandex|semrush|ahrefs|bytespider|gptbot|claudebot|anthropic|perplexity|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|whatsapp|preview/i.test(userAgent);
}

function isInteractiveBrowserUserAgent(userAgent: string): boolean {
  const ua = userAgent.toLowerCase();
  if (!ua) return false;
  if (/bot|crawler|spider|headless|preview|prerender|curl|wget|python|go-http|uptime|monitor/.test(ua)) return false;
  return /mozilla|chrome|safari|firefox|edg\//.test(ua);
}

function safeRefererPath(referer: string, request: NextRequest): string {
  if (!referer) return "";
  try {
    return new URL(referer, request.nextUrl).pathname;
  } catch {
    return "";
  }
}

function terminalShellResponse(pathname: string, host: string, reason: "bot" | "prefetch" | "inactive"): NextResponse {
  const family = routeFamily(pathname);
  const body = reason === "prefetch"
    ? null
    : `<!doctype html><html><head><meta name="robots" content="noindex,nofollow"><title>Walnut Market Terminal</title></head><body><main><h1>Walnut Market Terminal</h1><p>This app page is available to interactive users.</p></main></body></html>`;
  const response = new NextResponse(body, {
    status: reason === "prefetch" ? 204 : 200,
    headers: {
      "cache-control": "no-store",
      "x-robots-tag": "noindex, nofollow",
      "x-walnut-terminal-shell": reason,
    },
  });
  console.info(
    "terminal_ssr_bypass",
    JSON.stringify({
      path: pathname,
      host,
      family,
      reason,
    }),
  );
  return response;
}

async function resolveMemberCanonicalSlug(slug: string): Promise<string | null> {
  if (!isBioguideId(slug)) return null;

  try {
    const response = await fetch(`${API_BASE}/api/members/by-slug/${encodeURIComponent(slug)}?include_trades=0`, {
      headers: { accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) return null;
    const data = await response.json();
    const name = typeof data?.member?.name === "string" ? data.member.name : "";
    const canonicalSlug = name ? nameToSlug(name) : "";
    return canonicalSlug && canonicalSlug !== slug ? canonicalSlug : null;
  } catch {
    return null;
  }
}

export async function middleware(request: NextRequest) {
  const { pathname, search } = request.nextUrl;
  const host = (request.headers.get("x-forwarded-host") ?? request.headers.get("host") ?? "").split(":")[0]?.toLowerCase();
  const requestHeaders = new Headers(request.headers);
  const userAgent = request.headers.get("user-agent") ?? "";
  const referer = request.headers.get("referer") ?? "";
  const hasBackendSession = Boolean(request.cookies.get(authSessionCookieName)?.value);
  const hasAuthHint = request.cookies.get(authHintCookieName)?.value === "1";
  const prefetch = isPrefetchRequest(request);
  const bot = isBotUserAgent(userAgent);
  const family = routeFamily(pathname);
  const shouldNoindex = host === appHost && isNoindexAppRoute(pathname);
  const forwardedProto = request.headers.get("x-forwarded-proto")?.split(",")[0]?.trim().toLowerCase();
  const requestProto = forwardedProto || request.nextUrl.protocol.replace(/:$/, "");
  const isHttpCanonicalMarketingRequest = host === canonicalMarketingHost && requestProto === "http";

  if (legacyMarketingHosts.has(host) || isHttpCanonicalMarketingRequest) {
    const canonicalUrl = request.nextUrl.clone();
    canonicalUrl.protocol = "https:";
    canonicalUrl.hostname = canonicalMarketingHost;
    canonicalUrl.port = "";
    return NextResponse.redirect(canonicalUrl, 301);
  }

  if (pathname === "/robots.txt") {
    return robotsTxtResponse(host);
  }

  if (isTerminalRoute(pathname)) {
    console.info(
      "terminal_page_request",
      JSON.stringify({
        path: pathname,
        host,
        family,
        referer: safeRefererPath(referer, request),
        user_agent: userAgent.slice(0, 180),
        bot,
        prefetch,
        authenticated: hasBackendSession || hasAuthHint,
      }),
    );
  }

  const isMarketingStaticPage = publicStaticPaths.has(pathname) && publicLandingHosts.has(host);
  if (isMarketingStaticPage || publicAccountPaths.has(pathname)) {
    requestHeaders.set(landingHeaderName, "1");
    const response = NextResponse.next({
      request: {
        headers: requestHeaders,
      },
    });
    return shouldNoindex ? withNoindex(response) : response;
  }

  if (pathname === "/" && publicLandingHosts.has(host)) {
    requestHeaders.set(landingHeaderName, "1");
    const landingUrl = request.nextUrl.clone();
    landingUrl.pathname = "/landing";
    return NextResponse.rewrite(landingUrl, {
      request: {
        headers: requestHeaders,
      },
    });
  }

  if (canonicalMarketingHosts.has(host) && isPublicMarketingAsset(pathname)) {
    return NextResponse.next();
  }

  if (canonicalMarketingHosts.has(host) && isPublicTickerRoute(pathname)) {
    return NextResponse.next();
  }

  if (publicLandingHosts.has(host) && !publicStaticPaths.has(pathname) && !publicAccountPaths.has(pathname)) {
    const appUrl = request.nextUrl.clone();
    appUrl.protocol = "https:";
    appUrl.host = appHost;
    return NextResponse.redirect(appUrl, 307);
  }

  if (isTerminalRoute(pathname) && !isPublicTickerRoute(pathname) && !hasBackendSession && !hasAuthHint && (prefetch || bot || !isInteractiveBrowserUserAgent(userAgent))) {
    return terminalShellResponse(pathname, host, prefetch ? "prefetch" : bot ? "bot" : "inactive");
  }

  const memberMatch = pathname.match(/^\/member\/([^/]+)\/?$/);
  if (memberMatch) {
    const slug = (memberMatch[1] ?? "").trim();
    const canonicalSlug = await resolveMemberCanonicalSlug(slug);
    if (canonicalSlug) {
      const redirectUrl = request.nextUrl.clone();
      redirectUrl.pathname = `/member/${canonicalSlug}`;
      const response = NextResponse.redirect(redirectUrl, 307);
      return shouldNoindex ? withNoindex(response) : response;
    }
  }

  const protectedRoute = protectedPrefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
  if (!protectedRoute || hasBackendSession || hasAuthHint) {
    const response = NextResponse.next();
    return shouldNoindex ? withNoindex(response) : response;
  }

  const loginUrl = request.nextUrl.clone();
  loginUrl.pathname = "/login";
  loginUrl.search = "";
  loginUrl.searchParams.set("return_to", `${pathname}${search}`);
  const response = NextResponse.redirect(loginUrl);
  return shouldNoindex ? withNoindex(response) : response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico|apple-icon.png|icon.png).*)"],
};
