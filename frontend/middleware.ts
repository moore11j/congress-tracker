import { NextResponse, type NextRequest } from "next/server";
import { isBioguideId, nameToSlug } from "./lib/memberSlug";

const authSessionCookieName = "ct_session";
const authHintCookieName = "ct_auth_hint";
const protectedPrefixes = ["/watchlists", "/monitoring", "/signals", "/leaderboards"];
const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev";

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
  const memberMatch = pathname.match(/^\/member\/([^/]+)\/?$/);
  if (memberMatch) {
    const slug = (memberMatch[1] ?? "").trim();
    const canonicalSlug = await resolveMemberCanonicalSlug(slug);
    if (canonicalSlug) {
      const redirectUrl = request.nextUrl.clone();
      redirectUrl.pathname = `/member/${canonicalSlug}`;
      return NextResponse.redirect(redirectUrl, 307);
    }
  }

  const protectedRoute = protectedPrefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
  const hasBackendSession = Boolean(request.cookies.get(authSessionCookieName)?.value);
  const hasAuthHint = request.cookies.get(authHintCookieName)?.value === "1";
  if (!protectedRoute || hasBackendSession || hasAuthHint) {
    return NextResponse.next();
  }

  const loginUrl = request.nextUrl.clone();
  loginUrl.pathname = "/login";
  loginUrl.search = "";
  loginUrl.searchParams.set("return_to", `${pathname}${search}`);
  return NextResponse.redirect(loginUrl);
}

export const config = {
  matcher: ["/member/:path*", "/watchlists/:path*", "/monitoring/:path*", "/signals/:path*", "/leaderboards/:path*"],
};
