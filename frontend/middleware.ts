import { NextResponse, type NextRequest } from "next/server";

const authSessionCookieName = "ct_session";
const authHintCookieName = "ct_auth_hint";
const protectedPrefixes = ["/watchlists", "/monitoring", "/signals", "/leaderboards"];

export function middleware(request: NextRequest) {
  const { pathname, search } = request.nextUrl;
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
  matcher: ["/watchlists/:path*", "/monitoring/:path*", "/signals/:path*", "/leaderboards/:path*"],
};
