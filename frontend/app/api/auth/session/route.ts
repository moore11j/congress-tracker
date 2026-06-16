import { NextResponse, type NextRequest } from "next/server";

const authSessionCookieName = "ct_session";
const sessionMaxAgeSeconds = 60 * 60 * 24 * 30;

function bearerToken(request: NextRequest): string | null {
  const auth = request.headers.get("authorization") ?? "";
  if (!auth.toLowerCase().startsWith("bearer ")) return null;
  const token = auth.slice("bearer ".length).trim();
  return token && token.includes(".") ? token : null;
}

function secureCookie(request: NextRequest): boolean {
  const proto = request.headers.get("x-forwarded-proto") ?? request.nextUrl.protocol.replace(":", "");
  return proto === "https";
}

export async function POST(request: NextRequest) {
  const token = bearerToken(request);
  if (!token) {
    return NextResponse.json({ status: "missing_token" }, { status: 401 });
  }

  const response = NextResponse.json({ status: "ok" });
  response.cookies.set({
    name: authSessionCookieName,
    value: token,
    httpOnly: true,
    secure: secureCookie(request),
    sameSite: "lax",
    path: "/",
    maxAge: sessionMaxAgeSeconds,
  });
  return response;
}

export async function DELETE(request: NextRequest) {
  const response = NextResponse.json({ status: "ok" });
  response.cookies.set({
    name: authSessionCookieName,
    value: "",
    httpOnly: true,
    secure: secureCookie(request),
    sameSite: "lax",
    path: "/",
    maxAge: 0,
  });
  return response;
}
