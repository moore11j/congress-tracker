import { NextResponse, type NextRequest } from "next/server";

const authSessionCookieName = "ct_session";

function secureCookie(request: NextRequest): boolean {
  const proto = request.headers.get("x-forwarded-proto") ?? request.nextUrl.protocol.replace(":", "");
  return proto === "https";
}

export async function POST() {
  return NextResponse.json({ status: "unsupported" }, { status: 410 });
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
