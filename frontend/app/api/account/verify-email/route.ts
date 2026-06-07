import { NextResponse, type NextRequest } from "next/server";

export function GET(request: NextRequest) {
  const token = request.nextUrl.searchParams.get("token") ?? "";
  const url = new URL("/account/verify-email", request.nextUrl.origin);
  if (token) url.searchParams.set("token", token);
  return NextResponse.redirect(url);
}
