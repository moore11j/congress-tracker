import { NextResponse, type NextRequest } from "next/server";
import { buildBackendProxyHeaders } from "../proxy";

const API_BASE = (
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE_URL ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev"
).replace(/\/+$/, "");

export function GET(request: NextRequest) {
  const token = request.nextUrl.searchParams.get("token") ?? "";
  const url = new URL("/account/verify-email", request.nextUrl.origin);
  if (token) url.searchParams.set("token", token);
  return NextResponse.redirect(url);
}

export async function POST(request: NextRequest) {
  const queryToken = request.nextUrl.searchParams.get("token") ?? "";
  let bodyToken = "";
  try {
    const body = (await request.clone().json()) as { token?: unknown };
    bodyToken = typeof body?.token === "string" ? body.token : "";
  } catch {
    bodyToken = "";
  }
  const token = queryToken || bodyToken;
  const url = new URL("/api/account/verify-email", API_BASE);

  const response = await fetch(url.toString(), {
    method: "POST",
    headers: buildBackendProxyHeaders(request, { fallbackRefererPath: "/account/verify-email" }),
    body: JSON.stringify({ token }),
    cache: "no-store",
  });
  const text = await response.text();
  return new NextResponse(text, {
    status: response.status,
    headers: {
      "content-type": response.headers.get("content-type") ?? "application/json",
    },
  });
}
