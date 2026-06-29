import { NextResponse, type NextRequest } from "next/server";

const API_BASE = (
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE ??
  process.env.API_BASE_URL ??
  process.env.API_BASE ??
  "https://congress-tracker-api.fly.dev"
).replace(/\/+$/, "");

export async function POST(request: NextRequest) {
  let bodyText = "{}";
  try {
    bodyText = await request.text();
  } catch {
    bodyText = "{}";
  }

  const response = await fetch(`${API_BASE}/api/account/resend-verification`, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": request.headers.get("content-type") ?? "application/json",
      cookie: request.headers.get("cookie") ?? "",
    },
    body: bodyText || "{}",
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
