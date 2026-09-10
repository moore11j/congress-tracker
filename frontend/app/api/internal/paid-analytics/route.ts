import { createHmac, timingSafeEqual } from "node:crypto";
import { analytics } from "@heycatch/sdk/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const claimUrl = "https://congress-tracker-api.fly.dev/api/internal/analytics/paid/claim";

function signature(body: string, timestamp: string, scope: string, secret: string) {
  return createHmac("sha256", secret).update(`${timestamp}.${scope}.${body}`).digest("hex");
}

export async function POST(request: Request): Promise<Response> {
  const secret = process.env.ANALYTICS_FORWARDING_SECRET || "";
  const projectKey = process.env.NEXT_PUBLIC_HEYCATCH_PROJECT_KEY || "";
  if (process.env.NODE_ENV !== "production" ||
      (process.env.VERCEL_ENV && process.env.VERCEL_ENV !== "production") ||
      (process.env.NEXT_PUBLIC_VERCEL_ENV && process.env.NEXT_PUBLIC_VERCEL_ENV !== "production") ||
      secret.length < 32 || !projectKey.startsWith("hck_pk_")) return new Response(null, { status: 503 });
  const body = await request.text();
  const timestamp = request.headers.get("x-walnut-timestamp") || "";
  const supplied = request.headers.get("x-walnut-signature") || "";
  if (body.length > 256 || !/^\d{10}$/.test(timestamp) ||
      Math.abs(Date.now() / 1000 - Number(timestamp)) > 60 || !/^[a-f0-9]{64}$/.test(supplied) ||
      !timingSafeEqual(Buffer.from(supplied, "hex"), Buffer.from(signature(body, timestamp, "dispatch", secret), "hex"))) {
    return new Response(null, { status: 401 });
  }
  try {
    const input = JSON.parse(body) as { event_id?: unknown };
    if (!Number.isSafeInteger(input.event_id) || Number(input.event_id) <= 0) return new Response(null, { status: 400 });
    // Never trust the request to supply paid status, user, or plan. The backend
    // atomically consumes a verified Stripe row, including on signed replays.
    const claimTime = String(Math.floor(Date.now() / 1000));
    const response = await fetch(claimUrl, { method: "POST", body, cache: "no-store", redirect: "error",
      headers: { "Content-Type": "application/json", "X-Walnut-Timestamp": claimTime,
        "X-Walnut-Signature": signature(body, claimTime, "claim", secret) }, signal: AbortSignal.timeout(4000) });
    if (response.status === 204) return new Response(null, { status: 204 });
    if (!response.ok) return new Response(null, { status: 502 });
    const claimed = await response.json() as { user_id: string; properties: Record<string, string | number | boolean> };
    if (!/^\d+$/.test(claimed.user_id)) return new Response(null, { status: 502 });
    analytics.init({ projectKey });
    // The installed SDK awaits immediate capture and catches its own network
    // errors. Completion here means attempted, not proven dashboard ingestion.
    await analytics.trackEvent("subscription_completed", claimed.properties, { userId: claimed.user_id });
    return Response.json({ status: "attempted" });
  } catch {
    return new Response(null, { status: 502 });
  }
}
