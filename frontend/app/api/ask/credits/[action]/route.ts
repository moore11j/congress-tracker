import { NextResponse, type NextRequest } from "next/server";
import { creditSandboxBackend } from "@/lib/researchCredits";

export const dynamic = "force-dynamic";
async function proxy(request: NextRequest, context: { params: Promise<{ action: string }> }) {
  const { action } = await context.params;
  if (!((request.method === "GET" && action === "balance") || (request.method === "POST" && ["checkout", "confirm"].includes(action))))
    return NextResponse.json({ detail: "Not found" }, { status: 404 });
  const base = creditSandboxBackend(process.env);
  if (!base) return NextResponse.json({ detail: "Billing sandbox is not connected yet" }, { status: 503 });
  if (request.method === "POST" && request.headers.get("origin") !== request.nextUrl.origin)
    return NextResponse.json({ detail: "Untrusted origin" }, { status: 403 });
  const headers = new Headers({ accept: "application/json", "content-type": "application/json", origin: request.nextUrl.origin });
  for (const name of ["cookie", "authorization", "x-csrf-token"]) {
    const value = request.headers.get(name); if (value) headers.set(name, value);
  }
  try {
    const body = request.method === "POST" ? await request.text() : undefined;
    if (body && body.length > 2000) return NextResponse.json({ detail: "Request too large" }, { status: 413 });
    const response = await fetch(`${base}/api/research-credits/${action}`, {
      method: request.method, headers, body, cache: "no-store", redirect: "error", signal: AbortSignal.timeout(25000),
    });
    return new NextResponse(await response.text(), { status: response.status,
      headers: { "content-type": "application/json", "cache-control": "no-store" } });
  } catch {
    return NextResponse.json({ detail: "Sandbox request could not be completed. Your balance has not been confirmed." }, { status: 502 });
  }
}
export const GET = proxy;
export const POST = proxy;
