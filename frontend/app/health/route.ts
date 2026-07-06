import { NextResponse } from "next/server";

export const dynamic = "force-static";

export function GET() {
  return NextResponse.json(
    { status: "ok", surface: "app" },
    {
      status: 200,
      headers: {
        "cache-control": "public, max-age=60",
      },
    },
  );
}
