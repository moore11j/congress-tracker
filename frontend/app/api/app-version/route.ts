import { NextResponse } from "next/server";
import { appVersion } from "@/lib/appVersion";

export const dynamic = "force-dynamic";

export function GET() {
  return NextResponse.json(
    { version: appVersion() },
    {
      headers: {
        "Cache-Control": "no-store, max-age=0",
      },
    },
  );
}
