import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const validModes = ["all", "congress", "insider"];

export function middleware(request: NextRequest) {
  const { pathname, searchParams } = request.nextUrl;

  if (pathname === "/") {
    const mode = searchParams.get("mode");

    if (!mode || !validModes.includes(mode)) {
      const url = request.nextUrl.clone();
      url.searchParams.set("mode", "all");
      return NextResponse.redirect(url);
    }
  }

  return NextResponse.next();
}
