import { headers } from "next/headers";

const publicLandingHosts = new Set(["walnutmarkets.com", "www.walnutmarkets.com", "walnut-intel.com", "www.walnut-intel.com"]);

export async function legalPageChrome(): Promise<"public" | "embedded"> {
  const requestHeaders = await headers();
  const host = (requestHeaders.get("x-forwarded-host") ?? requestHeaders.get("host") ?? "").split(":")[0]?.toLowerCase();
  return publicLandingHosts.has(host) ? "public" : "embedded";
}
