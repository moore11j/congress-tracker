const productionAnalyticsHosts = new Set(["walnutmarkets.com", "app.walnutmarkets.com"]);

/**
 * Keep production measurement off local development, Vercel previews, and
 * staging hosts. Both Walnut production subdomains intentionally participate
 * in the same GA4 stream and first-party product analytics.
 */
export function isProductionAnalyticsHost(hostname?: string): boolean {
  if (typeof window === "undefined") return false;
  if (process.env.NODE_ENV !== "production") return false;
  if (process.env.NEXT_PUBLIC_VERCEL_ENV && process.env.NEXT_PUBLIC_VERCEL_ENV !== "production") return false;
  if (window.location.protocol !== "https:") return false;
  const host = (hostname ?? window.location.hostname).trim().toLowerCase().replace(/\.$/, "");
  return productionAnalyticsHosts.has(host);
}
