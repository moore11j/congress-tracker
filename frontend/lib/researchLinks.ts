const WALNUT_HOSTS = new Set(["walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"]);

/** Repair old generated ticker destinations without changing access to their data. */
export function researchLinkHref(value: string): string {
  const href = value.trim();
  if (!href || href.startsWith("//") || !/^(https?:\/\/|\/)/i.test(href)) return "";
  let url: URL;
  try { url = new URL(href, "https://walnutmarkets.com"); } catch { return ""; }
  if (!WALNUT_HOSTS.has(url.hostname)) return href;
  const legacy = url.pathname.match(/^\/ticker\/([A-Za-z0-9.^-]+)\/(earnings|financials)\/?$/i);
  if (legacy) return `https://app.walnutmarkets.com/ticker/${legacy[1].toUpperCase()}#financials`;
  if (/^\/ticker\/[^/]+\/?$/.test(url.pathname) || url.pathname === "/insights") {
    url.protocol = "https:";
    url.host = "app.walnutmarkets.com";
    return url.toString();
  }
  if (url.pathname === "/research" || url.pathname.startsWith("/research/")) {
    url.protocol = "https:";
    url.host = "walnutmarkets.com";
    return url.toString();
  }
  return href;
}
