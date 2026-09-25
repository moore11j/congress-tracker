const WALNUT_HOSTS = new Set(["walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"]);

// Exact destinations verified against the publishers during the Ahrefs audit.
const repairedSources: Record<string, string> = {
  "https://investor.apple.com/earnings/default.aspx": "https://investor.apple.com/investor-relations/default.aspx",
  "https://investor.apple.com/earnings-results/default.aspx": "https://investor.apple.com/investor-relations/default.aspx",
  "https://www.fool.com/investing/2026/06/04/goldman-sachs-just-predicted-spacex-ai-revenue-2030/": "https://www.fool.com/investing/2026/06/04/goldman-sachs-just-predicted-spacex-s-ai-revenue-will-be-this-number-by-2030/",
};

/** Repair old generated ticker destinations without changing access to their data. */
export function researchLinkHref(value: string): string {
  const href = value.trim();
  if (repairedSources[href]) return repairedSources[href];
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
