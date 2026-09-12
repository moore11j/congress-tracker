export const defaultPostLoginPath = "/?mode=all";
export const reactivatedBillingPath = "/account/billing?reactivated=1";

const appOrigin = "https://app.walnutmarkets.com";
const controlCharacters = /[\u0000-\u001f\u007f]/;

/** Resume the action a guest explicitly requested after either auth method. */
export function tickerFollowReturnPath(pathname: string, search: string): string {
  const params = new URLSearchParams(search);
  params.set("follow", "1");
  return safeAppReturnPath(`${pathname}?${params.toString()}`);
}

export function safeAppReturnPath(returnTo?: string | null, fallback = defaultPostLoginPath) {
  const raw = (returnTo ?? "").trim();
  if (!raw || !raw.startsWith("/") || raw.startsWith("//") || raw.includes("\\") || controlCharacters.test(raw)) {
    return fallback;
  }

  try {
    const url = new URL(raw, appOrigin);
    if (url.origin !== appOrigin || !url.pathname.startsWith("/") || url.pathname.startsWith("//")) {
      return fallback;
    }
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return fallback;
  }
}
