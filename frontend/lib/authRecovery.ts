import { safeAppReturnPath } from "./returnPaths";

const returnPathKey = "walnut:google-return-path";

/** Preview cookies cannot follow Google's production callback across origins. */
export function googleSignInReturnsToCurrentSite(authorizationUrl: string, currentOrigin: string): boolean {
  try {
    const authorization = new URL(authorizationUrl);
    const callback = new URL(authorization.searchParams.get("redirect_uri") || "");
    const current = new URL(currentOrigin);
    if (authorization.origin !== "https://accounts.google.com" || callback.pathname !== "/auth/google/callback") return false;
    if (callback.origin === current.origin) return true;
    // Preserve the existing marketing-site to app-site login flow.
    return ["https://walnutmarkets.com", "https://www.walnutmarkets.com"].includes(current.origin)
      && callback.origin === "https://app.walnutmarkets.com";
  } catch {
    return false;
  }
}

// Store only the requested app destination, never OAuth codes or tokens.
export function rememberGoogleReturnPath(path: string) {
  try { window.sessionStorage.setItem(returnPathKey, safeAppReturnPath(path)); } catch { /* Storage may be unavailable. */ }
}

export function googleReturnPath() {
  try { return safeAppReturnPath(window.sessionStorage.getItem(returnPathKey)); }
  catch { return safeAppReturnPath(); }
}

export function clearGoogleReturnPath() {
  try { window.sessionStorage.removeItem(returnPathKey); } catch { /* Optional recovery hint. */ }
}

/** Bound the UI wait without automatically replaying a one-use OAuth code. */
export function withAuthTimeout<T>(operation: Promise<T>, timeoutMs = 45_000): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Sign-in is taking too long. Please try signing in again.")), timeoutMs);
    operation.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}
