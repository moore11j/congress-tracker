import { cookies } from "next/headers";
import { redirect } from "next/navigation";

const authSessionCookieName = "ct_session";
const authHintCookieName = "ct_auth_hint";
const entitlementHintCookieName = "ct_entitlement_hint";

export async function requirePageAuth(returnTo: string): Promise<string> {
  const cookieStore = await cookies();
  const token = cookieStore.get(authSessionCookieName)?.value;
  if (token) {
    return token;
  }
  if (cookieStore.get(authHintCookieName)?.value !== "1") {
    redirect(`/login?return_to=${encodeURIComponent(returnTo)}`);
  }
  return "";
}

export async function requirePageAuthState(returnTo: string): Promise<{ token: string; entitlementHint: string | null }> {
  const cookieStore = await cookies();
  const token = cookieStore.get(authSessionCookieName)?.value;
  if (token) {
    return { token, entitlementHint: cookieStore.get(entitlementHintCookieName)?.value ?? null };
  }
  if (cookieStore.get(authHintCookieName)?.value !== "1") {
    redirect(`/login?return_to=${encodeURIComponent(returnTo)}`);
  }
  return { token: "", entitlementHint: cookieStore.get(entitlementHintCookieName)?.value ?? null };
}

export async function optionalPageAuthToken(): Promise<string | null> {
  const cookieStore = await cookies();
  return cookieStore.get(authSessionCookieName)?.value ?? null;
}

export async function optionalPageAuthState(): Promise<{ token: string | null; hasAuthHint: boolean; entitlementHint: string | null }> {
  const cookieStore = await cookies();
  return {
    token: cookieStore.get(authSessionCookieName)?.value ?? null,
    hasAuthHint: cookieStore.get(authHintCookieName)?.value === "1",
    entitlementHint: cookieStore.get(entitlementHintCookieName)?.value ?? null,
  };
}

export function buildReturnTo(pathname: string, params?: Record<string, string | string[] | undefined>): string {
  const query = new URLSearchParams();
  Object.entries(params ?? {}).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => {
        if (item) query.append(key, item);
      });
      return;
    }
    if (value) query.set(key, value);
  });
  const qs = query.toString();
  return qs ? `${pathname}?${qs}` : pathname;
}
