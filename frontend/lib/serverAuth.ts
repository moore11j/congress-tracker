import { cookies } from "next/headers";
import { redirect } from "next/navigation";

const authSessionCookieName = "ct_session";

export async function requirePageAuth(returnTo: string): Promise<string> {
  const cookieStore = await cookies();
  const token = cookieStore.get(authSessionCookieName)?.value;
  if (!token) {
    redirect(`/login?return_to=${encodeURIComponent(returnTo)}`);
  }
  return token;
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
