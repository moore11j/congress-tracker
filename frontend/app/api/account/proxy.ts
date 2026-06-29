import type { NextRequest } from "next/server";

const DEFAULT_APP_ORIGIN = "https://app.walnutmarkets.com";
const DEFAULT_TRUSTED_APP_ORIGINS = [
  DEFAULT_APP_ORIGIN,
  "https://walnutmarkets.com",
  "https://www.walnutmarkets.com",
  "https://app.walnut-intel.com",
  "https://walnut-intel.com",
  "https://www.walnut-intel.com",
  "https://congress-tracker-two.vercel.app",
  "http://localhost:3000",
  "http://localhost:3001",
  "http://127.0.0.1:3000",
  "http://127.0.0.1:3001",
];
const CSRF_TOKEN_HEADERS = ["x-csrf-token", "x-xsrf-token"];

function normalizeOrigin(value: string | null | undefined) {
  const raw = (value ?? "").trim().replace(/\/+$/, "");
  if (!raw || raw === "*") return null;
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol) || !parsed.hostname) return null;
    return parsed.origin;
  } catch {
    return null;
  }
}

function configuredAppOrigin() {
  for (const value of [
    process.env.NEXT_PUBLIC_APP_URL,
    process.env.APP_URL,
    process.env.NEXT_PUBLIC_APP_BASE_URL,
    process.env.APP_BASE_URL,
  ]) {
    const origin = normalizeOrigin(value);
    if (origin) return origin;
  }
  return null;
}

function appOriginFromRequestUrl(request: NextRequest) {
  const origin = normalizeOrigin(request.nextUrl.origin);
  return origin && trustedAppOrigins(request).has(origin) ? origin : null;
}

function trustedAppOrigins(request: NextRequest) {
  const origins = new Set(DEFAULT_TRUSTED_APP_ORIGINS.map((origin) => normalizeOrigin(origin)).filter(Boolean) as string[]);
  const configuredOrigin = configuredAppOrigin();
  if (configuredOrigin) origins.add(configuredOrigin);
  const requestOrigin = normalizeOrigin(request.nextUrl.origin);
  if (requestOrigin && (requestOrigin.includes("localhost") || requestOrigin.includes("127.0.0.1") || requestOrigin.endsWith(".walnutmarkets.com"))) {
    origins.add(requestOrigin);
  }
  return origins;
}

function trustedHeaderOrigin(value: string | null, trustedOrigins: Set<string>) {
  const origin = normalizeOrigin(value);
  return origin && trustedOrigins.has(origin) ? origin : null;
}

function trustedReferer(value: string | null, trustedOrigins: Set<string>) {
  const referer = (value ?? "").trim();
  const origin = normalizeOrigin(referer);
  return origin && trustedOrigins.has(origin) ? referer : null;
}

function safeProxyOrigin(request: NextRequest, trustedOrigins: Set<string>) {
  return (
    trustedHeaderOrigin(request.headers.get("origin"), trustedOrigins) ??
    trustedHeaderOrigin(request.headers.get("referer"), trustedOrigins) ??
    configuredAppOrigin() ??
    appOriginFromRequestUrl(request) ??
    DEFAULT_APP_ORIGIN
  );
}

export function buildBackendProxyHeaders(request: NextRequest, { fallbackRefererPath }: { fallbackRefererPath: string }) {
  const trustedOrigins = trustedAppOrigins(request);
  const origin = safeProxyOrigin(request, trustedOrigins);
  const referer = trustedReferer(request.headers.get("referer"), trustedOrigins) ?? `${origin}${fallbackRefererPath.startsWith("/") ? fallbackRefererPath : `/${fallbackRefererPath}`}`;
  const headers = new Headers({
    accept: "application/json",
    "content-type": "application/json",
    origin,
    referer,
  });

  const cookie = request.headers.get("cookie");
  if (cookie) headers.set("cookie", cookie);
  const authorization = request.headers.get("authorization");
  if (authorization) headers.set("authorization", authorization);
  for (const header of CSRF_TOKEN_HEADERS) {
    const value = request.headers.get(header);
    if (value) headers.set(header, value);
  }

  return headers;
}
