const securityHeaders = [
  {
    key: "Content-Security-Policy",
    value: [
      "default-src 'self'",
      "base-uri 'self'",
      "object-src 'none'",
      "frame-ancestors 'none'",
      "img-src 'self' data: blob: https:",
      "font-src 'self' data: https:",
      "style-src 'self' 'unsafe-inline'",
      "script-src 'self' 'unsafe-inline' 'unsafe-eval' https:",
      "connect-src 'self' https://congress-tracker-api.fly.dev https:",
      "frame-src 'self' https://accounts.google.com https://*.stripe.com https://checkout.stripe.com https://js.stripe.com https:",
      "form-action 'self' https://accounts.google.com https://*.stripe.com https://checkout.stripe.com",
    ].join("; "),
  },
  {
    key: "X-Content-Type-Options",
    value: "nosniff",
  },
  {
    key: "Referrer-Policy",
    value: "strict-origin-when-cross-origin",
  },
  {
    key: "Permissions-Policy",
    value: "camera=(), microphone=(), geolocation=(), payment=()",
  },
  {
    key: "X-Frame-Options",
    value: "DENY",
  },
];

const apiBase = (
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE ||
  process.env.API_BASE_URL ||
  process.env.API_BASE ||
  "https://congress-tracker-api.fly.dev"
).replace(/\/+$/, "");

/** @type {import('next').NextConfig} */
const nextConfig = {
  async headers() {
    return [
      {
        source: "/:path*",
        headers: securityHeaders,
      },
    ];
  },
  async rewrites() {
    return {
      fallback: [
        {
          source: "/api/:path*",
          destination: `${apiBase}/api/:path*`,
        },
      ],
    };
  },
};

module.exports = nextConfig;
