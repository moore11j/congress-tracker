import type { ReactNode } from "react";
import Link from "next/link";
import { headers } from "next/headers";
import { AccountNav } from "@/components/auth/AccountNav";
import { GlobalSearch } from "@/components/GlobalSearch";
import { SpeedInsights } from "@vercel/speed-insights/next";
import "./globals.css";

export const metadata = {
  title: "Walnut Intel Market Terminal",
  description: "Disclosure intelligence, market signals, and cross-source conviction for public-market investors.",
};

function WalnutBrandMark() {
  return (
    <span className="relative flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-emerald-300/30 bg-slate-950 shadow-[0_0_24px_rgba(16,185,129,0.16)]">
      <svg viewBox="0 0 48 48" aria-hidden="true" className="h-8 w-8 overflow-visible">
        <defs>
          <linearGradient id="walnut-mark-stroke" x1="8" x2="40" y1="7" y2="42" gradientUnits="userSpaceOnUse">
            <stop offset="0" stopColor="#34f5a2" />
            <stop offset="0.56" stopColor="#10b981" />
            <stop offset="1" stopColor="#14b8a6" />
          </linearGradient>
        </defs>
        <path
          d="M24 7c-4.5 0-7.8 3.2-8.1 7.5-4.2.5-7.3 3.9-7.3 8.1 0 1.6.4 3 1.2 4.3-2 1.6-3.1 3.9-3.1 6.5 0 4.7 3.8 8.6 8.5 8.6 2.6 0 4.8-1.1 6.4-2.9.7.2 1.5.3 2.4.3s1.7-.1 2.4-.3c1.6 1.8 3.8 2.9 6.4 2.9 4.7 0 8.5-3.9 8.5-8.6 0-2.6-1.1-4.9-3.1-6.5.8-1.3 1.2-2.7 1.2-4.3 0-4.2-3.1-7.6-7.3-8.1C31.8 10.2 28.5 7 24 7Z"
          fill="rgba(2,6,23,0.95)"
          stroke="url(#walnut-mark-stroke)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3.2"
        />
        <path
          d="M23.9 8.2v30.5M16 15.7c3.2 2.4 5.4 5.5 6.4 9.2M32 15.7c-3.2 2.4-5.4 5.5-6.4 9.2M10.1 26.7c4.1 1.5 7.1 3.9 9.1 7.4M37.9 26.7c-4.1 1.5-7.1 3.9-9.1 7.4"
          fill="none"
          stroke="url(#walnut-mark-stroke)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="2.7"
        />
        <path
          d="M16.5 34V24.5M23.8 34V19M31.1 34V15.3"
          fill="none"
          stroke="#dfffee"
          strokeLinecap="round"
          strokeWidth="3.2"
        />
      </svg>
    </span>
  );
}

export default async function RootLayout({ children }: { children: ReactNode }) {
  const requestHeaders = await headers();
  const isPublicLanding = requestHeaders.get("x-walnut-public-landing") === "1";

  if (isPublicLanding) {
    return (
      <html lang="en" className="h-full">
        <body className="min-h-full">
          {children}
          <SpeedInsights />
        </body>
      </html>
    );
  }

  return (
    <html lang="en" className="h-full">
      <body className="min-h-full">
        <div className="relative min-h-screen">
          <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,_rgba(45,212,191,0.18),_transparent_45%),radial-gradient(circle_at_20%_20%,_rgba(56,189,248,0.16),_transparent_38%),linear-gradient(180deg,_rgba(15,23,42,0.2),_rgba(2,6,23,1))]" />
          <header className="relative z-[1000] border-b border-white/10 bg-slate-950/80 backdrop-blur">
            <div className="mx-auto flex w-full max-w-screen-2xl flex-wrap items-center justify-between gap-x-4 gap-y-3 px-4 py-4 sm:px-6 lg:flex-nowrap lg:px-8 2xl:px-10">
              <Link href="/?mode=all" prefetch={false} className="group flex shrink-0 items-center gap-3 rounded-xl py-1 pr-2 text-lg font-semibold">
                <WalnutBrandMark />
                <span className="leading-none">
                  <span className="block whitespace-nowrap text-[1.08rem] font-semibold text-white">
                    Walnut Intel
                  </span>
                  <span className="mt-1 block whitespace-nowrap text-[0.72rem] font-medium text-emerald-200/80">
                    Walnut Market Terminal
                  </span>
                </span>
              </Link>
              <nav className="order-3 flex min-w-0 basis-full items-center gap-4 overflow-x-auto whitespace-nowrap text-sm font-medium text-slate-200 [scrollbar-width:none] lg:order-none lg:ml-auto lg:basis-auto [&::-webkit-scrollbar]:hidden">
                <Link href="/?mode=all" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Feed
                </Link>
                <Link href="/insights" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Insights
                </Link>
                <Link href="/signals" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Signals
                </Link>
                <Link href="/screener" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Screener
                </Link>
                <Link href="/leaderboards/congress-traders" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Leaderboards
                </Link>
                <Link href="/backtesting" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Backtesting
                </Link>
                <Link href="/pricing" prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Pricing
                </Link>
              </nav>
              <div className="order-2 shrink-0 lg:order-none">
                <GlobalSearch />
              </div>
              <div className="order-2 shrink-0 lg:order-none">
                <AccountNav />
              </div>
            </div>
          </header>
          <main className="relative z-0 mx-auto w-full max-w-screen-2xl px-4 py-10 sm:px-6 lg:px-8 2xl:px-10">{children}</main>
        </div>
        <SpeedInsights />
      </body>
    </html>
  );
}
