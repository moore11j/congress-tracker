import type { ReactNode } from "react";
import type { Metadata, Viewport } from "next";
import Link from "next/link";
import { headers } from "next/headers";
import { SpeedInsights } from "@vercel/speed-insights/next";
import { AccountNav } from "@/components/auth/AccountNav";
import { AppTopNav } from "@/components/AppTopNav";
import { GlobalSearch } from "@/components/GlobalSearch";
import { PageAnalyticsTracker } from "@/components/PageAnalyticsTracker";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { WALNUT_MARKETING_URL } from "@/lib/marketingMetadata";
import "./globals.css";

export const metadata: Metadata = {
  metadataBase: new URL(WALNUT_MARKETING_URL),
  title: "Walnut Market Terminal",
  description:
    "Walnut Intelligence Inc. operates Walnut Market Terminal for disclosure intelligence, market signals, and cross-source conviction for public-market investors.",
  icons: {
    icon: "/favicon.ico",
    apple: "/apple-touch-icon.png",
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

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
        <div className="relative min-h-screen min-w-0 max-w-full">
          <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,_rgba(45,212,191,0.18),_transparent_45%),radial-gradient(circle_at_20%_20%,_rgba(56,189,248,0.16),_transparent_38%),linear-gradient(180deg,_rgba(15,23,42,0.2),_rgba(2,6,23,1))]" />
          <header className="relative z-[1000] border-b border-white/10 bg-slate-950/80 backdrop-blur">
            <div className="mx-auto flex w-full min-w-0 max-w-screen-2xl flex-wrap items-center justify-between gap-x-3 gap-y-3 px-4 py-4 sm:px-6 lg:flex-nowrap lg:gap-x-4 lg:px-8 2xl:px-10">
              <Link href="https://walnutmarkets.com" prefetch={false} className="group flex min-w-0 shrink-0 items-center gap-3 rounded-xl py-1 pr-2 text-lg font-semibold">
                <WalnutBrandMark />
                <span className="leading-none">
                  <span className="block whitespace-nowrap text-[1.08rem] font-semibold text-white">
                    Walnut
                  </span>
                  <span className="mt-1 block whitespace-nowrap text-[0.72rem] font-medium text-emerald-200/80">
                    Market Terminal
                  </span>
                </span>
              </Link>
              <AppTopNav />
              <div className="order-2 shrink-0 lg:order-none">
                <GlobalSearch />
              </div>
              <div className="order-2 min-w-0 shrink lg:order-none lg:shrink-0">
                <AccountNav />
              </div>
            </div>
          </header>
          <PageAnalyticsTracker />
          <main className="relative z-0 mx-auto w-full min-w-0 max-w-screen-2xl px-4 py-10 sm:px-6 lg:px-8 2xl:px-10">{children}</main>
        </div>
        <SpeedInsights />
      </body>
    </html>
  );
}
