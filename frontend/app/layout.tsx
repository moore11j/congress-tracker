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
import { WALNUT_MARKETING_DESCRIPTION, WALNUT_MARKETING_TITLE, WALNUT_MARKETING_URL } from "@/lib/marketingMetadata";
import "./globals.css";

const GOOGLE_ANALYTICS_ID = "G-QQTFFK7FBH";
const REDDIT_PIXEL_ID = process.env.NEXT_PUBLIC_REDDIT_PIXEL_ID ?? "a2_jdfg5l7gwuw1";

export const metadata: Metadata = {
  metadataBase: new URL(WALNUT_MARKETING_URL),
  title: WALNUT_MARKETING_TITLE,
  description: WALNUT_MARKETING_DESCRIPTION,
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

function GoogleTag() {
  return (
    <>
      <script async src={`https://www.googletagmanager.com/gtag/js?id=${GOOGLE_ANALYTICS_ID}`} />
      <script
        dangerouslySetInnerHTML={{
          __html: `
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', '${GOOGLE_ANALYTICS_ID}');
`,
        }}
      />
    </>
  );
}

function RedditPixel() {
  if (!REDDIT_PIXEL_ID) return null;

  return (
    <script
      dangerouslySetInnerHTML={{
        __html: `
!function(w,d){if(!w.rdt){var p=w.rdt=function(){p.sendEvent?p.sendEvent.apply(p,arguments):p.callQueue.push(arguments)};p.callQueue=[];var t=d.createElement("script");t.src="https://www.redditstatic.com/ads/pixel.js?pixel_id=${REDDIT_PIXEL_ID}",t.async=!0;var s=d.getElementsByTagName("script")[0];s.parentNode.insertBefore(t,s)}}(window,document);
rdt('init','${REDDIT_PIXEL_ID}');
rdt('track', 'PageVisit');
`,
      }}
    />
  );
}

export default async function RootLayout({ children }: { children: ReactNode }) {
  const requestHeaders = await headers();
  const isPublicLanding = requestHeaders.get("x-walnut-public-landing") === "1";

  if (isPublicLanding) {
    return (
      <html lang="en" className="h-full">
        <head>
          <GoogleTag />
          <RedditPixel />
        </head>
        <body className="min-h-full">
          {children}
          <SpeedInsights />
        </body>
      </html>
    );
  }

  return (
    <html lang="en" className="h-full">
      <head>
        <GoogleTag />
        <RedditPixel />
      </head>
      <body className="min-h-full">
        <div className="relative min-h-screen min-w-0 max-w-full">
          <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,_rgba(14,116,144,0.08),_transparent_42%),radial-gradient(circle_at_20%_18%,_rgba(15,23,42,0.26),_transparent_36%),linear-gradient(180deg,_rgba(6,13,24,0.8),_rgba(2,6,23,1))]" />
          <header className="relative z-[1000] border-b border-white/10 bg-slate-950/80 backdrop-blur">
            <div className="mx-auto flex w-full min-w-0 max-w-[1728px] flex-wrap items-center justify-between gap-x-3 gap-y-1 px-4 py-1 sm:px-6 lg:flex-nowrap lg:gap-x-4 lg:px-8 2xl:px-10">
              <Link href="https://walnutmarkets.com" prefetch={false} className="group flex min-w-0 shrink-0 items-center gap-2 rounded-xl pr-2 text-lg font-semibold">
                <WalnutBrandMark className="relative flex h-6 w-6 shrink-0 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950 shadow-[0_0_20px_rgba(16,185,129,0.16)]" svgClassName="h-[18px] w-[18px] overflow-visible" />
                <span className="leading-none">
                  <span className="block whitespace-nowrap text-[0.95rem] font-semibold text-white">
                    Walnut
                  </span>
                  <span className="mt-0.5 block whitespace-nowrap text-[0.62rem] font-medium text-emerald-200/80">
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
          <main className="relative z-0 mx-auto w-full min-w-0 max-w-[1728px] px-4 py-1.5 sm:px-6 lg:px-8 2xl:px-10">{children}</main>
        </div>
        <SpeedInsights />
      </body>
    </html>
  );
}
