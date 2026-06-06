"use client";

import Link from "next/link";
import { HorizontalScrollIndicators, useHorizontalScrollAffordance } from "@/components/ui/HorizontalScrollAffordance";

const topNavLinks = [
  { href: "/?mode=all", label: "Feed" },
  { href: "/insights", label: "Insights" },
  { href: "/signals", label: "Signals" },
  { href: "/screener", label: "Screener" },
  { href: "/leaderboards/congress-traders", label: "Leaderboards" },
  { href: "/backtesting", label: "Backtesting" },
  { href: "/pricing", label: "Pricing" },
] as const;

export function AppTopNav() {
  const { scrollRef, canScrollLeft, canScrollRight, updateScrollState } =
    useHorizontalScrollAffordance<HTMLElement>();

  return (
    <div className="relative order-3 min-w-0 basis-full lg:order-none lg:ml-auto lg:basis-auto">
      <nav
        ref={scrollRef}
        onScroll={updateScrollState}
        className="flex min-w-0 items-center gap-4 overflow-x-auto whitespace-nowrap text-sm font-medium text-slate-200 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
      >
        {topNavLinks.map((link) => (
          <Link key={link.href} href={link.href} prefetch={false} className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
            {link.label}
          </Link>
        ))}
      </nav>
      <HorizontalScrollIndicators canScrollLeft={canScrollLeft} canScrollRight={canScrollRight} />
    </div>
  );
}
