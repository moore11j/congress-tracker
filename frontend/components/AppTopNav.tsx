"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { HorizontalScrollIndicators, useHorizontalScrollAffordance } from "@/components/ui/HorizontalScrollAffordance";

const topNavLinks = [
  { href: "/?mode=all", label: "Feed" },
  { href: "/insights", label: "Insights" },
  { href: "/signals", label: "Signals" },
  { href: "/screener", label: "Screener" },
  { href: "/leaderboards/congress-traders", label: "Leaderboards" },
  { href: "/backtesting", label: "Backtesting" },
  { href: "/market-pressure", label: "Market Pressure" },
  { href: "/pricing", label: "Pricing" },
] as const;

function isActiveNavLink(pathname: string | null, href: string) {
  const path = pathname || "/";
  if (href === "/?mode=all") return path === "/";
  const basePath = href.split("?")[0] || href;
  if (basePath === "/leaderboards/congress-traders") return path === basePath || path.startsWith("/leaderboards/");
  return path === basePath || path.startsWith(`${basePath}/`);
}

export function AppTopNav() {
  const pathname = usePathname();
  const { scrollRef, canScrollLeft, canScrollRight, updateScrollState } =
    useHorizontalScrollAffordance<HTMLElement>();

  return (
    <div className="relative order-3 min-w-0 basis-full lg:order-none lg:ml-auto lg:basis-auto">
      <nav
        ref={scrollRef}
        onScroll={updateScrollState}
        className="flex min-w-0 items-center gap-4 overflow-x-auto whitespace-nowrap text-sm font-medium text-slate-200 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
      >
        {topNavLinks.map((link) => {
          const active = isActiveNavLink(pathname, link.href);
          return (
            <Link
              key={link.href}
              href={link.href}
              prefetch={false}
              aria-current={active ? "page" : undefined}
              className={`rounded-full px-3 py-1 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${
                active
                  ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                  : "text-slate-200 hover:text-white"
              }`}
            >
              {link.label}
            </Link>
          );
        })}
      </nav>
      <HorizontalScrollIndicators canScrollLeft={canScrollLeft} canScrollRight={canScrollRight} />
    </div>
  );
}
