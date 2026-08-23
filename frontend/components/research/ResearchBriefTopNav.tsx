import Link from "next/link";
import { AccountNav } from "@/components/auth/AccountNav";
import { AppTopNav } from "@/components/AppTopNav";
import { GlobalSearch } from "@/components/GlobalSearch";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";

/**
 * Research briefs are served from the public marketing host, outside the
 * terminal layout. Keep the same navigation shell here so every brief has a
 * consistent route back into the live Walnut product.
 */
export function ResearchBriefTopNav() {
  return (
    <header className="relative z-[1000] border-b border-white/10 bg-slate-950/80 text-slate-100 backdrop-blur">
      <div className="mx-auto flex w-full min-w-0 max-w-[1728px] flex-wrap items-center justify-between gap-x-3 gap-y-1 px-4 py-1 sm:px-6 lg:flex-nowrap lg:gap-x-6 lg:px-8 lg:py-2.5 xl:gap-x-8 2xl:px-10">
        <Link href="https://walnutmarkets.com" prefetch={false} className="group flex min-w-0 shrink-0 items-center gap-2 rounded-xl pr-2 text-lg font-semibold">
          <WalnutBrandMark className="relative flex h-6 w-6 shrink-0 items-center justify-center rounded-lg border border-emerald-300/30 bg-slate-950 shadow-[0_0_20px_rgba(16,185,129,0.16)]" svgClassName="h-[18px] w-[18px] overflow-visible" />
          <span className="leading-none">
            <span className="block whitespace-nowrap text-[0.95rem] font-semibold text-white">Walnut</span>
            <span className="mt-0.5 block whitespace-nowrap text-[0.62rem] font-medium text-emerald-200/80">Market Terminal</span>
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
  );
}
