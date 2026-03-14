import Link from "next/link";
import React from "react";

interface TickerPillProps {
  symbol: string;
  href?: string | null;
  className?: string;
}

export function TickerPill({ symbol, href = null, className = "" }: TickerPillProps) {
  const pillClassName = `
    inline-flex items-center
    px-2.5 py-0.5
    rounded-full
    text-xs font-semibold
    bg-white/5 text-white/85
    border border-white/10
    shrink-0 w-fit
    ${href ? "cursor-pointer transition-colors hover:border-white/30 hover:bg-white/10 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/60 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950" : ""}
    ${className}
  `;

  if (href) {
    return <Link href={href} className={pillClassName}>{symbol}</Link>;
  }

  return <span className={pillClassName}>{symbol}</span>;
}
