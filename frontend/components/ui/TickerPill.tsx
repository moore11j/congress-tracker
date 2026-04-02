import Link from "next/link";
import React from "react";
import { compactInteractiveTitleClassName } from "@/lib/styles";

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
    bg-white/[0.04] text-slate-200
    border border-white/10
    shrink-0 w-fit
    ${href ? `group transition-all duration-150 hover:-translate-y-px hover:border-emerald-400/40 hover:bg-emerald-300/[0.08] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/60 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${compactInteractiveTitleClassName}` : ""}
    ${className}
  `;

  if (href) {
    return <Link href={href} prefetch={false} className={pillClassName}>{symbol}</Link>;
  }

  return <span className={pillClassName}>{symbol}</span>;
}
