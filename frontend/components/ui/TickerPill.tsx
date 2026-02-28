import React from "react";

interface TickerPillProps {
  symbol: string;
  className?: string;
}

export function TickerPill({ symbol, className = "" }: TickerPillProps) {
  return (
    <span
      className={`
        inline-flex items-center
        px-2.5 py-0.5
        rounded-full
        text-xs font-semibold
        bg-white/5 text-white/85
        border border-white/10
        shrink-0 w-fit
        ${className}
      `}
    >
      {symbol}
    </span>
  );
}
