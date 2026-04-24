"use client";

import { useState, type ReactNode } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";

type ScreenerUpgradeOverlayProps = {
  title: string;
  body: string;
  children: ReactNode;
  badge?: string;
  className?: string;
  buttonClassName?: string;
};

export function ScreenerUpgradeOverlay({
  title,
  body,
  children,
  badge = "Premium",
  className = "",
  buttonClassName = "",
}: ScreenerUpgradeOverlayProps) {
  const [open, setOpen] = useState(false);

  return (
    <>
      <div className={`relative ${className}`}>
        {children}
        <button
          type="button"
          onClick={() => setOpen(true)}
          className={`absolute inset-0 rounded-2xl ${buttonClassName}`}
          aria-label={title}
        />
        <span className="pointer-events-none absolute right-3 top-3 rounded-full border border-amber-300/30 bg-amber-300/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-100">
          {badge}
        </span>
      </div>

      {open ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-lg border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-xl">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-amber-300">Premium</p>
                <h2 className="mt-2 text-lg font-semibold">{title}</h2>
              </div>
              <button
                type="button"
                className="rounded-lg border border-white/10 px-2 py-1 text-sm text-slate-300 hover:text-white"
                onClick={() => setOpen(false)}
              >
                Close
              </button>
            </div>
            <div className="mt-4">
              <UpgradePrompt title={title} body={body} compact={true} />
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
