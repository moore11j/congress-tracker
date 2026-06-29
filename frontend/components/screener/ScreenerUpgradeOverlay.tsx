"use client";

import { useState, type ReactNode } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { WalnutModal } from "@/components/ui/WalnutModal";

type ScreenerUpgradeOverlayProps = {
  title: string;
  body: string;
  children: ReactNode;
  badge?: string | null;
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
        {badge ? (
          <span className="pointer-events-none absolute right-3 top-3 rounded-full border border-amber-300/30 bg-amber-300/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-100">
            {badge}
          </span>
        ) : null}
      </div>

      <WalnutModal
        open={open}
        title={title}
        eyebrow={badge || "Premium"}
        tone="warning"
        onClose={() => setOpen(false)}
        closeLabel="Close upgrade prompt"
        panelClassName="max-w-md"
      >
        <UpgradePrompt title={title} body={body} compact={true} />
      </WalnutModal>
    </>
  );
}
