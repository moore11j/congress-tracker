"use client";

import type { ReactNode } from "react";
import { useEffect, useState } from "react";

type CollapsibleFilterSectionProps = {
  title: string;
  description: string;
  defaultOpen?: boolean;
  storageKey: string;
  children: ReactNode;
};

const sectionCardClassName = "group rounded-2xl border border-slate-800 bg-slate-950/35 p-3";

export function CollapsibleFilterSection({
  title,
  description,
  defaultOpen = false,
  storageKey,
  children,
}: CollapsibleFilterSectionProps) {
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(storageKey);
      if (saved === "open") setOpen(true);
      if (saved === "closed") setOpen(false);
    } catch {
      setOpen(defaultOpen);
    }
  }, [defaultOpen, storageKey]);

  function handleToggle(nextOpen: boolean) {
    setOpen(nextOpen);
    try {
      window.localStorage.setItem(storageKey, nextOpen ? "open" : "closed");
    } catch {
      // Ignore storage failures; the section remains usable for this render.
    }
  }

  return (
    <details className={sectionCardClassName} open={open} onToggle={(event) => handleToggle(event.currentTarget.open)}>
      <summary className="flex cursor-pointer list-none flex-wrap items-center justify-between gap-2 [&::-webkit-details-marker]:hidden">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">{title}</p>
          <p className="mt-1 text-sm text-slate-400">{description}</p>
        </div>
        <span className="text-sm font-semibold text-slate-500 transition group-open:rotate-90">&gt;</span>
      </summary>
      <div className="mt-3">{children}</div>
    </details>
  );
}
