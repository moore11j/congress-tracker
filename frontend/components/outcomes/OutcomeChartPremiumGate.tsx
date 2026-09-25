"use client";

import type { ReactNode } from "react";
import { ContextualUpgrade } from "@/components/billing/ContextualUpgrade";

// Presentation gate over the existing public-ledger charts. It does not change
// public outcome data, headline metrics, or the table's separate access rules.
export function OutcomeChartPremiumGate({ unlocked, title, body, feature, children }: {
  unlocked: boolean;
  title: string;
  body: string;
  feature: string;
  children: ReactNode;
}) {
  if (unlocked) return <>{children}</>;
  return (
    <section aria-label={title} className="grid min-w-0 overflow-hidden rounded-md">
      <div aria-hidden="true" inert className="pointer-events-none min-w-0 select-none opacity-50 blur-[5px] [grid-area:1/1]">
        {children}
      </div>
      <div className="relative grid min-w-0 items-center bg-slate-950/65 p-4 [grid-area:1/1]">
        <ContextualUpgrade title={title} body={body} feature={feature} />
      </div>
    </section>
  );
}
