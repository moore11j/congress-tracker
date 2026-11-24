"use client";

import type { ReactNode } from "react";
import { usePathname } from "next/navigation";

/** The additive chat preview owns its shell; existing routes keep theirs. */
export function RouteChrome({ kind, children }: { kind: "header" | "main" | "footer"; children: ReactNode }) {
  const chat = usePathname() === "/ask";
  if (kind === "main") return <main className={chat ? "relative z-0 min-w-0" : "relative z-0 w-full min-w-0 px-4 py-1.5 sm:px-6 lg:px-8 2xl:px-10"}>{children}</main>;
  return chat ? null : <>{children}</>;
}
