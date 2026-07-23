"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { recordProductEvent } from "@/lib/api";

type CampaignCtaLinkProps = {
  href: string;
  eventName: string;
  className: string;
  children: ReactNode;
  properties?: Record<string, string | number | boolean | null>;
};

export function CampaignCtaLink({ href, eventName, className, children, properties }: CampaignCtaLinkProps) {
  return (
    <Link
      href={href}
      className={className}
      onClick={() => {
        recordProductEvent({ event_name: eventName, properties });
      }}
    >
      {children}
    </Link>
  );
}
