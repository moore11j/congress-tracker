"use client";

import { useEffect } from "react";
import type { ReactNode } from "react";
import { recordProductEvent } from "@/lib/api";

type AnalyticsProps = {
  eventName: string;
  path?: string | null;
  properties?: Record<string, string | number | boolean | null>;
};

export function CompareEventOnMount({ eventName, path, properties }: AnalyticsProps) {
  useEffect(() => {
    recordProductEvent({ event_name: eventName, path, properties });
  }, [eventName, path, properties]);
  return null;
}

export function CompareTrackedLink({
  href,
  className,
  children,
  eventName,
  path,
  properties,
}: AnalyticsProps & {
  href: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <a
      href={href}
      className={className}
      onClick={() => recordProductEvent({ event_name: eventName, path, properties })}
    >
      {children}
    </a>
  );
}
