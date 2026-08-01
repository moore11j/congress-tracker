"use client";

import type { ReactNode } from "react";
import { useEffect, useRef } from "react";
import { recordProductEvent } from "@/lib/api";
import { currentCampaignProperties, type CampaignProperties } from "@/lib/campaignAttribution";

type CampaignAnalyticsProps = {
  eventName: string;
  path?: string | null;
  properties?: CampaignProperties;
};

export function CampaignEventOnMount({ eventName, path, properties }: CampaignAnalyticsProps) {
  const trackedRef = useRef(false);

  useEffect(() => {
    if (trackedRef.current) return;
    trackedRef.current = true;
    recordProductEvent({ event_name: eventName, path, properties: currentCampaignProperties(properties) });
  }, [eventName, path, properties]);

  return null;
}

export function CampaignTrackedLink({
  href,
  className,
  children,
  eventName,
  secondaryEventName,
  path,
  properties,
}: CampaignAnalyticsProps & {
  href: string;
  className?: string;
  children: ReactNode;
  secondaryEventName?: string;
}) {
  return (
    <a
      href={href}
      className={className}
      onClick={() => {
        if (secondaryEventName) {
          recordProductEvent({ event_name: secondaryEventName, path, properties: currentCampaignProperties(properties) });
        }
        recordProductEvent({ event_name: eventName, path, properties: currentCampaignProperties(properties) });
      }}
    >
      {children}
    </a>
  );
}

export function trackCampaignEvent(eventName: string, properties?: CampaignProperties, path?: string | null) {
  recordProductEvent({ event_name: eventName, path, properties: currentCampaignProperties(properties) });
}
