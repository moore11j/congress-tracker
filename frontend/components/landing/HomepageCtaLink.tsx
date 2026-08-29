"use client";

import type { AnchorHTMLAttributes, ReactNode } from "react";
import { recordGoogleAnalyticsEvent } from "@/lib/googleAnalytics";
import { hasPrivacyConsent } from "@/lib/privacyConsent";

type HomepageCtaLinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & {
  eventName: string;
  children: ReactNode;
};

/**
 * Keeps homepage CTA measurement close to the links without making the
 * marketing page itself client-rendered. Navigation must always work even if
 * analytics is unavailable or consent is declined.
 */
export function HomepageCtaLink({ eventName, children, onClick, ...props }: HomepageCtaLinkProps) {
  return (
    <a
      {...props}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented || !hasPrivacyConsent("analytics")) return;
        recordGoogleAnalyticsEvent(eventName, {
          pathname: window.location.pathname,
          source_page_type: "homepage",
        });
      }}
    >
      {children}
    </a>
  );
}
