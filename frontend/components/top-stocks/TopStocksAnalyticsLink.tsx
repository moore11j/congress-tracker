"use client";

import type { AnchorHTMLAttributes, ReactNode } from "react";
import { useEffect } from "react";
import { recordProductEvent } from "@/lib/api";
import { recordGoogleAnalyticsEvent } from "@/lib/googleAnalytics";
import { hasPrivacyConsent } from "@/lib/privacyConsent";

type TopStocksAnalyticsLinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & {
  eventName: "top_stock_ticker_click" | "top_stocks_follow_click";
  symbol?: string | null;
  children: ReactNode;
};

export function TopStocksAnalyticsLink({ eventName, symbol = null, children, onClick, ...props }: TopStocksAnalyticsLinkProps) {
  return (
    <a
      {...props}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented || !hasPrivacyConsent("analytics")) return;
        recordGoogleAnalyticsEvent(eventName, {
          pathname: window.location.pathname,
          source_page_type: "top_stocks",
          symbol,
        });
        recordProductEvent({
          event_name: eventName,
          path: window.location.pathname,
          properties: { symbol },
        });
      }}
    >
      {children}
    </a>
  );
}

export function TopStocksViewAnalytics() {
  useEffect(() => {
    recordProductEvent({ event_name: "top_stocks_view", path: window.location.pathname });
    if (hasPrivacyConsent("analytics")) recordGoogleAnalyticsEvent("top_stocks_view", { source_page_type: "top_stocks" });
  }, []);
  return null;
}
