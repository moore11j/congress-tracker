"use client";

import { useEffect } from "react";

type FeedMountLoggerProps = {
  name: string;
  enabled?: boolean;
  detail?: Record<string, unknown>;
};

export function FeedMountLogger({ name, enabled = false, detail }: FeedMountLoggerProps) {
  useEffect(() => {
    if (!enabled) return;
    if (detail) {
      console.log(`[feed-debug] mount:${name}`, detail);
    } else {
      console.log(`[feed-debug] mount:${name}`);
    }

    return () => {
      if (detail) {
        console.log(`[feed-debug] unmount:${name}`, detail);
      } else {
        console.log(`[feed-debug] unmount:${name}`);
      }
    };
  }, [detail, enabled, name]);

  return null;
}
