"use client";

import { useEffect } from "react";

function hasDebugLifecycleFlag(search: string): boolean {
  const params = new URLSearchParams(search);
  return (
    params.get("debug") === "1" ||
    params.get("debug_lifecycle") === "1" ||
    params.get("debug_disable_feed_filters") === "1" ||
    params.get("debug_disable_feed_results") === "1" ||
    params.get("debug_plain_feed_shell") === "1"
  );
}

export function FeedLoadingMountProbe() {
  useEffect(() => {
    if (!hasDebugLifecycleFlag(window.location.search)) return;
    console.log("[feed-debug] mount:FeedLoading", { path: window.location.pathname });
    return () => {
      console.log("[feed-debug] unmount:FeedLoading", { path: window.location.pathname });
    };
  }, []);

  return null;
}
