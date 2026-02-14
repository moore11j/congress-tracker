"use client";

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";

type FeedDebugVisibilityProps = {
  initialQueryDebug: boolean;
  children: React.ReactNode;
};

function readHashDebug() {
  if (typeof window === "undefined") return false;
  return window.location.hash === "#debug";
}

export function FeedDebugVisibility({ initialQueryDebug, children }: FeedDebugVisibilityProps) {
  const searchParams = useSearchParams();
  const queryDebug = useMemo(() => searchParams.get("debug") === "1", [searchParams]);
  const [hashDebug, setHashDebug] = useState(false);

  useEffect(() => {
    const sync = () => setHashDebug(readHashDebug());
    sync();
    window.addEventListener("hashchange", sync);
    return () => window.removeEventListener("hashchange", sync);
  }, []);

  const debug = initialQueryDebug || queryDebug || hashDebug;
  if (!debug) return null;

  return <>{children}</>;
}
