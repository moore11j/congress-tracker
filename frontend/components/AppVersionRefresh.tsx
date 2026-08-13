"use client";

import { useEffect } from "react";

export function AppVersionRefresh({ version }: { version: string }) {
  useEffect(() => {
    let cancelled = false;

    async function checkVersion() {
      try {
        const response = await fetch(`/api/app-version?t=${Date.now()}`, {
          cache: "no-store",
          credentials: "same-origin",
        });
        if (!response.ok) return;
        const payload = await response.json() as { version?: string };
        if (!cancelled && payload.version && payload.version !== version) {
          window.location.reload();
        }
      } catch {
        // Ignore transient network failures; the next focus/interval check will retry.
      }
    }

    const onFocus = () => void checkVersion();
    const interval = window.setInterval(checkVersion, 60_000);
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onFocus);
    void checkVersion();

    return () => {
      cancelled = true;
      window.clearInterval(interval);
      window.removeEventListener("focus", onFocus);
      document.removeEventListener("visibilitychange", onFocus);
    };
  }, [version]);

  return null;
}
