"use client";
import { useEffect, useSyncExternalStore } from "react";
import { createCreditBalanceStore, emptyCreditSnapshot } from "./researchCreditBalance";

const store = createCreditBalanceStore({
  fetch: (...args) => fetch(...args), visible: () => document.visibilityState !== "hidden",
  schedule: (callback, delay) => setTimeout(callback, delay), cancel: timer => clearTimeout(timer),
});
let mounts = 0;
const refresh = () => void store.refresh();
const authChanged = () => void store.refresh(true);
export const refreshResearchCredits = refresh;
export function useResearchCreditBalance(enabled = true) {
  useEffect(() => {
    if (!enabled) return;
    if (++mounts === 1) {
      window.addEventListener("focus", refresh);
      document.addEventListener("visibilitychange", refresh);
      window.addEventListener("ct:auth-updated", authChanged);
      window.addEventListener("walnut:credits-changed", refresh);
    }
    return () => {
      if (--mounts === 0) {
        window.removeEventListener("focus", refresh);
        document.removeEventListener("visibilitychange", refresh);
        window.removeEventListener("ct:auth-updated", authChanged);
        window.removeEventListener("walnut:credits-changed", refresh);
      }
    };
  }, [enabled]);
  return useSyncExternalStore(enabled ? store.subscribe : () => () => {}, store.getSnapshot, () => emptyCreditSnapshot);
}
