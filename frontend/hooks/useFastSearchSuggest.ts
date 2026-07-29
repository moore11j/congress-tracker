"use client";

import { useEffect, useRef, useState } from "react";
import { cachedSearchSuggest, searchSuggest, type SearchSuggestResult } from "@/lib/api";

const DEFAULT_DEBOUNCE_MS = 100;
const DEFAULT_TIMEOUT_MS = 3500;

type FastSearchSuggestState = {
  results: SearchSuggestResult[];
  loading: boolean;
  error: boolean;
  settled: boolean;
};

export function useFastSearchSuggest(query: string, options?: { limit?: number; minLength?: number; source?: string; debounceMs?: number; timeoutMs?: number; includeDeepResults?: boolean; enabled?: boolean }) {
  const limit = options?.limit ?? 8;
  const minLength = options?.minLength ?? 2;
  const debounceMs = options?.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const includeDeepResults = options?.includeDeepResults ?? true;
  const source = options?.source ?? "FastSearchSuggest";
  const enabled = options?.enabled ?? true;
  const [state, setState] = useState<FastSearchSuggestState>({
    results: [],
    loading: false,
    error: false,
    settled: true,
  });
  const requestIdRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const trimmed = query.trim();
    abortRef.current?.abort();
    if (!enabled || trimmed.length < minLength) {
      requestIdRef.current += 1;
      setState({ results: [], loading: false, error: false, settled: true });
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const controller = new AbortController();
    abortRef.current = controller;
    const cached = cachedSearchSuggest(trimmed, limit);
    setState({
      results: cached?.items ?? [],
      loading: true,
      error: false,
      settled: Boolean(cached),
    });

    const timeout = window.setTimeout(async () => {
      let timedOut = false;
      let fastCompleted = false;
      const requestTimeout = window.setTimeout(() => {
        timedOut = true;
        controller.abort();
        if (fastCompleted) return;
        if (requestIdRef.current !== requestId) return;
        setState((current) => ({ results: current.results, loading: false, error: true, settled: true }));
      }, timeoutMs);
      try {
        const response = await searchSuggest(trimmed, limit, { signal: controller.signal, source });
        if (requestIdRef.current !== requestId) return;
        fastCompleted = true;
        setState({
          results: Array.isArray(response.items) ? response.items : [],
          loading: false,
          error: false,
          settled: true,
        });
        const fastItems = Array.isArray(response.items) ? response.items : [];
        const upperTrimmed = trimmed.toUpperCase();
        const hasExactTicker = fastItems.some((item) => item.kind === "ticker" && (item.symbol || item.id || "").toUpperCase() === upperTrimmed);
        const hasDeepEntity = fastItems.some((item) => item.kind === "insider" || item.kind === "institution");
        if (!includeDeepResults || trimmed.length < 3 || hasExactTicker || (fastItems.length >= limit && hasDeepEntity) || controller.signal.aborted) return;
        try {
          const deepResponse = await searchSuggest(trimmed, limit, { signal: controller.signal, source: `${source}Deep`, mode: "deep" });
          if (requestIdRef.current !== requestId) return;
          const deepItems = Array.isArray(deepResponse.items) ? deepResponse.items : [];
          const seen = new Set(fastItems.map((item) => `${item.kind}:${item.id || item.href}`));
          setState({
            results: [...fastItems, ...deepItems.filter((item) => {
              const key = `${item.kind}:${item.id || item.href}`;
              if (seen.has(key)) return false;
              seen.add(key);
              return true;
            })].slice(0, limit),
            loading: false,
            error: false,
            settled: true,
          });
        } catch (deepError) {
          if (deepError instanceof Error && deepError.name === "AbortError") return;
        }
      } catch (error) {
        if (error instanceof Error && error.name === "AbortError" && !timedOut) return;
        if (requestIdRef.current !== requestId) return;
        setState((current) => ({ results: current.results, loading: false, error: true, settled: true }));
      } finally {
        window.clearTimeout(requestTimeout);
      }
    }, debounceMs);

    return () => {
      window.clearTimeout(timeout);
      controller.abort();
    };
  }, [debounceMs, enabled, includeDeepResults, limit, minLength, query, source, timeoutMs]);

  return state;
}
