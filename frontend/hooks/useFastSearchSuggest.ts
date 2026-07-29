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

export function useFastSearchSuggest(query: string, options?: { limit?: number; minLength?: number; source?: string; debounceMs?: number; timeoutMs?: number; enabled?: boolean }) {
  const limit = options?.limit ?? 8;
  const minLength = options?.minLength ?? 2;
  const debounceMs = options?.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
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
      const requestTimeout = window.setTimeout(() => {
        timedOut = true;
        controller.abort();
        if (requestIdRef.current !== requestId) return;
        setState((current) => ({ results: current.results, loading: false, error: true, settled: true }));
      }, timeoutMs);
      try {
        const response = await searchSuggest(trimmed, limit, { signal: controller.signal, source });
        if (requestIdRef.current !== requestId) return;
        setState({
          results: Array.isArray(response.items) ? response.items : [],
          loading: false,
          error: false,
          settled: true,
        });
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
  }, [debounceMs, enabled, limit, minLength, query, source, timeoutMs]);

  return state;
}
