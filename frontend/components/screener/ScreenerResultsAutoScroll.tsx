"use client";

import { useEffect, useRef } from "react";
import { requestScreenerResultsScroll, screenerResultsScrollStorageKey } from "@/lib/screenerResultsScroll";

type Props = {
  formId: string;
  resultsId: string;
  triggerKey: string;
};

export function ScreenerResultsAutoScroll({ formId, resultsId, triggerKey }: Props) {
  const resultsRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const form = document.getElementById(formId);
    if (!(form instanceof HTMLFormElement)) return;

    form.addEventListener("submit", requestScreenerResultsScroll);
    return () => {
      form.removeEventListener("submit", requestScreenerResultsScroll);
    };
  }, [formId]);

  useEffect(() => {
    const markScreenerLink = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const link = target.closest<HTMLAnchorElement>("a[data-screener-scroll-link='true']");
      if (!link) return;
      requestScreenerResultsScroll();
    };

    document.addEventListener("click", markScreenerLink, true);
    return () => {
      document.removeEventListener("click", markScreenerLink, true);
    };
  }, []);

  useEffect(() => {
    if (window.sessionStorage.getItem(screenerResultsScrollStorageKey) !== "1") return;
    window.sessionStorage.removeItem(screenerResultsScrollStorageKey);
    window.requestAnimationFrame(() => {
      resultsRef.current = document.getElementById(resultsId);
      resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }, [resultsId, triggerKey]);

  return null;
}
