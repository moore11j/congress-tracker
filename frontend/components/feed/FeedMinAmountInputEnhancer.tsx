"use client";

import { useEffect } from "react";

type FeedMinAmountInputEnhancerProps = {
  formId: string;
  inputName: string;
};

function toDigitsOnly(value: string): string {
  return value.replace(/[^\d]/g, "");
}

function addCommas(value: string): string {
  if (!value) return "";
  return value.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function FeedMinAmountInputEnhancer({ formId, inputName }: FeedMinAmountInputEnhancerProps) {
  useEffect(() => {
    const form = document.getElementById(formId);
    if (!(form instanceof HTMLFormElement)) return;

    const input = form.elements.namedItem(inputName);
    if (!(input instanceof HTMLInputElement)) return;

    const formatInputValue = () => {
      input.value = addCommas(toDigitsOnly(input.value));
    };

    const stripInputValue = () => {
      input.value = toDigitsOnly(input.value);
    };

    formatInputValue();

    input.addEventListener("input", formatInputValue);
    input.addEventListener("blur", formatInputValue);
    form.addEventListener("submit", stripInputValue);

    return () => {
      input.removeEventListener("input", formatInputValue);
      input.removeEventListener("blur", formatInputValue);
      form.removeEventListener("submit", stripInputValue);
    };
  }, [formId, inputName]);

  return null;
}
