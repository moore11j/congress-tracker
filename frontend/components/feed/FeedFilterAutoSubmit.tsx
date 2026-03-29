"use client";

import { useEffect } from "react";

type FeedFilterAutoSubmitProps = {
  formId: string;
};

export function FeedFilterAutoSubmit({ formId }: FeedFilterAutoSubmitProps) {
  useEffect(() => {
    const form = document.getElementById(formId);
    if (!(form instanceof HTMLFormElement)) return;

    const selects = Array.from(form.querySelectorAll("select"));
    const onSelectChange = () => form.requestSubmit();

    selects.forEach((select) => select.addEventListener("change", onSelectChange));
    return () => {
      selects.forEach((select) => select.removeEventListener("change", onSelectChange));
    };
  }, [formId]);

  return null;
}
