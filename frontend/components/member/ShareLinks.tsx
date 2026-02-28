"use client";

import { useEffect, useState } from "react";
import { ghostButtonClassName } from "@/lib/styles";

type Props = {
  canonicalUrl: string;
};

export function ShareLinks({ canonicalUrl }: Props) {
  const [copied, setCopied] = useState(false);
  const [canNativeShare, setCanNativeShare] = useState(false);

  useEffect(() => {
    setCanNativeShare(typeof navigator.share === "function");
  }, []);

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(canonicalUrl);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  }

  async function nativeShare() {
    if (!canNativeShare) {
      await copyLink();
      return;
    }

    try {
      await navigator.share({ url: canonicalUrl });
    } catch {
      // user cancelled or share unavailable
    }
  }

  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        onClick={nativeShare}
        className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}
      >
        Share
      </button>
      <button
        type="button"
        onClick={copyLink}
        className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}
      >
        {copied ? "Copied" : "Copy link"}
      </button>
    </div>
  );
}
