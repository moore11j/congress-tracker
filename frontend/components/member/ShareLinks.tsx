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
    <>
      <button
        type="button"
        onClick={nativeShare}
        className={`${ghostButtonClassName} min-w-0 whitespace-nowrap px-3 py-2 text-xs sm:px-4 sm:text-sm`}
      >
        Share
      </button>
      <button
        type="button"
        onClick={copyLink}
        className={`${ghostButtonClassName} min-w-0 whitespace-nowrap px-3 py-2 text-xs sm:px-4 sm:text-sm`}
      >
        {copied ? (
          "Copied"
        ) : (
          <>
            <span className="sm:hidden">Copy</span>
            <span className="hidden sm:inline">Copy Link</span>
          </>
        )}
      </button>
    </>
  );
}
