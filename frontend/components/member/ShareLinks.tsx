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

  const twitterHref = `https://twitter.com/intent/tweet?url=${encodeURIComponent(canonicalUrl)}`;
  const linkedInHref = `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(canonicalUrl)}`;

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
    if (!canNativeShare) return;
    try {
      await navigator.share({ url: canonicalUrl });
    } catch {
      // user cancelled or share unavailable
    }
  }

  return (
    <div className="flex items-center gap-2">
      {canNativeShare ? (
        <button type="button" onClick={nativeShare} className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}>
          Share
        </button>
      ) : null}
      <button type="button" onClick={copyLink} className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}>
        {copied ? "Copied" : "Copy link"}
      </button>
      <a href={twitterHref} target="_blank" rel="noreferrer" className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}>
        X
      </a>
      <a href={linkedInHref} target="_blank" rel="noreferrer" className={`${ghostButtonClassName} px-3 py-1.5 text-xs`}>
        LinkedIn
      </a>
    </div>
  );
}
