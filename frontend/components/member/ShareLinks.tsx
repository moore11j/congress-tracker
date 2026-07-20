"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
import { ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type Props = {
  canonicalUrl: string;
  showCopyButton?: boolean;
  buttonClassName?: string;
};

export function ShareLinks({ canonicalUrl, showCopyButton = true, buttonClassName }: Props) {
  const [copied, setCopied] = useState(false);
  const [open, setOpen] = useState(false);
  const [canNativeShare, setCanNativeShare] = useState(false);
  const popoverId = useId();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const copiedTimerRef = useRef<number | null>(null);

  const shareTargets = useMemo(() => {
    const encodedUrl = encodeURIComponent(canonicalUrl);
    return {
      x: `https://twitter.com/intent/tweet?url=${encodedUrl}`,
      linkedin: `https://www.linkedin.com/sharing/share-offsite/?url=${encodedUrl}`,
      email: `mailto:?subject=${encodeURIComponent("Walnut Markets")}&body=${encodedUrl}`,
    };
  }, [canonicalUrl]);

  useEffect(() => {
    setCanNativeShare(typeof navigator.share === "function");
  }, []);

  useEffect(() => {
    if (!open) return undefined;

    const handlePointerDown = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open]);

  useEffect(() => {
    return () => {
      if (copiedTimerRef.current !== null) window.clearTimeout(copiedTimerRef.current);
    };
  }, []);

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(canonicalUrl);
      setCopied(true);
      if (copiedTimerRef.current !== null) window.clearTimeout(copiedTimerRef.current);
      copiedTimerRef.current = window.setTimeout(() => setCopied(false), 1600);
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
      <div ref={containerRef} className="relative inline-flex">
        <button
          type="button"
          onClick={() => setOpen((current) => !current)}
          className={buttonClassName ?? `${ghostButtonClassName} min-w-0 whitespace-nowrap px-3 py-2 text-xs sm:px-4 sm:text-sm`}
          aria-expanded={open}
          aria-controls={open ? popoverId : undefined}
        >
          Share
        </button>
        {open ? (
          <div
            id={popoverId}
            role="dialog"
            aria-label="Share this page"
            className="absolute right-0 top-full z-50 mt-2 w-[min(calc(100vw_-_2rem),22rem)] overflow-hidden rounded-2xl border border-emerald-300/20 bg-slate-950/95 text-left text-slate-100 shadow-2xl shadow-black/50 ring-1 ring-white/[0.04] backdrop-blur"
          >
            <div className="border-b border-white/10 bg-[radial-gradient(circle_at_top_left,rgba(16,185,129,0.18),transparent_42%)] p-4">
              <div className="flex items-start gap-3">
                <WalnutBrandMark className="relative flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-emerald-300/30 bg-slate-950 shadow-[0_0_24px_rgba(16,185,129,0.16)]" svgClassName="h-7 w-7 overflow-visible" />
                <div className="min-w-0">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-emerald-200">Walnut Markets</p>
                  <p className="mt-1 text-sm font-semibold text-white">Share this page</p>
                  <p className="mt-1 truncate text-xs text-slate-400">{canonicalUrl}</p>
                </div>
              </div>
            </div>
            <div className="space-y-3 p-4">
              <div className={`grid gap-2 ${canNativeShare ? "grid-cols-2" : "grid-cols-1"}`}>
                {canNativeShare ? (
                  <button
                    type="button"
                    onClick={nativeShare}
                    className={`${subtlePrimaryButtonClassName} h-9 rounded-xl px-3 text-xs`}
                  >
                    Share sheet
                  </button>
                ) : null}
                <button
                  type="button"
                  onClick={copyLink}
                  className={`${canNativeShare ? ghostButtonClassName : subtlePrimaryButtonClassName} h-9 rounded-xl px-3 text-xs`}
                >
                  {copied ? "Copied" : "Copy link"}
                </button>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <a
                  href={shareTargets.x}
                  target="_blank"
                  rel="noreferrer"
                  className={`${ghostButtonClassName} h-9 rounded-xl px-3 text-xs`}
                >
                  X
                </a>
                <a
                  href={shareTargets.linkedin}
                  target="_blank"
                  rel="noreferrer"
                  className={`${ghostButtonClassName} h-9 rounded-xl px-3 text-xs`}
                >
                  LinkedIn
                </a>
                <a href={shareTargets.email} className={`${ghostButtonClassName} h-9 rounded-xl px-3 text-xs`}>
                  Email
                </a>
              </div>
            </div>
          </div>
        ) : null}
      </div>
      {showCopyButton ? (
        <button
          type="button"
          onClick={copyLink}
          className={buttonClassName ?? `${ghostButtonClassName} min-w-0 whitespace-nowrap px-3 py-2 text-xs sm:px-4 sm:text-sm`}
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
      ) : null}
    </>
  );
}
