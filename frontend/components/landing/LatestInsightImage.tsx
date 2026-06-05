"use client";

import { useState } from "react";

type LatestInsightImageProps = {
  src: string | null;
  alt: string;
};

function FallbackPanel() {
  return (
    <div className="flex h-full min-h-0 items-center justify-center bg-[linear-gradient(135deg,rgba(16,185,129,0.16),rgba(14,165,233,0.08),rgba(2,6,23,0.98))]">
      <div className="text-center">
        <p className="font-mono text-xs font-semibold uppercase tracking-[0.22em] text-emerald-200">Walnut</p>
        <p className="mt-2 text-sm font-semibold text-slate-200">Market Terminal brief</p>
      </div>
    </div>
  );
}

export function LatestInsightImage({ src, alt }: LatestInsightImageProps) {
  const [failed, setFailed] = useState(false);

  return (
    <div className="mb-5 aspect-[16/9] overflow-hidden rounded-lg border border-white/10 bg-slate-900">
      {src && !failed ? (
        <img
          src={src}
          alt={alt}
          className="h-full w-full object-cover"
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setFailed(true)}
        />
      ) : (
        <FallbackPanel />
      )}
    </div>
  );
}
