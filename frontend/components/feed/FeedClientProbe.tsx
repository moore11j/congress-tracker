"use client";

type FeedClientProbeProps = {
  label: string;
};

export function FeedClientProbe({ label }: FeedClientProbeProps) {
  return (
    <div className="rounded-lg border border-fuchsia-400/50 bg-fuchsia-500/10 px-3 py-2 text-xs font-semibold text-fuchsia-100">
      client-probe:{label}
    </div>
  );
}
