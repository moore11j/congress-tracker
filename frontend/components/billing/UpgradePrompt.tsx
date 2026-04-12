import Link from "next/link";

type UpgradePromptProps = {
  title: string;
  body: string;
  compact?: boolean;
};

export function UpgradePrompt({ title, body, compact = false }: UpgradePromptProps) {
  const primaryClassName =
    "inline-flex h-10 items-center justify-center rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20";
  const secondaryClassName =
    "inline-flex items-center justify-center rounded-lg border border-white/10 bg-transparent px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";

  return (
    <div className={`rounded-lg border border-emerald-300/25 bg-emerald-300/[0.06] ${compact ? "p-3" : "p-4"}`}>
      <div className="font-semibold text-emerald-100">{title}</div>
      <p className="mt-1 text-sm text-slate-300">{body}</p>
      <div className="mt-3 flex flex-wrap gap-2">
        <Link href="/account/billing" prefetch={false} className={primaryClassName}>
          Upgrade
        </Link>
        <Link href="/account/billing#compare" prefetch={false} className={secondaryClassName}>
          Compare plans
        </Link>
      </div>
    </div>
  );
}
