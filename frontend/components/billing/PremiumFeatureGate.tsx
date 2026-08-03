import Link from "next/link";
import { subtlePrimaryButtonClassName } from "@/lib/styles";

type PremiumFeatureGateProps = {
  title?: string;
  body?: string;
  eyebrow?: string;
  className?: string;
};

export function PremiumFeatureGate({
  title = "Upgrade to Premium",
  body = "Upgrade to Premium to unlock the full workflow.",
  eyebrow = "Upgrade required",
  className = "",
}: PremiumFeatureGateProps) {
  return (
    <div className={`flex min-h-[21rem] items-center justify-center rounded-md border border-dashed border-white/15 bg-slate-950/45 px-4 py-10 text-center ${className}`}>
      <div className="max-w-xl">
        <p className="text-xs font-medium uppercase tracking-[0.22em] text-emerald-200/80">{eyebrow}</p>
        <h2 className="mt-3 text-xl font-medium text-white">{title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-300">{body}</p>
        <div className="mt-5 flex flex-wrap items-center justify-center gap-2">
          <Link href="/pricing" prefetch={false} className={`${subtlePrimaryButtonClassName} inline-flex h-10 rounded-md px-4`}>
            Upgrade to Premium
          </Link>
          <Link href="/pricing#compare" prefetch={false} className="inline-flex h-10 items-center justify-center rounded-md border border-white/10 px-4 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
            Compare plans
          </Link>
        </div>
      </div>
    </div>
  );
}
