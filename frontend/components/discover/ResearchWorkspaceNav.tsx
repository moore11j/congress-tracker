import Link from "next/link";

export function ResearchWorkspaceNav({ active }: { active: "discover" | "my-stocks" }) {
  return (
    <nav aria-label="Research workspace" className="flex flex-wrap gap-2 border-b border-white/10 pb-4">
      {([
        ["discover", "/discover", "Discover"],
        ["my-stocks", "/my-stocks", "My Stocks"],
      ] as const).map(([key, href, label]) => (
        <Link key={key} href={href} prefetch={false} aria-current={active === key ? "page" : undefined}
          className={`rounded-xl px-4 py-3 text-sm font-semibold ${active === key ? "bg-emerald-300/10 text-emerald-200 ring-1 ring-emerald-300/30" : "text-slate-300 hover:bg-white/5 hover:text-white"}`}>
          {label}
        </Link>
      ))}
      <Link href="/discover#advanced-research" prefetch={false} className="rounded-xl px-4 py-3 text-sm text-slate-300 hover:text-white">Advanced Research</Link>
    </nav>
  );
}
