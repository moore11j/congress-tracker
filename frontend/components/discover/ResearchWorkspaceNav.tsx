import Link from "next/link";

export function ResearchWorkspaceNav({ active }: { active: "discover" | "my-stocks" }) {
  return (
    <nav aria-label="Research workspace" className="flex flex-wrap items-center gap-x-6 border-b border-white/10">
      {([
        ["discover", "/discover", "Discover"],
        ["my-stocks", "/my-stocks", "My Stocks"],
      ] as const).map(([key, href, label]) => (
        <Link key={key} href={href} prefetch={false} aria-current={active === key ? "page" : undefined}
          className={`border-b-2 py-3 text-sm font-medium transition ${active === key ? "border-emerald-300 text-white" : "border-transparent text-slate-500 hover:text-white"}`}>
          {label}
        </Link>
      ))}
      <Link href="/discover#advanced-research" prefetch={false} className="ml-auto py-3 text-xs text-slate-500 hover:text-white">Research tools <span aria-hidden="true">↗</span></Link>
    </nav>
  );
}
