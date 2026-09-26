export function RankingLocks({signupUrl, stocks = false}: {signupUrl: string; stocks?: boolean}) {
  return <>{[1, 2].map(rank => <article key={rank} className="min-w-0 rounded-lg border border-emerald-300/25 bg-emerald-300/[0.04] p-5">
    <p className="font-mono text-sm font-semibold text-emerald-300">#{rank}</p>
    <h3 className="mt-3 text-lg font-semibold text-white">{stocks ? rank === 1 ? "Unlock today's top idea" : "Unlock today's #2 idea" : `Unlock the #${rank} performer`}</h3>
    <p className="mt-2 text-sm text-slate-400">Included with your free account.</p>
    <a href={signupUrl} className="mt-4 inline-flex text-sm font-semibold text-emerald-200 underline underline-offset-4">Unlock the Top 2</a>
  </article>)}</>;
}
