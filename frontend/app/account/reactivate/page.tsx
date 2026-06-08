import { Suspense } from "react";
import { ReactivateAccountPanel } from "@/components/auth/ReactivateAccountPanel";

type SearchParams = Record<string, string | string[] | undefined>;

function getParam(searchParams: SearchParams, key: string): string {
  const value = searchParams[key];
  return typeof value === "string" ? value : "";
}

export default async function ReactivateAccountPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  return (
    <Suspense fallback={<div className="rounded-lg border border-white/10 bg-slate-900/70 p-5 text-sm text-slate-300">Checking reactivation link.</div>}>
      <ReactivateAccountPanel token={getParam(sp, "token")} />
    </Suspense>
  );
}
