import type { Metadata } from "next";
import { Suspense } from "react";
import TickerPage from "@/app/ticker/[symbol]/page";
import { WalnutChatWorkspace } from "@/components/chat/WalnutChatWorkspace";
import { tickerRouteSymbol } from "@/lib/ticker";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "Ask Walnut | Walnut Markets", robots: { index: false, follow: true } };

export default async function AskPage({ searchParams }: { searchParams?: Promise<Record<string, string | string[] | undefined>> }) {
  const query = await searchParams ?? {};
  const symbol = typeof query.symbol === "string" ? tickerRouteSymbol(query.symbol) : null;
  const rankings = query.view === "rankings" && !symbol;
  return <WalnutChatWorkspace symbol={symbol} rankings={rankings}>
    {symbol ? <Suspense key={symbol} fallback={<p role="status" className="p-6 text-sm text-slate-400">Loading {symbol} research…</p>}>
      <TickerPage params={Promise.resolve({ symbol })} searchParams={Promise.resolve({})} />
    </Suspense> : null}
  </WalnutChatWorkspace>;
}
