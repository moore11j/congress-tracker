"use client";

import { useSearchParams } from "next/navigation";
import { useState } from "react";
import { trackCampaignEvent } from "@/components/campaign/CampaignAnalytics";
import { campaignParamKeys, preserveCurrentPath } from "@/lib/campaignAttribution";

type Props = {
  className?: string;
};

export function RedditTickerSearchForm({ className }: Props) {
  const searchParams = useSearchParams();
  const [query, setQuery] = useState("");

  return (
    <form
      action="/search"
      className={className}
      onSubmit={() => {
        trackCampaignEvent("reddit_ticker_search_start", {
          page_path: preserveCurrentPath(window.location.pathname, searchParams),
          auth_state: "unknown",
          plan: "unknown",
          query_entered: Boolean(query.trim()),
        });
      }}
    >
      {campaignParamKeys.map((key) => {
        const value = searchParams.get(key);
        return value ? <input key={key} type="hidden" name={key} value={value} /> : null;
      })}
      <input type="hidden" name="referring_landing_page" value="/reddit/stock-research" />
      <input
        name="q"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Enter a ticker"
        className="min-h-11 min-w-0 flex-1 rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-emerald-300/50"
      />
      <button type="submit" className="inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
        Search
      </button>
    </form>
  );
}
