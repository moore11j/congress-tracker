import Link from "next/link";
import { CampaignEventOnMount, CampaignTrackedLink } from "@/components/campaign/CampaignAnalytics";
import { WatchlistQuickAddButton } from "@/components/campaign/WatchlistQuickAddButton";
import { campaignPropertiesFromRecord, pathWithCampaignParams, registerHref, type SearchParamRecord } from "@/lib/campaignAttribution";
import type { EntitlementTier } from "@/lib/entitlements";

type Props = {
  tickerA: string;
  tickerB: string;
  currentPath: string;
  searchParams: SearchParamRecord;
  authState: "logged_out" | "authenticated";
  plan: EntitlementTier | "logged_out" | string;
};

const compactPrimaryButtonClassName =
  "inline-flex min-h-10 items-center justify-center rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-wait disabled:opacity-70";
const compactSecondaryButtonClassName =
  "inline-flex min-h-10 items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";

function contextualProperties({ tickerA, tickerB, currentPath, searchParams, authState, plan }: Props) {
  return {
    ...campaignPropertiesFromRecord(searchParams),
    ticker_a: tickerA,
    ticker_b: tickerB,
    auth_state: authState,
    plan: String(plan || authState),
    page_path: currentPath,
  };
}

export function CompareContextualCta(props: Props) {
  const { tickerA, tickerB, currentPath, searchParams, authState, plan } = props;
  const properties = contextualProperties(props);
  const compareAnotherHref = pathWithCampaignParams("/compare/_/_", searchParams);
  const signupHref = registerHref(currentPath);
  const isLoggedOut = authState === "logged_out";
  const headline = isLoggedOut
    ? "Track this comparison."
    : plan === "free"
      ? "Keep following this matchup."
      : "Save this matchup.";
  const copy = isLoggedOut
    ? "Create a free account to save this matchup, compare more stocks and follow what changes."
    : plan === "free"
      ? "Add both stocks to your watchlist so you can return when the data or stronger stock changes."
      : "Add both stocks to a watchlist so you can return when the data or stronger stock changes.";

  return (
    <section className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-4">
      <CampaignEventOnMount eventName="compare_contextual_cta_view" path={currentPath} properties={properties} />
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-300">Follow-up workflow</p>
          <h2 className="mt-2 text-lg font-semibold text-white">{headline}</h2>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-slate-300">{copy}</p>
        </div>
        <div className="flex shrink-0 flex-wrap gap-2">
          {isLoggedOut ? (
            <CampaignTrackedLink
              href={signupHref}
              eventName="compare_contextual_cta_click"
              secondaryEventName="compare_signup_start"
              path={currentPath}
              properties={{ ...properties, cta: "create_free_account" }}
              className={compactPrimaryButtonClassName}
            >
              Create Free Account
            </CampaignTrackedLink>
          ) : (
            <WatchlistQuickAddButton
              symbols={[tickerA, tickerB]}
              label="Add Both to Watchlist"
              eventName="compare_add_watchlist_click"
              properties={properties}
              className={compactPrimaryButtonClassName}
            />
          )}
          <Link href={compareAnotherHref} className={compactSecondaryButtonClassName}>
            Compare Another Pair
          </Link>
        </div>
      </div>
    </section>
  );
}
