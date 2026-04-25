import { BacktestingWorkbench } from "@/components/backtesting/BacktestingWorkbench";
import { getBacktestPresets, getEntitlements } from "@/lib/api";
import { defaultEntitlements } from "@/lib/entitlements";
import { optionalPageAuthToken } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

function one(value: string | string[] | undefined) {
  return typeof value === "string" ? value : "";
}

function fallbackPresets() {
  return {
    today: new Date().toISOString().slice(0, 10),
    defaults: {
      benchmark: "^GSPC" as const,
      weighting: "equal" as const,
      hold_days: 90 as const,
      lookback_days: 365,
      start_balance: 10000,
      contribution_amount: 0,
      contribution_frequency: "none" as const,
      rebalancing_frequency: "monthly" as const,
    },
    access: {
      tier: "free" as const,
      can_run: false,
      signed_in: false,
    },
    strategy_types: [
      { key: "watchlist" as const, label: "Watchlist" },
      { key: "saved_screen" as const, label: "Saved Screen" },
      { key: "congress" as const, label: "Congress" },
      { key: "insider" as const, label: "Insider" },
    ],
    lookback_options: [
      { days: 30, label: "30D" },
      { days: 90, label: "90D" },
      { days: 180, label: "180D" },
      { days: 365, label: "1Y" },
      { days: 1095, label: "3Y" },
    ],
    hold_day_options: [
      { days: 30 as const, label: "30" },
      { days: 60 as const, label: "60" },
      { days: 90 as const, label: "90" },
      { days: 180 as const, label: "180" },
      { days: 365 as const, label: "365" },
    ],
    benchmark_options: [{ symbol: "^GSPC" as const, label: "S&P 500" }],
    contribution_frequency_options: [
      { key: "none" as const, label: "None" },
      { key: "monthly" as const, label: "Monthly" },
      { key: "quarterly" as const, label: "Quarterly" },
      { key: "annually" as const, label: "Annually" },
    ],
    rebalancing_frequency_options: [
      { key: "monthly" as const, label: "Monthly" },
      { key: "quarterly" as const, label: "Quarterly" },
      { key: "semi_annually" as const, label: "Semi-annually" },
      { key: "annually" as const, label: "Annually" },
    ],
    source_scopes: {
      congress: [
        { key: "all_congress" as const, label: "All Congress" },
        { key: "house" as const, label: "House" },
        { key: "senate" as const, label: "Senate" },
        { key: "member" as const, label: "Specific Member" },
      ],
      insider: [
        { key: "all_insiders" as const, label: "All Insiders" },
        { key: "insider" as const, label: "Specific Insider" },
      ],
    },
    watchlists: [],
    saved_screens: [],
  };
}

export default async function BacktestingPage({ searchParams }: Props) {
  const authToken = await optionalPageAuthToken();
  const sp = (await searchParams) ?? {};
  const [entitlements, presets] = await Promise.all([
    getEntitlements(authToken ?? undefined).catch(() => defaultEntitlements),
    getBacktestPresets(authToken ?? undefined).catch(() => fallbackPresets()),
  ]);

  return (
    <BacktestingWorkbench
      initialEntitlements={entitlements}
      initialPresets={presets}
      initialQuery={{
        strategy: one(sp.strategy),
        watchlist_id: one(sp.watchlist_id),
        saved_screen_id: one(sp.saved_screen_id),
        scope: one(sp.scope),
        member_id: one(sp.member_id),
        insider_cik: one(sp.insider_cik),
      }}
    />
  );
}
