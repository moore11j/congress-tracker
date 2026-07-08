import { BacktestingWorkbench } from "@/components/backtesting/BacktestingWorkbench";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { getBacktestPresets, getEntitlements } from "@/lib/api";
import { defaultEntitlements, entitlementsFromTierHint } from "@/lib/entitlements";
import { buildReturnTo, requirePageAuthState } from "@/lib/serverAuth";

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
      max_position_weight: 1,
    },
    access: {
      tier: "free" as const,
      can_run: false,
      signed_in: false,
    },
    strategy_types: [
      { key: "watchlist" as const, label: "Watchlist" },
      { key: "saved_screen" as const, label: "Screens" },
      { key: "congress" as const, label: "Congress" },
      { key: "insider" as const, label: "Insider" },
      { key: "custom_tickers" as const, label: "Custom" },
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
    benchmark_options: [
      { symbol: "^GSPC" as const, label: "S&P 500", components: [{ symbol: "^GSPC", weight: 1 }] },
      { symbol: "QQQ" as const, label: "QQQ - Invesco QQQ Trust", components: [{ symbol: "QQQ", weight: 1 }] },
      { symbol: "IWM" as const, label: "IWM - iShares Russell 2000 ETF", components: [{ symbol: "IWM", weight: 1 }] },
      { symbol: "VT" as const, label: "VT - Vanguard Total World Stock ETF", components: [{ symbol: "VT", weight: 1 }] },
      {
        symbol: "SPY_TLT_60_40" as const,
        label: "60/40 Portfolio (SPY/TLT)",
        components: [{ symbol: "SPY", weight: 0.6 }, { symbol: "TLT", weight: 0.4 }],
      },
      {
        symbol: "BOGLEHEADS_3_FUND" as const,
        label: "Bogleheads 3 Fund (60/20/20)",
        components: [{ symbol: "VTI", weight: 0.6 }, { symbol: "VXUS", weight: 0.2 }, { symbol: "BND", weight: 0.2 }],
      },
    ],
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
  const sp = (await searchParams) ?? {};
  const returnTo = buildReturnTo("/backtesting", sp);
  const authState = await requirePageAuthState(returnTo);
  const authToken = authState.token;
  const [entitlements, presets] = authToken
    ? await Promise.all([
        getEntitlements(authToken).catch(() => defaultEntitlements),
        getBacktestPresets(authToken).catch(() => fallbackPresets()),
      ])
    : [entitlementsFromTierHint(authState.entitlementHint), fallbackPresets()];

  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <BacktestingWorkbench
        initialEntitlements={entitlements}
        initialPresets={presets}
        initialAuthPending={!authToken}
        initialQuery={{
          strategy: one(sp.strategy),
          watchlist_id: one(sp.watchlist_id),
          saved_screen_id: one(sp.saved_screen_id),
          scope: one(sp.scope),
          member_id: one(sp.member_id),
          insider_cik: one(sp.insider_cik),
          lookback_days: one(sp.lookback_days),
          hold_days: one(sp.hold_days),
          tickers: one(sp.tickers),
        }}
      />
    </VerifiedSessionGuard>
  );
}
