"use client";

import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import {
  deleteNotificationSubscription,
  getEntitlements,
  listNotificationSubscriptions,
  saveNotificationSubscription,
  type AlertTriggerType,
  type NotificationSubscription,
} from "@/lib/api";
import { defaultEntitlements, hasEntitlement, type Entitlements } from "@/lib/entitlements";
import { subtlePrimaryButtonClassName } from "@/lib/styles";

type NotificationPreferencesProps = {
  sourceType: "watchlist" | "saved_view";
  sourceId: string;
  sourceName: string;
  sourcePayload?: Record<string, unknown>;
  useAccountEmailDestination?: boolean;
  compact?: boolean;
};

type DeliveryMode = "off" | "daily" | "intraday" | "both";
type DeliveryCategory =
  | "bullish_bearish_monitor"
  | "congress"
  | "conviction_threshold"
  | "cross_source"
  | "fundamentals"
  | "government_contracts"
  | "insiders"
  | "institutional_activity"
  | "large_trade_contract"
  | "news"
  | "press_releases";

const deliveryCategories: { value: DeliveryCategory; label: string; description: string; trigger: AlertTriggerType }[] = [
  { value: "bullish_bearish_monitor", label: "Bullish/bearish monitor", description: "Alerts when a ticker's overall signal shifts more positive or more negative.", trigger: "monitor_state" },
  { value: "congress", label: "Congress", description: "Alerts when a member of Congress reports buying or selling a ticker.", trigger: "congress_activity" },
  { value: "conviction_threshold", label: "Conviction threshold", description: "Alerts when the signal strength crosses an important threshold, such as moving into strong-conviction territory.", trigger: "smart_score_threshold" },
  { value: "cross_source", label: "Cross-source", description: "Alerts when multiple independent data sources point in the same direction.", trigger: "cross_source_confirmation" },
  { value: "fundamentals", label: "Fundamentals", description: "Alerts about the company's business health, including earnings, revenue, margins, valuation, or guidance.", trigger: "fundamentals" },
  { value: "government_contracts", label: "Government contracts", description: "Alerts about meaningful government awards, renewals, or contract activity tied to a company.", trigger: "government_contract" },
  { value: "insiders", label: "Insiders", description: "Alerts when company executives, directors, or other insiders report buying or selling shares.", trigger: "insider_activity" },
  { value: "institutional_activity", label: "Institutional activity", description: "Alerts about reported fund and institutional position changes in a ticker.", trigger: "institutional_activity" },
  { value: "large_trade_contract", label: "Large trade / contract", description: "Alerts about unusually large market trades or meaningful business contracts.", trigger: "large_trade_threshold" },
  { value: "news", label: "News", description: "Alerts when new relevant news coverage is available for a ticker.", trigger: "news" },
  { value: "press_releases", label: "Press releases", description: "Alerts when a company publishes an official announcement or filing-related release.", trigger: "press_releases" },
];

function AlertTypeHelp({ description }: { description: string }) {
  return (
    <span className="group relative inline-flex shrink-0">
      <span
        tabIndex={0}
        role="img"
        aria-label={`About this alert: ${description}`}
        className="inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-slate-500/80 text-[10px] font-bold leading-none text-slate-300 outline-none transition hover:border-emerald-300 hover:text-emerald-200 focus-visible:border-emerald-300 focus-visible:text-emerald-200 focus-visible:ring-2 focus-visible:ring-emerald-300/30"
      >
        i
      </span>
      <span
        role="tooltip"
        className="pointer-events-none absolute bottom-[calc(100%+0.5rem)] left-0 z-30 w-64 rounded-lg border border-emerald-300/20 bg-slate-950 px-3 py-2 text-xs font-normal leading-5 text-slate-200 opacity-0 shadow-xl transition duration-150 group-hover:opacity-100 group-focus-within:opacity-100"
      >
        {description}
      </span>
    </span>
  );
}

const defaultDeliveryModes = (): Record<DeliveryCategory, DeliveryMode> =>
  Object.fromEntries(deliveryCategories.map((category) => [category.value, "both"])) as Record<DeliveryCategory, DeliveryMode>;

function normalizeDeliveryModes(value: unknown): Record<DeliveryCategory, DeliveryMode> {
  const raw = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const defaults = Object.fromEntries(deliveryCategories.map((category) => [category.value, "off"])) as Record<DeliveryCategory, DeliveryMode>;
  for (const category of deliveryCategories) {
    const mode = raw[category.value];
    if (mode === "off" || mode === "daily" || mode === "intraday" || mode === "both") defaults[category.value] = mode;
  }
  return defaults;
}

const emailStorageKey = "ct:notificationEmail";

const triggerOptions: { value: AlertTriggerType; label: string }[] = [
  { value: "cross_source_confirmation", label: "Cross-source" },
  { value: "smart_score_threshold", label: "Conviction threshold" },
  { value: "monitor_state", label: "Bullish/bearish monitor" },
  { value: "large_trade_threshold", label: "Large trade / contract" },
  { value: "congress_activity", label: "Congress" },
  { value: "insider_activity", label: "Insiders" },
  { value: "government_contract", label: "Government contracts" },
  { value: "institutional_activity", label: "Institutional activity" },
  { value: "price_volume", label: "Price/volume" },
  { value: "fundamentals", label: "Fundamentals" },
];

function DigestSwitch({
  checked,
  disabled,
  label,
  description,
  onCheckedChange,
}: {
  checked: boolean;
  disabled: boolean;
  label: string;
  description: string;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onCheckedChange(!checked)}
      className="group flex w-full items-center justify-between gap-4 rounded-lg border border-white/10 bg-slate-950/50 px-3 py-2.5 text-left transition hover:border-emerald-300/30 disabled:cursor-not-allowed disabled:opacity-60"
    >
      <span>
        <span className="block text-sm font-semibold text-slate-100">{label}</span>
        <span className="mt-0.5 block text-xs leading-5 text-slate-400">{description}</span>
      </span>
      <span
        className={`relative h-6 w-11 shrink-0 rounded-full border transition ${
          checked
            ? "border-emerald-300/50 bg-emerald-300/20"
            : "border-white/15 bg-slate-900"
        }`}
      >
        <span
          className={`absolute top-1 h-4 w-4 rounded-full transition ${
            checked ? "left-6 bg-emerald-200 shadow-[0_0_14px_rgba(110,231,183,0.35)]" : "left-1 bg-slate-500"
          }`}
        />
      </span>
    </button>
  );
}

export function NotificationPreferences({
  sourceType,
  sourceId,
  sourceName,
  sourcePayload,
  useAccountEmailDestination = false,
  compact = false,
}: NotificationPreferencesProps) {
  const [email, setEmail] = useState("");
  const [onlyIfNew, setOnlyIfNew] = useState(true);
  const [dailyDigestEnabled, setDailyDigestEnabled] = useState(true);
  const [intradayAlertsEnabled, setIntradayAlertsEnabled] = useState(true);
  const [watchlistNewsEnabled, setWatchlistNewsEnabled] = useState(false);
  const [deliveryModes, setDeliveryModes] = useState<Record<DeliveryCategory, DeliveryMode>>(defaultDeliveryModes);
  const [triggers, setTriggers] = useState<AlertTriggerType[]>([
    "cross_source_confirmation",
    "smart_score_threshold",
    "monitor_state",
    "large_trade_threshold",
    "congress_activity",
    "insider_activity",
    "government_contract",
    "institutional_activity",
    "price_volume",
    "fundamentals",
  ]);
  const [subscription, setSubscription] = useState<NotificationSubscription | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(false);

  const minSmartScore = useMemo(() => (triggers.includes("smart_score_threshold") ? 80 : null), [triggers]);
  const largeTradeAmount = useMemo(() => (triggers.includes("large_trade_threshold") ? 250000 : null), [triggers]);
  const canUseDigests = entitlementsLoaded && hasEntitlement(entitlements, "notification_digests");
  const visibleTriggerOptions = useMemo(
    () =>
      triggerOptions.filter((option) => {
        if (option.value === "institutional_activity") {
          return hasEntitlement(entitlements, "institutional_feed");
        }
        return true;
      }),
    [entitlements],
  );
  const accountEmailDestination = sourceType === "watchlist" && useAccountEmailDestination;
  const panelClassName = compact
    ? "min-w-[20rem] space-y-4 font-sans"
    : "min-h-[13.5rem] rounded-lg border border-white/10 bg-slate-950/45 p-5 font-sans shadow-[0_18px_42px_-32px_rgba(15,23,42,0.95)]";
  const active = sourceType === "watchlist"
    ? Object.values(deliveryModes).some((mode) => mode !== "off")
    : dailyDigestEnabled || intradayAlertsEnabled;
  const alertState = subscription ? (active ? "Active" : "Paused") : "Not subscribed";
  const alertStateClassName = subscription
    ? active
      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
      : "border-amber-300/30 bg-amber-300/10 text-amber-100"
    : "border-white/10 bg-white/[0.03] text-slate-300";
  const eyebrow = sourceType === "watchlist" ? "Watchlist monitoring emails" : "Saved view monitoring emails";

  useEffect(() => {
    if (!accountEmailDestination) {
      const storedEmail = window.localStorage.getItem(emailStorageKey) ?? "";
      setEmail(storedEmail);
    }
    let cancelled = false;
    setEntitlementsLoaded(false);
    getEntitlements()
      .then((next) => {
        if (!cancelled) {
          setEntitlements(next);
          setEntitlementsLoaded(true);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setEntitlements(defaultEntitlements);
          setEntitlementsLoaded(true);
        }
      });
    listNotificationSubscriptions({ source_type: sourceType, source_id: sourceId })
      .then((data) => {
        if (cancelled) return;
        const match = data.items[0] ?? null;
        setSubscription(match);
        if (match) {
          if (!accountEmailDestination) setEmail(match.email);
          setOnlyIfNew(match.only_if_new);
          const payload = match.source_payload ?? {};
          setDailyDigestEnabled(typeof payload.daily_digest_enabled === "boolean" ? payload.daily_digest_enabled : match.active);
          setIntradayAlertsEnabled(typeof payload.intraday_alerts_enabled === "boolean" ? payload.intraday_alerts_enabled : match.active);
          setWatchlistNewsEnabled(typeof payload.watchlist_news_enabled === "boolean" ? payload.watchlist_news_enabled : false);
          const selectedTriggers = match.alert_triggers.length ? match.alert_triggers : [];
          setTriggers(selectedTriggers);
          if (payload.alert_delivery_modes && typeof payload.alert_delivery_modes === "object") {
            setDeliveryModes(normalizeDeliveryModes(payload.alert_delivery_modes));
          } else {
            const daily = typeof payload.daily_digest_enabled === "boolean" ? payload.daily_digest_enabled : match.active;
            const intraday = typeof payload.intraday_alerts_enabled === "boolean" ? payload.intraday_alerts_enabled : match.active;
            const legacyMode: DeliveryMode = daily && intraday ? "both" : daily ? "daily" : intraday ? "intraday" : "off";
            setDeliveryModes(Object.fromEntries(deliveryCategories.map((category) => [
              category.value,
              selectedTriggers.includes(category.trigger) ? legacyMode : "off",
            ])) as Record<DeliveryCategory, DeliveryMode>);
          }
          if (!accountEmailDestination) window.localStorage.setItem(emailStorageKey, match.email);
        }
      })
      .catch(() => {
        if (!cancelled) setStatus("Preferences are unavailable right now.");
      });
    return () => {
      cancelled = true;
    };
  }, [accountEmailDestination, sourceId, sourceType]);

  useEffect(() => {
    if (!entitlementsLoaded || hasEntitlement(entitlements, "institutional_feed")) return;
    setTriggers((current) => current.filter((trigger) => trigger !== "institutional_activity"));
  }, [entitlements, entitlementsLoaded]);

  const toggleTrigger = (trigger: AlertTriggerType) => {
    setTriggers((current) => (current.includes(trigger) ? current.filter((item) => item !== trigger) : [...current, trigger]));
  };

  const save = async (nextDeliveryModes = deliveryModes) => {
    if (!canUseDigests) {
      setStatus("Email digests and high-signal alerts are included with Premium.");
      return;
    }
    const trimmedEmail = email.trim();
    if (!accountEmailDestination && (!trimmedEmail || !trimmedEmail.includes("@"))) {
      setStatus("Enter an email address.");
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      const matrixTriggers = deliveryCategories
        .filter((category) => nextDeliveryModes[category.value] !== "off")
        .map((category) => category.trigger);
      const next = await saveNotificationSubscription({
        ...(accountEmailDestination ? {} : { email: trimmedEmail }),
        source_type: sourceType,
        source_id: sourceId,
        source_name: sourceName,
        source_payload: {
          ...(sourcePayload ?? {}),
          daily_digest_enabled: dailyDigestEnabled,
          intraday_alerts_enabled: intradayAlertsEnabled,
          watchlist_news_enabled: watchlistNewsEnabled,
          ...(sourceType === "watchlist" ? { alert_delivery_modes: nextDeliveryModes } : {}),
        },
        only_if_new: onlyIfNew,
        active: sourceType === "watchlist" ? Object.values(nextDeliveryModes).some((mode) => mode !== "off") : active,
        alert_triggers: sourceType === "watchlist" ? matrixTriggers : triggers,
        min_smart_score: minSmartScore,
        large_trade_amount: largeTradeAmount,
      });
      setSubscription(next);
      if (!accountEmailDestination) window.localStorage.setItem(emailStorageKey, trimmedEmail);
      setStatus("Digest saved.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to save digest.");
    } finally {
      setLoading(false);
    }
  };

  const selectDeliveryMode = (category: DeliveryCategory, mode: DeliveryMode) => {
    const next = { ...deliveryModes, [category]: mode };
    setDeliveryModes(next);
    void save(next);
  };

  const remove = async () => {
    if (!subscription) return;
    setLoading(true);
    setStatus(null);
    try {
      await deleteNotificationSubscription(subscription.id);
      setSubscription(null);
      setStatus("Digest removed.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to remove digest.");
    } finally {
      setLoading(false);
    }
  };

  if (sourceType === "watchlist") {
    return (
      <section className="min-w-0 rounded-2xl border border-white/10 bg-slate-950/45 p-4 shadow-[0_18px_42px_-32px_rgba(15,23,42,0.95)]">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Monitoring alerts</h2>
            <p className="mt-1 text-sm text-slate-400">Choose how each type of activity is delivered for this watchlist.</p>
          </div>
          <span className={`rounded-lg border px-2.5 py-1 text-xs font-semibold ${alertStateClassName}`}>{alertState}</span>
        </div>
        {!entitlementsLoaded ? (
          <div className="mt-4 h-24 animate-pulse rounded-xl bg-white/[0.04]" aria-busy="true" />
        ) : !canUseDigests ? (
          <div className="mt-4"><UpgradePrompt title="Premium alerts" body="Email digests and high-signal alerts are included with Premium." compact={true} /></div>
        ) : (
          <div className="mt-4 overflow-x-auto rounded-xl border border-white/10">
            <div className="min-w-[660px]">
              <div className="grid grid-cols-[minmax(230px,1.8fr)_repeat(4,minmax(82px,0.62fr))] border-b border-white/10 bg-white/[0.04] text-xs font-semibold text-slate-300">
                <div className="px-3 py-2.5">Alert type</div>
                {(["off", "daily", "intraday", "both"] as DeliveryMode[]).map((mode) => <div key={mode} className="px-2 py-2.5 text-center capitalize">{mode}</div>)}
              </div>
              {deliveryCategories.map((category) => (
                <div key={category.value} className="grid grid-cols-[minmax(230px,1.8fr)_repeat(4,minmax(82px,0.62fr))] border-b border-white/10 last:border-b-0">
                  <div className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-slate-100">
                    <span>{category.label}</span>
                    <AlertTypeHelp description={category.description} />
                  </div>
                  {(["off", "daily", "intraday", "both"] as DeliveryMode[]).map((mode) => {
                    const selected = deliveryModes[category.value] === mode;
                    return (
                      <button
                        key={mode}
                        type="button"
                        role="radio"
                        aria-checked={selected}
                        aria-label={`${category.label}: ${mode}`}
                        disabled={loading}
                        onClick={() => selectDeliveryMode(category.value, mode)}
                        className={`flex items-center justify-center border-l border-white/10 py-2 transition disabled:opacity-60 ${selected ? "bg-emerald-400/10" : "hover:bg-white/[0.03]"}`}
                      >
                        <span className={`h-4 w-4 rounded-full border-2 ${selected ? "border-emerald-200 bg-emerald-300 shadow-[0_0_0_3px_rgba(52,211,153,0.15)]" : "border-slate-500"}`} />
                      </button>
                    );
                  })}
                </div>
              ))}
            </div>
          </div>
        )}
        <div className="mt-4 border-t border-white/10 pt-4">
          <DigestSwitch
            checked={onlyIfNew}
            disabled={!canUseDigests || loading}
            label="Daily digest only when new"
            description="Skip the daily email unless qualifying activity occurred."
            onCheckedChange={(checked) => { setOnlyIfNew(checked); }}
          />
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <button type="button" onClick={() => void save()} disabled={loading || !canUseDigests} className={subtlePrimaryButtonClassName}>
              {loading ? "Saving..." : subscription ? "Save digest setting" : "Start monitoring"}
            </button>
            <span className="text-xs text-slate-500">Each row independently controls daily and intraday delivery.</span>
          </div>
        </div>
        {status ? <p className="mt-3 text-sm text-slate-400" role="status">{status}</p> : null}
      </section>
    );
  }

  return (
    <div className={panelClassName}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-emerald-300/80">{eyebrow}</p>
          <h2 className="mt-1 text-lg font-semibold text-white">Monitoring emails</h2>
          <p className="mt-1 text-sm leading-6 text-slate-400">
            {accountEmailDestination
              ? "This watchlist can send Daily Monitoring Digests and Intraday Monitoring Alerts to your account email."
              : "This source can send Daily Monitoring Digests and Intraday Monitoring Alerts."}
          </p>
        </div>
        <div className={`rounded-lg border px-3 py-2 text-right ${alertStateClassName}`}>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] opacity-70">Alert state</p>
          <p className="mt-0.5 text-sm font-semibold">{alertState}</p>
        </div>
      </div>

      {!entitlementsLoaded ? (
        <div className="rounded-lg border border-white/10 bg-white/[0.03] p-3" aria-busy="true" aria-live="polite">
          <div className="h-4 w-36 animate-pulse rounded bg-white/10" />
          <div className="mt-2 h-3 w-full max-w-sm animate-pulse rounded bg-white/10" />
        </div>
      ) : !canUseDigests ? (
        <UpgradePrompt
          title="Premium alerts"
          body="Email digests and high-signal alert triggers are included with Premium."
          compact={true}
        />
      ) : null}

      <div className="grid gap-4 xl:grid-cols-[minmax(15rem,1fr)_minmax(18rem,1.2fr)]">
        <div className="space-y-3">
          {!accountEmailDestination ? (
            <label className="grid gap-1 font-semibold uppercase tracking-wide text-slate-400">
              Email
              <input
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@example.com"
                disabled={!canUseDigests}
                className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-sans text-sm normal-case tracking-normal text-slate-100 placeholder:text-slate-500/40"
              />
            </label>
          ) : null}

          <DigestSwitch
            checked={dailyDigestEnabled}
            disabled={!canUseDigests}
            label="Daily Monitoring Digests"
            description="Send a summary of the prior day's monitoring alerts."
            onCheckedChange={setDailyDigestEnabled}
          />

          <DigestSwitch
            checked={intradayAlertsEnabled}
            disabled={!canUseDigests}
            label="Intraday Monitoring Alerts"
            description="Send alerts as soon as eligible monitoring events happen."
            onCheckedChange={setIntradayAlertsEnabled}
          />

          <DigestSwitch
            checked={onlyIfNew}
            disabled={!canUseDigests || !dailyDigestEnabled}
            label="Daily monitoring digest only when new"
            description="Skip the daily digest unless this source has fresh activity."
            onCheckedChange={setOnlyIfNew}
          />

        </div>

        <div className="space-y-2">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Monitoring Alerts</div>
          <p className="text-xs leading-5 text-slate-500">
            Choose which qualified monitoring changes can send during the day and roll into daily summaries.
          </p>
          <div className="flex flex-wrap gap-2">
            {visibleTriggerOptions.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => toggleTrigger(option.value)}
                disabled={!canUseDigests}
                className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
                  triggers.includes(option.value)
                    ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100"
                    : "border-white/10 text-slate-300 hover:border-white/20"
                }`}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <p className="text-xs leading-5 text-slate-500">
        Watchlist-level settings control this watchlist's monitoring emails. Trigger chips are enforced by the intraday sweep and daily alert summary.
      </p>

      <div className="flex flex-wrap items-center gap-2 pt-1">
        <button
          type="button"
          onClick={() => void save()}
          disabled={loading || !canUseDigests}
          className={subtlePrimaryButtonClassName}
        >
          {subscription ? "Update" : "Subscribe"}
        </button>
        {subscription ? (
          <button
            type="button"
            onClick={remove}
            disabled={loading}
            className="inline-flex h-10 items-center justify-center rounded-lg border border-white/10 px-4 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:opacity-60"
          >
            Remove
          </button>
        ) : null}
      </div>
      {status ? <div className="text-sm text-slate-400">{status}</div> : null}
    </div>
  );
}
