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

const emailStorageKey = "ct:notificationEmail";

const triggerOptions: { value: AlertTriggerType; label: string }[] = [
  { value: "cross_source_confirmation", label: "cross-source" },
  { value: "smart_score_threshold", label: "score >= 80" },
  { value: "large_trade_threshold", label: "$250k+" },
  { value: "congress_activity", label: "Congress" },
  { value: "insider_activity", label: "insiders" },
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
  const [active, setActive] = useState(true);
  const [triggers, setTriggers] = useState<AlertTriggerType[]>(["cross_source_confirmation", "smart_score_threshold"]);
  const [subscription, setSubscription] = useState<NotificationSubscription | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);

  const minSmartScore = useMemo(() => (triggers.includes("smart_score_threshold") ? 80 : null), [triggers]);
  const largeTradeAmount = useMemo(() => (triggers.includes("large_trade_threshold") ? 250000 : null), [triggers]);
  const canUseDigests = hasEntitlement(entitlements, "notification_digests");
  const accountEmailDestination = sourceType === "watchlist" && useAccountEmailDestination;
  const panelClassName = compact
    ? "min-w-[20rem] space-y-4 font-sans"
    : "min-h-[13.5rem] rounded-lg border border-white/10 bg-slate-950/45 p-5 font-sans shadow-[0_18px_42px_-32px_rgba(15,23,42,0.95)]";
  const alertState = subscription ? (active ? "Active" : "Paused") : "Not subscribed";
  const alertStateClassName = subscription
    ? active
      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
      : "border-amber-300/30 bg-amber-300/10 text-amber-100"
    : "border-white/10 bg-white/[0.03] text-slate-300";
  const eyebrow = sourceType === "watchlist" ? "Watchlist alert" : "Saved view alert";

  useEffect(() => {
    if (!accountEmailDestination) {
      const storedEmail = window.localStorage.getItem(emailStorageKey) ?? "";
      setEmail(storedEmail);
    }
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    listNotificationSubscriptions({ source_type: sourceType, source_id: sourceId })
      .then((data) => {
        if (cancelled) return;
        const match = data.items[0] ?? null;
        setSubscription(match);
        if (match) {
          if (!accountEmailDestination) setEmail(match.email);
          setOnlyIfNew(match.only_if_new);
          setActive(match.active);
          setTriggers(match.alert_triggers.length ? match.alert_triggers : []);
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

  const toggleTrigger = (trigger: AlertTriggerType) => {
    setTriggers((current) => (current.includes(trigger) ? current.filter((item) => item !== trigger) : [...current, trigger]));
  };

  const save = async () => {
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
      const next = await saveNotificationSubscription({
        ...(accountEmailDestination ? {} : { email: trimmedEmail }),
        source_type: sourceType,
        source_id: sourceId,
        source_name: sourceName,
        source_payload: sourcePayload,
        only_if_new: onlyIfNew,
        active,
        alert_triggers: triggers,
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

  return (
    <div className={panelClassName}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-emerald-300/80">{eyebrow}</p>
          <h2 className="mt-1 text-lg font-semibold text-white">Email Digest</h2>
          <p className="mt-1 text-sm leading-6 text-slate-400">
            {accountEmailDestination ? "Sent to your account email on file." : "Daily, compact, alert-first."}
          </p>
        </div>
        <div className={`rounded-lg border px-3 py-2 text-right ${alertStateClassName}`}>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] opacity-70">Alert state</p>
          <p className="mt-0.5 text-sm font-semibold">{alertState}</p>
        </div>
      </div>

      {!canUseDigests ? (
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
                className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-sans text-sm normal-case tracking-normal text-slate-100 placeholder:text-slate-500"
              />
            </label>
          ) : null}

          <DigestSwitch
            checked={active}
            disabled={!canUseDigests}
            label="Active"
            description="Keep this digest eligible for delivery."
            onCheckedChange={setActive}
          />

          <DigestSwitch
            checked={onlyIfNew}
            disabled={!canUseDigests}
            label="Only send new items"
            description="Skip the email unless the watchlist has fresh activity."
            onCheckedChange={setOnlyIfNew}
          />
        </div>

        <div className="space-y-2">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">High-signal alerts</div>
          <div className="flex flex-wrap gap-2">
            {triggerOptions.map((option) => (
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

      <div className="flex flex-wrap items-center gap-2 pt-1">
        <button
          type="button"
          onClick={save}
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
