"use client";

import { useEffect, useMemo, useState } from "react";
import {
  deleteNotificationSubscription,
  listNotificationSubscriptions,
  saveNotificationSubscription,
  type AlertTriggerType,
  type NotificationSubscription,
} from "@/lib/api";
import { subtlePrimaryButtonClassName } from "@/lib/styles";

type NotificationPreferencesProps = {
  sourceType: "watchlist" | "saved_view";
  sourceId: string;
  sourceName: string;
  sourcePayload?: Record<string, unknown>;
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

export function NotificationPreferences({
  sourceType,
  sourceId,
  sourceName,
  sourcePayload,
  compact = false,
}: NotificationPreferencesProps) {
  const [email, setEmail] = useState("");
  const [onlyIfNew, setOnlyIfNew] = useState(true);
  const [active, setActive] = useState(true);
  const [triggers, setTriggers] = useState<AlertTriggerType[]>(["cross_source_confirmation", "smart_score_threshold"]);
  const [subscription, setSubscription] = useState<NotificationSubscription | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const minSmartScore = useMemo(() => (triggers.includes("smart_score_threshold") ? 80 : null), [triggers]);
  const largeTradeAmount = useMemo(() => (triggers.includes("large_trade_threshold") ? 250000 : null), [triggers]);

  useEffect(() => {
    const storedEmail = window.localStorage.getItem(emailStorageKey) ?? "";
    setEmail(storedEmail);
    let cancelled = false;
    listNotificationSubscriptions({ source_type: sourceType, source_id: sourceId })
      .then((data) => {
        if (cancelled) return;
        const match = data.items[0] ?? null;
        setSubscription(match);
        if (match) {
          setEmail(match.email);
          setOnlyIfNew(match.only_if_new);
          setActive(match.active);
          setTriggers(match.alert_triggers.length ? match.alert_triggers : []);
          window.localStorage.setItem(emailStorageKey, match.email);
        }
      })
      .catch(() => {
        if (!cancelled) setStatus("Preferences are unavailable right now.");
      });
    return () => {
      cancelled = true;
    };
  }, [sourceId, sourceType]);

  const toggleTrigger = (trigger: AlertTriggerType) => {
    setTriggers((current) => (current.includes(trigger) ? current.filter((item) => item !== trigger) : [...current, trigger]));
  };

  const save = async () => {
    const trimmedEmail = email.trim();
    if (!trimmedEmail || !trimmedEmail.includes("@")) {
      setStatus("Enter an email address.");
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      const next = await saveNotificationSubscription({
        email: trimmedEmail,
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
      window.localStorage.setItem(emailStorageKey, trimmedEmail);
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
    <div className={compact ? "min-w-[19rem] space-y-4 font-sans text-xs" : "min-h-[13.5rem] rounded-lg border border-white/10 bg-white/[0.03] p-5 font-sans text-xs"}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="font-semibold text-white">Email digest</div>
          <div className="text-slate-400">Daily, compact, alert-first.</div>
        </div>
        <span className={`rounded border px-2 py-1 font-semibold ${subscription ? "border-emerald-300/30 text-emerald-100" : "border-white/10 text-slate-400"}`}>
          {subscription ? "on" : "off"}
        </span>
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(15rem,1fr)_minmax(18rem,1.2fr)]">
        <div className="space-y-3">
          <label className="grid gap-1 font-semibold uppercase tracking-wide text-slate-400">
            Email
            <input
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              placeholder="you@example.com"
              className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-sans text-sm normal-case tracking-normal text-slate-100 placeholder:text-slate-500"
            />
          </label>

          <label className="flex items-center gap-2 font-sans text-slate-200">
            <input type="checkbox" checked={onlyIfNew} onChange={(event) => setOnlyIfNew(event.target.checked)} />
            only send if there are new items
          </label>

          <label className="flex items-center gap-2 font-sans text-slate-200">
            <input type="checkbox" checked={active} onChange={(event) => setActive(event.target.checked)} />
            active
          </label>
        </div>

        <div className="space-y-2">
          <div className="font-semibold uppercase tracking-wide text-slate-400">High-signal alerts</div>
          <div className="flex flex-wrap gap-2">
            {triggerOptions.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => toggleTrigger(option.value)}
                className={`rounded-lg border px-2.5 py-1 font-sans font-semibold transition ${
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

      <div className="flex flex-wrap gap-2 pt-1">
        <button
          type="button"
          onClick={save}
          disabled={loading}
          className={subtlePrimaryButtonClassName}
        >
          {subscription ? "Update" : "Subscribe"}
        </button>
        {subscription ? (
          <button
            type="button"
            onClick={remove}
            disabled={loading}
            className="rounded-lg border border-white/10 px-3 py-1.5 font-semibold text-slate-200 disabled:opacity-60"
          >
            Remove
          </button>
        ) : null}
      </div>
      {status ? <div className="text-slate-400">{status}</div> : null}
    </div>
  );
}
