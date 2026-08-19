"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  createCustomAlertRule,
  deleteCustomAlertRule,
  duplicateCustomAlertRule,
  listCustomAlertRules,
  updateCustomAlertRule,
  type CustomAlertCondition,
  type CustomAlertMetric,
  type CustomAlertRule,
} from "@/lib/api";
import { ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type ScopeType = "any_watchlist_ticker" | "specific_ticker" | "watchlist_aggregate";
type Delivery = "immediate" | "daily" | "both";
type Draft = {
  id?: number;
  name: string;
  enabled: boolean;
  scope: { type: ScopeType; ticker: string | null };
  match_type: "all" | "any";
  conditions: CustomAlertCondition[];
  delivery: Delivery;
};

const operatorLabels: Record<string, string> = {
  gt: "is above",
  gte: "is at least",
  lt: "is below",
  lte: "is at most",
  crosses_above: "crosses above",
  crosses_below: "crosses below",
  increases_by: "increases by",
  decreases_by: "decreases by",
};

const baseCondition: CustomAlertCondition = {
  metric: "price_change_pct",
  operator: "increases_by",
  comparison_type: "value",
  comparison_value: 10,
  time_window: { value: 1, unit: "day" },
};

function draftFromRule(rule?: CustomAlertRule): Draft {
  return rule
    ? { id: rule.id, name: rule.name, enabled: rule.enabled, scope: rule.scope, match_type: rule.match_type, conditions: rule.conditions, delivery: rule.delivery }
    : { name: "", enabled: true, scope: { type: "any_watchlist_ticker", ticker: null }, match_type: "all", conditions: [baseCondition], delivery: "immediate" };
}

function defaultCondition(metric: CustomAlertMetric): CustomAlertCondition {
  const operator = metric.operators.includes("increases_by") ? "increases_by" : metric.operators.includes("lte") ? "lte" : metric.operators[0] ?? "gte";
  return {
    metric: metric.key,
    metric_params: paramsWithDefaults(metric),
    operator,
    comparison_type: "value",
    comparison_value: metric.key === "rsi" ? 35 : metric.key.includes("count") || metric.key.includes("buyers") ? 2 : 0,
    time_window: metric.requires_window ? { value: metric.key.includes("price") ? 1 : 7, unit: "day" } : null,
  };
}

function paramsWithDefaults(metric?: CustomAlertMetric): Record<string, number> {
  return metric?.params ? Object.fromEntries(Object.entries(metric.params).map(([key, value]) => [key, value.default])) : {};
}

function formatDate(value: string | null) {
  if (!value) return "Never triggered";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Triggered recently" : `Last triggered: ${date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" })}`;
}

export function CustomAlertRules({ watchlistId, tickers, canUseCustomAlerts }: { watchlistId: number; tickers: string[]; canUseCustomAlerts: boolean }) {
  const [rules, setRules] = useState<CustomAlertRule[]>([]);
  const [metrics, setMetrics] = useState<CustomAlertMetric[]>([]);
  const [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  const metricMap = useMemo(() => new Map(metrics.map((metric) => [metric.key, metric])), [metrics]);
  const groupedMetrics = useMemo(() => {
    const query = search.trim().toLowerCase();
    return metrics
      .filter((metric) => !query || `${metric.label} ${metric.category}`.toLowerCase().includes(query))
      .reduce<Record<string, CustomAlertMetric[]>>((groups, metric) => {
        (groups[metric.category] ??= []).push(metric);
        return groups;
      }, {});
  }, [metrics, search]);

  const reload = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await listCustomAlertRules(watchlistId);
      setRules(response.items);
      setMetrics(response.metrics);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to load custom alert rules.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { void reload(); }, [watchlistId]);

  const save = async () => {
    if (!draft) return;
    setBusy(true);
    setError(null);
    const payload = { name: draft.name, enabled: draft.enabled, scope: draft.scope, match_type: draft.match_type, conditions: draft.conditions, delivery: draft.delivery };
    try {
      const saved = draft.id
        ? await updateCustomAlertRule(watchlistId, draft.id, payload)
        : await createCustomAlertRule(watchlistId, payload);
      setRules((current) => draft.id ? current.map((rule) => rule.id === saved.id ? saved : rule) : [saved, ...current]);
      setDraft(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to save this alert.");
    } finally {
      setBusy(false);
    }
  };

  const setCondition = (index: number, next: CustomAlertCondition) => setDraft((current) => current ? {
    ...current,
    conditions: current.conditions.map((item, itemIndex) => itemIndex === index ? next : item),
  } : current);

  const changeMetric = (index: number, key: string) => {
    const metric = metricMap.get(key);
    if (metric) setCondition(index, defaultCondition(metric));
  };

  const updateRule = async (rule: CustomAlertRule, patch: Partial<Draft>) => {
    setBusy(true);
    setError(null);
    try {
      const saved = await updateCustomAlertRule(watchlistId, rule.id, patch);
      setRules((current) => current.map((item) => item.id === rule.id ? saved : item));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to update this alert.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/45 p-4 shadow-[0_18px_42px_-32px_rgba(15,23,42,0.95)]">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Custom Alert Rules</p>
            {!canUseCustomAlerts ? <span className="rounded border border-amber-300/30 bg-amber-300/10 px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide text-amber-100">Pro</span> : null}
          </div>
          <p className="mt-1 text-sm text-slate-400">Build your own alerts using price, technical, fundamental, and activity data.</p>
        </div>
        {canUseCustomAlerts ? <button type="button" onClick={() => { setError(null); setDraft(draftFromRule()); }} className={subtlePrimaryButtonClassName}>+ Create alert</button> : null}
      </div>

      {!canUseCustomAlerts ? (
        <div className="mt-4 rounded-xl border border-amber-300/15 bg-white/[0.025] p-4">
          <p className="text-sm font-semibold text-white">Define the setup. Walnut watches for it.</p>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-slate-400">Custom Alert Rules are included with Walnut Pro. Combine price, technical, Congress, insider, government-contract, and Walnut data in the exact setup you care about.</p>
          <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-3">
            <div className="rounded-md border border-white/10 bg-slate-950/40 p-2.5">Price increases at least 10% over 1 day</div>
            <div className="rounded-md border border-white/10 bg-slate-950/40 p-2.5">RSI (14) below 35 AND Price above SMA (200)</div>
            <div className="rounded-md border border-white/10 bg-slate-950/40 p-2.5">At least 2 Congress members buy within 7 days</div>
          </div>
          {rules.length ? <div className="mt-3 space-y-2">{rules.map((rule) => <div key={rule.id} className="flex items-center justify-between rounded-md border border-white/10 px-3 py-2 text-sm"><span className="text-slate-300">Locked: {rule.name}<span className="ml-2 text-xs text-slate-500">{rule.summary}</span></span></div>)}</div> : null}
          <Link href={`/pricing?return_to=${encodeURIComponent(`/watchlists/${watchlistId}`)}`} className={`mt-4 ${subtlePrimaryButtonClassName}`}>Upgrade to Pro</Link>
        </div>
      ) : null}

      {canUseCustomAlerts && draft ? <RuleBuilder draft={draft} tickers={tickers} metrics={metrics} metricMap={metricMap} groupedMetrics={groupedMetrics} search={search} setSearch={setSearch} busy={busy} onChange={setDraft} onMetricChange={changeMetric} onConditionChange={setCondition} onCancel={() => setDraft(null)} onSave={() => void save()} /> : null}
      {error ? <p role="alert" className="mt-3 text-sm text-rose-200">{error}</p> : null}
      {canUseCustomAlerts && loading ? <div className="mt-4 h-20 animate-pulse rounded-xl bg-white/[0.04]" /> : null}
      {canUseCustomAlerts && !loading && !rules.length && !draft ? <div className="mt-4 rounded-xl border border-dashed border-white/15 px-4 py-6 text-center"><p className="font-semibold text-slate-100">No custom alerts yet.</p><p className="mt-1 text-sm text-slate-400">Create a rule to have Walnut monitor exactly what matters to you.</p><button type="button" onClick={() => setDraft(draftFromRule())} className={`mt-3 ${ghostButtonClassName}`}>+ Create alert</button></div> : null}
      {canUseCustomAlerts && !loading && rules.length ? <div className="mt-4 space-y-2">{rules.map((rule) => <RuleCard key={rule.id} rule={rule} busy={busy} onEdit={() => setDraft(draftFromRule(rule))} onDuplicate={async () => { setBusy(true); try { const copy = await duplicateCustomAlertRule(watchlistId, rule.id); setRules((current) => [copy, ...current]); setDraft(draftFromRule(copy)); } catch (reason) { setError(reason instanceof Error ? reason.message : "Unable to duplicate this alert."); } finally { setBusy(false); } }} onToggle={() => void updateRule(rule, { enabled: !rule.enabled })} onDelete={async () => { if (!window.confirm(`Delete “${rule.name}”? This cannot be undone.`)) return; setBusy(true); try { await deleteCustomAlertRule(watchlistId, rule.id); setRules((current) => current.filter((item) => item.id !== rule.id)); } catch (reason) { setError(reason instanceof Error ? reason.message : "Unable to delete this alert."); } finally { setBusy(false); } }} />)}</div> : null}
    </section>
  );
}

function RuleBuilder({ draft, tickers, metrics, metricMap, groupedMetrics, search, setSearch, busy, onChange, onMetricChange, onConditionChange, onCancel, onSave }: {
  draft: Draft;
  tickers: string[];
  metrics: CustomAlertMetric[];
  metricMap: Map<string, CustomAlertMetric>;
  groupedMetrics: Record<string, CustomAlertMetric[]>;
  search: string;
  setSearch: (value: string) => void;
  busy: boolean;
  onChange: (value: Draft) => void;
  onMetricChange: (index: number, key: string) => void;
  onConditionChange: (index: number, value: CustomAlertCondition) => void;
  onCancel: () => void;
  onSave: () => void;
}) {
  const addCondition = () => {
    const fallback = metricMap.get("rsi") ?? metrics[0];
    if (fallback) onChange({ ...draft, conditions: [...draft.conditions, defaultCondition(fallback)] });
  };
  return (
    <div className="mt-4 rounded-xl border border-emerald-300/25 bg-slate-950/60 p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-200">When</p>
      <div className="mt-3 grid gap-2 md:grid-cols-3">
        <select value={draft.scope.type} onChange={(event) => onChange({ ...draft, scope: { type: event.target.value as ScopeType, ticker: null } })} className="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100">
          <option value="any_watchlist_ticker">Any ticker in this watchlist</option>
          <option value="specific_ticker">Specific ticker</option>
          <option value="watchlist_aggregate">Watchlist aggregate</option>
        </select>
        {draft.scope.type === "specific_ticker" ? <select value={draft.scope.ticker ?? ""} onChange={(event) => onChange({ ...draft, scope: { ...draft.scope, ticker: event.target.value || null } })} className="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100"><option value="">Choose ticker</option>{tickers.map((ticker) => <option key={ticker} value={ticker}>{ticker}</option>)}</select> : null}
        <select value={draft.match_type} onChange={(event) => onChange({ ...draft, match_type: event.target.value as "all" | "any" })} className="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100"><option value="all">All conditions (AND)</option><option value="any">Any condition (OR)</option></select>
      </div>
      <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search metrics" className="mt-3 w-full rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500" />
      <div className="mt-3 space-y-3">
        {draft.conditions.map((condition, index) => <ConditionRow key={index} index={index} condition={condition} matchType={draft.match_type} conditionCount={draft.conditions.length} metrics={metrics} metricMap={metricMap} groupedMetrics={groupedMetrics} onMetricChange={onMetricChange} onChange={(next) => onConditionChange(index, next)} onRemove={() => onChange({ ...draft, conditions: draft.conditions.filter((_, currentIndex) => currentIndex !== index) })} />)}
      </div>
      <button type="button" disabled={draft.conditions.length >= 10 || !metrics.length} onClick={addCondition} className={`mt-3 ${ghostButtonClassName}`}>+ Add condition</button>
      <p className="mt-5 text-xs font-semibold uppercase tracking-[0.2em] text-emerald-200">Then</p>
      <div className="mt-2 flex flex-wrap gap-2">
        <select value={draft.delivery} onChange={(event) => onChange({ ...draft, delivery: event.target.value as Delivery })} className="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100"><option value="immediate">Send immediately</option><option value="daily">Daily digest</option><option value="both">Both</option></select>
        <input value={draft.name} onChange={(event) => onChange({ ...draft, name: event.target.value })} placeholder="Alert name (optional)" className="min-w-60 flex-1 rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500" />
      </div>
      <div className="mt-4 flex justify-end gap-2"><button type="button" onClick={onCancel} className={ghostButtonClassName}>Cancel</button><button type="button" disabled={busy} onClick={onSave} className={subtlePrimaryButtonClassName}>{busy ? "Saving…" : draft.id ? "Save alert" : "Create alert"}</button></div>
    </div>
  );
}

function ConditionRow({ index, condition, matchType, conditionCount, metrics, metricMap, groupedMetrics, onMetricChange, onChange, onRemove }: {
  index: number;
  condition: CustomAlertCondition;
  matchType: "all" | "any";
  conditionCount: number;
  metrics: CustomAlertMetric[];
  metricMap: Map<string, CustomAlertMetric>;
  groupedMetrics: Record<string, CustomAlertMetric[]>;
  onMetricChange: (index: number, key: string) => void;
  onChange: (condition: CustomAlertCondition) => void;
  onRemove: () => void;
}) {
  const metric = metricMap.get(condition.metric);
  const targetMetric = metricMap.get(condition.comparison_metric ?? "");
  const metricParam = metric?.params?.period;
  const targetParam = targetMetric?.params?.period;
  const supportsMetricComparison = Boolean(metric?.metric_comparison);
  const showWindow = Boolean(metric?.requires_window || condition.time_window);
  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.025] p-3">
      {index ? <p className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">{matchType === "all" ? "AND" : "OR"}</p> : null}
      <div className="grid gap-2 lg:grid-cols-6">
        <select value={condition.metric} onChange={(event) => onMetricChange(index, event.target.value)} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100">
          {Object.entries(groupedMetrics).map(([category, options]) => <optgroup key={category} label={category}>{options.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</optgroup>)}
        </select>
        {metricParam ? <PeriodSelect value={condition.metric_params?.period ?? metricParam.default} spec={metricParam} onChange={(period) => onChange({ ...condition, metric_params: { ...condition.metric_params, period } })} /> : null}
        <select value={condition.operator} onChange={(event) => onChange({ ...condition, operator: event.target.value })} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100">{(metric?.operators ?? []).map((operator) => <option key={operator} value={operator}>{operatorLabels[operator] ?? operator}</option>)}</select>
        {supportsMetricComparison ? <select value={condition.comparison_type} onChange={(event) => onChange({ ...condition, comparison_type: event.target.value as "value" | "metric", comparison_metric: event.target.value === "metric" ? (condition.comparison_metric ?? "sma") : null })} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100"><option value="value">Value</option><option value="metric">Metric</option></select> : null}
        {condition.comparison_type === "metric" ? <><select value={condition.comparison_metric ?? "sma"} onChange={(event) => { const nextMetric = metricMap.get(event.target.value); onChange({ ...condition, comparison_metric: event.target.value, comparison_metric_params: paramsWithDefaults(nextMetric) }); }} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100">{metrics.filter((item) => item.kind === "numeric").map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</select>{targetParam ? <PeriodSelect value={condition.comparison_metric_params?.period ?? targetParam.default} spec={targetParam} onChange={(period) => onChange({ ...condition, comparison_metric_params: { ...condition.comparison_metric_params, period } })} /> : null}</> : <input type="number" value={condition.comparison_value ?? 0} onChange={(event) => onChange({ ...condition, comparison_value: Number(event.target.value) })} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100" />}
        {showWindow ? <div className="flex gap-1"><input type="number" min="1" max="365" value={condition.time_window?.value ?? 1} onChange={(event) => onChange({ ...condition, time_window: { value: Math.max(1, Number(event.target.value)), unit: condition.time_window?.unit ?? "day" } })} className="w-16 rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100" /><select value={condition.time_window?.unit ?? "day"} onChange={(event) => onChange({ ...condition, time_window: { value: condition.time_window?.value ?? 1, unit: event.target.value as "hour" | "day" | "month" } })} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100"><option value="hour">hours</option><option value="day">days</option><option value="month">months</option></select></div> : null}
        {conditionCount > 1 ? <button type="button" onClick={onRemove} className="text-sm text-rose-200 hover:text-rose-100">Remove</button> : null}
      </div>
    </div>
  );
}

function PeriodSelect({ value, spec, onChange }: { value: number; spec: { min: number; max: number }; onChange: (value: number) => void }) {
  const values = [2, 5, 9, 12, 14, 20, 26, 50, 100, 200, 252, 400].filter((item) => item >= spec.min && item <= spec.max);
  return <select value={value} onChange={(event) => onChange(Number(event.target.value))} className="rounded-md border border-white/10 bg-slate-900 px-2 py-2 text-sm text-slate-100">{values.map((period) => <option key={period} value={period}>{period}</option>)}</select>;
}

function RuleCard({ rule, busy, onEdit, onDuplicate, onToggle, onDelete }: { rule: CustomAlertRule; busy: boolean; onEdit: () => void; onDuplicate: () => void; onToggle: () => void; onDelete: () => void }) {
  const scopeLabel = rule.scope.type === "specific_ticker" ? rule.scope.ticker : rule.scope.type === "watchlist_aggregate" ? "Watchlist aggregate" : "Any ticker";
  return <div className="rounded-xl border border-white/10 bg-white/[0.025] p-3"><div className="flex flex-wrap items-start justify-between gap-3"><div><div className="flex items-center gap-2"><span className={`h-2 w-2 rounded-full ${rule.enabled ? "bg-emerald-300" : "bg-slate-500"}`} /><p className="font-semibold text-slate-100">{rule.name}</p></div><p className="mt-1 text-sm text-slate-400">{scopeLabel} · {rule.summary}</p><p className="mt-1 text-xs text-slate-500">{rule.delivery === "both" ? "Immediate + Daily" : rule.delivery === "daily" ? "Daily digest" : "Immediate email"} · {formatDate(rule.last_triggered_at)}{rule.last_triggered_ticker ? ` · ${rule.last_triggered_ticker}` : ""}</p></div><div className="flex flex-wrap gap-2"><button type="button" disabled={busy} onClick={onToggle} className={ghostButtonClassName}>{rule.enabled ? "Disable" : "Enable"}</button><button type="button" disabled={busy} onClick={onEdit} className={ghostButtonClassName}>Edit</button><button type="button" disabled={busy} onClick={onDuplicate} className={ghostButtonClassName}>Duplicate</button><button type="button" disabled={busy} onClick={onDelete} className="rounded-lg border border-rose-300/25 px-3 py-2 text-sm font-semibold text-rose-200 hover:bg-rose-300/10">Delete</button></div></div></div>;
}
