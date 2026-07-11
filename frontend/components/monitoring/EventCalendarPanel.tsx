"use client";

import { useEffect, useMemo, useRef, useState, type RefObject } from "react";
import { createPortal } from "react-dom";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import {
  ApiError,
  getEventCalendar,
  listNotificationSubscriptions,
  saveNotificationSubscription,
  type EventCalendarItem,
  type EventCalendarKind,
  type NotificationSubscription,
} from "@/lib/api";

type EventCalendarPanelProps = {
  canUseEventCalendar: boolean;
  loadingEntitlements: boolean;
};

type EconomicCategoryId = "inflation" | "jobs" | "rates" | "growth" | "consumer" | "housing" | "energy" | "trade" | "other";
type EventPopoverState = {
  date: string;
  items: EventCalendarItem[];
  left: number;
  top: number;
  pinned: boolean;
};

const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const fullMonthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const calendarKinds: EventCalendarKind[] = ["economic", "earnings", "dividend", "ipo", "split"];
const calendarAlertKinds: EventCalendarKind[] = ["economic", "earnings", "dividend", "ipo", "split"];

const kindClassNames: Record<EventCalendarKind, string> = {
  economic: "border-cyan-300/30 bg-cyan-300/10 text-cyan-100",
  earnings: "border-emerald-300/30 bg-emerald-300/10 text-emerald-100",
  dividend: "border-sky-300/30 bg-sky-300/10 text-sky-100",
  ipo: "border-fuchsia-300/30 bg-fuchsia-300/10 text-fuchsia-100",
  split: "border-amber-300/35 bg-amber-300/10 text-amber-100",
};

const kindLabels: Record<EventCalendarKind, string> = {
  economic: "Eco",
  earnings: "Earn",
  dividend: "Div",
  ipo: "IPO",
  split: "Split",
};

const defaultKindFilters: Record<EventCalendarKind, boolean> = {
  economic: true,
  earnings: true,
  dividend: true,
  ipo: true,
  split: true,
};

const defaultCalendarAlertKinds = Object.fromEntries(calendarAlertKinds.map((kind) => [kind, true])) as Record<EventCalendarKind, boolean>;

const economicCategories: { id: EconomicCategoryId; label: string; patterns: string[]; defaultOn: boolean }[] = [
  { id: "inflation", label: "Inflation", patterns: ["cpi", "pce", "ppi", "inflation", "price index", "prices"], defaultOn: true },
  { id: "jobs", label: "Jobs", patterns: ["payroll", "employment", "unemployment", "jobless", "jobs", "jolts", "wage", "claims"], defaultOn: true },
  { id: "rates", label: "Rates", patterns: ["fomc", "fed", "interest rate", "rate decision", "central bank", "ecb", "boe", "boj"], defaultOn: true },
  { id: "growth", label: "Growth", patterns: ["gdp", "pmi", "industrial production", "durable goods", "factory orders", "productivity"], defaultOn: true },
  { id: "consumer", label: "Consumer", patterns: ["retail", "consumer", "sentiment", "confidence", "spending", "sales"], defaultOn: true },
  { id: "housing", label: "Housing", patterns: ["housing", "home", "mortgage", "building permits", "starts", "construction"], defaultOn: true },
  { id: "energy", label: "Energy", patterns: ["crude", "oil", "gasoline", "natural gas", "eia", "rig"], defaultOn: false },
  { id: "trade", label: "Trade", patterns: ["trade balance", "exports", "imports", "current account"], defaultOn: false },
  { id: "other", label: "Other", patterns: [], defaultOn: false },
];

const defaultEconomicCategoryFilters = economicCategories.reduce((filters, category) => {
  filters[category.id] = category.defaultOn;
  return filters;
}, {} as Record<EconomicCategoryId, boolean>);

function monthStart(value: Date) {
  return new Date(value.getFullYear(), value.getMonth(), 1);
}

function addMonths(value: Date, months: number) {
  return new Date(value.getFullYear(), value.getMonth() + months, 1);
}

function monthEnd(value: Date) {
  return new Date(value.getFullYear(), value.getMonth() + 1, 0);
}

function dateKey(value: Date) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function daysForMonth(month: Date) {
  const first = monthStart(month);
  const last = monthEnd(month);
  const leading = (first.getDay() + 6) % 7;
  const cells: { key: string; day: number | null; date?: string }[] = [];
  for (let index = 0; index < leading; index += 1) {
    cells.push({ key: `blank-start-${index}`, day: null });
  }
  for (let day = 1; day <= last.getDate(); day += 1) {
    const value = new Date(first.getFullYear(), first.getMonth(), day);
    cells.push({ key: dateKey(value), day, date: dateKey(value) });
  }
  while (cells.length % 7 !== 0) {
    cells.push({ key: `blank-end-${cells.length}`, day: null });
  }
  return cells;
}

function itemSortLabel(item: EventCalendarItem) {
  return [item.symbol, item.country, item.title].filter(Boolean).join(" | ");
}

function eventDetail(item: EventCalendarItem) {
  return [item.symbol, item.company, item.country, item.subtitle].filter(Boolean).join(" | ");
}

function economicCategoryForItem(item: EventCalendarItem): EconomicCategoryId {
  const payloadEvent = typeof item.payload?.event === "string" ? item.payload.event : "";
  const text = [item.title, item.subtitle, payloadEvent].filter(Boolean).join(" ").toLowerCase();
  for (const category of economicCategories) {
    if (category.id === "other") continue;
    if (category.patterns.some((pattern) => text.includes(pattern))) return category.id;
  }
  return "other";
}

function calendarErrorMessage(errors?: { kind: string; reason: string }[]) {
  const reasons = Array.from(new Set((errors ?? []).map((error) => error.reason).filter(Boolean)));
  if (reasons.length === 0) return "Calendar providers are temporarily unavailable.";
  if (reasons.includes("provider_disabled") || reasons.includes("background_provider_disabled")) return "FMP calendar provider is disabled.";
  if (reasons.includes("page_fetch_blocked")) return "FMP calendar live fetches are blocked by provider settings.";
  if (reasons.includes("provider_entitlement")) return "FMP calendar endpoints are blocked by API auth or plan entitlement.";
  if (reasons.includes("provider_rate_limited")) return "FMP calendar provider is rate-limited.";
  return `Calendar provider issue: ${reasons.slice(0, 2).join(", ")}.`;
}

function selectedYearRange(anchor: Date) {
  const currentYear = new Date().getFullYear();
  const anchorYear = anchor.getFullYear();
  const min = Math.min(currentYear - 2, anchorYear - 2);
  const max = Math.max(currentYear + 3, anchorYear + 3);
  return Array.from({ length: max - min + 1 }, (_, index) => min + index);
}

function dateHeading(value: string) {
  const [year, month, day] = value.split("-").map(Number);
  if (!year || !month || !day) return value;
  return new Date(year, month - 1, day).toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function itemDisplayTitle(item: EventCalendarItem) {
  return item.kind === "economic" ? item.title : `${item.symbol || item.company || "Company"} ${kindLabels[item.kind]}`;
}

function savedCalendarAlertKinds(payload?: Record<string, unknown> | null) {
  const rawKinds = payload?.calendar_kinds;
  if (!Array.isArray(rawKinds)) return defaultCalendarAlertKinds;
  const selected = new Set(rawKinds.filter((kind): kind is EventCalendarKind => calendarAlertKinds.includes(kind as EventCalendarKind)));
  return Object.fromEntries(calendarAlertKinds.map((kind) => [kind, selected.has(kind)])) as Record<EventCalendarKind, boolean>;
}

function selectedCalendarAlertKinds(kinds: Record<EventCalendarKind, boolean>) {
  return calendarAlertKinds.filter((kind) => kinds[kind]);
}

function EventPopover({
  state,
  popoverRef,
  onMouseEnter,
  onMouseLeave,
}: {
  state: EventPopoverState;
  popoverRef: RefObject<HTMLDivElement | null>;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}) {
  if (typeof document === "undefined") return null;

  return createPortal(
    <div
      ref={popoverRef}
      role="dialog"
      aria-label={`Events for ${state.date}`}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      className="fixed z-[9999] w-[min(24rem,calc(100vw-2rem))] overflow-hidden rounded-lg border border-white/15 bg-slate-950/95 shadow-2xl shadow-black/50 backdrop-blur"
      style={{ left: state.left, top: state.top }}
    >
      <div className="flex items-start justify-between gap-3 border-b border-white/10 px-3 py-2">
        <div>
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Calendar events</div>
          <div className="mt-0.5 text-sm font-semibold text-white">{dateHeading(state.date)}</div>
        </div>
        {state.pinned ? <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-100">Pinned</span> : null}
      </div>
      <div className="max-h-80 overflow-y-auto p-2 [scrollbar-width:thin] [scrollbar-color:rgba(148,163,184,0.45)_transparent]">
        <div className="space-y-1.5">
          {state.items.map((item) => {
            const detail = eventDetail(item);
            return (
              <div key={item.id} className="rounded-md border border-white/10 bg-slate-900/80 p-2">
                <div className="flex items-start gap-2">
                  <span className={`mt-0.5 shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-semibold ${kindClassNames[item.kind]}`}>{kindLabels[item.kind]}</span>
                  <div className="min-w-0">
                    <div className="text-sm font-semibold leading-5 text-slate-100">{itemDisplayTitle(item)}</div>
                    {detail ? <div className="mt-0.5 text-xs leading-4 text-slate-400">{detail}</div> : null}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>,
    document.body,
  );
}

function CalendarMonth({
  month,
  itemsByDate,
  muted,
  onPreviewItems,
  onPinItems,
  onLeavePreview,
}: {
  month: Date;
  itemsByDate: Map<string, EventCalendarItem[]>;
  muted?: boolean;
  onPreviewItems: (date: string, items: EventCalendarItem[], element: HTMLElement) => void;
  onPinItems: (date: string, items: EventCalendarItem[], element: HTMLElement) => void;
  onLeavePreview: () => void;
}) {
  return (
    <section className={`min-w-0 rounded-lg border border-white/10 bg-slate-950/45 p-3 ${muted ? "opacity-55" : ""}`}>
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-white">{fullMonthNames[month.getMonth()]}</h3>
        <span className="text-xs text-slate-500">{month.getFullYear()}</span>
      </div>
      <div className="mt-3 grid grid-cols-7 gap-1 text-center text-[10px] font-semibold uppercase tracking-wide text-slate-500">
        {weekdays.map((weekday) => (
          <span key={weekday}>{weekday}</span>
        ))}
      </div>
      <div className="mt-1 grid grid-cols-7 gap-1">
        {daysForMonth(month).map((cell) => {
          const items = cell.date ? itemsByDate.get(cell.date) ?? [] : [];
          return (
            <div
              key={cell.key}
              className={`min-h-[5.75rem] rounded-md border p-1.5 ${
                cell.day ? "border-white/10 bg-slate-900/60" : "border-transparent"
              }`}
            >
              {cell.day ? (
                <>
                  <div className="text-[11px] font-semibold text-slate-300">{cell.day}</div>
                  <div className="mt-1 space-y-1">
                    {items.slice(0, 3).map((item) => (
                      <button
                        key={item.id}
                        type="button"
                        title={`${item.title}${eventDetail(item) ? ` | ${eventDetail(item)}` : ""}`}
                        onMouseEnter={(event) => cell.date && onPreviewItems(cell.date, items, event.currentTarget)}
                        onMouseLeave={onLeavePreview}
                        onClick={(event) => {
                          event.stopPropagation();
                          if (cell.date) onPinItems(cell.date, items, event.currentTarget);
                        }}
                        className={`block w-full truncate rounded border px-1.5 py-0.5 text-left text-[10px] font-semibold transition hover:border-white/35 ${kindClassNames[item.kind]}`}
                      >
                        {item.symbol || kindLabels[item.kind]} {item.kind === "economic" ? item.title : kindLabels[item.kind]}
                      </button>
                    ))}
                    {items.length > 3 ? (
                      <button
                        type="button"
                        onMouseEnter={(event) => cell.date && onPreviewItems(cell.date, items, event.currentTarget)}
                        onMouseLeave={onLeavePreview}
                        onClick={(event) => {
                          event.stopPropagation();
                          if (cell.date) onPinItems(cell.date, items, event.currentTarget);
                        }}
                        className="block w-full rounded px-1 py-0.5 text-left text-[10px] text-slate-500 transition hover:bg-white/[0.04] hover:text-slate-300"
                      >
                        +{items.length - 3} more
                      </button>
                    ) : null}
                  </div>
                </>
              ) : null}
            </div>
          );
        })}
      </div>
    </section>
  );
}

export function EventCalendarPanel({ canUseEventCalendar, loadingEntitlements }: EventCalendarPanelProps) {
  const [anchorMonth, setAnchorMonth] = useState(() => monthStart(new Date()));
  const [items, setItems] = useState<EventCalendarItem[]>([]);
  const [activeKinds, setActiveKinds] = useState<Record<EventCalendarKind, boolean>>(defaultKindFilters);
  const [activeEconomicCategories, setActiveEconomicCategories] = useState<Record<EconomicCategoryId, boolean>>(defaultEconomicCategoryFilters);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [alertKinds, setAlertKinds] = useState<Record<EventCalendarKind, boolean>>(defaultCalendarAlertKinds);
  const [subscription, setSubscription] = useState<NotificationSubscription | null>(null);
  const [prefStatus, setPrefStatus] = useState<string | null>(null);
  const [savingPref, setSavingPref] = useState(false);
  const [popover, setPopover] = useState<EventPopoverState | null>(null);
  const popoverRef = useRef<HTMLDivElement | null>(null);
  const closePopoverTimer = useRef<number | null>(null);

  const months = useMemo(() => [addMonths(anchorMonth, -1), anchorMonth, addMonths(anchorMonth, 1)], [anchorMonth]);
  const start = dateKey(months[0]);
  const end = dateKey(monthEnd(months[2]));
  const years = selectedYearRange(anchorMonth);
  const filteredItems = useMemo(
    () =>
      items.filter((item) => {
        if (!activeKinds[item.kind]) return false;
        if (item.kind === "economic") return activeEconomicCategories[economicCategoryForItem(item)];
        return true;
      }),
    [activeEconomicCategories, activeKinds, items],
  );
  const countsByKind = useMemo(() => {
    const counts = Object.fromEntries(calendarKinds.map((kind) => [kind, 0])) as Record<EventCalendarKind, number>;
    for (const item of items) {
      counts[item.kind] = (counts[item.kind] ?? 0) + 1;
    }
    return counts;
  }, [items]);
  const visibleCountsByKind = useMemo(() => {
    const counts = Object.fromEntries(calendarKinds.map((kind) => [kind, 0])) as Record<EventCalendarKind, number>;
    for (const item of filteredItems) {
      counts[item.kind] = (counts[item.kind] ?? 0) + 1;
    }
    return counts;
  }, [filteredItems]);
  const corporateCount = calendarKinds
    .filter((kind) => kind !== "economic")
    .reduce((sum, kind) => sum + (countsByKind[kind] ?? 0), 0);
  const hiddenByFilters = Math.max(items.length - filteredItems.length, 0);
  const itemsByDate = useMemo(() => {
    const map = new Map<string, EventCalendarItem[]>();
    for (const item of filteredItems) {
      map.set(item.date, [...(map.get(item.date) ?? []), item].sort((a, b) => itemSortLabel(a).localeCompare(itemSortLabel(b))));
    }
    return map;
  }, [filteredItems]);
  const upcomingItems = useMemo(() => {
    const today = dateKey(new Date());
    return filteredItems.filter((item) => item.date >= today).slice(0, 6);
  }, [filteredItems]);

  const clearPopoverCloseTimer = () => {
    if (closePopoverTimer.current !== null) {
      window.clearTimeout(closePopoverTimer.current);
      closePopoverTimer.current = null;
    }
  };

  const popoverPosition = (element: HTMLElement) => {
    const rect = element.getBoundingClientRect();
    const margin = 16;
    const gap = 8;
    const width = Math.min(384, window.innerWidth - margin * 2);
    const estimatedHeight = 360;
    const left = Math.min(Math.max(rect.left, margin), Math.max(margin, window.innerWidth - width - margin));
    let top = rect.bottom + gap;
    if (top + estimatedHeight > window.innerHeight - margin && rect.top - estimatedHeight - gap > margin) {
      top = rect.top - estimatedHeight - gap;
    }
    return { left, top: Math.max(margin, Math.min(top, window.innerHeight - 120)) };
  };

  const showPopover = (date: string, nextItems: EventCalendarItem[], element: HTMLElement, pinned: boolean) => {
    if (nextItems.length === 0) return;
    clearPopoverCloseTimer();
    const position = popoverPosition(element);
    setPopover({
      date,
      items: nextItems,
      left: position.left,
      top: position.top,
      pinned,
    });
  };

  const schedulePopoverClose = () => {
    clearPopoverCloseTimer();
    closePopoverTimer.current = window.setTimeout(() => {
      setPopover((current) => (current?.pinned ? current : null));
    }, 120);
  };

  const toggleKind = (kind: EventCalendarKind) => {
    setActiveKinds((current) => ({ ...current, [kind]: !current[kind] }));
  };

  const toggleEconomicCategory = (category: EconomicCategoryId) => {
    setActiveEconomicCategories((current) => ({ ...current, [category]: !current[category] }));
  };

  const toggleAlertKind = (kind: EventCalendarKind) => {
    setAlertKinds((current) => ({ ...current, [kind]: !current[kind] }));
  };

  const clearAlertKinds = () => {
    setAlertKinds(Object.fromEntries(calendarAlertKinds.map((kind) => [kind, false])) as Record<EventCalendarKind, boolean>);
  };

  const applyAllAlertKinds = () => {
    setAlertKinds(defaultCalendarAlertKinds);
  };

  useEffect(() => {
    if (!canUseEventCalendar) return;
    const controller = new AbortController();
    setLoading(true);
    setStatus(null);
    getEventCalendar({ start, end, scope: "watchlist", signal: controller.signal, source: "MonitoringEventCalendar" })
      .then((response) => {
        setItems(response.items);
        if (response.status === "partial") setStatus(calendarErrorMessage(response.errors));
        else if (response.status === "unavailable") setStatus(calendarErrorMessage(response.errors));
      })
      .catch((error) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setItems([]);
        setStatus(error instanceof ApiError && error.status === 402 ? "Upgrade required for event calendar overlays." : "Event calendar is temporarily unavailable.");
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [canUseEventCalendar, end, start]);

  useEffect(() => {
    if (!canUseEventCalendar) return;
    let cancelled = false;
    listNotificationSubscriptions({ source_type: "event_calendar" })
      .then((response) => {
        if (cancelled) return;
        const next = response.items[0] ?? null;
        setSubscription(next);
        if (!next || !next.active || next.source_id === "none") {
          setAlertKinds(Object.fromEntries(calendarAlertKinds.map((kind) => [kind, false])) as Record<EventCalendarKind, boolean>);
        } else if (next.source_id === "watchlist") {
          setAlertKinds(savedCalendarAlertKinds(next.source_payload));
        }
      })
      .catch(() => {
        if (!cancelled) setPrefStatus("Calendar alert preferences are unavailable.");
      });
    return () => {
      cancelled = true;
    };
  }, [canUseEventCalendar]);

  useEffect(() => {
    return () => clearPopoverCloseTimer();
  }, []);

  useEffect(() => {
    if (!popover?.pinned) return;

    const onPointerDown = (event: MouseEvent) => {
      const target = event.target;
      if (target instanceof Node && popoverRef.current?.contains(target)) return;
      setPopover(null);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setPopover(null);
    };

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [popover?.pinned]);

  useEffect(() => {
    setPopover(null);
  }, [end, start, activeKinds, activeEconomicCategories]);

  const savePreference = async () => {
    setSavingPref(true);
    setPrefStatus(null);
    const selectedKinds = selectedCalendarAlertKinds(alertKinds);
    const active = selectedKinds.length > 0;
    try {
      const next = await saveNotificationSubscription({
        source_type: "event_calendar",
        source_id: active ? "watchlist" : "none",
        source_name: "Event calendar alerts",
        source_payload: { scope: active ? "watchlist" : "none", calendar_kinds: selectedKinds },
        only_if_new: false,
        active,
        alert_triggers: ["event_calendar"],
      });
      setSubscription(next);
      setPrefStatus(active ? "Calendar alert filters saved." : "Calendar alerts paused.");
    } catch (error) {
      setPrefStatus(error instanceof Error ? error.message : "Unable to save calendar alerts.");
    } finally {
      setSavingPref(false);
    }
  };

  if (loadingEntitlements) {
    return (
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4" aria-busy="true">
        <div className="h-5 w-48 animate-pulse rounded bg-white/10" />
        <div className="mt-3 h-32 animate-pulse rounded bg-white/10" />
      </section>
    );
  }

  if (!canUseEventCalendar) {
    return (
      <UpgradePrompt
        title="Premium event calendar"
        body="Earnings, dividends, IPOs, splits, and economic releases on the monitoring calendar are available with Premium and Pro."
      />
    );
  }

  return (
    <>
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Event calendar</p>
          <h2 className="mt-1 text-lg font-semibold text-white">Earnings and market dates</h2>
          <p className="mt-1 max-w-3xl text-sm text-slate-400">
            Economic releases plus corporate dates from FMP calendars.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => setAnchorMonth((current) => addMonths(current, -1))}
            className="h-9 w-9 rounded-lg border border-white/10 text-lg font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            aria-label="Previous month"
          >
            {"<"}
          </button>
          <select
            value={anchorMonth.getMonth()}
            onChange={(event) => setAnchorMonth(new Date(anchorMonth.getFullYear(), Number(event.target.value), 1))}
            className="h-9 rounded-lg border border-white/10 bg-slate-950 px-3 text-sm font-semibold text-slate-100 outline-none focus:border-emerald-300/40"
            aria-label="Calendar month"
          >
            {monthNames.map((month, index) => (
              <option key={month} value={index}>
                {month}
              </option>
            ))}
          </select>
          <select
            value={anchorMonth.getFullYear()}
            onChange={(event) => setAnchorMonth(new Date(Number(event.target.value), anchorMonth.getMonth(), 1))}
            className="h-9 rounded-lg border border-white/10 bg-slate-950 px-3 text-sm font-semibold text-slate-100 outline-none focus:border-emerald-300/40"
            aria-label="Calendar year"
          >
            {years.map((year) => (
              <option key={year} value={year}>
                {year}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => setAnchorMonth((current) => addMonths(current, 1))}
            className="h-9 w-9 rounded-lg border border-white/10 text-lg font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            aria-label="Next month"
          >
            {">"}
          </button>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-2">
        <span className="rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-3 py-1.5 text-xs font-semibold text-emerald-100">
          Watchlists
        </span>
        <div className="flex flex-wrap gap-1 pl-1">
          {calendarKinds.map((kind) => (
            <button
              key={kind}
              type="button"
              onClick={() => toggleKind(kind)}
              aria-pressed={activeKinds[kind]}
              className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold transition ${
                activeKinds[kind] ? kindClassNames[kind] : "border-white/10 bg-slate-800/50 text-slate-500 hover:border-white/20 hover:text-slate-300"
              }`}
              title={`${activeKinds[kind] ? "Hide" : "Show"} ${kindLabels[kind]} events`}
            >
              {kindLabels[kind]} {visibleCountsByKind[kind] > 0 || countsByKind[kind] > 0 ? `(${visibleCountsByKind[kind]}/${countsByKind[kind]})` : "(0)"}
            </button>
          ))}
        </div>
        {loading ? <span className="text-xs text-slate-500">Loading calendar...</span> : null}
        {status ? <span className="text-xs text-amber-200">{status}</span> : null}
        {hiddenByFilters > 0 ? <span className="text-xs text-slate-500">{hiddenByFilters.toLocaleString()} hidden by filters</span> : null}
      </div>

      {activeKinds.economic ? (
        <div className="mt-3 rounded-lg border border-white/10 bg-slate-950/35 p-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Economic filters</span>
            {economicCategories.map((category) => (
              <button
                key={category.id}
                type="button"
                onClick={() => toggleEconomicCategory(category.id)}
                aria-pressed={activeEconomicCategories[category.id]}
                className={`rounded-lg border px-2.5 py-1 text-xs font-semibold transition ${
                  activeEconomicCategories[category.id]
                    ? "border-cyan-300/35 bg-cyan-300/10 text-cyan-100"
                    : "border-white/10 bg-slate-800/50 text-slate-500 hover:border-white/20 hover:text-slate-300"
                }`}
              >
                {category.label}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {!loading && corporateCount === 0 ? (
        <div className="mt-3 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 text-xs leading-5 text-amber-100">
          No watchlist earnings, dividends, or split dates, and no IPOs in this visible window.
        </div>
      ) : null}

      <div className="mt-4 grid gap-3 xl:grid-cols-3">
        {months.map((month, index) => (
          <CalendarMonth
            key={`${month.getFullYear()}-${month.getMonth()}`}
            month={month}
            itemsByDate={itemsByDate}
            muted={index === 0}
            onPreviewItems={(date, nextItems, element) => showPopover(date, nextItems, element, false)}
            onPinItems={(date, nextItems, element) => showPopover(date, nextItems, element, true)}
            onLeavePreview={schedulePopoverClose}
          />
        ))}
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(18rem,0.7fr)]">
        <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Next dates</div>
          <div className="mt-2 space-y-2">
            {upcomingItems.length ? upcomingItems.map((item) => (
              <div key={item.id} className="grid gap-2 rounded-md border border-white/10 bg-slate-900/70 p-2 text-sm sm:grid-cols-[5rem_1fr_auto] sm:items-center">
                <span className="font-semibold text-slate-300">{item.date.slice(5)}</span>
                <span className="min-w-0 truncate text-slate-100">{item.title}</span>
                <span className={`w-fit rounded border px-1.5 py-0.5 text-[10px] font-semibold ${kindClassNames[item.kind]}`}>{kindLabels[item.kind]}</span>
              </div>
            )) : (
              <div className="rounded-md border border-dashed border-white/15 p-3 text-sm text-slate-400">No upcoming dates in this visible range.</div>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-950/45 p-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Calendar alerts</div>
              <p className="mt-1 text-xs leading-5 text-slate-500">Choose what upcoming calendar dates can be included in monitoring alerts and digests.</p>
            </div>
            <span className={`rounded-lg border px-2 py-1 text-xs font-semibold ${subscription?.active ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-400"}`}>
              {subscription?.active ? "Active" : "Not active"}
            </span>
          </div>
          <div className="mt-3 grid gap-2 sm:grid-cols-2">
            {calendarAlertKinds.map((kind) => (
              <label key={kind} className="flex cursor-pointer items-center gap-2 rounded-lg border border-white/10 bg-slate-900/60 px-3 py-2 text-sm font-semibold text-slate-200">
                <input
                  type="checkbox"
                  checked={alertKinds[kind]}
                  onChange={() => toggleAlertKind(kind)}
                  className="h-4 w-4 rounded border-white/20 bg-slate-950 accent-emerald-300"
                />
                <span className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold ${kindClassNames[kind]}`}>{kindLabels[kind]}</span>
                <span>{kind === "dividend" ? "Dividends" : kind.charAt(0).toUpperCase() + kind.slice(1)}</span>
              </label>
            ))}
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={savePreference}
              disabled={savingPref}
              className="inline-flex h-9 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 text-sm font-semibold text-emerald-100 transition hover:border-emerald-200/60 disabled:opacity-60"
            >
              Save alerts
            </button>
            <button
              type="button"
              onClick={clearAlertKinds}
              className="inline-flex h-9 items-center justify-center rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-300 transition hover:border-white/25 hover:text-white"
            >
              Clear All
            </button>
            <button
              type="button"
              onClick={applyAllAlertKinds}
              className="inline-flex h-9 items-center justify-center rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-300 transition hover:border-white/25 hover:text-white"
            >
              Apply All
            </button>
          </div>
          {prefStatus ? <div className="mt-2 text-xs text-slate-400">{prefStatus}</div> : null}
        </div>
      </div>
    </section>
    {popover ? (
      <EventPopover
        state={popover}
        popoverRef={popoverRef}
        onMouseEnter={clearPopoverCloseTimer}
        onMouseLeave={schedulePopoverClose}
      />
    ) : null}
    </>
  );
}
