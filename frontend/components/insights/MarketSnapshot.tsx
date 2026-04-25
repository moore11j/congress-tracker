import type { ReactNode } from "react";

import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
import type { MacroSnapshotPoint, MacroSnapshotResponse, SectorPerformancePoint } from "@/lib/types";

type Props = {
  snapshot: MacroSnapshotResponse;
};

function formatValue(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(value);
}

function formatPercent(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-400";
}

function SectionShell({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/55 p-4">
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
      </div>
      <div className="mt-4">{children}</div>
    </section>
  );
}

function UnavailableState() {
  return <div className="text-sm text-slate-500">Unavailable</div>;
}

function MacroPointList({ items, suffix = "" }: { items: MacroSnapshotPoint[]; suffix?: string }) {
  if (items.length === 0) return <UnavailableState />;
  return (
    <div className="space-y-3">
      {items.map((item) => (
        <div key={`${item.label}-${item.date ?? "na"}`} className="flex items-baseline justify-between gap-3">
          <div>
            <div className="text-sm font-medium text-slate-100">{item.label}</div>
            <div className="text-xs text-slate-500">{formatDateShort(item.date ?? null)}</div>
          </div>
          <div className="text-right text-sm font-semibold text-slate-200">
            {formatValue(item.value)}
            {suffix}
          </div>
        </div>
      ))}
    </div>
  );
}

function SectorList({ items }: { items: SectorPerformancePoint[] }) {
  if (items.length === 0) return <UnavailableState />;
  return (
    <div className="space-y-3">
      {items.slice(0, 6).map((item) => (
        <div key={item.sector} className="flex items-center justify-between gap-3">
          <div className="text-sm font-medium text-slate-100">{item.sector}</div>
          <div className={`text-sm font-semibold ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct)}</div>
        </div>
      ))}
    </div>
  );
}

export function MarketSnapshot({ snapshot }: Props) {
  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Market Snapshot</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">Market Snapshot</h2>
          <p className="mt-2 text-sm text-slate-400">A compact macro read on indexes, rates, economics, and sector breadth.</p>
        </div>
        <div className="text-right text-xs text-slate-500">
          <div>Status: {snapshot.status}</div>
          <div>{formatDateShort(snapshot.generated_at)}</div>
        </div>
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SectionShell title="Indexes">
          {snapshot.indexes.length === 0 ? (
            <UnavailableState />
          ) : (
            <div className="space-y-3">
              {snapshot.indexes.map((item) => (
                <div key={item.symbol} className="flex items-baseline justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-slate-100">{item.label}</div>
                    <div className="text-xs text-slate-500">{item.symbol}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-semibold text-slate-200">{formatValue(item.value)}</div>
                    <div className={`text-xs ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct)}</div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </SectionShell>

        <SectionShell title="Treasury">
          <MacroPointList items={snapshot.treasury} suffix="%" />
        </SectionShell>

        <SectionShell title="Macro">
          <MacroPointList items={snapshot.economics} />
        </SectionShell>

        <SectionShell title="Sectors">
          <SectorList items={snapshot.sector_performance} />
        </SectionShell>
      </div>
    </section>
  );
}
