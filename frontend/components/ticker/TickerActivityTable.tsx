import type { ReactNode } from "react";

type TickerActivityTableProps = {
  ariaLabel: string;
  headers: ReactNode[];
  children: ReactNode;
  minWidthClassName?: string;
};

/** A compact, horizontally-scrollable activity table that remains usable on phones. */
export function TickerActivityTable({
  ariaLabel,
  headers,
  children,
  minWidthClassName = "min-w-[48rem]",
}: TickerActivityTableProps) {
  return (
    <div
      data-activity-scroll-region
      className={[
        "max-h-[35rem] max-w-full overflow-auto rounded-xl border border-white/10",
        "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
        "[&::-webkit-scrollbar]:h-1.5 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-white/[0.03]",
        "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60",
      ].join(" ")}
    >
      <table aria-label={ariaLabel} className={`w-full border-collapse text-left ${minWidthClassName}`}>
        <thead className="sticky top-0 z-10 bg-slate-950/95 shadow-[0_1px_0_rgba(255,255,255,0.1)] backdrop-blur">
          <tr>
            {headers.map((header, index) => (
              <th key={index} scope="col" className="whitespace-nowrap px-3 py-2.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500 first:pl-4 last:pr-4">
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10">{children}</tbody>
      </table>
    </div>
  );
}

export const tickerActivityCellClassName = "px-3 py-3 align-middle text-sm first:pl-4 last:pr-4";
