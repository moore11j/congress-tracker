import { buildSmartSignalPillModel } from "@/lib/smartSignal";

export function SmartSignalPill({
  score,
  band,
  size = "compact",
  className = "",
}: {
  score: number | null;
  band: string | null;
  size?: "compact" | "full";
  className?: string;
}) {
  const signal = buildSmartSignalPillModel({ score, band });
  if (!signal) return null;

  const sizeClassName =
    size === "full"
      ? "gap-2 rounded-full px-3 py-1 text-xs font-medium"
      : "gap-1 rounded-md px-1.5 py-0.5 text-[11px] font-semibold";
  const labelClassName = size === "compact" ? "font-mono" : "";
  const text = size === "compact" ? signal.compactLabel : signal.fullLabel;

  return (
    <span className={`inline-flex items-center border ${sizeClassName} ${signal.className} ${className}`.trim()}>
      <span className={`h-2 w-2 rounded-full ${signal.dotClassName}`} />
      <span className={labelClassName}>{text}</span>
    </span>
  );
}
