export function formatChartCurrency(value: number, maximumFractionDigits = 2) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits,
  }).format(value);
}

export function formatChartCompact(value: number) {
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000_000_000) return `${value < 0 ? "-" : ""}$${(absolute / 1_000_000_000_000).toFixed(1)}T`;
  if (absolute >= 1_000_000_000) return `${value < 0 ? "-" : ""}$${(absolute / 1_000_000_000).toFixed(1)}B`;
  if (absolute >= 1_000_000) return `${value < 0 ? "-" : ""}$${(absolute / 1_000_000).toFixed(1)}M`;
  if (absolute >= 1_000) return `${value < 0 ? "-" : ""}$${(absolute / 1_000).toFixed(1)}K`;
  return formatChartCurrency(value, 0);
}

export function formatChartPercent(value: number, digits = 1) {
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}%`;
}
