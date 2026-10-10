export function formatStatementMoney(value, currency = "USD") {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  if (!/^[A-Z]{3}$/.test(currency)) return "Unavailable";
  const prefix = currency === "USD" ? "$" : `${currency} `;
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  for (const [scale, suffix] of [[1e12, "T"], [1e9, "B"], [1e6, "M"], [1e3, "K"]]) {
    if (abs >= scale) return `${sign}${prefix}${(abs / scale).toFixed(1)}${suffix}`;
  }
  return `${sign}${prefix}${abs.toFixed(0)}`;
}
