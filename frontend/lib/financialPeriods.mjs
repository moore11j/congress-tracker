/** A missing half-year cannot silently change the year-over-year comparison. */
export function comparableHalfYear(points, index) {
  const current = points[index];
  if (!current?.date || !current.periodStart) return null;
  const end = Date.parse(current.date), start = Date.parse(current.periodStart);
  if (!Number.isFinite(end) || !Number.isFinite(start)) return null;
  const day = 86400000, length = (end - start) / day + 1;
  if (length < 160 || length > 200) return null;
  const candidates = points.slice(0, index).filter(point => {
    if (!point.date || !point.periodStart) return false;
    const priorEnd = Date.parse(point.date), priorStart = Date.parse(point.periodStart);
    const gap = (end - priorEnd) / day, duration = (priorEnd - priorStart) / day + 1;
    return gap >= 350 && gap <= 378 && Math.abs(duration - length) <= 7;
  });
  return candidates.length === 1 ? candidates[0] : null;
}


/** Compact the printed end date without guessing an issuer's fiscal quarter. */
export function financialAxisLabel(period) {
  const match = /^(?:Year|Quarter|Half-year) ended (\d{4})-(\d{2})-(\d{2})$/.exec(period);
  if (!match) return period.replace(" 20", " '");
  const iso = `${match[1]}-${match[2]}-${match[3]}`;
  const parsed = new Date(`${iso}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== iso) return period;
  const month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][Number(match[2]) - 1];
  return `${month} '${match[1].slice(-2)}`;
}
