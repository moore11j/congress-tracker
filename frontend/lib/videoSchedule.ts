export function localVideoScheduleInput(date: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function parseVideoSchedule(value: string, now = Date.now()): {iso: string | null; error: string} {
  if (!value) return {iso: null, error: "Choose a date and time."};
  const date = new Date(value);
  // Local Date normalizes invalid dates and skipped daylight-saving hours;
  // reject those instead of silently scheduling for a different wall-clock time.
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value) || !Number.isFinite(date.getTime()) || localVideoScheduleInput(date) !== value)
    return {iso: null, error: "This local time does not exist. Choose another date or time."};
  if (date.getTime() < now + 10 * 60_000)
    return {iso: null, error: "Choose a time at least 10 minutes from now."};
  return {iso: date.toISOString(), error: ""};
}

export function formatVideoSchedule(value: string, timeZone?: string | null): string {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "Unknown time";
  return new Intl.DateTimeFormat("en-US", {
    year: "numeric", month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
    timeZone: timeZone || undefined, timeZoneName: "short",
  }).format(date);
}
