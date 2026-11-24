export const purchaseCentsPerCredit = 2;
export const freeFirstMonthCredits = 250;
export const creditPlans = [
  { name: "Free", monthly: "$0", annual: null, credits: 100 },
  { name: "Premium", monthly: "$24.95", annual: "$249.50", credits: 2000 },
  { name: "Pro", monthly: "$39.95", annual: "$399.95", credits: 4000 },
] as const;
export const creditTasks = [
  { id: "browse", name: "Open a company or chart", credits: 0, description: "Find a company and explore its existing research." },
  { id: "answer", name: "Short sourced answer", credits: 25, description: "A concise explanation with supporting sources." },
  { id: "analysis", name: "Research or comparison", credits: 100, description: "A focused analysis or company comparison with cited evidence." },
  { id: "report", name: "Focused research report", credits: 400, description: "A sourced report covering the question, supporting evidence and risks." },
] as const;
export type ResearchMode = typeof creditTasks[number]["id"];
export function creditAmountCents(value: string): number | null {
  if (!/^\d{1,4}(?:\.\d{1,2})?$/.test(value.trim())) return null;
  const [whole, fraction = ""] = value.trim().split(".");
  const cents = Number(whole) * 100 + Number(fraction.padEnd(2, "0"));
  return cents >= 500 && cents <= 100000 && cents % purchaseCentsPerCredit === 0 ? cents : null;
}

/** No production fallback, including when ordinary app API variables are set. */
export function creditSandboxBackend(env: Record<string, string | undefined>): string | null {
  if (env.WALNUT_CREDITS_SANDBOX_ENABLED !== "1" || env.VERCEL_ENV === "production") return null;
  try {
    const url = new URL(env.WALNUT_CREDITS_SANDBOX_API_URL || "");
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return null;
    if (["congress-tracker-api.fly.dev", "app.walnutmarkets.com", "walnutmarkets.com", "www.walnutmarkets.com"].includes(url.hostname)) return null;
    if (url.protocol !== "https:" && !(url.protocol === "http:" && ["localhost", "127.0.0.1"].includes(url.hostname))) return null;
    return url.origin;
  } catch { return null; }
}
