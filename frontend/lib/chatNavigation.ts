import { tickerRouteSymbol } from "./ticker";

export const chatSections = ["overview", "chart", "financials", "news", "events", "ownership", "research"] as const;
export type ChatSection = typeof chatSections[number];
export type ChatIntent = { kind: "rankings" } | { kind: "section"; section: ChatSection; label: string } | { kind: "search"; query: string };

// Navigation only, not an AI answer or a substitute for understanding a question.
export function chatIntent(input: string, hasCompany: boolean): ChatIntent {
  const value = input.trim().replace(/[?.!]+$/, "").toLowerCase();
  if (/^(?:show |show me |open |explore )?(?:the )?(?:rankings|ranked stocks|leaderboards?|top stocks)$/.test(value)) return { kind: "rankings" };
  const sections: [RegExp, ChatSection, string][] = [
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:chart|price chart|price history)$/, "chart", "Chart"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:financials|financial statements|revenue|earnings)$/, "financials", "Financials"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:news|latest news|headlines)$/, "news", "News"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:filings|sec filings|events)$/, "events", "Filings"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:ownership|institutional holdings)$/, "ownership", "Ownership"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?|what are (?:the |its )?)(?:risks|catalysts|overview)$/, "overview", "Overview, catalysts and risks"],
    [/^(?:(?:show|open)(?: me)? (?:the |its )?)?(?:research|research memory)$/, "research", "Research"],
  ];
  if (hasCompany) for (const [pattern, section, label] of sections) if (pattern.test(value)) return { kind: "section", section, label };
  const query = input.trim().replace(/^(?:research|analy[sz]e|look up|search for|tell me about|show me)\s+/i, "").replace(/[?]+$/, "").trim();
  return { kind: "search", query };
}

export function chatResearchHref(symbol: string, section: ChatSection = "overview") {
  const route = tickerRouteSymbol(symbol);
  if (!route || !(chatSections as readonly string[]).includes(section)) return null;
  return `/ask?symbol=${encodeURIComponent(route)}#${section}`;
}
