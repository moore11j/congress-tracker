export type WikipediaHeadshot = {
  src: string;
  pageUrl: string;
  title: string;
};

type WikipediaSummary = {
  title?: string;
  description?: string;
  extract?: string;
  content_urls?: {
    desktop?: {
      page?: string;
    };
  };
  thumbnail?: {
    source?: string;
    width?: number;
    height?: number;
  };
};

type HeadshotKind = "member" | "insider";

type ResolveWikipediaHeadshotOptions = {
  kind: HeadshotKind;
  company?: string | null;
  role?: string | null;
  symbol?: string | null;
};

const WIKIPEDIA_SUMMARY_BASE = "https://en.wikipedia.org/api/rest_v1/page/summary";
const WIKIPEDIA_REVALIDATE_SECONDS = 60 * 60 * 24 * 7;
const WIKIMEDIA_UPLOAD_PREFIX = "https://upload.wikimedia.org/";
const REQUEST_TIMEOUT_MS = 1800;
const WIKIPEDIA_HEADSHOT_ALIASES = new Map<string, string[]>([
  ["huang jen hsun", ["Jensen Huang", "Jen-Hsun Huang"]],
  ["jen hsun huang", ["Jensen Huang", "Jen-Hsun Huang"]],
]);

function normalizeText(value: string | null | undefined) {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/gi, " ")
    .trim()
    .toLowerCase();
}

function wikipediaTitle(name: string) {
  return name.trim().replace(/\s+/g, "_");
}

function headshotNameCandidates(name: string) {
  const candidates = [name.trim()];
  const aliases = WIKIPEDIA_HEADSHOT_ALIASES.get(normalizeText(name)) ?? [];
  for (const alias of aliases) {
    if (!candidates.some((candidate) => normalizeText(candidate) === normalizeText(alias))) {
      candidates.push(alias);
    }
  }
  return candidates;
}

function titleLooksLikePerson(title: string | undefined, name: string) {
  const normalizedTitle = normalizeText(title);
  const normalizedName = normalizeText(name);
  if (!normalizedTitle || !normalizedName) return false;
  if (normalizedTitle === normalizedName || normalizedTitle.includes(normalizedName) || normalizedName.includes(normalizedTitle)) return true;
  const titleTokens = normalizedTitle.split(/\s+/).filter(Boolean).sort();
  const nameTokens = normalizedName.split(/\s+/).filter(Boolean).sort();
  return titleTokens.length === nameTokens.length && titleTokens.every((token, index) => token === nameTokens[index]);
}

function contentFor(summary: WikipediaSummary) {
  return normalizeText([summary.title, summary.description, summary.extract].filter(Boolean).join(" "));
}

function isWikimediaThumbnail(src: string | undefined) {
  if (!src) return false;
  try {
    const url = new URL(src);
    return url.protocol === "https:" && src.startsWith(WIKIMEDIA_UPLOAD_PREFIX);
  } catch {
    return false;
  }
}

function significantTokens(...values: Array<string | null | undefined>) {
  const stop = new Set(["inc", "corp", "corporation", "company", "holdings", "limited", "ltd", "plc", "class", "common", "stock"]);
  return values
    .flatMap((value) => normalizeText(value).split(/\s+/))
    .filter((token) => token.length >= 3 && !stop.has(token));
}

function summaryMatchesKind(summary: WikipediaSummary, options: ResolveWikipediaHeadshotOptions) {
  const content = contentFor(summary);
  if (options.kind === "member") {
    return /\b(politician|representative|congress|congressional|house of representatives|senator)\b/.test(content);
  }

  const contextTokens = significantTokens(options.company, options.role, options.symbol);
  const hasContextMatch = contextTokens.some((token) => content.includes(token));
  const hasBusinessContext = /\b(business|businessman|businesswoman|executive|chief executive|ceo|founder|officer|director|chairman|chairwoman|entrepreneur)\b/.test(content);
  return hasContextMatch || hasBusinessContext;
}

async function fetchWikipediaSummary(title: string): Promise<WikipediaSummary | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const init: RequestInit & { next?: { revalidate: number } } = {
      headers: {
        Accept: "application/json",
        "User-Agent": "WalnutMarkets/1.0 (https://walnutmarkets.com)",
      },
      signal: controller.signal,
      next: { revalidate: WIKIPEDIA_REVALIDATE_SECONDS },
    };
    const response = await fetch(`${WIKIPEDIA_SUMMARY_BASE}/${encodeURIComponent(title)}`, init);
    if (!response.ok) return null;
    return (await response.json()) as WikipediaSummary;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

export async function resolveWikipediaHeadshot(
  name: string | null | undefined,
  options: ResolveWikipediaHeadshotOptions,
): Promise<WikipediaHeadshot | null> {
  const cleanName = (name ?? "").trim();
  if (!cleanName) return null;

  for (const candidate of headshotNameCandidates(cleanName)) {
    const summary = await fetchWikipediaSummary(wikipediaTitle(candidate));
    const src = summary?.thumbnail?.source;
    if (!summary || !titleLooksLikePerson(summary.title, candidate) || !isWikimediaThumbnail(src)) continue;
    if (!summaryMatchesKind(summary, options)) continue;

    return {
      src: src!,
      pageUrl: summary.content_urls?.desktop?.page ?? `https://en.wikipedia.org/wiki/${encodeURIComponent(wikipediaTitle(candidate))}`,
      title: summary.title ?? candidate,
    };
  }

  return null;
}
