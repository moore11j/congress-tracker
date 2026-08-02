import { isApprovedSeoPilotPath } from "@/lib/seoQuality";

export type EvergreenSeoPageType =
  | "ticker-vs-ticker"
  | "stock-analysis-comparison"
  | "sector-peer-comparison"
  | "member-activity-summary"
  | "insider-activity-summary";

export type EvergreenEditorialStatus = "draft" | "approved" | "published" | "archived";

export type EvergreenSeoPageDefinition = {
  type: EvergreenSeoPageType;
  pathname: string;
  canonicalPath: string;
  title: string;
  description: string;
  editorialStatus: EvergreenEditorialStatus;
  publishedAt?: string | null;
  updatedAt?: string | null;
  evidence: readonly string[];
  qualityRequirements: readonly string[];
};

export const evergreenSeoQualityRequirements: Record<EvergreenSeoPageType, readonly string[]> = {
  "ticker-vs-ticker": [
    "Both symbols resolve to public-company ticker pages",
    "Visible comparison contains more than quote data",
    "At least two meaningful research categories are available for each ticker",
    "The pair is editorially approved before indexing",
  ],
  "stock-analysis-comparison": [
    "The page answers a distinct search intent",
    "Claims are supported by visible Walnut data or source-reviewed copy",
    "No copied competitor language or unsupported replacement claims",
  ],
  "sector-peer-comparison": [
    "Peers are selected from a real sector or industry relationship",
    "The page explains why the comparison set is useful",
    "Empty or low-data peers are excluded from indexable pages",
  ],
  "member-activity-summary": [
    "The member identity is canonical and unambiguous",
    "The page has meaningful disclosure history or useful historical context",
    "Disclosure timing limitations are visible",
  ],
  "insider-activity-summary": [
    "The reporting CIK and insider identity are valid",
    "The page has issuer relationship or filing history",
    "The copy avoids implying insider activity is predictive by itself",
  ],
};

export const evergreenSeoPilotPages: readonly EvergreenSeoPageDefinition[] = [
  {
    type: "ticker-vs-ticker",
    pathname: "/compare/NVDA/MU",
    canonicalPath: "/compare/NVDA/MU",
    title: "NVDA vs MU Stock Comparison | Walnut Markets",
    description: "Compare NVDA and MU using Walnut's approved pilot stock comparison workflow.",
    editorialStatus: "published",
    publishedAt: "2026-08-01",
    updatedAt: "2026-08-01",
    evidence: ["ticker profiles", "fundamentals", "price volume", "comparison decision layer"],
    qualityRequirements: evergreenSeoQualityRequirements["ticker-vs-ticker"],
  },
];

export function evergreenPageIsIndexable(page: EvergreenSeoPageDefinition): boolean {
  return page.editorialStatus === "published"
    && page.evidence.length >= 2
    && page.qualityRequirements.length > 0
    && isApprovedSeoPilotPath(page.canonicalPath);
}

export function evergreenRobots(page: EvergreenSeoPageDefinition) {
  return evergreenPageIsIndexable(page)
    ? { index: true, follow: true }
    : { index: false, follow: true };
}
