export type FeedMode = "congress" | "insider" | "government_contracts" | "institutional" | "all";

export const feedModeOptions = [
  ["all", "All"],
  ["congress", "Congress"],
  ["insider", "Insider"],
  ["government_contracts", "Government Contracts"],
  ["institutional", "Institutional"],
] as const satisfies readonly (readonly [FeedMode, string])[];

export const validFeedModes = feedModeOptions.map(([value]) => value) as FeedMode[];

export function isValidFeedMode(value: string): value is FeedMode {
  return (validFeedModes as readonly string[]).includes(value);
}

export function isInstitutionalFeedMode(mode: FeedMode): boolean {
  return mode === "institutional";
}

export function isCompactFeedFilterMode(mode: FeedMode): boolean {
  return mode === "government_contracts" || mode === "institutional";
}
