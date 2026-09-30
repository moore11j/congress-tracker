/** Presentation only. Authorization and limits continue to come from plan-config. */
export const planBenefits = {
  free: {
    purpose: "Get a useful first look at a stock, then save it for later.",
    benefits: ["Core stock research and public disclosure feeds", "Top 5 ranked ideas with short reasons", "Optional weekly Top 5 email"],
  },
  premium: {
    purpose: "See where the data agrees, compare stocks, and follow what changes.",
    benefits: ["Confirmation Score and detailed ranking context", "Side-by-side stock comparisons", "Saved-screen monitoring and research email alerts"],
  },
  pro: {
    purpose: "Add reported institutional activity and deeper market context to your research.",
    benefits: ["Everything in Premium, with higher limits", "Institutional activity and macro positioning", "Custom multi-condition alerts and Walnut Strategies"],
  },
} as const;

export const futureFeaturesCopy = "Options flow, API access and webhooks are coming soon and are not included as available features today.";
export const emailAccessCopy = "Free includes an optional weekly Top 5 ideas email. Premium and Pro add daily Top Ideas delivery and separate research alert subscriptions. Email delivery starts only after you opt in.";

export function pricingFeatureCopy(key: string, label: string, description: string) {
  return key === "notification_digests"
    ? {label: "Research alert subscriptions", description: "Email alerts and digests for monitored research. Separate from the weekly Top 5 ideas email available on Free."}
    : {label, description};
}
