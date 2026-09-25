import { appPageMetadata } from "@/lib/marketingMetadata";

export const screenerMetadata = appPageMetadata("/screener", {
  title: "Stock Screener | Walnut Markets",
  description: "Filter stocks by fundamentals, technicals, and public disclosures in the Walnut Markets stock research terminal.",
  robots: { index: false, follow: true },
});
