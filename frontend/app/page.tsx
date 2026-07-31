import { FeedPageClientDeferred } from "@/components/feed/FeedPageClientDeferred";
import type { Metadata } from "next";
import { WALNUT_APP_URL, appCanonicalUrl } from "@/lib/marketingMetadata";

// PR summary: Home feed ships a static shell first; the client hydrates mode-aware filters and the unified event tape after page load.
export const dynamic = "force-static";

export const metadata: Metadata = {
  metadataBase: new URL(WALNUT_APP_URL),
  title: "Live Stock Research Feed | Walnut Markets",
  description:
    "Walnut Markets app feed for stock research activity, public disclosures, market context, and ticker events.",
  robots: {
    index: false,
    follow: true,
  },
  alternates: {
    canonical: appCanonicalUrl("/"),
  },
};

export default function FeedPage() {
  return <FeedPageClientDeferred />;
}
