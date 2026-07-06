import { FeedPageClientDeferred } from "@/components/feed/FeedPageClientDeferred";
import type { Metadata } from "next";

// PR summary: Home feed ships a static shell first; the client hydrates mode-aware filters and the unified event tape after page load.
export const dynamic = "force-static";

export const metadata: Metadata = {
  alternates: {
    canonical: "/",
  },
};

export default function FeedPage() {
  return <FeedPageClientDeferred />;
}
