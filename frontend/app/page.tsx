import { FeedPageClient } from "@/components/feed/FeedPageClient";
import { FeedShellFallback } from "@/components/feed/FeedShellFallback";
import type { Metadata } from "next";
import { Suspense } from "react";

// PR summary: Home feed ships a static shell first; the client hydrates mode-aware filters and the unified event tape after page load.
export const dynamic = "force-static";

export const metadata: Metadata = {
  alternates: {
    canonical: "/",
  },
};

export default function FeedPage() {
  return (
    <Suspense fallback={<FeedShellFallback />}>
      <FeedPageClient />
    </Suspense>
  );
}
