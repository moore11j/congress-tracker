"use client";

import dynamic from "next/dynamic";
import { FeedShellFallback } from "@/components/feed/FeedShellFallback";

const FeedPageClient = dynamic(
  () => import("@/components/feed/FeedPageClient").then((module) => module.FeedPageClient),
  {
    ssr: false,
    loading: () => <FeedShellFallback />,
  },
);

export function FeedPageClientDeferred() {
  return <FeedPageClient />;
}
