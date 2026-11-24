import type { Metadata } from "next";
import { ResearchCreditsPanel } from "@/components/chat/ResearchCreditsPanel";
export const metadata: Metadata = { title: "Research credits | Walnut 2.0", robots: { index: false, follow: true } };
export default function CreditsPage() { return <ResearchCreditsPanel />; }
