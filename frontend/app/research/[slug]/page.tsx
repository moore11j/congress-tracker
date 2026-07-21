import { GeneratedResearchBriefPage } from "@/components/research/GeneratedResearchBriefPage";

export const dynamic = "force-dynamic";

export default async function GeneratedResearchPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  return <GeneratedResearchBriefPage slug={slug} />;
}
