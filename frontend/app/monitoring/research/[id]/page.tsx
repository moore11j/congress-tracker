import { ResearchMemoryDetail } from "@/components/research-memory/ResearchMemoryWorkspace";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { getResearchMemory } from "@/lib/api";
import { requirePageAuth } from "@/lib/serverAuth";

export default async function ResearchMemoryDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const authToken = await requirePageAuth(`/monitoring/research/${id}`);
  const thesis = authToken ? await getResearchMemory(id, authToken).catch(() => null) : null;
  return <VerifiedSessionGuard returnTo={`/monitoring/research/${id}`} initiallyAuthorized={Boolean(authToken)}>{thesis ? <ResearchMemoryDetail thesis={thesis} /> : <div className="rounded-xl border border-amber-300/25 bg-amber-300/10 p-4 text-sm text-amber-100">Research Memory not found or unavailable.</div>}</VerifiedSessionGuard>;
}
