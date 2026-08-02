import { GeneratedResearchBriefPage } from "@/components/research/GeneratedResearchBriefPage";
import { getGeneratedResearchBrief } from "@/lib/api";
import { marketingCanonicalUrl, marketingPageMetadata } from "@/lib/marketingMetadata";
import { buildReturnTo, optionalPageAuthToken } from "@/lib/serverAuth";
import type { Metadata } from "next";
import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

async function loadGeneratedResearchBrief(slug: string, authToken?: string | null) {
  try {
    return await getGeneratedResearchBrief(slug, { authToken, source: "ResearchBrief" });
  } catch {
    return null;
  }
}

export async function generateMetadata({ params }: { params: Promise<{ slug: string }> }): Promise<Metadata> {
  const { slug } = await params;
  const draft = await loadGeneratedResearchBrief(slug);
  if (!draft) {
    return marketingPageMetadata(`/research/${slug}`, {
      title: "Research brief unavailable | Walnut Markets",
      description: "This Walnut Markets research brief is not published or could not be loaded.",
      robots: {
        index: false,
        follow: true,
      },
    });
  }
  const article = draft.article;
  const title = article.seo?.title || `${article.title} | Walnut Markets Research`;
  const description = article.seo?.description || article.summary;
  return marketingPageMetadata(`/research/${article.slug || slug}`, {
    title,
    description,
    robots: {
      index: true,
      follow: true,
    },
    openGraph: {
      type: "article",
      title,
      description,
      url: marketingCanonicalUrl(`/research/${article.slug || slug}`),
      siteName: "Walnut Markets",
      publishedTime: draft.published_at || draft.created_at,
      modifiedTime: draft.updated_at,
    },
  });
}

export default async function GeneratedResearchPage({
  params,
  searchParams,
}: {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const authToken = await optionalPageAuthToken();
  const draft = await loadGeneratedResearchBrief(slug, authToken);
  if (!draft) notFound();
  return <GeneratedResearchBriefPage draft={draft} returnTo={buildReturnTo(`/research/${draft.article.slug || slug}`, sp)} authenticated={Boolean(authToken)} />;
}
