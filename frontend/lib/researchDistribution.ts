type ShareArticle = { slug: string; title: string; summary?: string; preview_body?: string; current_data_as_of?: string; risks?: string[]; key_points?: string[]; premium_required?: boolean };

export function researchDistributionDraft(article: ShareArticle, platform: string) {
  const channel = ["reddit", "instagram", "tiktok"].includes(platform) ? platform : "reddit";
  const plain = (value: string) => value.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1").replace(/[*_`#]+/g, "").replace(/\s+/g, " ").trim();
  const finding = plain(article.preview_body || article.summary || article.title);
  const caveat = article.premium_required ? "Reported data can lag current activity." : plain(article.risks?.[0] || "Reported data can lag current activity.");
  const link = new URL(`/research/${encodeURIComponent(article.slug)}`, "https://walnutmarkets.com");
  link.searchParams.set("utm_source", channel);
  link.searchParams.set("utm_medium", "organic_social");
  link.searchParams.set("utm_campaign", "research_briefs");
  link.searchParams.set("utm_content", article.slug);
  return `${finding}\n\n${caveat}\n\nRead the research on Walnut: ${link.toString()}`;
}
