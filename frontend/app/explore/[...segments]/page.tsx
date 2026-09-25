import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { appPageMetadata, appCanonicalUrl } from "@/lib/marketingMetadata";
import { directoryCategories, directoryPath, DIRECTORY_PAGE_SIZE, getDirectoryEntries, isDirectoryCategory } from "@/lib/publicDirectory";

export const dynamic = "force-dynamic";
type Props = { params: Promise<{ segments: string[] }> };

async function directoryView(params: Props["params"]) {
  const { segments } = await params;
  const [category, number] = segments;
  if (segments.length > 2 || !isDirectoryCategory(category) || (number !== undefined && !/^[1-9]\d*$/.test(number))) notFound();
  const page = number ? Number(number) : 1;
  // Page one has one URL; reject duplicate /1 and nonsensical page numbers.
  if ((number && page === 1) || !Number.isSafeInteger(page)) notFound();
  const entries = await getDirectoryEntries(category);
  const pages = Math.max(1, Math.ceil(entries.length / DIRECTORY_PAGE_SIZE));
  if (!entries.length || page > pages) notFound();
  return { category, page, pages, total: entries.length, entries: entries.slice((page - 1) * DIRECTORY_PAGE_SIZE, page * DIRECTORY_PAGE_SIZE) };
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const view = await directoryView(params);
  const topic = directoryCategories[view.category];
  return appPageMetadata(directoryPath(view.category, view.page), {
    title: `${topic.title} Directory${view.page > 1 ? ` — Page ${view.page}` : ""} | Walnut Markets`,
    description: `Browse ${topic.title.toLowerCase()} profiles and dated public records on Walnut Markets. Alphabetical directory, page ${view.page} of ${view.pages}.`,
  });
}

export default async function DirectoryPage({ params }: Props) {
  const view = await directoryView(params);
  const topic = directoryCategories[view.category];
  const data = {
    "@context": "https://schema.org", "@type": "CollectionPage",
    name: `${topic.title} directory — page ${view.page}`, url: appCanonicalUrl(directoryPath(view.category, view.page)),
    mainEntity: { "@type": "ItemList", itemListElement: view.entries.map((entry, index) => ({
      "@type": "ListItem", position: (view.page - 1) * DIRECTORY_PAGE_SIZE + index + 1, name: entry.name, url: appCanonicalUrl(entry.path),
    })) },
  };
  return <section className="mx-auto max-w-6xl py-8">
    <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(data).replace(/</g, "\\u003c") }} />
    <Link href="/explore" prefetch={false} className="text-sm text-emerald-200">All research directories</Link>
    <h1 className="mt-4 text-3xl font-semibold text-white">{topic.title} directory{view.page > 1 ? ` — page ${view.page}` : ""}</h1>
    <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-300">{topic.description}</p>
    <p className="mt-3 text-sm text-slate-400">{view.total.toLocaleString("en-US")} profiles · Page {view.page} of {view.pages} · Alphabetical order</p>
    <ul className="mt-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {view.entries.map(entry => <li key={entry.path} className="min-w-0 rounded-lg border border-white/10 bg-slate-900/40 p-4">
        <Link href={entry.path} prefetch={false} className="break-words font-medium text-emerald-200 hover:underline">{entry.name}</Link>
        {entry.date ? <p className="mt-2 text-xs text-slate-400">Latest tracked record: {entry.date.slice(0, 10)}</p> : null}
      </li>)}
    </ul>
    <nav aria-label={`${topic.title} directory pages`} className="mt-8 flex flex-wrap gap-2">
      {Array.from({ length: view.pages }, (_, index) => index + 1).map(page => <Link key={page} href={directoryPath(view.category, page)} prefetch={false} aria-current={page === view.page ? "page" : undefined} className={`rounded border px-3 py-2 text-sm ${page === view.page ? "border-emerald-300 text-emerald-200" : "border-white/10 text-slate-300 hover:text-white"}`}>Page {page}</Link>)}
    </nav>
  </section>;
}
