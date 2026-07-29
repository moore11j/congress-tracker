import { SearchResultsClient } from "./SearchResultsClient";

export const dynamic = "force-dynamic";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

export default async function SearchPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const query = one(sp, "q").trim();
  return <SearchResultsClient initialQuery={query} />;
}
