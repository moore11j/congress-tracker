import { anonymousPublicRequestHeaders } from "@/lib/anonymousPublicRender";
import { ScreenerPageRenderer } from "@/app/screener/page";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function AnonymousPublicScreenerPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  return ScreenerPageRenderer({
    searchParams,
    requestHeaders: anonymousPublicRequestHeaders(),
  });
}
