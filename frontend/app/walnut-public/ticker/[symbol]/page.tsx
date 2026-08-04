import { anonymousPublicRequestHeaders } from "@/lib/anonymousPublicRender";
import { TickerPageRenderer, generateMetadata } from "@/app/ticker/[symbol]/page";

type Props = {
  params: Promise<{ symbol: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export { generateMetadata };

export default async function AnonymousPublicTickerPage(props: Props) {
  return TickerPageRenderer({
    ...props,
    requestHeaders: anonymousPublicRequestHeaders(),
  });
}
