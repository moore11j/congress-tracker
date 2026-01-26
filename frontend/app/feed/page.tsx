import Page from "../page";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function FeedPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  return <Page searchParams={sp} />;
}
