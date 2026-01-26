import Page from "../page";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default function FeedPage({ searchParams }: Props) {
  return <Page searchParams={searchParams} />;
}
