import Page from "../page";

type Props = {
  searchParams?: { [key: string]: string | string[] | undefined };
};

export default function FeedPage(props: Props) {
  return <Page {...props} />;
}
