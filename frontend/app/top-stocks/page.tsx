import { redirect } from "next/navigation";

/** The standalone page is retired to prevent duplicate Top Stocks search content. */
export default function TopStocksPage() {
  redirect("/leaderboards#top-stocks");
}
