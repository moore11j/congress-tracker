import { redirect } from "next/navigation";

/** Retained only for existing bookmarks; the unified dashboard is canonical. */
export default function CongressTradersLeaderboardPage() {
  redirect("/leaderboards#congress");
}
