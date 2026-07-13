import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default function MacroPositioningFeedPage() {
  redirect("/insights#macro-positioning");
}
