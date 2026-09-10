import { ContextualUpgrade } from "./ContextualUpgrade";

type UpgradePromptProps = { title: string; body: string; compact?: boolean; gatedFeature?: string; tier?: "Premium" | "Pro" };
export function UpgradePrompt({ title, body, compact = false, gatedFeature = "premium_workflow", tier = "Premium" }: UpgradePromptProps) {
  return <ContextualUpgrade title={title} body={body} feature={gatedFeature} tier={tier} compact={compact} />;
}
