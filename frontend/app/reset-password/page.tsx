import { ResetPasswordPanel } from "@/components/auth/ResetPasswordPanel";

type SearchParams = Record<string, string | string[] | undefined>;

function getParam(searchParams: SearchParams, key: string): string {
  const value = searchParams[key];
  return typeof value === "string" ? value : "";
}

export default async function ResetPasswordPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  return <ResetPasswordPanel token={getParam(sp, "token")} />;
}
