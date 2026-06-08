import { LoginRegisterPanel } from "@/components/auth/LoginRegisterPanel";

type SearchParams = Record<string, string | string[] | undefined>;

function getParam(searchParams: SearchParams, key: string): string {
  const value = searchParams[key];
  return typeof value === "string" ? value : "";
}

export default async function LoginPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  return (
    <LoginRegisterPanel
      resetStatus={getParam(sp, "reset")}
      returnTo={getParam(sp, "return_to")}
      accountDeleted={getParam(sp, "account_deleted") === "1"}
      reactivated={getParam(sp, "reactivated") === "1"}
    />
  );
}
