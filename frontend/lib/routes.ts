export function isAdminRoute(pathname: string | null | undefined): boolean {
  return Boolean(pathname?.startsWith("/admin"));
}
