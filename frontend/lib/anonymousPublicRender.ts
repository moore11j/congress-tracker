type HeaderReader = Pick<Headers, "get">;

const anonymousPublicHeaders = new Map<string, string>([
  ["accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"],
  ["user-agent", "Mozilla/5.0 WalnutPublicRender"],
  ["x-walnut-anonymous-public-render", "1"],
]);

export function anonymousPublicRequestHeaders(): HeaderReader {
  return {
    get(name: string) {
      return anonymousPublicHeaders.get(name.toLowerCase()) ?? null;
    },
  };
}
