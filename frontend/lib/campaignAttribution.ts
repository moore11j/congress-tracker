export const campaignParamKeys = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"] as const;

export type CampaignParamKey = (typeof campaignParamKeys)[number];
export type CampaignProperties = Record<string, string | number | boolean | null>;
export type SearchParamValue = string | string[] | undefined;
export type SearchParamRecord = Record<string, SearchParamValue>;

export function firstSearchParam(value: SearchParamValue): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export function campaignParamsFromRecord(searchParams: SearchParamRecord): URLSearchParams {
  const params = new URLSearchParams();
  for (const key of campaignParamKeys) {
    const value = firstSearchParam(searchParams[key]);
    if (value) params.set(key, value);
  }
  return params;
}

export function campaignPropertiesFromRecord(searchParams: SearchParamRecord): CampaignProperties {
  const properties: CampaignProperties = {};
  for (const key of campaignParamKeys) properties[key] = firstSearchParam(searchParams[key]) ?? null;
  return properties;
}

export function currentCampaignProperties(extra: CampaignProperties = {}): CampaignProperties {
  if (typeof window === "undefined") return extra;
  const params = new URLSearchParams(window.location.search);
  const properties: CampaignProperties = {
    page_path: `${window.location.pathname}${window.location.search}`,
    referrer: document.referrer || null,
  };
  for (const key of campaignParamKeys) properties[key] = params.get(key);
  return { ...properties, ...extra };
}

export function pathWithCampaignParams(pathname: string, searchParams: SearchParamRecord, extra?: Record<string, string>): string {
  const params = campaignParamsFromRecord(searchParams);
  Object.entries(extra ?? {}).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const query = params.toString();
  return `${pathname}${query ? `?${query}` : ""}`;
}

export function preserveCurrentPath(pathname: string, searchParams: URLSearchParams): string {
  const query = searchParams.toString();
  return `${pathname}${query ? `?${query}` : ""}`;
}

export function registerHref(returnTo: string): string {
  return `/login?mode=register&return_to=${encodeURIComponent(returnTo)}`;
}
