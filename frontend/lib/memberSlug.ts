export function nameToSlug(name: string): string {
  return name
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, "")
    .replace(/ /g, "_");
}

type MemberHrefInput = {
  slug?: string | null;
  name?: string | null;
  memberId?: string | number | null;
};

function asTrimmedString(value: string | number | null | undefined): string {
  if (typeof value === "number") return String(value);
  if (typeof value !== "string") return "";
  return value.trim();
}

function isLegacyMemberId(value: string): boolean {
  return /^FMP_/i.test(value);
}

export function memberHref({ slug, name, memberId }: MemberHrefInput): string {
  const cleanSlug = asTrimmedString(slug);
  if (cleanSlug && !isLegacyMemberId(cleanSlug)) {
    return `/member/${encodeURIComponent(cleanSlug)}`;
  }

  const cleanName = asTrimmedString(name);
  if (cleanName) {
    return `/member/${nameToSlug(cleanName)}`;
  }

  const cleanMemberId = asTrimmedString(memberId);
  if (cleanMemberId) {
    return `/member/${encodeURIComponent(cleanMemberId)}`;
  }

  if (cleanSlug) {
    return `/member/${encodeURIComponent(cleanSlug)}`;
  }

  return "/member/UNKNOWN";
}
