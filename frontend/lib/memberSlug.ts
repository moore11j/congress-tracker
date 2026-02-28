export function nameToSlug(name: string): string {
  return name
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, "")
    .replace(/ /g, "_");
}

