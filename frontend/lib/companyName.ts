const preservedCompanyWords: Record<string, string> = {
  ETF: "ETF",
  LLC: "LLC",
  LLP: "LLP",
  LP: "LP",
  ADR: "ADR",
  ADS: "ADS",
  REIT: "REIT",
  SPAC: "SPAC",
  PLC: "PLC",
  NV: "NV",
  SA: "SA",
  AG: "AG",
  SE: "SE",
  CORP: "Corp",
  CORPORATION: "Corporation",
  INC: "Inc",
  CO: "Co",
  LTD: "Ltd",
  SPDR: "SPDR",
  NVIDIA: "NVIDIA",
  ISHARES: "iShares",
};

function titleCaseWord(word: string): string {
  const preserved = preservedCompanyWords[word];
  if (preserved) return preserved;
  if (/^\d+$/.test(word)) return word;
  if (word.length === 1) return word;
  return `${word.charAt(0).toUpperCase()}${word.slice(1).toLowerCase()}`;
}

export function formatCompanyName(name?: string | null): string {
  const trimmed = name?.trim();
  if (!trimmed) return "";
  if (/[a-z]/.test(trimmed) || !/[A-Z]/.test(trimmed)) return trimmed;
  return trimmed.replace(/[A-Z0-9]+/g, (word) => titleCaseWord(word));
}
