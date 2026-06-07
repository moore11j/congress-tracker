export type PasswordStrength = {
  label: "Weak" | "Fair" | "Good" | "Strong";
  score: number;
  className: string;
};

export const MIN_PASSWORD_LENGTH = 8;

export function passwordChecks(value: string) {
  return {
    length: value.length >= MIN_PASSWORD_LENGTH,
    alpha: /[A-Za-z]/.test(value),
    number: /\d/.test(value),
    special: /[^A-Za-z0-9]/.test(value),
  };
}

export function passwordMeetsMinimum(value: string) {
  const checks = passwordChecks(value);
  return Object.values(checks).filter(Boolean).length >= 3;
}

export function passwordStrength(value: string): PasswordStrength {
  const checks = passwordChecks(value);
  const score = Object.values(checks).filter(Boolean).length;
  if (!value || score <= 1) return { label: "Weak", score: Math.max(score, 1), className: "bg-rose-300/70" };
  if (score === 2) return { label: "Weak", score, className: "bg-amber-300/70" };
  if (score === 3) return { label: "Good", score, className: "bg-sky-300/70" };
  return { label: "Strong", score, className: "bg-emerald-300/80" };
}
