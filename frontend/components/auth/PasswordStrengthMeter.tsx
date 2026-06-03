import { passwordChecks, passwordStrength } from "@/lib/passwordStrength";

export function PasswordStrengthMeter({
  password,
  confirmPassword,
  className = "mt-4",
  mismatchMessage = "Confirm password must match the new password.",
}: {
  password: string;
  confirmPassword?: string;
  className?: string;
  mismatchMessage?: string;
}) {
  const checks = passwordChecks(password);
  const strength = passwordStrength(password);

  return (
    <div className={`${className} rounded-lg border border-white/10 bg-slate-950/40 p-4`}>
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="font-semibold text-slate-200">Password strength</span>
        <span className="text-slate-300">{strength.label}</span>
      </div>
      <div className="mt-2 h-2 rounded-full bg-white/10">
        <div className={`h-2 rounded-full ${strength.className}`} style={{ width: `${(strength.score / 4) * 100}%` }} />
      </div>
      <div className="mt-3 grid gap-2 text-xs text-slate-400 sm:grid-cols-4">
        <Rule passed={checks.length} label="8 or more characters" />
        <Rule passed={checks.alpha} label="One letter" />
        <Rule passed={checks.number} label="One number" />
        <Rule passed={checks.special} label="One special character" />
      </div>
      {confirmPassword && password !== confirmPassword ? (
        <p className="mt-3 text-sm text-rose-200">{mismatchMessage}</p>
      ) : null}
    </div>
  );
}

function Rule({ passed, label }: { passed: boolean; label: string }) {
  return <span className={passed ? "text-emerald-200" : "text-slate-500"}>{label}</span>;
}
