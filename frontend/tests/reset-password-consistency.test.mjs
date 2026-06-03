import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const resetPanel = read("components/auth/ResetPasswordPanel.tsx");
const accountSettingsPanel = read("components/auth/AccountSettingsPanel.tsx");
const strengthMeter = read("components/auth/PasswordStrengthMeter.tsx");
const passwordStrength = read("lib/passwordStrength.ts");
const api = read("lib/api.ts");

test("reset password requires confirmation and shared account-settings strength meter", () => {
  assert.match(resetPanel, /Confirm new password/);
  assert.match(resetPanel, /confirmPassword/);
  assert.match(resetPanel, /PasswordStrengthMeter/);
  assert.match(resetPanel, /passwordMeetsMinimum\(password\)/);
  assert.match(resetPanel, /disabled=\{loading \|\| !passwordValid\}/);
  assert.match(resetPanel, /Passwords do not match\./);
  assert.match(resetPanel, /Password is too weak\./);
  assert.match(resetPanel, /Password must be at least \$\{MIN_PASSWORD_LENGTH\} characters\./);
  assert.match(resetPanel, /confirmPasswordReset\(\{ token, password, confirm_password: confirmPassword \}\)/);
  assert.match(resetPanel, /window\.location\.replace\("\/feed"\)/);
});

test("reset and account settings share one password scoring implementation", () => {
  assert.match(accountSettingsPanel, /PasswordStrengthMeter/);
  assert.match(accountSettingsPanel, /passwordMeetsMinimum\(newPassword\)/);
  assert.match(strengthMeter, /passwordChecks\(password\)/);
  assert.match(strengthMeter, /passwordStrength\(password\)/);
  assert.match(passwordStrength, /export function passwordStrength/);
  assert.match(passwordStrength, /export function passwordMeetsMinimum/);
  assert.doesNotMatch(accountSettingsPanel, /function passwordStrength/);
  assert.doesNotMatch(accountSettingsPanel, /function passwordChecks/);
});

test("reset password API submits confirmation with included credentials", () => {
  assert.match(api, /confirmPasswordReset\(payload: \{ token: string; password: string; confirm_password: string \}\)/);
  assert.match(api, /buildApiUrl\("\/api\/auth\/password-reset\/confirm"\)/);
  assert.match(api, /credentials:\s*fetchInit\.credentials \?\? "include"/);
});
