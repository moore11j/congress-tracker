import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const adminUsersViewPath = path.join(process.cwd(), "components", "admin", "AdminUsersView.tsx");
const source = fs.readFileSync(adminUsersViewPath, "utf8");
const adminSettingsPanel = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminSettingsPanel.tsx"), "utf8");
const accountDisplay = fs.readFileSync(path.join(process.cwd(), "lib", "accountDisplay.ts"), "utf8");

test("admin users table renders price and billing columns near plan", () => {
  assert.match(source, /<th className="px-3 py-3">Plan<\/th>\s*<th className="px-3 py-3">Price<\/th>\s*<th className="px-3 py-3">Billing<\/th>/);
  assert.match(source, /displayBillingPrice\(user\)/);
  assert.match(source, /displayBillingFrequency\(user\)/);
  assert.match(source, /colSpan=\{16\}/);
});

test("admin users table renders display-safe User ID before User Name", () => {
  assert.match(source, /<th className="px-3 py-3">User ID<\/th>\s*<th className="px-3 py-3">User name<\/th>/);
  assert.match(source, /formatUserDisplayId\(user\)/);
  assert.match(accountDisplay, /return `U-\$\{String\(user\.id\)\.padStart\(6, "0"\)\}`;/);
  assert.doesNotMatch(accountDisplay, /email/);
});

test("admin users billing helpers preserve currency and monthly annual labels", () => {
  assert.match(source, /new Intl\.NumberFormat\("en-US", \{ style: "currency", currency \}\)/);
  assert.match(source, /return `\$\{currency\} \$\{formatted\}`;/);
  assert.match(source, /return "Monthly";/);
  assert.match(source, /return "Annual";/);
  assert.match(source, /return "—";/);
});
test("admin users search renders with filters and debounces requests", () => {
  assert.match(source, /<span className="block font-medium text-slate-200">Search<\/span>[\s\S]*placeholder="Search ID, name, or email\.\.\."/);
  assert.match(source, /window\.setTimeout\(\(\) => \{[\s\S]*setPage\(1\);[\s\S]*setDebouncedSearch\(search\.trim\(\)\);[\s\S]*\}, 300\)/);
  assert.match(source, /search: debouncedSearch \|\| undefined/);
  assert.match(source, /onChange=\{\(event\) => setSearch\(event\.target\.value\)\}/);
  assert.match(source, /downloadAdminUsers\(format, \{[\s\S]*\.\.\.query,[\s\S]*page: undefined,[\s\S]*page_size: undefined/);
});

test("admin panel refresh forwards active Users tab refresh token", () => {
  assert.match(adminSettingsPanel, /const \[usersRefreshToken, setUsersRefreshToken\] = useState\(0\)/);
  assert.match(adminSettingsPanel, /if \(activeTab === "users"\) \{[\s\S]*setUsersRefreshToken\(\(current\) => current \+ 1\);[\s\S]*return;/);
  assert.match(adminSettingsPanel, /onClick=\{refreshActiveTab\}/);
  assert.match(adminSettingsPanel, /<AdminUsersView refreshToken=\{usersRefreshToken\} \/>/);
  assert.match(source, /export function AdminUsersView\(\{ refreshToken = 0 \}: AdminUsersViewProps\)/);
  assert.match(source, /\}, \[query, refreshToken\]\);/);
});
