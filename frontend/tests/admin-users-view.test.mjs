import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const adminUsersViewPath = path.join(process.cwd(), "components", "admin", "AdminUsersView.tsx");
const source = fs.readFileSync(adminUsersViewPath, "utf8");

test("admin users table renders price and billing columns near plan", () => {
  assert.match(source, /<th className="px-3 py-3">Plan<\/th>\s*<th className="px-3 py-3">Price<\/th>\s*<th className="px-3 py-3">Billing<\/th>/);
  assert.match(source, /displayBillingPrice\(user\)/);
  assert.match(source, /displayBillingFrequency\(user\)/);
  assert.match(source, /colSpan=\{15\}/);
});

test("admin users billing helpers preserve currency and monthly annual labels", () => {
  assert.match(source, /new Intl\.NumberFormat\("en-US", \{ style: "currency", currency \}\)/);
  assert.match(source, /return `\$\{currency\} \$\{formatted\}`;/);
  assert.match(source, /return "Monthly";/);
  assert.match(source, /return "Annual";/);
  assert.match(source, /return "—";/);
});
