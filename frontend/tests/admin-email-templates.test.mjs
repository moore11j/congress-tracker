import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const viewPath = path.join(process.cwd(), "components", "admin", "AdminEmailTemplatesView.tsx");
const apiPath = path.join(process.cwd(), "lib", "api.ts");

const viewSource = fs.readFileSync(viewPath, "utf8");
const apiSource = fs.readFileSync(apiPath, "utf8");

test("admin email templates expose reset to branded default action", () => {
  assert.match(apiSource, /adminResetEmailTemplateDefault/);
  assert.match(apiSource, /adminResetEmailTemplateDefaults/);
  assert.match(apiSource, /\/api\/admin\/email\/templates\/\$\{encodeURIComponent\(templateKey\)\}\/reset-default/);
  assert.match(apiSource, /\/api\/admin\/email\/templates\/reset-defaults/);
  assert.match(viewSource, /Reset to branded default/);
  assert.match(viewSource, /Reset all system templates/);
  assert.match(viewSource, /Reset this template to the Walnut branded default\?/);
  assert.match(viewSource, /This will replace all system email templates with the shipped Walnut branded defaults\./);
  assert.match(viewSource, /Template reset to branded default\./);
  assert.match(viewSource, /All system templates reset to branded defaults\./);
  assert.match(viewSource, /adminPreviewEmailTemplate\(next\.template_key, nextContext\)/);
});

test("admin email templates bind API sender fields without hardcoded legacy branding", () => {
  assert.match(apiSource, /from_name: string/);
  assert.match(viewSource, /TextInput label="From name" value=\{draft\.from_name\}/);
  assert.doesNotMatch(viewSource, /Walnut Markets/);
});

test("admin email preview sample snippets use styled Walnut digest tables", () => {
  assert.match(viewSource, /sampleItemsHtml/);
  assert.match(viewSource, /sampleSignalsHtml/);
  assert.match(viewSource, /border:1px solid #dbe6ea/);
  assert.match(viewSource, /background:#ecfeff/);
  assert.doesNotMatch(viewSource, /<table><tr><td>NVDA<\/td><td>Congress trade<\/td><td>Score 82<\/td><\/tr><\/table>/);
  assert.doesNotMatch(viewSource, /<table><tr><td>NVDA<\/td><td>82<\/td><td>Bullish<\/td><\/tr><\/table>/);
});

test("admin email preview labels rendered fields and hides raw HTML behind disclosure", () => {
  assert.match(viewSource, /Rendered subject/);
  assert.match(viewSource, /Rendered text/);
  assert.match(viewSource, /Rendered HTML email preview/);
  assert.match(viewSource, /<details className="mt-3 rounded-lg border border-white\/10 bg-slate-950">/);
  assert.match(viewSource, /Raw HTML/);
  assert.match(viewSource, /h-\[520px\]/);
});

test("digest skipped test sends map precise reasons", () => {
  assert.match(viewSource, /delivery_disabled: "Email delivery is disabled\."/);
  assert.match(viewSource, /no_new_items: "No new items in this window\. Use force test to send a sample anyway\."/);
  assert.match(viewSource, /duplicate_window_already_sent: "Digest already sent for this window\. Use force test to resend\."/);
  assert.match(viewSource, /watchlist_digest_inactive: "Watchlist digest is inactive for this watchlist\."/);
  assert.match(viewSource, /user_email_notifications_disabled: "User email notifications are off\."/);
  assert.match(viewSource, /user_alerts_disabled: "This monitoring email type is off for the user\."/);
  assert.match(viewSource, /trigger_disabled: "The watchlist's intraday trigger preferences do not include this candidate type\."/);
  assert.match(viewSource, /skipReasonFromApiError/);
  assert.match(viewSource, /Status: Test email skipped\. \$\{SKIP_REASON_MESSAGES\[skipReason\]\}/);
  assert.doesNotMatch(viewSource, /Test email skipped because delivery is disabled/);
});

test("recent deliveries expose recipient, status, template, date, and pagination controls", () => {
  assert.match(viewSource, /Search recipient email\.\.\./);
  assert.match(viewSource, /All statuses/);
  assert.match(viewSource, /All templates/);
  assert.match(viewSource, /Last 30 days/);
  assert.match(viewSource, /Page size/);
  assert.match(viewSource, />\s*First\s*</);
  assert.match(viewSource, />\s*Previous\s*</);
  assert.match(viewSource, />\s*Next\s*</);
  assert.match(viewSource, />\s*Last\s*</);
  assert.match(viewSource, /No delivery logs match these filters\./);
  assert.match(apiSource, /recipient\?: string/);
  assert.match(apiSource, /date_window\?: string/);
});

test("recent delivery filters reset paging and refresh preserves active filters", () => {
  assert.match(viewSource, /setDeliveryRecipientSearch\(event\.target\.value\);\s+setDeliveryPage\(1\);/);
  assert.match(viewSource, /setDeliveryStatus\(event\.target\.value\);\s+setDeliveryPage\(1\);/);
  assert.match(viewSource, /setDeliveryTemplateKey\(event\.target\.value\);\s+setDeliveryPage\(1\);/);
  assert.match(viewSource, /setDeliveryDateWindow\(event\.target\.value\);\s+setDeliveryPage\(1\);/);
  assert.match(viewSource, /setDeliveryPageSize\(Number\(event\.target\.value\)\);\s+setDeliveryPage\(1\);/);
  assert.match(viewSource, /recipient: debouncedDeliveryRecipient/);
  assert.match(viewSource, /status: deliveryStatus \|\| undefined/);
  assert.match(viewSource, /template_key: deliveryTemplateKey \|\| undefined/);
  assert.match(viewSource, /date_window: deliveryDateWindow/);
  assert.match(viewSource, /onClick=\{\(\) => refreshDeliveries\(\)\}/);
});
