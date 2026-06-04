"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  adminSendDigestTest,
  adminSendMonthlyStatementTest,
  adminPreviewEmailTemplate,
  adminResetEmailTemplateDefault,
  adminSendTestEmailTemplate,
  adminUpdateEmailTemplate,
  getAdminEmailDeliveries,
  getAdminEmailTemplate,
  getAdminEmailTemplates,
  getMe,
  adminResetEmailTemplateDefaults,
  type AdminEmailDelivery,
  type AdminEmailDeliveriesResponse,
  type AdminEmailRendered,
  type AdminEmailTemplate,
} from "@/lib/api";
import type { AdminToastApi } from "@/components/admin/AdminToast";

type DeliveryScope = "template" | "all";

const TEST_PREVIEW_TOKEN = "test-preview-token";
const DEFAULT_APP_BASE_URL = (
  process.env.NEXT_PUBLIC_APP_BASE_URL ||
  process.env.NEXT_PUBLIC_APP_URL ||
  process.env.NEXT_PUBLIC_SITE_URL ||
  "https://app.walnut-intel.com"
).replace(/\/+$/, "");
const DEFAULT_API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE || "https://congress-tracker-api.fly.dev").replace(/\/+$/, "");

const TEMPLATE_DESCRIPTIONS: Record<string, string> = {
  "account.password_reset": "Sends immediately when a user requests a password reset.",
  "account.password_changed": "Sends after a successful password reset or account-settings password change.",
  "account.verify_email": "Sends on registration and explicit verification resend.",
  "alerts.monitoring_digest": "Admin/cron-triggered digest of recent confirmation monitoring changes for a watchlist.",
  "alerts.signal_alert": "Admin/cron-triggered digest of notable signal activity for a user's watchlist universe.",
  "alerts.watchlist_activity": "Admin/cron-triggered digest of new filings and events for an active watchlist digest subscription.",
  "billing.monthly_statement": "Admin-triggered monthly billing statement email for a selected account.",
};

const SKIP_REASON_MESSAGES: Record<string, string> = {
  delivery_disabled: "Email delivery is disabled.",
  template_disabled: "This email template is disabled.",
  user_email_notifications_disabled: "User email notifications are off.",
  user_alerts_disabled: "User alert notifications are off.",
  watchlist_digest_inactive: "Watchlist digest is inactive for this watchlist.",
  no_new_items: "No new items in this window. Use force test to send a sample anyway.",
  duplicate_window_already_sent: "Digest already sent for this window. Use force test to resend.",
  missing_watchlist: "Watchlist was not found.",
  missing_user: "User was not found.",
  invalid_email: "The selected user does not have a valid email address.",
  user_suspended: "The selected user is suspended.",
};

type TemplateDraft = {
  name: string;
  category: string;
  from_name: string;
  from_email: string;
  reply_to: string;
  subject: string;
  preheader: string;
  body_text: string;
  body_html: string;
  variables_json: string;
  enabled: boolean;
};

function formatDate(value?: string | null) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function draftFromTemplate(template: AdminEmailTemplate): TemplateDraft {
  return {
    name: template.name ?? "",
    category: template.category ?? "",
    from_name: template.from_name ?? "",
    from_email: template.from_email ?? "",
    reply_to: template.reply_to ?? "",
    subject: template.subject ?? "",
    preheader: template.preheader ?? "",
    body_text: template.body_text ?? "",
    body_html: template.body_html ?? "",
    variables_json: template.variables_json || JSON.stringify(template.variables ?? [], null, 2),
    enabled: template.enabled,
  };
}

function sampleItemsHtml() {
  return [
    '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:18px 0 0 0;border-collapse:collapse;border:1px solid #dbe6ea;border-radius:6px;overflow:hidden;">',
    '<thead><tr><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Ticker</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Event</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Actor</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Score</th></tr></thead>',
    '<tbody><tr><td style="padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;">NVDA</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">Congress trade</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">Member Example</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">82</td></tr><tr><td style="padding:10px;font-weight:700;color:#0f172a;">MSFT</td><td style="padding:10px;color:#334155;">Institutional buy</td><td style="padding:10px;color:#334155;">Institutional filing</td><td style="padding:10px;color:#334155;">74</td></tr></tbody>',
    "</table>",
  ].join("");
}

function sampleSignalsHtml() {
  return [
    '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:18px 0 0 0;border-collapse:collapse;border:1px solid #dbe6ea;border-radius:6px;overflow:hidden;">',
    '<thead><tr><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Ticker</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Score</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Direction</th><th align="left" style="padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;">Why</th></tr></thead>',
    '<tbody><tr><td style="padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;">NVDA</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">82</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">bullish</td><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;">Cross-source confirmation strengthened.</td></tr><tr><td style="padding:10px;font-weight:700;color:#0f172a;">AMD</td><td style="padding:10px;color:#334155;">76</td><td style="padding:10px;color:#334155;">mixed</td><td style="padding:10px;color:#334155;">Options flow and filing context changed.</td></tr></tbody>',
    "</table>",
  ].join("");
}

function sampleContextFor(template: AdminEmailTemplate): Record<string, string | number> {
  if (template.template_key === "account.password_changed") {
    return {
      first_name: "Admin",
      changed_at: "June 3, 2026 at 9:30 PM",
      support_email: "support@walnut-intel.com",
      login_url: `${DEFAULT_APP_BASE_URL}/login`,
    };
  }
  const context: Record<string, string | number> = {};
  for (const variable of template.variables ?? []) {
    if (template.template_key === "account.password_reset" && variable === "reset_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/reset-password?token=${TEST_PREVIEW_TOKEN}`;
    } else if (template.template_key === "account.verify_email" && variable === "verification_url") {
      context[variable] = `${DEFAULT_API_BASE_URL}/api/account/verify-email?token=${TEST_PREVIEW_TOKEN}`;
    } else if (template.template_key === "billing.monthly_statement" && variable === "statement_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/account/billing?statement=${TEST_PREVIEW_TOKEN}`;
    } else if (variable === "activity_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/watchlists/1`;
    } else if (variable === "digest_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/watchlists/1`;
    } else if (variable === "signal_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/ticker/NVDA`;
    } else if (variable === "items_text") {
      context[variable] = "- NVDA congress trade | Member Example | purchase | $15,001 - $50,000 | 2026-06-03 | score 82";
    } else if (variable === "items_html") {
      context[variable] = sampleItemsHtml();
    } else if (variable === "signals_text") {
      context[variable] = "- NVDA: score 82 | bullish | multi-source activity | Congress and insider stack | 2026-06-03";
    } else if (variable === "signals_html") {
      context[variable] = sampleSignalsHtml();
    } else if (variable === "watchlist_name") {
      context[variable] = "AI Infrastructure";
    } else if (variable === "ticker") {
      context[variable] = "NVDA";
    } else if (variable === "signal_score") {
      context[variable] = 82;
    } else if (variable === "direction") {
      context[variable] = "bullish";
    } else if (variable === "why_notable") {
      context[variable] = "Cross-source confirmation strengthened.";
    } else if (variable === "source_stack") {
      context[variable] = "Congress, insider, and institutional activity.";
    } else if (variable === "cautions") {
      context[variable] = "Review source filings and liquidity before acting.";
    } else if (variable === "billing_period") {
      context[variable] = "2026-06-01 - 2026-06-30";
    } else if (variable === "plan") {
      context[variable] = "Premium";
    } else if (variable === "payment_status") {
      context[variable] = "paid";
    } else if (variable.endsWith("_url") || variable === "verification_url" || variable === "reset_url" || variable === "statement_url") {
      context[variable] = `${DEFAULT_APP_BASE_URL}/?preview=${TEST_PREVIEW_TOKEN}`;
    } else if (variable.includes("minutes")) {
      context[variable] = 30;
    } else if (variable === "currency") {
      context[variable] = "USD";
    } else if (variable.includes("amount")) {
      context[variable] = "$29.00";
    } else if (variable.includes("date")) {
      context[variable] = new Date().toISOString().slice(0, 10);
    } else if (variable === "symbol") {
      context[variable] = "NVDA";
    } else if (variable === "first_name") {
      context[variable] = "Admin";
    } else {
      context[variable] = `Sample ${variable.replaceAll("_", " ")}`;
    }
  }
  return context;
}

function parseContext(raw: string): Record<string, unknown> {
  const parsed = JSON.parse(raw || "{}");
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Context must be a JSON object.");
  }
  return parsed as Record<string, unknown>;
}

function contextForTemplate(template: AdminEmailTemplate, raw: string): Record<string, unknown> {
  const defaults = sampleContextFor(template);
  return { ...defaults, ...parseContext(raw) };
}

function testDeliveryStatusMessage(delivery: AdminEmailDelivery, fallbackEmail: string) {
  const recipient = delivery.to_email || fallbackEmail || "current admin";
  if (delivery.status === "sent") {
    return `Status: Test email sent to ${recipient} via ${delivery.provider || "email provider"}.`;
  }
  if (delivery.status === "queued") {
    return `Status: Test email queued for ${recipient}.`;
  }
  if (delivery.status === "log_only") {
    return `Status: Test email rendered in log-only mode for ${recipient}.`;
  }
  if (delivery.status === "skipped") {
    const reason = delivery.error ? SKIP_REASON_MESSAGES[delivery.error] || delivery.error : "The delivery service skipped this email.";
    return `Status: Test email skipped. ${reason}`;
  }
  if (delivery.status === "failed") {
    return `Status: Test email failed${delivery.error ? `: ${delivery.error}` : "."}`;
  }
  return `Status: Test email ${delivery.status || "completed"} for ${recipient}.`;
}

function skipReasonFromApiError(error: unknown): string | null {
  if (!(error instanceof ApiError)) return null;
  try {
    const parsed = JSON.parse(error.body) as { detail?: unknown };
    return typeof parsed.detail === "string" ? parsed.detail : null;
  } catch {
    return null;
  }
}

function prependDelivery(
  current: AdminEmailDeliveriesResponse | null,
  delivery: AdminEmailDelivery,
  selectedTemplateKey: string,
  deliveryScope: DeliveryScope,
): AdminEmailDeliveriesResponse | null {
  if (!current || !delivery.id) return current;
  if (deliveryScope === "template" && delivery.template_key !== selectedTemplateKey) return current;
  const items = [delivery, ...current.items.filter((item) => item.id !== delivery.id)].slice(0, current.page_size);
  const alreadyListed = current.items.some((item) => item.id === delivery.id);
  return {
    ...current,
    items,
    total: alreadyListed ? current.total : current.total + 1,
  };
}

export function AdminEmailTemplatesView({ showToast }: AdminToastApi) {
  const [templates, setTemplates] = useState<AdminEmailTemplate[]>([]);
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [selectedTemplate, setSelectedTemplate] = useState<AdminEmailTemplate | null>(null);
  const [draft, setDraft] = useState<TemplateDraft | null>(null);
  const [contextDraft, setContextDraft] = useState("{}");
  const [preview, setPreview] = useState<AdminEmailRendered | null>(null);
  const [deliveries, setDeliveries] = useState<AdminEmailDeliveriesResponse | null>(null);
  const [deliveryScope, setDeliveryScope] = useState<DeliveryScope>("template");
  const [currentAdminEmail, setCurrentAdminEmail] = useState<string>("");
  const [testEmail, setTestEmail] = useState("");
  const [digestEmail, setDigestEmail] = useState("");
  const [digestUserId, setDigestUserId] = useState("");
  const [digestWatchlistId, setDigestWatchlistId] = useState("");
  const [digestLookbackDays, setDigestLookbackDays] = useState("1");
  const [digestForce, setDigestForce] = useState(false);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const selectedTemplateKey = selectedTemplate?.template_key ?? selectedKey;
  const categories = useMemo(
    () => Array.from(new Set(templates.map((template) => template.category).filter(Boolean))).sort(),
    [templates],
  );

  const loadTemplates = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const [templateResponse, meResponse] = await Promise.all([
        getAdminEmailTemplates(),
        getMe().catch(() => null),
      ]);
      setTemplates(templateResponse.items);
      setCurrentAdminEmail(meResponse?.user?.email ?? "");
      setSelectedKey((current) => current || templateResponse.items[0]?.template_key || "");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to load email templates.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const refreshDeliveries = async () => {
    try {
      const response = await getAdminEmailDeliveries({
        template_key: deliveryScope === "template" ? selectedTemplateKey : undefined,
        page: 1,
        page_size: 25,
      });
      setDeliveries(response);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to load email deliveries.";
      setStatus(message);
      showToast({ message, tone: "error" });
    }
  };

  useEffect(() => {
    loadTemplates();
  }, []);

  useEffect(() => {
    if (!selectedKey) return;
    let ignore = false;
    const loadTemplate = async () => {
      setBusy(true);
      setStatus(null);
      try {
        const template = await getAdminEmailTemplate(selectedKey);
        if (ignore) return;
        setSelectedTemplate(template);
        setDraft(draftFromTemplate(template));
        setContextDraft(JSON.stringify(sampleContextFor(template), null, 2));
        setPreview(null);
      } catch (error) {
        if (!ignore) {
          const message = error instanceof Error ? error.message : "Unable to load template.";
          setStatus(message);
          showToast({ message, tone: "error" });
        }
      } finally {
        if (!ignore) setBusy(false);
      }
    };
    loadTemplate();
    return () => {
      ignore = true;
    };
  }, [selectedKey]);

  useEffect(() => {
    if (!selectedTemplateKey) return;
    refreshDeliveries();
  }, [deliveryScope, selectedTemplateKey]);

  const updateDraft = (patch: Partial<TemplateDraft>) => {
    setDraft((current) => (current ? { ...current, ...patch } : current));
  };

  const saveTemplate = async () => {
    if (!selectedTemplate || !draft) return;
    setBusy(true);
    setStatus(null);
    try {
      const next = await adminUpdateEmailTemplate(selectedTemplate.template_key, {
        name: draft.name,
        category: draft.category,
        from_name: draft.from_name,
        from_email: draft.from_email,
        reply_to: draft.reply_to.trim() || null,
        subject: draft.subject,
        preheader: draft.preheader.trim() || null,
        body_text: draft.body_text,
        body_html: draft.body_html.trim() || null,
        variables_json: draft.variables_json,
        enabled: draft.enabled,
      });
      setSelectedTemplate(next);
      setDraft(draftFromTemplate(next));
      setTemplates((current) => current.map((template) => (template.template_key === next.template_key ? next : template)));
      try {
        const nextContext = contextForTemplate(next, contextDraft);
        setContextDraft(JSON.stringify(nextContext, null, 2));
        const previewResponse = await adminPreviewEmailTemplate(next.template_key, nextContext);
        setPreview(previewResponse.rendered);
        const message = "Email template saved and preview refreshed.";
        setStatus(message);
        showToast(message);
      } catch (previewError) {
        setPreview(null);
        const previewMessage = previewError instanceof Error ? previewError.message : "Unable to refresh preview.";
        const message = `Email template saved. Preview refresh failed: ${previewMessage}`;
        setStatus(message);
        showToast({ message, tone: "error" });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to save template.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const resetTemplateToDefault = async () => {
    if (!selectedTemplate) return;
    const confirmed = window.confirm(
      "This will replace this template's subject and body with the shipped Walnut branded default. Continue?",
    );
    if (!confirmed) return;
    setBusy(true);
    setStatus(null);
    try {
      const next = await adminResetEmailTemplateDefault(selectedTemplate.template_key);
      setSelectedTemplate(next);
      setDraft(draftFromTemplate(next));
      setTemplates((current) => current.map((template) => (template.template_key === next.template_key ? next : template)));
      const nextContext = contextForTemplate(next, contextDraft);
      setContextDraft(JSON.stringify(nextContext, null, 2));
      const previewResponse = await adminPreviewEmailTemplate(next.template_key, nextContext);
      setPreview(previewResponse.rendered);
      const message = "Template reset to branded default.";
      setStatus(message);
      showToast(message);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to reset template.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const resetAllTemplatesToDefaults = async () => {
    const confirmed = window.confirm(
      "This will replace all system email templates with the shipped Walnut branded defaults. Continue?",
    );
    if (!confirmed) return;
    setBusy(true);
    setStatus(null);
    try {
      const response = await adminResetEmailTemplateDefaults();
      setTemplates(response.items);
      const nextSelected = response.items.find((template) => template.template_key === selectedTemplateKey) ?? response.items[0] ?? null;
      if (nextSelected) {
        setSelectedTemplate(nextSelected);
        setSelectedKey(nextSelected.template_key);
        setDraft(draftFromTemplate(nextSelected));
        const nextContext = contextForTemplate(nextSelected, contextDraft);
        setContextDraft(JSON.stringify(nextContext, null, 2));
        const previewResponse = await adminPreviewEmailTemplate(nextSelected.template_key, nextContext);
        setPreview(previewResponse.rendered);
      }
      const message = "All system templates reset to branded defaults.";
      setStatus(message);
      showToast(message);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to reset all templates.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const previewTemplate = async () => {
    if (!selectedTemplateKey) return;
    setBusy(true);
    setStatus(null);
    try {
      const response = await adminPreviewEmailTemplate(selectedTemplateKey, parseContext(contextDraft));
      setPreview(response.rendered);
      const message = "Preview rendered.";
      setStatus(message);
      showToast(message);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to render preview.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const sendTest = async () => {
    if (!selectedTemplateKey) return;
    setBusy(true);
    setStatus(null);
    try {
      const delivery = await adminSendTestEmailTemplate(selectedTemplateKey, {
        to_email: testEmail.trim() || null,
        context: parseContext(contextDraft),
      });
      setDeliveries((current) => prependDelivery(current, delivery, selectedTemplateKey, deliveryScope));
      const message = testDeliveryStatusMessage(delivery, testEmail.trim() || currentAdminEmail);
      setStatus(message);
      showToast({ message, tone: delivery.status === "failed" ? "error" : "success" });
      await refreshDeliveries();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to send test email.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const digestPayload = () => ({
    user_id: digestUserId.trim() ? Number(digestUserId.trim()) : null,
    email: digestEmail.trim() || null,
    watchlist_id: digestWatchlistId.trim() ? Number(digestWatchlistId.trim()) : null,
    lookback_days: Number(digestLookbackDays || "1"),
    force: digestForce,
  });

  const sendDigestTest = async (kind: "watchlist_activity" | "monitoring" | "signals" | "billing") => {
    setBusy(true);
    setStatus(null);
    try {
      const delivery =
        kind === "billing"
          ? await adminSendMonthlyStatementTest({
              user_id: digestUserId.trim() ? Number(digestUserId.trim()) : null,
              email: digestEmail.trim() || null,
              force: digestForce,
            })
          : await adminSendDigestTest(kind, digestPayload());
      const message = testDeliveryStatusMessage(delivery, digestEmail.trim() || currentAdminEmail);
      setStatus(message);
      showToast({ message, tone: delivery.status === "failed" ? "error" : "success" });
      await refreshDeliveries();
    } catch (error) {
      const skipReason = skipReasonFromApiError(error);
      const message =
        skipReason && SKIP_REASON_MESSAGES[skipReason]
          ? `Status: Test email skipped. ${SKIP_REASON_MESSAGES[skipReason]}`
          : error instanceof Error
            ? error.message
            : "Unable to send digest test.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">Email templates</h2>
            <p className="mt-2 text-sm text-slate-400">
              Review template content, render a sample payload, and send explicit admin test emails.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={resetAllTemplatesToDefaults}
              disabled={busy || templates.length === 0}
              className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
            >
              Reset all system templates
            </button>
            <button
              type="button"
              onClick={loadTemplates}
              disabled={busy}
              className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
            >
              Refresh
            </button>
          </div>
        </div>
        {status ? <p className="mt-3 text-sm text-slate-400">{status.startsWith("Status:") ? status : `Status: ${status}`}</p> : null}
      </section>

      <div className="grid gap-6 xl:grid-cols-[20rem_minmax(0,1fr)]">
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <div className="flex items-center justify-between gap-3">
            <h3 className="font-semibold text-white">Templates</h3>
            <span className="text-xs text-slate-500">{templates.length} total</span>
          </div>
          <div className="mt-4 space-y-4">
            {categories.map((category) => (
              <div key={category}>
                <div className="px-2 text-xs font-semibold uppercase tracking-wide text-slate-500">{category}</div>
                <div className="mt-2 space-y-2">
                  {templates
                    .filter((template) => template.category === category)
                    .map((template) => (
                      <button
                        key={template.template_key}
                        type="button"
                        onClick={() => setSelectedKey(template.template_key)}
                        className={`w-full rounded-lg border p-3 text-left transition ${
                          selectedTemplateKey === template.template_key
                            ? "border-emerald-300/40 bg-emerald-300/10"
                            : "border-white/10 bg-slate-950/40 hover:border-white/20"
                        }`}
                      >
                        <span className="block text-sm font-semibold text-white">{template.name}</span>
                        <span className="mt-1 block break-all text-xs text-slate-500">{template.template_key}</span>
                        <span className="mt-2 block text-xs leading-5 text-slate-400">
                          {TEMPLATE_DESCRIPTIONS[template.template_key] ?? "Editable Walnut email template."}
                        </span>
                        <span
                          className={`mt-2 inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${
                            template.enabled
                              ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
                              : "border-white/10 bg-slate-950/60 text-slate-400"
                          }`}
                        >
                          {template.enabled ? "Enabled" : "Disabled"}
                        </span>
                      </button>
                    ))}
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
          {selectedTemplate && draft ? (
            <div className="space-y-5">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <p className="break-all text-xs font-semibold uppercase tracking-wide text-emerald-300">
                    {selectedTemplate.template_key}
                  </p>
                  <h3 className="mt-1 text-xl font-semibold text-white">{selectedTemplate.name}</h3>
                  <p className="mt-2 text-sm text-slate-400">
                    Updated {formatDate(selectedTemplate.updated_at)}. Variables:{" "}
                    {selectedTemplate.variables.length ? selectedTemplate.variables.join(", ") : "none"}
                  </p>
                  <p className="mt-2 text-sm text-slate-400">
                    {TEMPLATE_DESCRIPTIONS[selectedTemplate.template_key] ?? "Editable Walnut email template."}
                  </p>
                </div>
                <label className="flex items-center gap-2 text-sm font-medium text-slate-200">
                  <input
                    type="checkbox"
                    checked={draft.enabled}
                    onChange={(event) => updateDraft({ enabled: event.target.checked })}
                    className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                  />
                  Enabled
                </label>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <TextInput label="Name" value={draft.name} onChange={(value) => updateDraft({ name: value })} />
                <TextInput label="Category" value={draft.category} onChange={(value) => updateDraft({ category: value })} />
                <TextInput label="From name" value={draft.from_name} onChange={(value) => updateDraft({ from_name: value })} />
                <TextInput label="From email" value={draft.from_email} onChange={(value) => updateDraft({ from_email: value })} />
                <TextInput label="Reply to" value={draft.reply_to} onChange={(value) => updateDraft({ reply_to: value })} />
                <TextInput label="Preheader" value={draft.preheader} onChange={(value) => updateDraft({ preheader: value })} />
              </div>

              <TextInput label="Subject" value={draft.subject} onChange={(value) => updateDraft({ subject: value })} />

              <label className="block text-sm">
                <span className="block font-medium text-slate-200">Text body</span>
                <textarea
                  value={draft.body_text}
                  onChange={(event) => updateDraft({ body_text: event.target.value })}
                  rows={9}
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-mono text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>

              <label className="block text-sm">
                <span className="block font-medium text-slate-200">HTML body</span>
                <textarea
                  value={draft.body_html}
                  onChange={(event) => updateDraft({ body_html: event.target.value })}
                  rows={8}
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-mono text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>

              <label className="block text-sm">
                <span className="block font-medium text-slate-200">Allowed variables JSON</span>
                <textarea
                  value={draft.variables_json}
                  onChange={(event) => updateDraft({ variables_json: event.target.value })}
                  rows={3}
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-mono text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={saveTemplate}
                  disabled={busy}
                  className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
                >
                  Save template
                </button>
                <button
                  type="button"
                  onClick={resetTemplateToDefault}
                  disabled={busy}
                  className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
                >
                  Reset to branded default
                </button>
              </div>
            </div>
          ) : (
            <p className="text-sm text-slate-400">Select a template to open it.</p>
          )}
        </section>
      </div>

      {selectedTemplate ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h3 className="text-xl font-semibold text-white">Preview and test</h3>
              <p className="mt-2 text-sm text-slate-400">
                Blank recipient sends the test to {currentAdminEmail || "the current admin account"}.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={previewTemplate}
                disabled={busy}
                className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
              >
                Preview
              </button>
              <button
                type="button"
                onClick={sendTest}
                disabled={busy}
                className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
              >
                Send test
              </button>
            </div>
          </div>

          <div className="mt-5 grid gap-5 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
            <div className="space-y-4">
              <label className="block text-sm">
                <span className="block font-medium text-slate-200">Test recipient</span>
                <input
                  type="email"
                  value={testEmail}
                  onChange={(event) => setTestEmail(event.target.value)}
                  placeholder={currentAdminEmail || "Current admin email"}
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>
              <label className="block text-sm">
                <span className="block font-medium text-slate-200">Context JSON</span>
                <textarea
                  value={contextDraft}
                  onChange={(event) => setContextDraft(event.target.value)}
                  rows={12}
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 font-mono text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>
            </div>

            <div className="space-y-4">
              <PreviewBlock title="Rendered subject" content={preview?.subject ?? "Run preview to render the subject."} />
              <PreviewBlock title="Rendered text" content={preview?.body_text ?? "Run preview to render the text body."} tall />
              <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Rendered HTML email preview</div>
                {preview?.body_html ? (
                  <iframe
                    title="Rendered email HTML preview"
                    sandbox=""
                    srcDoc={preview.body_html}
                    className="mt-3 h-[520px] w-full rounded-lg border border-white/10 bg-white"
                  />
                ) : (
                  <p className="mt-3 text-sm text-slate-400">Run preview to render the HTML body.</p>
                )}
                {preview?.body_html ? (
                  <details className="mt-3 rounded-lg border border-white/10 bg-slate-950">
                    <summary className="cursor-pointer px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                      Raw HTML
                    </summary>
                    <pre className="max-h-56 overflow-auto whitespace-pre-wrap border-t border-white/10 p-3 font-mono text-xs text-slate-300">
                      {preview.body_html}
                    </pre>
                  </details>
                ) : null}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-xl font-semibold text-white">Digest test sends</h3>
            <p className="mt-2 text-sm text-slate-400">
              Sends one explicit admin test through the delivery service. Watchlist digests require a user and watchlist.
            </p>
          </div>
        </div>
        <div className="mt-5 grid gap-4 lg:grid-cols-5">
          <TextInput label="User ID" value={digestUserId} onChange={setDigestUserId} />
          <TextInput label="User email" value={digestEmail} onChange={setDigestEmail} />
          <TextInput label="Watchlist ID" value={digestWatchlistId} onChange={setDigestWatchlistId} />
          <TextInput label="Lookback days" value={digestLookbackDays} onChange={setDigestLookbackDays} />
          <label className="flex items-end gap-2 pb-2 text-sm font-medium text-slate-200">
            <input
              type="checkbox"
              checked={digestForce}
              onChange={(event) => setDigestForce(event.target.checked)}
              className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
            />
            Force test
          </label>
        </div>
        <div className="mt-4 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={() => sendDigestTest("monitoring")}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Send monitoring
          </button>
          <button
            type="button"
            onClick={() => sendDigestTest("watchlist_activity")}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Send watchlist activity
          </button>
          <button
            type="button"
            onClick={() => sendDigestTest("signals")}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Send signals
          </button>
          <button
            type="button"
            onClick={() => sendDigestTest("billing")}
            disabled={busy}
            className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
          >
            Send monthly statement
          </button>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-xl font-semibold text-white">Recent deliveries</h3>
            <p className="mt-2 text-sm text-slate-400">
              Showing {deliveries?.items.length ?? 0} of {deliveries?.total ?? 0} recent delivery logs.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              value={deliveryScope}
              onChange={(event) => setDeliveryScope(event.target.value as DeliveryScope)}
              className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
            >
              <option value="template">Selected template</option>
              <option value="all">All deliveries</option>
            </select>
            <button
              type="button"
              onClick={refreshDeliveries}
              disabled={busy}
              className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
            >
              Refresh logs
            </button>
          </div>
        </div>

        <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
          <table className="min-w-full divide-y divide-white/10 text-left text-sm text-slate-300">
            <thead className="bg-slate-950/70 text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-3 py-3">Created</th>
                <th className="px-3 py-3">Status</th>
                <th className="px-3 py-3">Template</th>
                <th className="px-3 py-3">Recipient</th>
                <th className="px-3 py-3">Subject</th>
                <th className="px-3 py-3">Provider</th>
                <th className="px-3 py-3">Error</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10 bg-slate-950/30">
              {deliveries?.items.length ? (
                deliveries.items.map((delivery) => (
                  <tr key={delivery.id}>
                    <td className="whitespace-nowrap px-3 py-3">{formatDate(delivery.created_at)}</td>
                    <td className="whitespace-nowrap px-3 py-3">
                      <span
                        className={`rounded-md border px-2 py-1 text-xs font-semibold ${
                          delivery.status === "sent"
                            ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
                            : delivery.status === "failed"
                              ? "border-rose-300/30 bg-rose-300/10 text-rose-100"
                              : "border-white/10 bg-slate-900 text-slate-300"
                        }`}
                      >
                        {delivery.status}
                      </span>
                    </td>
                    <td className="break-all px-3 py-3">{delivery.template_key ?? "-"}</td>
                    <td className="break-all px-3 py-3">{delivery.to_email}</td>
                    <td className="min-w-64 px-3 py-3">{delivery.subject ?? "-"}</td>
                    <td className="whitespace-nowrap px-3 py-3">{delivery.provider ?? "-"}</td>
                    <td className="min-w-56 px-3 py-3 text-rose-100">{delivery.error ?? "-"}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={7} className="px-3 py-6 text-center text-slate-500">
                    No delivery logs found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function TextInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <label className="block text-sm">
      <span className="block font-medium text-slate-200">{label}</span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
      />
    </label>
  );
}

function PreviewBlock({ title, content, tall = false }: { title: string; content: string; tall?: boolean }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</div>
      <pre className={`mt-3 overflow-auto whitespace-pre-wrap font-mono text-sm text-slate-200 ${tall ? "max-h-80" : "max-h-28"}`}>
        {content}
      </pre>
    </div>
  );
}
