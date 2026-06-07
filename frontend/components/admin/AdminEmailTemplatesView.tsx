"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { WalnutConfirmDialog } from "@/components/ui/WalnutConfirmDialog";
import {
  ApiError,
  adminRunEmailDigestsNow,
  adminRunIntradayEmailAlertsNow,
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
  type AdminDigestRunNowResponse,
  type AdminDigestSendResult,
  type AdminIntradayRunNowResponse,
} from "@/lib/api";
import type { AdminToastApi } from "@/components/admin/AdminToast";

type ResetDialog = "template" | "all" | null;

const TEST_PREVIEW_TOKEN = "test-preview-token";
const DEFAULT_APP_BASE_URL = (
  process.env.NEXT_PUBLIC_APP_BASE_URL ||
  process.env.NEXT_PUBLIC_APP_URL ||
  process.env.NEXT_PUBLIC_SITE_URL ||
  "https://app.walnut-intel.com"
).replace(/\/+$/, "");

const TEMPLATE_DESCRIPTIONS: Record<string, string> = {
  "account.password_reset": "Sends immediately when a user requests a password reset.",
  "account.password_changed": "Sends after a successful password reset or account-settings password change.",
  "account.welcome": "Sends once when a new Google OAuth account is created.",
  "account.verify_email": "Sends on registration and explicit verification resend.",
  "alerts.monitoring_digest": "Admin/cron-triggered digest of recent confirmation monitoring changes for a watchlist.",
  "alerts.signal_alert": "Admin/cron-triggered daily digest of notable signal activity for a user's watchlist universe.",
  "alerts.signal_intraday": "Intraday high-conviction signal alert. Production sends require EMAIL_ALERT_INTRADAY_ENABLED.",
  "alerts.watchlist_intraday": "Intraday high-priority watchlist activity alert. Production sends require EMAIL_ALERT_INTRADAY_ENABLED.",
  "alerts.watchlist_activity": "Admin/cron-triggered digest of new filings and events for an active watchlist digest subscription.",
  "billing.monthly_statement": "Admin-triggered monthly billing statement email for a selected account.",
};

const DELIVERY_STATUS_OPTIONS = ["sent", "failed", "skipped", "log_only", "queued"];
const DELIVERY_DATE_WINDOW_OPTIONS = [
  { value: "today", label: "Today" },
  { value: "last_7", label: "Last 7 days" },
  { value: "last_14", label: "Last 14 days" },
  { value: "last_30", label: "Last 30 days" },
  { value: "last_month", label: "Last month" },
  { value: "all_time", label: "All time" },
];
const DELIVERY_PAGE_SIZE_OPTIONS = [10, 25, 50];

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
  low_priority: "Candidate did not clear intraday watchlist materiality thresholds.",
  low_conviction: "Candidate did not clear intraday signal conviction thresholds.",
  monitoring_digest_only: "Monitoring change remains in the daily digest.",
  outside_market_hours: "Intraday alerts only send during market hours.",
  intraday_disabled: "Intraday email alerts are disabled by environment.",
  duplicate_alert_already_sent: "Intraday alert already sent for this event.",
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
  if (template.template_key === "account.welcome") {
    return {
      first_name: "Nancy",
      app_url: `${DEFAULT_APP_BASE_URL}/feed`,
      support_email: "support@walnut-intel.com",
    };
  }
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
      context[variable] = `${DEFAULT_APP_BASE_URL}/account/verify-email?token=${TEST_PREVIEW_TOKEN}`;
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

function digestStatusMessage(delivery: AdminDigestSendResult, fallbackEmail: string) {
  const recipient = delivery.to_email || fallbackEmail || "selected account";
  const itemCount = Number(delivery.item_count ?? delivery.items_count ?? delivery.rendered_preview?.items_count ?? 0);
  const skipReason = delivery.skip_reason || delivery.error || "";
  if (delivery.status === "sent" || delivery.status === "log_only" || delivery.status === "queued") {
    const verb = delivery.status === "sent" ? "Sent" : delivery.status === "log_only" ? "Rendered" : "Queued";
    return `Status: ${verb} ${itemCount} ${itemCount === 1 ? "item" : "items"} to ${recipient}.`;
  }
  if (delivery.status === "skipped" && skipReason === "no_new_items") {
    return "Status: No new items in this window.";
  }
  if (delivery.status === "skipped") {
    return `Status: Digest skipped. ${SKIP_REASON_MESSAGES[skipReason] || skipReason || "The delivery service skipped this email."}`;
  }
  if (delivery.status === "would_send") {
    return `Status: Dry run found ${itemCount} ${itemCount === 1 ? "item" : "items"} for ${recipient}.`;
  }
  return testDeliveryStatusMessage(delivery, fallbackEmail);
}

function runNowStatusMessage(response: AdminDigestRunNowResponse) {
  const { summary } = response;
  if (response.dry_run) {
    return `Status: Dry run checked ${summary.total} target${summary.total === 1 ? "" : "s"}; ${summary.would_send} would send, ${summary.skipped} skipped, ${summary.item_count} item${summary.item_count === 1 ? "" : "s"}.`;
  }
  return `Status: Ran ${response.kind}; sent ${summary.sent}, rendered ${summary.log_only}, queued ${summary.queued}, skipped ${summary.skipped}, failed ${summary.failed}, items ${summary.item_count}.`;
}

function intradayRunNowStatusMessage(response: AdminIntradayRunNowResponse) {
  const { summary } = response;
  if (response.dry_run) {
    return `Status: Intraday dry run checked ${summary.candidate_count} candidate${summary.candidate_count === 1 ? "" : "s"}; ${summary.would_send_count} would send, ${summary.skipped_count} skipped.`;
  }
  return `Status: Intraday sweep sent ${summary.sent_count}, skipped ${summary.skipped_count}, failed ${summary.failed_count}, candidates ${summary.candidate_count}.`;
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

export function AdminEmailTemplatesView({ showToast }: AdminToastApi) {
  const [templates, setTemplates] = useState<AdminEmailTemplate[]>([]);
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [selectedTemplate, setSelectedTemplate] = useState<AdminEmailTemplate | null>(null);
  const [draft, setDraft] = useState<TemplateDraft | null>(null);
  const [contextDraft, setContextDraft] = useState("{}");
  const [preview, setPreview] = useState<AdminEmailRendered | null>(null);
  const [deliveries, setDeliveries] = useState<AdminEmailDeliveriesResponse | null>(null);
  const [deliveryRecipientSearch, setDeliveryRecipientSearch] = useState("");
  const [debouncedDeliveryRecipient, setDebouncedDeliveryRecipient] = useState("");
  const [deliveryStatus, setDeliveryStatus] = useState("");
  const [deliveryTemplateKey, setDeliveryTemplateKey] = useState("");
  const [deliveryDateWindow, setDeliveryDateWindow] = useState("last_30");
  const [deliveryPage, setDeliveryPage] = useState(1);
  const [deliveryPageSize, setDeliveryPageSize] = useState(10);
  const [currentAdminEmail, setCurrentAdminEmail] = useState<string>("");
  const [testEmail, setTestEmail] = useState("");
  const [digestEmail, setDigestEmail] = useState("");
  const [digestUserId, setDigestUserId] = useState("");
  const [digestWatchlistId, setDigestWatchlistId] = useState("");
  const [digestLookbackDays, setDigestLookbackDays] = useState("1");
  const [digestForce, setDigestForce] = useState(false);
  const [digestRunKind, setDigestRunKind] = useState<"watchlist_activity" | "monitoring" | "signals">("watchlist_activity");
  const [digestRunLimit, setDigestRunLimit] = useState("100");
  const [digestRunDryRun, setDigestRunDryRun] = useState(true);
  const [intradayLookbackMinutes, setIntradayLookbackMinutes] = useState("60");
  const [intradayRunLimit, setIntradayRunLimit] = useState("100");
  const [intradayRunDryRun, setIntradayRunDryRun] = useState(true);
  const [intradayMarketHoursOnly, setIntradayMarketHoursOnly] = useState(true);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [resetDialog, setResetDialog] = useState<ResetDialog>(null);
  const deliveryRequestId = useRef(0);

  const selectedTemplateKey = selectedTemplate?.template_key ?? selectedKey;
  const categories = useMemo(
    () => Array.from(new Set(templates.map((template) => template.category).filter(Boolean))).sort(),
    [templates],
  );
  const deliveryTemplateOptions = useMemo(
    () => templates.map((template) => template.template_key).sort((a, b) => a.localeCompare(b)),
    [templates],
  );
  const deliveryTotal = deliveries?.total ?? 0;
  const deliveryPageCount = deliveries?.total_pages ?? 1;
  const deliveryPageStart = deliveryTotal > 0 && deliveries ? (deliveries.page - 1) * deliveries.page_size + 1 : 0;
  const deliveryPageEnd = deliveryTotal > 0 && deliveries ? Math.min(deliveryTotal, deliveryPageStart + deliveries.items.length - 1) : 0;
  const canGoToPreviousDeliveryPage = Boolean(deliveries && deliveries.page > 1);
  const canGoToNextDeliveryPage = Boolean(deliveries && deliveries.page < deliveries.total_pages);

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

  const refreshDeliveries = useCallback(async (pageOverride?: number) => {
    const requestId = deliveryRequestId.current + 1;
    deliveryRequestId.current = requestId;
    try {
      const requestedPage = pageOverride ?? deliveryPage;
      const response = await getAdminEmailDeliveries({
        recipient: debouncedDeliveryRecipient,
        status: deliveryStatus || undefined,
        template_key: deliveryTemplateKey || undefined,
        date_window: deliveryDateWindow,
        page: requestedPage,
        page_size: deliveryPageSize,
      });
      if (requestId !== deliveryRequestId.current) return;
      setDeliveries(response);
      if (response.page !== deliveryPage) {
        setDeliveryPage(response.page);
      }
    } catch (error) {
      if (requestId !== deliveryRequestId.current) return;
      const message = error instanceof Error ? error.message : "Unable to load email deliveries.";
      setStatus(message);
      showToast({ message, tone: "error" });
    }
  }, [
    debouncedDeliveryRecipient,
    deliveryDateWindow,
    deliveryPage,
    deliveryPageSize,
    deliveryStatus,
    deliveryTemplateKey,
    showToast,
  ]);

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
    const timeoutId = window.setTimeout(() => {
      setDebouncedDeliveryRecipient(deliveryRecipientSearch.trim());
    }, 300);
    return () => window.clearTimeout(timeoutId);
  }, [deliveryRecipientSearch]);

  useEffect(() => {
    setDeliveryPage(1);
  }, [debouncedDeliveryRecipient, deliveryDateWindow, deliveryPageSize, deliveryStatus, deliveryTemplateKey, selectedTemplateKey]);

  useEffect(() => {
    if (!selectedTemplateKey) return;
    setDeliveries(null);
    refreshDeliveries();
  }, [refreshDeliveries, selectedTemplateKey]);

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
      setResetDialog(null);
    }
  };

  const resetAllTemplatesToDefaults = async () => {
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
      setResetDialog(null);
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
      const message = testDeliveryStatusMessage(delivery, testEmail.trim() || currentAdminEmail);
      setStatus(message);
      showToast({ message, tone: delivery.status === "failed" ? "error" : "success" });
      setDeliveryPage(1);
      await refreshDeliveries(1);
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
      const message = kind === "billing" ? testDeliveryStatusMessage(delivery, digestEmail.trim() || currentAdminEmail) : digestStatusMessage(delivery, digestEmail.trim() || currentAdminEmail);
      setStatus(message);
      showToast({ message, tone: delivery.status === "failed" ? "error" : "success" });
      setDeliveryPage(1);
      await refreshDeliveries(1);
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

  const runDigestJobNow = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const response = await adminRunEmailDigestsNow({
        kind: digestRunKind,
        lookback_days: Number(digestLookbackDays || "1"),
        limit: Number(digestRunLimit || "100"),
        force: digestForce,
        dry_run: digestRunDryRun,
      });
      const message = runNowStatusMessage(response);
      setStatus(message);
      showToast({ message, tone: response.summary.failed > 0 ? "error" : "success" });
      setDeliveryPage(1);
      await refreshDeliveries(1);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to run digest job.";
      setStatus(message);
      showToast({ message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const runIntradayAlertsNow = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const response = await adminRunIntradayEmailAlertsNow({
        lookback_minutes: Number(intradayLookbackMinutes || "60"),
        limit: Number(intradayRunLimit || "100"),
        dry_run: intradayRunDryRun,
        market_hours_only: intradayMarketHoursOnly,
      });
      const message = intradayRunNowStatusMessage(response);
      setStatus(message);
      showToast({ message, tone: response.summary.failed_count > 0 ? "error" : "success" });
      setDeliveryPage(1);
      await refreshDeliveries(1);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to run intraday alert sweep.";
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
              onClick={() => setResetDialog("all")}
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
                  onClick={() => setResetDialog("template")}
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
              Sends one explicit admin test through the delivery service. Watchlist digests require a user and watchlist. Scheduled runs use the bounded job controls below.
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

        <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/45 p-4">
          <div className="flex flex-wrap items-end gap-4">
            <label className="block min-w-56 text-sm">
              <span className="block font-medium text-slate-200">Run digest job</span>
              <select
                value={digestRunKind}
                onChange={(event) => setDigestRunKind(event.target.value as "watchlist_activity" | "monitoring" | "signals")}
                className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
              >
                <option value="watchlist_activity">Watchlist activity</option>
                <option value="monitoring">Monitoring</option>
                <option value="signals">Signals</option>
              </select>
            </label>
            <TextInput label="Run limit" value={digestRunLimit} onChange={setDigestRunLimit} />
            <label className="flex items-center gap-2 pb-2 text-sm font-medium text-slate-200">
              <input
                type="checkbox"
                checked={digestRunDryRun}
                onChange={(event) => setDigestRunDryRun(event.target.checked)}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Dry run
            </label>
            <button
              type="button"
              onClick={runDigestJobNow}
              disabled={busy}
              className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
            >
              Run now
            </button>
          </div>
          <p className="mt-3 text-sm text-slate-400">
            Uses the scheduled digest engine with the selected lookback window, account toggles, watchlist digest settings, and idempotency checks.
          </p>
        </div>

        <div className="mt-5 rounded-lg border border-amber-300/20 bg-slate-950/45 p-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h4 className="font-semibold text-white">Intraday Alerts</h4>
              <p className="mt-2 text-sm text-slate-400">
                Runs the high-priority intraday sweep for watchlist activity and high-conviction signal matches. Keep dry run on until candidate counts and skip reasons look right.
              </p>
            </div>
          </div>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <TextInput label="Lookback minutes" value={intradayLookbackMinutes} onChange={setIntradayLookbackMinutes} />
            <TextInput label="Run limit" value={intradayRunLimit} onChange={setIntradayRunLimit} />
            <label className="flex items-center gap-2 pb-2 text-sm font-medium text-slate-200">
              <input
                type="checkbox"
                checked={intradayRunDryRun}
                onChange={(event) => setIntradayRunDryRun(event.target.checked)}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Dry run
            </label>
            <label className="flex items-center gap-2 pb-2 text-sm font-medium text-slate-200">
              <input
                type="checkbox"
                checked={intradayMarketHoursOnly}
                onChange={(event) => setIntradayMarketHoursOnly(event.target.checked)}
                className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
              />
              Market hours only
            </label>
            <button
              type="button"
              onClick={runIntradayAlertsNow}
              disabled={busy}
              className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
            >
              Run intraday sweep
            </button>
          </div>
          <p className="mt-3 text-sm text-slate-400">
            Daily Digests remain separate and include lower or medium-priority activity that does not qualify for intraday email.
          </p>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h3 className="text-xl font-semibold text-white">Recent deliveries</h3>
            <p className="mt-2 text-sm text-slate-400">
              {deliveryTotal > 0
                ? `Showing ${deliveryPageStart}-${deliveryPageEnd} of ${deliveryTotal} matching delivery logs.`
                : "Showing 0 matching delivery logs."}
            </p>
          </div>
        </div>

        <div className="mt-5 flex flex-wrap items-end gap-3">
          <label className="block min-w-[16rem] flex-1 text-sm md:flex-none">
            <span className="block font-medium text-slate-200">Recipient</span>
            <input
              type="search"
              value={deliveryRecipientSearch}
              onChange={(event) => {
                setDeliveryRecipientSearch(event.target.value);
                setDeliveryPage(1);
              }}
              placeholder="Search recipient email..."
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
            />
          </label>
          <label className="block min-w-40 text-sm">
            <span className="block font-medium text-slate-200">Status</span>
            <select
              value={deliveryStatus}
              onChange={(event) => {
                setDeliveryStatus(event.target.value);
                setDeliveryPage(1);
              }}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
            >
              <option value="">All statuses</option>
              {DELIVERY_STATUS_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label className="block min-w-64 text-sm">
            <span className="block font-medium text-slate-200">Template</span>
            <select
              value={deliveryTemplateKey}
              onChange={(event) => {
                setDeliveryTemplateKey(event.target.value);
                setDeliveryPage(1);
              }}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
            >
              <option value="">All templates</option>
              {deliveryTemplateOptions.map((templateKey) => (
                <option key={templateKey} value={templateKey}>
                  {templateKey}
                </option>
              ))}
            </select>
          </label>
          <label className="block min-w-40 text-sm">
            <span className="block font-medium text-slate-200">Date window</span>
            <select
              value={deliveryDateWindow}
              onChange={(event) => {
                setDeliveryDateWindow(event.target.value);
                setDeliveryPage(1);
              }}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
            >
              {DELIVERY_DATE_WINDOW_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="block min-w-28 text-sm">
            <span className="block font-medium text-slate-200">Page size</span>
            <select
              value={deliveryPageSize}
              onChange={(event) => {
                setDeliveryPageSize(Number(event.target.value));
                setDeliveryPage(1);
              }}
              className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
            >
              {DELIVERY_PAGE_SIZE_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            onClick={() => refreshDeliveries()}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Refresh logs
          </button>
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
                    No delivery logs match these filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-400">
          <span>
            Page {deliveries?.page ?? deliveryPage} of {deliveryPageCount}
          </span>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => setDeliveryPage(1)}
              disabled={busy || !canGoToPreviousDeliveryPage}
              className="rounded-lg border border-white/10 px-3 py-2 font-semibold text-slate-200 disabled:cursor-not-allowed disabled:opacity-50"
            >
              First
            </button>
            <button
              type="button"
              onClick={() => setDeliveryPage((current) => Math.max(1, current - 1))}
              disabled={busy || !canGoToPreviousDeliveryPage}
              className="rounded-lg border border-white/10 px-3 py-2 font-semibold text-slate-200 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Previous
            </button>
            <button
              type="button"
              onClick={() => setDeliveryPage((current) => Math.min(deliveryPageCount, current + 1))}
              disabled={busy || !canGoToNextDeliveryPage}
              className="rounded-lg border border-white/10 px-3 py-2 font-semibold text-slate-200 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Next
            </button>
            <button
              type="button"
              onClick={() => setDeliveryPage(deliveryPageCount)}
              disabled={busy || !canGoToNextDeliveryPage}
              className="rounded-lg border border-white/10 px-3 py-2 font-semibold text-slate-200 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Last
            </button>
          </div>
        </div>
      </section>

      <WalnutConfirmDialog
        open={resetDialog === "template"}
        eyebrow="Reset template"
        title="Reset this template to the Walnut branded default?"
        description="This will replace the selected template's subject and body with the shipped Walnut branded default. You can continue editing it after reset."
        confirmLabel={busy ? "Resetting..." : "Reset template"}
        tone="success"
        isBusy={busy}
        onClose={() => setResetDialog(null)}
        onConfirm={resetTemplateToDefault}
      />
      <WalnutConfirmDialog
        open={resetDialog === "all"}
        eyebrow="Reset system templates"
        title="Reset all system email templates?"
        description="This will replace all system email templates with the shipped Walnut branded defaults. Existing custom edits will be replaced."
        confirmLabel={busy ? "Resetting..." : "Reset all templates"}
        tone="success"
        isBusy={busy}
        onClose={() => setResetDialog(null)}
        onConfirm={resetAllTemplatesToDefaults}
      />
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
