"use client";

import { useEffect, useMemo, useState } from "react";
import {
  adminPreviewEmailTemplate,
  adminSendTestEmailTemplate,
  adminUpdateEmailTemplate,
  getAdminEmailDeliveries,
  getAdminEmailTemplate,
  getAdminEmailTemplates,
  getMe,
  type AdminEmailDeliveriesResponse,
  type AdminEmailRendered,
  type AdminEmailTemplate,
} from "@/lib/api";
import type { AdminToastApi } from "@/components/admin/AdminToast";

type DeliveryScope = "template" | "all";

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

function sampleContextFor(template: AdminEmailTemplate): Record<string, string | number> {
  const context: Record<string, string | number> = {};
  for (const variable of template.variables ?? []) {
    if (variable.endsWith("_url") || variable === "verification_url" || variable === "reset_url" || variable === "statement_url") {
      context[variable] = "https://walnut-intel.com/admin/settings";
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
      setPreview(null);
      const message = "Email template saved.";
      setStatus(message);
      showToast(message);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to save template.";
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
      await adminSendTestEmailTemplate(selectedTemplateKey, {
        to_email: testEmail.trim() || null,
        context: parseContext(contextDraft),
      });
      const message = `Test email queued for ${testEmail.trim() || currentAdminEmail || "current admin"}.`;
      setStatus(message);
      showToast(message);
      await refreshDeliveries();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to send test email.";
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
          <button
            type="button"
            onClick={loadTemplates}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Refresh
          </button>
        </div>
        {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}
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
                <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Rendered HTML</div>
                {preview?.body_html ? (
                  <iframe
                    title="Rendered email HTML preview"
                    sandbox=""
                    srcDoc={preview.body_html}
                    className="mt-3 h-80 w-full rounded-lg border border-white/10 bg-white"
                  />
                ) : (
                  <p className="mt-3 text-sm text-slate-400">Run preview to render the HTML body.</p>
                )}
                {preview?.body_html ? (
                  <pre className="mt-3 max-h-56 overflow-auto whitespace-pre-wrap rounded-lg border border-white/10 bg-slate-950 p-3 font-mono text-xs text-slate-300">
                    {preview.body_html}
                  </pre>
                ) : null}
              </div>
            </div>
          </div>
        </section>
      ) : null}

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
