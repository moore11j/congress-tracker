"use client";

import { useState, type FormEvent } from "react";

type ContactStatus = "idle" | "sending" | "sent" | "error";

const requestTypes = ["Feedback", "Reporting a bug", "Requesting a new feature", "General inquiry"] as const;
const successMessage = "Your message was successfully sent. We will try to respond within the next 2-3 business days.";

export function ContactForm() {
  const [status, setStatus] = useState<ContactStatus>("idle");
  const [statusMessage, setStatusMessage] = useState("");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    setStatus("sending");
    setStatusMessage("");

    try {
      const response = await fetch("/api/contact", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          request_type: String(formData.get("request_type") || ""),
          email: String(formData.get("email") || ""),
          message: String(formData.get("message") || ""),
        }),
      });
      const payload = (await response.json().catch(() => ({}))) as { message?: string; detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "We could not send your message. Please try again.");
      }
      form.reset();
      setStatus("sent");
      setStatusMessage(payload.message || successMessage);
    } catch (error) {
      setStatus("error");
      setStatusMessage(error instanceof Error ? error.message : "We could not send your message. Please try again.");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="grid gap-4">
      <label className="grid gap-2">
        <span className="text-sm font-semibold text-slate-200">Request type</span>
        <select name="request_type" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-sm text-slate-100 outline-none transition focus:border-emerald-300/50">
          {requestTypes.map((type) => (
            <option key={type} value={type}>
              {type}
            </option>
          ))}
        </select>
      </label>
      <label className="grid gap-2">
        <span className="text-sm font-semibold text-slate-200">Your email</span>
        <input
          name="email"
          type="email"
          required
          placeholder="you@example.com"
          className="rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-emerald-300/50"
        />
      </label>
      <label className="grid gap-2">
        <span className="text-sm font-semibold text-slate-200">Message</span>
        <textarea
          name="message"
          required
          rows={7}
          placeholder="How can we help?"
          className="rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-emerald-300/50"
        />
      </label>
      <button
        type="submit"
        disabled={status === "sending"}
        className="inline-flex w-fit items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200 disabled:cursor-wait disabled:bg-emerald-300/60"
      >
        {status === "sending" ? "Sending..." : "Send Message"}
      </button>
      {statusMessage ? (
        <p className={`rounded-lg border px-3 py-3 text-sm leading-6 ${status === "sent" ? "border-emerald-300/25 bg-emerald-300/10 text-emerald-100" : "border-red-300/25 bg-red-400/10 text-red-100"}`}>
          {statusMessage}
        </p>
      ) : null}
    </form>
  );
}
