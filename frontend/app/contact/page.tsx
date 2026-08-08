import type { Metadata } from "next";
import { LegalPageShell, LegalSection } from "@/components/landing/LegalPageShell";
import { legalPageChrome } from "@/lib/legalPageChrome";
import { marketingPageMetadata } from "@/lib/marketingMetadata";

const lastUpdated = "August 8, 2026";

export const metadata: Metadata = marketingPageMetadata("/contact", {
  title: "Contact Walnut Markets | Support",
  description: "Contact Walnut Markets for feedback, bug reports, feature requests, and general inquiries.",
});

export default async function ContactPage() {
  const chrome = await legalPageChrome();

  return (
    <LegalPageShell
      eyebrow="Support"
      title="Contact Walnut Markets"
      description="Send feedback, report a bug, request a feature, or ask a general question. Messages are directed to support@walnutmarkets.com."
      lastUpdated={lastUpdated}
      chrome={chrome}
    >
      <LegalSection title="Send a Message">
        <form action="mailto:support@walnutmarkets.com" method="post" encType="text/plain" className="grid gap-4">
          <label className="grid gap-2">
            <span className="text-sm font-semibold text-slate-200">Request type</span>
            <select name="request_type" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-3 text-sm text-slate-100 outline-none transition focus:border-emerald-300/50">
              <option value="Feedback">Feedback</option>
              <option value="Reporting a bug">Reporting a bug</option>
              <option value="Requesting a new feature">Requesting a new feature</option>
              <option value="General inquiry">General inquiry</option>
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
          <button type="submit" className="inline-flex w-fit items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200">
            Email Support
          </button>
          <p className="text-xs leading-5 text-slate-500">
            If your browser does not open an email draft, send your message directly to{" "}
            <a className="text-emerald-200 hover:text-emerald-100" href="mailto:support@walnutmarkets.com">
              support@walnutmarkets.com
            </a>
            .
          </p>
        </form>
      </LegalSection>
    </LegalPageShell>
  );
}
