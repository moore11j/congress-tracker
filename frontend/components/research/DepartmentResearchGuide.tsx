import { departmentSlug } from "@/lib/departments";
import { appCanonicalUrl, marketingCanonicalUrl } from "@/lib/marketingMetadata";

const guides: Record<string, { title: string; explanation: string; source: string; sourceLabel: string; research?: { href: string; label: string } }> = {
  "department-of-energy": {
    title: "How to research Department of Energy contracts",
    explanation: "DOE contract research includes laboratory operations and site management, not just energy equipment. The department's facility-management list identifies the contracting entity, contract number, and term. Check that entity before attributing an award to a listed company: a laboratory operator or joint venture is not necessarily the public parent itself.",
    source: "https://www.energy.gov/management/doennsa-major-site-facility-management-contracts",
    sourceLabel: "DOE/NNSA facility-management contracts",
  },
  nasa: {
    title: "How to research NASA contract recipients",
    explanation: "A NASA contract can cover technology and services as well as space missions. For example, NASA's June 22, 2026 SEWP VI announcement describes an IT procurement vehicle with individual orders. A vehicle's maximum value is not the same as orders funded for one recipient. Check the contract number, recipient, performance period, and amount type before comparing companies.",
    source: "https://www.nasa.gov/news-release/nasa-awards-solutions-for-federal-enterprise-procurement-contracts/",
    sourceLabel: "NASA's SEWP VI contract announcement",
    research: { href: "/research/public-companies-winning-nasa-contracts", label: "Research: public companies receiving NASA contracts" },
  },
  "department-of-defense": {
    title: "How to research DoD contract awards",
    explanation: "For a search such as 'DoD contracts awarded today', distinguish the announcement date from the award or modification date in the underlying record. Use the official contract announcements to check the named recipient and work, then compare the records shown here with the public company's financials. This profile is a research view of tracked awards, not a complete live list of today's announcements.",
    source: "https://www.defense.gov/News/Contracts/",
    sourceLabel: "Official defense contract announcements",
    research: { href: "/research/public-companies-winning-department-of-defense-contracts", label: "Research: public companies receiving defense contracts" },
  },
};

export function DepartmentResearchGuide({ name }: { name: string }) {
  const guide = guides[departmentSlug(name) ?? ""];
  if (!guide) return null;
  const linkStyle = "text-emerald-200 underline decoration-emerald-300/30 underline-offset-4 hover:text-emerald-100";
  return (
    <section className="mt-4 rounded-lg border border-slate-700/70 bg-slate-950/55 p-4 sm:p-5" aria-label="Contract research guide">
      <h2 className="text-xl font-semibold text-white">{guide.title}</h2>
      <p className="mt-2 text-xs text-slate-400">Research guide by <a href={appCanonicalUrl("/about")} className={linkStyle}>Walnut Markets</a>. Guide updated September 25, 2026; award dates are shown separately.</p>
      <p className="mt-3 max-w-4xl text-sm leading-7 text-slate-300">{guide.explanation}</p>
      <p className="mt-3 text-sm leading-6 text-slate-400">Primary source: <a href={guide.source} className={linkStyle}>{guide.sourceLabel}</a>.</p>
      <div className="mt-5 grid gap-4 md:grid-cols-3">
        <div>
          <h3 className="font-semibold text-slate-100">Check the recipient</h3>
          <p className="mt-2 text-sm leading-6 text-slate-400">Compare the legal recipient with the linked ticker. Subsidiaries and joint ventures can make the parent company's economic exposure different from the headline award.</p>
        </div>
        <div>
          <h3 className="font-semibold text-slate-100">Check the amount</h3>
          <p className="mt-2 text-sm leading-6 text-slate-400">An obligation commits government funds; an outlay is a payment. A potential contract ceiling is neither cash received nor company revenue. Read the amount definition in the source record before combining values.</p>
        </div>
        <div>
          <h3 className="font-semibold text-slate-100">Check the dates</h3>
          <p className="mt-2 text-sm leading-6 text-slate-400">An award date, an announcement date, and a reporting update can differ. Compare awards over a consistent period; an older latest award does not establish when the dataset was refreshed.</p>
        </div>
      </div>
      <p className="mt-4 text-xs leading-6 text-slate-400">Definitions: <a href="https://www.usaspending.gov/data/Federal-Spending-Guide.pdf" className={linkStyle}>USAspending's federal spending guide</a>. Tracked records and ticker mappings are not a complete agency spending inventory.</p>
      <nav aria-label="Related contract research" className="mt-4 flex flex-col items-start gap-3 text-sm">
        {guide.research ? <a href={marketingCanonicalUrl(guide.research.href)} className={linkStyle}>{guide.research.label}</a> : null}
        <a href={marketingCanonicalUrl("/government-contracts")} className={linkStyle}>Government contracts: compare agencies and understand award values</a>
        <a href={marketingCanonicalUrl("/research")} className={linkStyle}>Browse Walnut research briefs</a>
      </nav>
    </section>
  );
}
