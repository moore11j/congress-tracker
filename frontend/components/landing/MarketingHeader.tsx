import { WalnutBrandMark } from "@/components/WalnutBrandMark";

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");

const navLinks = [
  { label: "Feed", href: `${appUrl}/feed` },
  { label: "Insights", href: `${appUrl}/insights` },
  { label: "Signals", href: `${appUrl}/signals` },
  { label: "Leaderboards", href: `${appUrl}/leaderboards/congress-traders` },
  { label: "Strategies", href: `${appUrl}/strategies` },
  { label: "Outcomes", href: `${appUrl}/outcomes` },
] as const;

const profilesNavLinks = [
  { label: "Overview", href: `${appUrl}/profiles`, description: "Explore Walnut profile datasets." },
  { label: "Congress", href: `${appUrl}/members`, description: "Congress trading and member portfolios." },
  { label: "Insiders", href: `${appUrl}/insiders`, description: "Executive and director transactions." },
  { label: "Institutions", href: `${appUrl}/institutions`, description: "13F holdings and position changes." },
  { label: "Departments", href: `${appUrl}/departments`, description: "Government contracts and vendors." },
] as const;

const toolsNavLinks = [
  { label: "Stock Screener", href: `${appUrl}/screener`, description: "Screen public companies by Walnut evidence and market data." },
  { label: "Stock Comparisons", href: `${appUrl}/compare`, description: "Compare two tickers across the research workflow." },
  { label: "Backtesting", href: `${appUrl}/backtesting`, description: "Test saved screens and disclosure strategies against history." },
  { label: "Strategies", href: `${appUrl}/strategies`, beta: true, description: "Explore published strategies with transparent methodology and performance." },
] as const;

const companyNavLinks = [
  { label: "About", href: "/about", description: "Who operates Walnut Markets." },
  { label: "FAQ", href: "/faq", description: "Answers about data, billing, privacy, and support." },
  { label: "Contact", href: "/contact", description: "Send feedback, bug reports, and requests." },
  { label: "Terms", href: "/terms", description: "Terms of Use." },
  { label: "Privacy", href: "/privacy", description: "Privacy Policy." },
] as const;

function LandingNavLink({ href, label, className = "" }: { href: string; label: string; className?: string }) {
  return (
    <a href={href} className={`transition hover:text-white ${className}`}>
      {label}
    </a>
  );
}

function NavMenuItems({
  items,
  mobile = false,
}: {
  items: readonly ({ label: string; href?: string; description: string; comingSoon?: boolean; beta?: boolean })[];
  mobile?: boolean;
}) {
  const itemClassName = mobile
    ? "rounded-lg border border-white/10 bg-slate-950/65 px-3 py-3 text-slate-300 transition hover:border-emerald-300/35 hover:text-white"
    : "block rounded-md px-3 py-2.5 text-slate-300 transition hover:bg-white/[0.055] hover:text-white";
  const disabledClassName = mobile
    ? "rounded-lg border border-white/10 bg-white/[0.025] px-3 py-3 text-slate-500"
    : "rounded-md px-3 py-2.5 text-slate-500";

  return (
    <>
      {items.map((item) =>
        item.href ? (
          <a key={item.label} href={item.href} className={itemClassName}>
            <span className="flex items-center justify-between gap-3">
              <span className="font-semibold text-slate-100">{item.label}</span>
              {item.beta ? (
                <span className="rounded border border-emerald-300/25 bg-emerald-300/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-emerald-200">
                  Beta
                </span>
              ) : null}
            </span>
            <span className="mt-1 block text-xs leading-5 text-slate-400">{item.description}</span>
          </a>
        ) : (
          <div key={item.label} aria-disabled="true" className={disabledClassName}>
            <span className="flex items-center justify-between gap-3">
              <span className="font-semibold text-slate-300">{item.label}</span>
              {item.comingSoon ? (
                <span className="rounded border border-cyan-300/25 bg-cyan-300/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-cyan-200">
                  Coming Soon
                </span>
              ) : null}
            </span>
            <span className="mt-1 block text-xs leading-5">{item.description}</span>
          </div>
        ),
      )}
    </>
  );
}

function DesktopMenu({
  label,
  heading,
  items,
}: {
  label: string;
  heading: string;
  items: readonly ({ label: string; href?: string; description: string; comingSoon?: boolean; beta?: boolean })[];
}) {
  return (
    <div className="group relative isolate z-[9000]" style={{ zIndex: 9000 }}>
      <button
        type="button"
        aria-haspopup="menu"
        className="flex cursor-default items-center gap-1 rounded-md px-1 py-1 transition hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50"
      >
        <span>{label}</span>
        <span aria-hidden="true" className="text-[10px] text-emerald-200 transition group-hover:rotate-180">
          &#9662;
        </span>
      </button>
      <div
        role="menu"
        className="pointer-events-none invisible absolute left-1/2 top-full z-[10000] w-80 -translate-x-1/2 pt-3 opacity-0 transition group-hover:pointer-events-auto group-hover:visible group-hover:opacity-100"
        style={{ zIndex: 10000 }}
      >
        <div className="rounded-lg border border-white/15 bg-[#030712] p-2 shadow-2xl shadow-black ring-1 ring-black">
          <div className="px-3 pb-2 pt-1 text-[10px] font-bold uppercase tracking-[0.22em] text-slate-500">{heading}</div>
          <div className="grid gap-1">
            <NavMenuItems items={items} />
          </div>
        </div>
      </div>
    </div>
  );
}

export function MarketingHeader({ pricingHref = "/pricing" }: { pricingHref?: string }) {
  return (
    <header className="sticky top-0 isolate z-[8000] border-b border-white/10 bg-slate-950/95 backdrop-blur" style={{ zIndex: 8000 }}>
      <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
        <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
          <WalnutBrandMark
            className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]"
            svgClassName="h-6 w-6 overflow-visible"
          />
          <span className="leading-none">
            <span className="block whitespace-nowrap text-base font-semibold text-white">Walnut</span>
            <span className="mt-1 block whitespace-nowrap text-[11px] font-medium text-slate-400">Market Terminal</span>
          </span>
        </a>
        <nav className="hidden items-center gap-3 text-xs font-medium text-slate-300 lg:flex xl:gap-5 xl:text-sm" aria-label="Primary navigation">
          <LandingNavLink href={navLinks[0].href} label={navLinks[0].label} />
          <LandingNavLink href={navLinks[1].href} label={navLinks[1].label} />
          <DesktopMenu label="Profiles" heading="Profiles" items={profilesNavLinks} />
          {navLinks.slice(2).map((link) => (
            <LandingNavLink key={link.label} href={link.href} label={link.label} />
          ))}
          <DesktopMenu label="Tools" heading="Research tools" items={toolsNavLinks} />
          <LandingNavLink href="/compare" label="Compare Walnut" />
          <LandingNavLink href={pricingHref} label="Pricing" />
          <DesktopMenu label="Company" heading="Company" items={companyNavLinks} />
        </nav>
        <div className="flex shrink-0 items-center gap-2">
          <a
            href={`${appUrl}/login`}
            className="hidden rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/25 hover:text-white md:inline-flex"
          >
            Login / Register
          </a>
          <a
            href={`${appUrl}/login`}
            className="whitespace-nowrap rounded-lg border border-emerald-200 bg-emerald-300 px-3 py-1.5 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 md:hidden"
          >
            Login / Register
          </a>
          <a
            href={appUrl}
            className="hidden rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200 md:inline-flex"
          >
            Launch Terminal
          </a>
        </div>
      </div>
      <div className="mx-auto flex max-w-7xl justify-end px-4 pb-4 sm:px-6 lg:hidden lg:px-8">
        <details className="group relative z-[9000] isolate" style={{ zIndex: 9000 }}>
          <summary className="flex cursor-pointer list-none items-center gap-1 rounded-lg border border-white/10 bg-white/[0.03] px-3 py-1.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/35 hover:text-white [&::-webkit-details-marker]:hidden">
            <span>Menu</span>
            <span aria-hidden="true" className="text-[10px] text-emerald-200 transition group-open:rotate-180">
              &#9662;
            </span>
          </summary>
          <div className="absolute right-0 top-full z-[10000] mt-3 w-[min(calc(100vw-2rem),22rem)] rounded-lg border border-white/15 bg-[#030712] p-3 shadow-2xl shadow-black ring-1 ring-black" style={{ zIndex: 10000 }}>
            <nav aria-label="Mobile primary navigation" className="grid gap-1 text-sm">
              <LandingNavLink href={navLinks[0].href} label={navLinks[0].label} className="rounded-lg px-3 py-2.5 text-slate-200" />
              <LandingNavLink href={navLinks[1].href} label={navLinks[1].label} className="rounded-lg px-3 py-2.5 text-slate-200" />
              <details className="group/profiles">
                <summary className="flex cursor-pointer list-none items-center justify-between rounded-lg px-3 py-2.5 font-semibold text-slate-100 transition hover:text-white [&::-webkit-details-marker]:hidden">
                  <span>Profiles</span>
                  <span aria-hidden="true" className="text-[10px] text-emerald-200 transition group-open/profiles:rotate-180">
                    &#9662;
                  </span>
                </summary>
                <div className="mt-1 grid gap-2 border-l border-white/10 pl-3">
                  <NavMenuItems items={profilesNavLinks} mobile />
                </div>
              </details>
              {navLinks.slice(2).map((link) => (
                <LandingNavLink key={link.label} href={link.href} label={link.label} className="rounded-lg px-3 py-2.5 text-slate-200" />
              ))}
              <details className="group/tools">
                <summary className="flex cursor-pointer list-none items-center justify-between rounded-lg px-3 py-2.5 font-semibold text-slate-100 transition hover:text-white [&::-webkit-details-marker]:hidden">
                  <span>Tools</span>
                  <span aria-hidden="true" className="text-[10px] text-emerald-200 transition group-open/tools:rotate-180">
                    &#9662;
                  </span>
                </summary>
                <div className="mt-1 grid gap-2 border-l border-white/10 pl-3">
                  <NavMenuItems items={toolsNavLinks} mobile />
                </div>
              </details>
              <LandingNavLink href="/compare" label="Compare Walnut" className="rounded-lg px-3 py-2.5 text-slate-200" />
              <LandingNavLink href={pricingHref} label="Pricing" className="rounded-lg px-3 py-2.5 text-slate-200" />
              <details className="group/company">
                <summary className="flex cursor-pointer list-none items-center justify-between rounded-lg px-3 py-2.5 font-semibold text-slate-100 transition hover:text-white [&::-webkit-details-marker]:hidden">
                  <span>Company</span>
                  <span aria-hidden="true" className="text-[10px] text-emerald-200 transition group-open/company:rotate-180">
                    &#9662;
                  </span>
                </summary>
                <div className="mt-1 grid gap-2 border-l border-white/10 pl-3">
                  <NavMenuItems items={companyNavLinks} mobile />
                </div>
              </details>
            </nav>
          </div>
        </details>
      </div>
    </header>
  );
}
