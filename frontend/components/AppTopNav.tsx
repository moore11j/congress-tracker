"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Fragment, useCallback, useEffect, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { HorizontalScrollIndicators, useHorizontalScrollAffordance } from "@/components/ui/HorizontalScrollAffordance";

const topNavLinks = [
  { href: "/?mode=all", label: "Feed" },
  { href: "/insights", label: "Insights" },
  { href: "/signals", label: "Signals" },
  { href: "/leaderboards", label: "Leaderboards" },
  ...(process.env.NEXT_PUBLIC_STRATEGIES_ENABLED === "0" ? [] : [{ href: "/strategies", label: "Strategies" }]),
  ...(process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0" ? [] : [{ href: "/outcomes", label: "Outcomes" }]),
] as const;

const profilesNavItems = [
  { href: "/profiles", label: "Overview", icon: "◈", description: "Explore Walnut profile datasets" },
  { href: "/members", label: "Congress", icon: "◇", description: "Congress trading and member portfolios" },
  { href: "/insiders", label: "Insiders", icon: "◎", description: "Executive and director transactions" },
  { href: "/institutions", label: "Institutions", icon: "▣", description: "13F holdings and position changes" },
  { href: "/departments", label: "Departments", icon: "▤", description: "Government contracts and vendors" },
] as const;

const toolsNavGroups = [
  {
    label: "Stock Research",
    items: [
      { href: "/screener", label: "Screener", icon: "◉", description: "Find stocks matching specific criteria" },
      { href: "/compare", label: "Compare", icon: "⇄", description: "Compare companies and Walnut evidence" },
    ],
  },
  {
    label: "Analysis",
    items: [
      { href: "/backtesting", label: "Backtesting", icon: "↗︎", description: "Test strategies against history" },
      { href: "/market-pressure", label: "Market Maps", icon: "◫", description: "Visualize market pressure" },
    ],
  },
] as const;

const companyNavItems = [
  { href: "/about", label: "About", icon: "•", description: "Who operates Walnut Markets" },
  { href: "/faq", label: "FAQ", icon: "•", description: "Answers about data, billing, privacy, and support" },
  { href: "/contact", label: "Contact", icon: "•", description: "Feedback, bug reports, feature requests, and inquiries" },
  { href: "/terms", label: "Terms", icon: "•", description: "Terms of Use" },
  { href: "/privacy", label: "Privacy", icon: "•", description: "Privacy Policy" },
] as const;

function isActiveNavLink(pathname: string | null, href: string) {
  const path = pathname || "/";
  if (href === "/?mode=all") return path === "/";
  const basePath = href.split("?")[0] || href;
  if (basePath === "/leaderboards") return path === basePath || path.startsWith("/leaderboards/");
  if (basePath === "/compare") return path === "/compare" || path.startsWith("/compare/");
  return path === basePath || path.startsWith(`${basePath}/`);
}

function isActiveToolsLink(pathname: string | null) {
  return toolsNavGroups.some((group) => group.items.some((item) => isActiveNavLink(pathname, item.href)));
}

function isActiveProfilesLink(pathname: string | null) {
  const path = pathname || "/";
  return (
    profilesNavItems.some((item) => isActiveNavLink(pathname, item.href)) ||
    path.startsWith("/member/") ||
    path.startsWith("/insider/") ||
    path.startsWith("/institution/")
  );
}

function isActiveCompanyLink(pathname: string | null) {
  return companyNavItems.some((item) => isActiveNavLink(pathname, item.href));
}

export function AppTopNav() {
  const pathname = usePathname();
  const [profilesOpen, setProfilesOpen] = useState(false);
  const [profilesMenuPosition, setProfilesMenuPosition] = useState<{ left: number; top: number } | null>(null);
  const [toolsOpen, setToolsOpen] = useState(false);
  const [toolsMenuPosition, setToolsMenuPosition] = useState<{ left: number; top: number } | null>(null);
  const [companyOpen, setCompanyOpen] = useState(false);
  const [companyMenuPosition, setCompanyMenuPosition] = useState<{ left: number; top: number } | null>(null);
  const profilesRef = useRef<HTMLDivElement | null>(null);
  const profilesButtonRef = useRef<HTMLButtonElement | null>(null);
  const toolsRef = useRef<HTMLDivElement | null>(null);
  const toolsButtonRef = useRef<HTMLButtonElement | null>(null);
  const companyRef = useRef<HTMLDivElement | null>(null);
  const companyButtonRef = useRef<HTMLButtonElement | null>(null);
  const { scrollRef, canScrollLeft, canScrollRight, updateScrollState } =
    useHorizontalScrollAffordance<HTMLElement>();
  const toolsActive = isActiveToolsLink(pathname);
  const profilesActive = isActiveProfilesLink(pathname);
  const companyActive = isActiveCompanyLink(pathname);

  const updateProfilesMenuPosition = useCallback(() => {
    const rect = profilesButtonRef.current?.getBoundingClientRect();
    if (!rect) return;
    const menuWidth = 256;
    const viewportPadding = 16;
    const left = Math.min(Math.max(rect.left, viewportPadding), window.innerWidth - menuWidth - viewportPadding);
    setProfilesMenuPosition({ left, top: rect.bottom });
  }, []);

  const updateToolsMenuPosition = useCallback(() => {
    const rect = toolsButtonRef.current?.getBoundingClientRect();
    if (!rect) return;
    const menuWidth = 256;
    const viewportPadding = 16;
    const left = Math.min(Math.max(rect.left, viewportPadding), window.innerWidth - menuWidth - viewportPadding);
    setToolsMenuPosition({ left, top: rect.bottom });
  }, []);

  const updateCompanyMenuPosition = useCallback(() => {
    const rect = companyButtonRef.current?.getBoundingClientRect();
    if (!rect) return;
    const menuWidth = 256;
    const viewportPadding = 16;
    const left = Math.min(Math.max(rect.left, viewportPadding), window.innerWidth - menuWidth - viewportPadding);
    setCompanyMenuPosition({ left, top: rect.bottom });
  }, []);

  useEffect(() => {
    if (!profilesOpen) return;
    updateProfilesMenuPosition();
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (profilesRef.current && !profilesRef.current.contains(event.target as Node)) setProfilesOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setProfilesOpen(false);
    };
    document.addEventListener("mousedown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    window.addEventListener("resize", updateProfilesMenuPosition);
    window.addEventListener("scroll", updateProfilesMenuPosition, true);
    return () => {
      document.removeEventListener("mousedown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
      window.removeEventListener("resize", updateProfilesMenuPosition);
      window.removeEventListener("scroll", updateProfilesMenuPosition, true);
    };
  }, [profilesOpen, updateProfilesMenuPosition]);

  useEffect(() => {
    if (!toolsOpen) return;
    updateToolsMenuPosition();
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (toolsRef.current && !toolsRef.current.contains(event.target as Node)) setToolsOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setToolsOpen(false);
    };
    document.addEventListener("mousedown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    window.addEventListener("resize", updateToolsMenuPosition);
    window.addEventListener("scroll", updateToolsMenuPosition, true);
    return () => {
      document.removeEventListener("mousedown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
      window.removeEventListener("resize", updateToolsMenuPosition);
      window.removeEventListener("scroll", updateToolsMenuPosition, true);
    };
  }, [toolsOpen, updateToolsMenuPosition]);

  useEffect(() => {
    if (!companyOpen) return;
    updateCompanyMenuPosition();
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (companyRef.current && !companyRef.current.contains(event.target as Node)) setCompanyOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setCompanyOpen(false);
    };
    document.addEventListener("mousedown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    window.addEventListener("resize", updateCompanyMenuPosition);
    window.addEventListener("scroll", updateCompanyMenuPosition, true);
    return () => {
      document.removeEventListener("mousedown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
      window.removeEventListener("resize", updateCompanyMenuPosition);
      window.removeEventListener("scroll", updateCompanyMenuPosition, true);
    };
  }, [companyOpen, updateCompanyMenuPosition]);

  function handleProfilesKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (!profilesOpen) setProfilesOpen(true);
      window.requestAnimationFrame(() => {
        profilesRef.current?.querySelector<HTMLAnchorElement>("[data-profiles-link]")?.focus();
      });
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (!profilesOpen) setProfilesOpen(true);
      window.requestAnimationFrame(() => {
        const links = Array.from(profilesRef.current?.querySelectorAll<HTMLAnchorElement>("[data-profiles-link]") ?? []);
        links[links.length - 1]?.focus();
      });
    } else if (event.key === "Escape") {
      event.preventDefault();
      setProfilesOpen(false);
    }
  }

  function handleToolsKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (!toolsOpen) setToolsOpen(true);
      window.requestAnimationFrame(() => {
        toolsRef.current?.querySelector<HTMLAnchorElement>("[data-tools-link]")?.focus();
      });
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (!toolsOpen) setToolsOpen(true);
      window.requestAnimationFrame(() => {
        const links = Array.from(toolsRef.current?.querySelectorAll<HTMLAnchorElement>("[data-tools-link]") ?? []);
        links[links.length - 1]?.focus();
      });
    } else if (event.key === "Escape") {
      event.preventDefault();
      setToolsOpen(false);
    }
  }

  function handleCompanyKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (!companyOpen) setCompanyOpen(true);
      window.requestAnimationFrame(() => {
        companyRef.current?.querySelector<HTMLAnchorElement>("[data-company-link]")?.focus();
      });
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (!companyOpen) setCompanyOpen(true);
      window.requestAnimationFrame(() => {
        const links = Array.from(companyRef.current?.querySelectorAll<HTMLAnchorElement>("[data-company-link]") ?? []);
        links[links.length - 1]?.focus();
      });
    } else if (event.key === "Escape") {
      event.preventDefault();
      setCompanyOpen(false);
    }
  }

  return (
    <div className="relative order-3 min-w-0 basis-full lg:order-none lg:min-w-[34rem] lg:flex-1 lg:basis-0">
      <nav
        ref={scrollRef}
        onScroll={updateScrollState}
        className="flex min-w-0 items-center gap-3 overflow-x-auto whitespace-nowrap py-0.5 text-sm font-medium text-slate-200 [scrollbar-width:none] lg:gap-4 lg:py-1 xl:gap-5 [&::-webkit-scrollbar]:hidden"
      >
        {topNavLinks.map((link) => {
          const active = isActiveNavLink(pathname, link.href);
          const navLink = (
            <Link
              key={link.href}
              href={link.href}
              prefetch={false}
              aria-current={active ? "page" : undefined}
              className={`flex min-h-11 items-center rounded-full px-3 py-2 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 lg:min-h-0 lg:px-2.5 lg:py-1 ${
                active
                  ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                  : "text-slate-200 hover:text-white"
              }`}
            >
              {link.label}
            </Link>
          );
          if (link.href !== "/insights") return navLink;
          return (
            <Fragment key="insights-profiles">
              {navLink}
              <div
                ref={profilesRef}
                className="relative"
                onKeyDown={handleProfilesKeyDown}
                onMouseEnter={() => {
                  updateProfilesMenuPosition();
                  setProfilesOpen(true);
                  setToolsOpen(false);
                  setCompanyOpen(false);
                }}
                onMouseLeave={() => setProfilesOpen(false)}
              >
                <button
                  ref={profilesButtonRef}
                  type="button"
                  aria-haspopup="menu"
                  aria-expanded={profilesOpen}
                  onClick={() => {
                    updateProfilesMenuPosition();
                    setProfilesOpen(true);
                    setToolsOpen(false);
                    setCompanyOpen(false);
                  }}
                  className={`flex min-h-11 items-center rounded-full px-3 py-2 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 lg:min-h-0 lg:px-2.5 lg:py-1 ${
                    profilesActive || profilesOpen
                      ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                      : "text-slate-200 hover:text-white"
                  }`}
                >
                  Profiles <span aria-hidden="true">&#9662;</span>
                </button>
                {profilesOpen ? (
                  <div
                    role="menu"
                    aria-label="Profiles"
                    style={profilesMenuPosition ?? undefined}
                    className="fixed z-[1100] w-64 rounded-md border border-white/10 bg-slate-950/95 p-4 text-sm shadow-2xl shadow-black/40 ring-1 ring-black/20"
                  >
                    <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.22em] text-slate-400">Profiles</div>
                    {profilesNavItems.map((item) => {
                      const itemActive = isActiveNavLink(pathname, item.href);
                      return (
                        <Link
                          key={item.href}
                          href={item.href}
                          prefetch={false}
                          role="menuitem"
                          data-profiles-link
                          aria-current={itemActive ? "page" : undefined}
                          onClick={() => setProfilesOpen(false)}
                          className={`grid grid-cols-[1.5rem_minmax(0,1fr)] gap-2 rounded-md px-1 py-2.5 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 ${
                            itemActive ? "bg-emerald-400/15 text-emerald-100" : "text-slate-200 hover:bg-white/5 hover:text-white"
                          }`}
                        >
                          <span className="pt-0.5 text-lg leading-none text-emerald-300">{item.icon}</span>
                          <span>
                            <span className="block font-semibold leading-5 text-white">{item.label}</span>
                            <span className="mt-0.5 block whitespace-normal text-xs leading-4 text-slate-400">{item.description}</span>
                          </span>
                        </Link>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            </Fragment>
          );
        })}
        <div
          ref={toolsRef}
          className="relative"
          onKeyDown={handleToolsKeyDown}
          onMouseEnter={() => {
            updateToolsMenuPosition();
            setToolsOpen(true);
            setProfilesOpen(false);
            setCompanyOpen(false);
          }}
          onMouseLeave={() => setToolsOpen(false)}
        >
          <button
            ref={toolsButtonRef}
            type="button"
            aria-haspopup="menu"
            aria-expanded={toolsOpen}
            onClick={() => {
              updateToolsMenuPosition();
              setToolsOpen(true);
              setProfilesOpen(false);
              setCompanyOpen(false);
            }}
            className={`flex min-h-11 items-center rounded-full px-3 py-2 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 lg:min-h-0 lg:px-2.5 lg:py-1 ${
              toolsActive || toolsOpen
                ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                : "text-slate-200 hover:text-white"
            }`}
          >
            Tools <span aria-hidden="true">&#9662;</span>
          </button>
          {toolsOpen ? (
            <div
              role="menu"
              aria-label="Tools"
              style={toolsMenuPosition ?? undefined}
              className="fixed z-[1100] w-64 rounded-md border border-white/10 bg-slate-950/95 p-4 text-sm shadow-2xl shadow-black/40 ring-1 ring-black/20"
            >
              <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.22em] text-slate-400">Tools</div>
              {toolsNavGroups.map((group) => (
                <div key={group.label}>
                  {group.items.map((item) => {
                    const active = isActiveNavLink(pathname, item.href);
                    return (
                      <Link
                        key={item.href}
                        href={item.href}
                        prefetch={false}
                        role="menuitem"
                        data-tools-link
                        aria-current={active ? "page" : undefined}
                        onClick={() => setToolsOpen(false)}
                        className={`grid grid-cols-[1.5rem_minmax(0,1fr)] gap-2 rounded-md px-1 py-2.5 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 ${
                          active ? "bg-emerald-400/15 text-emerald-100" : "text-slate-200 hover:bg-white/5 hover:text-white"
                        }`}
                      >
                        <span className="pt-0.5 text-lg leading-none text-emerald-300">{item.icon}</span>
                        <span>
                          <span className="block font-semibold leading-5 text-white">{item.label}</span>
                          <span className="mt-0.5 block whitespace-normal text-xs leading-4 text-slate-400">{item.description}</span>
                        </span>
                      </Link>
                    );
                  })}
                </div>
              ))}
            </div>
          ) : null}
        </div>
        {(() => {
          const active = isActiveNavLink(pathname, "/pricing");
          return (
            <Link
              href="/pricing"
              prefetch={false}
              aria-current={active ? "page" : undefined}
              className={`flex min-h-11 items-center rounded-full px-3 py-2 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 lg:min-h-0 lg:px-2.5 lg:py-1 ${
                active
                  ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                  : "text-slate-200 hover:text-white"
              }`}
            >
              Pricing
            </Link>
          );
        })()}
        <div
          ref={companyRef}
          className="relative"
          onKeyDown={handleCompanyKeyDown}
          onMouseEnter={() => {
            updateCompanyMenuPosition();
            setCompanyOpen(true);
            setProfilesOpen(false);
            setToolsOpen(false);
          }}
          onMouseLeave={() => setCompanyOpen(false)}
        >
          <button
            ref={companyButtonRef}
            type="button"
            aria-haspopup="menu"
            aria-expanded={companyOpen}
            onClick={() => {
              updateCompanyMenuPosition();
              setCompanyOpen(true);
              setProfilesOpen(false);
              setToolsOpen(false);
            }}
            className={`flex min-h-11 items-center rounded-full px-3 py-2 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 lg:min-h-0 lg:px-2.5 lg:py-1 ${
              companyActive || companyOpen
                ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                : "text-slate-200 hover:text-white"
            }`}
          >
            Company <span aria-hidden="true">&#9662;</span>
          </button>
          {companyOpen ? (
            <div
              role="menu"
              aria-label="Company"
              style={companyMenuPosition ?? undefined}
              className="fixed z-[1100] w-64 rounded-md border border-white/10 bg-slate-950/95 p-4 text-sm shadow-2xl shadow-black/40 ring-1 ring-black/20"
            >
              <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.22em] text-slate-400">Company</div>
              {companyNavItems.map((item) => {
                const itemActive = isActiveNavLink(pathname, item.href);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    prefetch={false}
                    role="menuitem"
                    data-company-link
                    aria-current={itemActive ? "page" : undefined}
                    onClick={() => setCompanyOpen(false)}
                    className={`grid grid-cols-[1.5rem_minmax(0,1fr)] gap-2 rounded-md px-1 py-2.5 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 ${
                      itemActive ? "bg-emerald-400/15 text-emerald-100" : "text-slate-200 hover:bg-white/5 hover:text-white"
                    }`}
                  >
                    <span className="pt-0.5 text-lg leading-none text-emerald-300">{item.icon}</span>
                    <span>
                      <span className="block font-semibold leading-5 text-white">{item.label}</span>
                      <span className="mt-0.5 block whitespace-normal text-xs leading-4 text-slate-400">{item.description}</span>
                    </span>
                  </Link>
                );
              })}
            </div>
          ) : null}
        </div>
      </nav>
      <HorizontalScrollIndicators canScrollLeft={canScrollLeft} canScrollRight={canScrollRight} />
    </div>
  );
}
