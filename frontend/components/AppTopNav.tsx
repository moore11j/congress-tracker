"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useCallback, useEffect, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { HorizontalScrollIndicators, useHorizontalScrollAffordance } from "@/components/ui/HorizontalScrollAffordance";

const topNavLinks = [
  { href: "/?mode=all", label: "Feed" },
  { href: "/insights", label: "Insights" },
  { href: "/signals", label: "Signals" },
  ...(process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0" ? [] : [{ href: "/outcomes", label: "Outcomes" }]),
  { href: "/leaderboards/congress-traders", label: "Leaderboards" },
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

function isActiveNavLink(pathname: string | null, href: string) {
  const path = pathname || "/";
  if (href === "/?mode=all") return path === "/";
  const basePath = href.split("?")[0] || href;
  if (basePath === "/leaderboards/congress-traders") return path === basePath || path.startsWith("/leaderboards/");
  if (basePath === "/compare") return path === "/compare" || path.startsWith("/compare/");
  return path === basePath || path.startsWith(`${basePath}/`);
}

function isActiveToolsLink(pathname: string | null) {
  return toolsNavGroups.some((group) => group.items.some((item) => isActiveNavLink(pathname, item.href)));
}

export function AppTopNav() {
  const pathname = usePathname();
  const [toolsOpen, setToolsOpen] = useState(false);
  const [toolsMenuPosition, setToolsMenuPosition] = useState<{ left: number; top: number } | null>(null);
  const toolsRef = useRef<HTMLDivElement | null>(null);
  const toolsButtonRef = useRef<HTMLButtonElement | null>(null);
  const { scrollRef, canScrollLeft, canScrollRight, updateScrollState } =
    useHorizontalScrollAffordance<HTMLElement>();
  const toolsActive = isActiveToolsLink(pathname);

  const updateToolsMenuPosition = useCallback(() => {
    const rect = toolsButtonRef.current?.getBoundingClientRect();
    if (!rect) return;
    const menuWidth = 256;
    const viewportPadding = 16;
    const left = Math.min(Math.max(rect.left, viewportPadding), window.innerWidth - menuWidth - viewportPadding);
    setToolsMenuPosition({ left, top: rect.bottom + 8 });
  }, []);

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

  return (
    <div className="relative order-3 min-w-0 basis-full lg:order-none lg:min-w-[34rem] lg:flex-1 lg:basis-0">
      <nav
        ref={scrollRef}
        onScroll={updateScrollState}
        className="flex min-w-0 items-center gap-3 overflow-x-auto whitespace-nowrap text-sm font-medium text-slate-200 [scrollbar-width:none] lg:gap-4 xl:gap-5 [&::-webkit-scrollbar]:hidden"
      >
        {topNavLinks.map((link) => {
          const active = isActiveNavLink(pathname, link.href);
          return (
            <Link
              key={link.href}
              href={link.href}
              prefetch={false}
              aria-current={active ? "page" : undefined}
              className={`rounded-full px-2.5 py-1 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${
                active
                  ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                  : "text-slate-200 hover:text-white"
              }`}
            >
              {link.label}
            </Link>
          );
        })}
        <div ref={toolsRef} className="relative" onKeyDown={handleToolsKeyDown}>
          <button
            ref={toolsButtonRef}
            type="button"
            aria-haspopup="menu"
            aria-expanded={toolsOpen}
            onClick={() => {
              updateToolsMenuPosition();
              setToolsOpen((open) => !open);
            }}
            className={`rounded-full px-2.5 py-1 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${
              toolsActive || toolsOpen
                ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                : "text-slate-200 hover:text-white"
            }`}
          >
            Tools <span aria-hidden="true">▾</span>
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
              className={`rounded-full px-2.5 py-1 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${
                active
                  ? "bg-emerald-400/15 text-emerald-100 ring-1 ring-emerald-300/30"
                  : "text-slate-200 hover:text-white"
              }`}
            >
              Pricing
            </Link>
          );
        })()}
      </nav>
      <HorizontalScrollIndicators canScrollLeft={canScrollLeft} canScrollRight={canScrollRight} />
    </div>
  );
}
