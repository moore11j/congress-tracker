"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { ApiError, getMe, getMonitoringUnreadCount, logout, type AccountUser } from "@/lib/api";

function displayName(user: AccountUser): string {
  const name = user.name?.trim();
  if (name) return name;
  return user.email.split("@")[0] || "there";
}

export function AccountNav() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [authUnavailable, setAuthUnavailable] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled) {
          setAuthUnavailable(false);
          setUser(response.user);
        }
      })
      .catch((error) => {
        if (!cancelled && error instanceof ApiError && error.status === 401) {
          setAuthUnavailable(false);
          setUser(null);
        } else if (!cancelled) {
          setAuthUnavailable(true);
        }
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!user) {
      setUnreadCount(0);
      setMenuOpen(false);
      return;
    }

    let cancelled = false;
    const loadUnread = () => {
      getMonitoringUnreadCount()
        .then((response) => {
          if (!cancelled) setUnreadCount(Math.max(Number(response.unread_count) || 0, 0));
        })
        .catch(() => {
          if (!cancelled) setUnreadCount(0);
        });
    };

    loadUnread();
    const interval = window.setInterval(loadUnread, 60_000);
    const onUpdated = (event: Event) => {
      const detail = event instanceof CustomEvent ? event.detail : null;
      const nextUnread = Number(detail?.unreadCount);
      if (Number.isFinite(nextUnread) && nextUnread >= 0) {
        setUnreadCount(nextUnread);
        return;
      }
      loadUnread();
    };
    window.addEventListener("ct:monitoring-unread-updated", onUpdated);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
      window.removeEventListener("ct:monitoring-unread-updated", onUpdated);
    };
  }, [user]);

  useEffect(() => {
    if (!menuOpen) return;

    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (target instanceof Node && menuRef.current?.contains(target)) return;
      setMenuOpen(false);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenuOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [menuOpen]);

  const label = useMemo(() => (user ? `Hello, ${displayName(user)}!` : "Login / Register"), [user]);
  const unreadLabel = unreadCount > 9 ? "9+" : String(unreadCount);

  if (!loaded || (!user && !authUnavailable)) {
    return (
      <Link
        href="/login"
        prefetch={false}
        className="whitespace-nowrap rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-sm font-medium text-emerald-100 transition hover:bg-emerald-300/15"
      >
        {label}
      </Link>
    );
  }

  if (!user && authUnavailable) {
    return (
      <Link
        href="/login"
        prefetch={false}
        className="whitespace-nowrap rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-sm font-medium text-emerald-100 transition hover:bg-emerald-300/15"
      >
        {label}
      </Link>
    );
  }

  const authenticatedUser = user;
  if (!authenticatedUser) return null;

  return (
    <div
      ref={menuRef}
      className="relative z-[1100] shrink-0 whitespace-nowrap rounded-lg border border-white/10 bg-white/[0.04] px-2 py-1 text-sm font-medium"
      onMouseEnter={() => setMenuOpen(true)}
      onMouseLeave={() => setMenuOpen(false)}
      onBlur={(event) => {
        const nextTarget = event.relatedTarget;
        if (!(nextTarget instanceof Node) || !event.currentTarget.contains(nextTarget)) setMenuOpen(false);
      }}
    >
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={menuOpen}
        onClick={() => setMenuOpen((open) => !open)}
        onFocus={(event) => {
          if (event.currentTarget.matches(":focus-visible")) setMenuOpen(true);
        }}
        className="relative block px-2 py-1 pr-5 text-slate-100 transition hover:text-white"
      >
        {label}
        {unreadCount > 0 ? (
          <span className="pointer-events-none absolute -right-1 -top-1 inline-flex min-h-5 min-w-5 items-center justify-center rounded-full bg-red-500 px-1 text-[10px] font-bold leading-none text-white shadow-lg shadow-red-950/40">
            {unreadLabel}
          </span>
        ) : null}
      </button>
      <div
        className={`absolute right-0 top-full z-[1200] min-w-44 pt-2 transition ${
          menuOpen ? "visible opacity-100" : "invisible opacity-0"
        }`}
      >
        <div className="rounded-lg border border-white/10 bg-slate-950/95 p-1 shadow-xl shadow-slate-950/40 backdrop-blur">
          <Link
            href="/monitoring"
            prefetch={false}
            onClick={() => setMenuOpen(false)}
            className="flex items-center justify-between gap-4 rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            <span>Inbox</span>
            {unreadCount > 0 ? (
              <span className="inline-flex min-w-5 items-center justify-center rounded-full bg-red-500/15 px-1.5 py-0.5 text-xs font-bold text-red-200">
                {unreadLabel}
              </span>
            ) : null}
          </Link>
          <Link
            href="/watchlists"
            prefetch={false}
            onClick={() => setMenuOpen(false)}
            className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Watchlists
          </Link>
          <Link
            href="/account/settings"
            prefetch={false}
            onClick={() => setMenuOpen(false)}
            className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Account settings
          </Link>
          <Link
            href="/account/billing"
            prefetch={false}
            onClick={() => setMenuOpen(false)}
            className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Billing
          </Link>
          {authenticatedUser.is_admin || authenticatedUser.role === "admin" ? (
            <Link
              href="/admin/settings"
              prefetch={false}
              onClick={() => setMenuOpen(false)}
              className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
            >
              Admin
            </Link>
          ) : null}
          <button
            type="button"
            onClick={() => {
              setMenuOpen(false);
              logout().finally(() => {
                setUser(null);
                window.location.replace("/login");
              });
            }}
            className="block w-full rounded-md px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Sign out
          </button>
        </div>
      </div>
    </div>
  );
}
